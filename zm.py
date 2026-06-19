from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests
from requests.adapters import HTTPAdapter

try:                                  # urllib3 v1/v2 compatibility
    from urllib3.util.retry import Retry
except Exception:                     # pragma: no cover
    Retry = None

from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("jobscraper")

# ── Output columns (exact order expected by the WordPress/AppScript pipeline) ──
APPSCRIPT_COLUMNS = [
    "Job Title", "Job Type", "Job Qualifications", "Job Experience",
    "Job Location", "Job Field", "Date Posted", "Deadline",
    "Job Description", "Application", "Company URL", "Company Name",
    "Company Logo", "Company Industry", "Company Founded", "Company Type",
    "Company Website", "Company Address", "Company Details", "Job URL",
    "Estimated Deadline", "Salary Range",
]

# ── Normalised job-type vocabulary ────────────────────────────────────────────
JOB_TYPE_MAPPING = {
    "full-time": "full-time", "full time": "full-time", "fulltime": "full-time",
    "permanent": "full-time",
    "part-time": "part-time", "part time": "part-time", "parttime": "part-time",
    "contract": "contract", "contractor": "contract", "contracting": "contract",
    "fixed-term": "contract", "fixed term": "contract",
    "temporary": "temporary", "temp": "temporary", "seasonal": "temporary",
    "freelance": "freelance",
    "internship": "internship", "intern": "internship", "graduate": "internship",
    "volunteer": "volunteer",
}

# ── Politeness / safety knobs ─────────────────────────────────────────────────
REQUEST_DELAY_SECONDS = 2.0       # min delay between requests to the same host
REQUEST_TIMEOUT       = 20        # per-request timeout
MAX_RETRIES           = 3
USER_AGENT = (
    "DataAxisNodeJobBot/1.0 (+https://dataaxisnode.com; aggregator; "
    "contact admin@dataaxisnode.com)"
)
RESPECT_ROBOTS = True             # honour a real robots.txt; ignore bot-block 403s

# ── Deadlines ─────────────────────────────────────────────────────────────────
DEFAULT_DEADLINE_DAYS = 30        # Estimated Deadline = Date Posted + this many days

# ── Mistral (paraphrasing) ────────────────────────────────────────────────────
MISTRAL_MODEL = "mistral-small-latest"
MISTRAL_URL   = "https://api.mistral.ai/v1/chat/completions"

# ── Country-specific settings ─────────────────────────────────────────────────
COUNTRY_KEY      = "zambia"
COUNTRY_NAME     = "Zambia"
DEFAULT_LOCATION = "Zambia"
BASE_URL         = "https://jobwebzambia.com"
TRACKER_FILE     = "processed_zambia.csv"   # dedupe state, committed back in CI
OUTPUT_CSV       = "scraped_zambia.csv"      # 22-field rows, committed back in CI


# ════════════════════════════════════════════════════════════════════════════
# Secrets — read ONLY from the environment. Nothing sensitive is ever hardcoded
# in this file. In GitHub Actions these come from repository secrets; locally,
# export them before running. Required: MISTRAL_API_KEY, WP_BASE_URL,
# WP_USERNAME, WP_APP_PASSWORD.  Optional: WP_VERIFY_SSL.
# ════════════════════════════════════════════════════════════════════════════
def get_secret(name: str, default: str | None = None, required: bool = False) -> str:
    val = os.environ.get(name, default)
    if required and not val:
        raise RuntimeError(
            f"Missing required secret: {name}. "
            f"Add it under Settings -> Secrets -> Actions (or export it locally)."
        )
    return val or ""


# ════════════════════════════════════════════════════════════════════════════
# Polite HTTP client (retries, real UA, per-host throttle, robots-aware)
# ════════════════════════════════════════════════════════════════════════════
class HttpClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        if Retry is not None:
            retry = Retry(
                total=MAX_RETRIES, backoff_factor=1.0,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=frozenset(["GET", "HEAD"]),
            )
            adapter = HTTPAdapter(max_retries=retry)
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)
        self._last_hit: dict[str, float] = {}
        self._robots: dict[str, RobotFileParser | None] = {}

    def _allowed(self, url: str) -> bool:
        if not RESPECT_ROBOTS:
            return True
        parsed = urlparse(url)
        host = f"{parsed.scheme}://{parsed.netloc}"
        if host not in self._robots:
            rp = None
            try:
                # Fetch ourselves so we control status handling. Many job boards
                # sit behind Cloudflare and 403 the robots fetch — that must NOT
                # be read as "disallow everything".
                resp = self.session.get(f"{host}/robots.txt", timeout=10)
                if resp.status_code == 200 and resp.text.strip():
                    rp = RobotFileParser()
                    rp.parse(resp.text.splitlines())
            except Exception:
                rp = None
            self._robots[host] = rp  # None => treat as allow
        rp = self._robots[host]
        if rp is None:
            return True
        try:
            return rp.can_fetch(USER_AGENT, url)
        except Exception:
            return True

    def _throttle(self, url: str):
        host = urlparse(url).netloc
        last = self._last_hit.get(host, 0.0)
        wait = REQUEST_DELAY_SECONDS - (time.time() - last)
        if wait > 0:
            time.sleep(wait)
        self._last_hit[host] = time.time()

    def get(self, url: str, timeout: int | None = None):
        if not self._allowed(url):
            logger.warning("robots.txt disallows %s — skipping", url)
            return None
        self._throttle(url)
        try:
            r = self.session.get(url, timeout=timeout or REQUEST_TIMEOUT)
            r.raise_for_status()
            r.encoding = r.apparent_encoding or "utf-8"
            return r
        except Exception as e:
            logger.warning("GET failed %s — %s", url, e)
            return None

    def get_text(self, url: str, timeout: int | None = None) -> str:
        r = self.get(url, timeout=timeout)
        return r.text if r is not None else ""


# ════════════════════════════════════════════════════════════════════════════
# Text cleaning
# ════════════════════════════════════════════════════════════════════════════
_MOJIBAKE = [
    ("\u00e2\u20ac\u2122", "'"), ("\u00e2\u20ac\u0153", '"'), ("\u00e2\u20ac\x9d", '"'),
    ("\u00e2\u20ac\u201c", "\u2013"), ("\u00e2\u20ac\u201d", "\u2014"),
    ("\u00e2\u20ac\u00a2", "\u2022"), ("\u00e2\u201e\u00a2", "\u2122"),
    ("\u00c2", ""), ("\u00e2\u20ac", '"'),
    ("\u00a0", " "), ("\u200b", ""), ("\ufeff", ""), ("&amp;", "&"),
    ("&#160;", " "), ("&nbsp;", " "),
]


def fix_mojibake(text: str) -> str:
    for a, b in _MOJIBAKE:
        text = text.replace(a, b)
    return re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", "", text)


def sanitize_text(text, is_url: bool = False, is_email: bool = False) -> str:
    if text is None:
        return ""
    text = str(text).strip()
    if text.lower() in ("nan", "none", "n/a", "na", ""):
        return ""
    text = fix_mojibake(text)
    if is_url or is_email:
        return re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"#+\s*", "", text)
    text = re.sub(r"\*\*", "", text)
    text = re.sub(r"[^\x20-\x7E\n\u00C0-\u017F\u2013\u2014\u2018-\u201D\u2022]", "", text)
    text = re.sub(r"[ \t]+", " ", text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


# ════════════════════════════════════════════════════════════════════════════
# Date parsing -> ISO (YYYY-MM-DD)
# ════════════════════════════════════════════════════════════════════════════
_MONTHS = {m.lower(): i for i, m in enumerate(
    ["", "January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"]) if m}
_MONTHS.update({m[:3]: i for m, i in list(_MONTHS.items())})


def parse_date(raw: str, fallback_today: bool = False) -> str:
    raw = sanitize_text(raw)
    if not raw:
        return datetime.now().strftime("%Y-%m-%d") if fallback_today else ""

    raw = re.sub(r"(?i)\b(posted|apply by|closing date|deadline|on)\b[:\s]*", "", raw).strip()
    raw = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", raw, flags=re.I)

    # numeric and month-abbrev slash/dash forms, incl. 18/Jun/2026
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y", "%d.%m.%Y",
                "%d/%b/%Y", "%d-%b-%Y", "%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(raw[:11].strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass

    # textual: "25 June 2026" / "June 25, 2026" / "25 June"
    m = re.search(r"(\d{1,2})\s+([A-Za-z]+)\s*(\d{4})?", raw)
    if m and m.group(2).lower()[:3] in _MONTHS:
        d, mon, y = int(m.group(1)), _MONTHS[m.group(2).lower()[:3]], m.group(3)
        y = int(y) if y else datetime.now().year
        try:
            return datetime(y, mon, d).strftime("%Y-%m-%d")
        except ValueError:
            pass
    m = re.search(r"([A-Za-z]+)\s+(\d{1,2}),?\s*(\d{4})?", raw)
    if m and m.group(1).lower()[:3] in _MONTHS:
        mon, d, y = _MONTHS[m.group(1).lower()[:3]], int(m.group(2)), m.group(3)
        y = int(y) if y else datetime.now().year
        try:
            return datetime(y, mon, d).strftime("%Y-%m-%d")
        except ValueError:
            pass

    return datetime.now().strftime("%Y-%m-%d") if fallback_today else ""


def estimated_deadline(date_posted: str, deadline: str) -> str:
    if deadline:
        return deadline
    base = parse_date(date_posted, fallback_today=True)
    try:
        dt = datetime.strptime(base, "%Y-%m-%d") + timedelta(days=DEFAULT_DEADLINE_DAYS)
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return ""


def make_job_id(job_url: str, title: str = "", company: str = "") -> str:
    src = sanitize_text(job_url, is_url=True)
    seed = src if src else f"{title}|{company}"
    return hashlib.md5(seed.encode("utf-8")).hexdigest()[:16]


def normalise_job_type(raw: str) -> str:
    raw = sanitize_text(raw).lower()
    for key, val in JOB_TYPE_MAPPING.items():
        if key in raw:
            return val
    return "full-time"


def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""


# ════════════════════════════════════════════════════════════════════════════
# Application-route extraction
# ════════════════════════════════════════════════════════════════════════════
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
APPLY_CTX = re.compile(
    r"(how to apply|method of application|to apply|apply (?:now|here|online|via|through|by)|"
    r"send (?:your )?(?:cv|resume|application)|submit (?:your )?(?:cv|application)|"
    r"forward (?:your )?(?:cv|application)|email (?:your )?(?:cv|application))",
    re.I,
)
EMAIL_BLOCKLIST = re.compile(
    r"(noreply|no-reply|donotreply|webmaster|privacy|unsubscribe|example\.|sentry|"
    r"wordpress|wixpress|sentry\.io|@2x|\.png|\.jpg|\.svg)",
    re.I,
)


def _visible_text(soup: BeautifulSoup) -> str:
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text("\n", strip=True)


def extract_application(html: str, page_url: str) -> str:
    """Best application route: external apply URL, mailto, or an email near a
    'how to apply' cue. Empty string if nothing usable is found."""
    soup = BeautifulSoup(html, "html.parser")
    site_domain = domain_of(page_url)
    text = _visible_text(soup)

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().startswith("mailto:"):
            email = href.split(":", 1)[1].split("?")[0].strip()
            if email and not EMAIL_BLOCKLIST.search(email):
                return sanitize_text(email, is_email=True)

    for a in soup.find_all("a", href=True):
        label = (a.get_text(" ", strip=True) or "").lower()
        href = urljoin(page_url, a["href"].strip())
        if not href.lower().startswith("http"):
            continue
        if re.search(r"\bapply\b|application", label) or re.search(r"\bapply\b", href.lower()):
            d = domain_of(href)
            if d and d != site_domain and "linkedin" not in d:
                return sanitize_text(href, is_url=True)

    cue = APPLY_CTX.search(text)
    if cue:
        tail = text[cue.start():cue.start() + 800]
        for em in EMAIL_RE.findall(tail):
            if not EMAIL_BLOCKLIST.search(em) and domain_of("http://" + em.split("@")[1]) != site_domain:
                return sanitize_text(em, is_email=True)

    for em in EMAIL_RE.findall(text):
        if not EMAIL_BLOCKLIST.search(em) and em.split("@")[1].lower() not in (site_domain,):
            return sanitize_text(em, is_email=True)

    return ""


def has_application(record: dict) -> bool:
    app = sanitize_text(record.get("Application", ""))
    if not app:
        return False
    is_email = bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", app))
    is_url = app.lower().startswith("http")
    return is_email or is_url


# ════════════════════════════════════════════════════════════════════════════
# Company-website enrichment
# ════════════════════════════════════════════════════════════════════════════
COMPANY_FIELDS = [
    "Company Website", "Company Logo", "Company Details", "Company Industry",
    "Company Founded", "Company Address", "Company Type",
]
_FOUNDED_RE = re.compile(
    r"(?:founded|established|incorporated|since|operating since)\s*(?:in\s*)?(\d{4})", re.I)


class CompanyEnricher:
    """Fills blank company fields (and a missing application route) by visiting
    the employer's own website. Best-effort and time-bounded."""

    def __init__(self, http: HttpClient):
        self.http = http
        self._cache: dict[str, dict] = {}

    def enrich(self, record: dict) -> dict:
        site = sanitize_text(record.get("Company Website", ""), is_url=True)
        if not site:
            site = self._guess_site(record)
        if not site or not site.startswith("http"):
            return record

        data = self._cache.get(site) or self._scrape_site(site)
        self._cache[site] = data

        if not record.get("Company Website"):
            record["Company Website"] = site

        for field, key in [
            ("Company Details", "details"), ("Company Logo", "logo"),
            ("Company Industry", "industry"), ("Company Founded", "founded"),
            ("Company Address", "address"), ("Company URL", "url"),
        ]:
            if not sanitize_text(record.get(field, "")) and data.get(key):
                record[field] = data[key]

        if not has_application(record) and data.get("email"):
            record["Application"] = data["email"]
        return record

    def _guess_site(self, record: dict) -> str:
        app = sanitize_text(record.get("Application", ""))
        if "@" in app and not app.startswith("http"):
            dom = app.split("@")[1].strip()
            free = ("gmail.", "yahoo.", "hotmail.", "outlook.", "live.", "icloud.")
            if dom and not any(dom.startswith(f) or f in dom for f in free):
                return f"https://{dom}"
        return ""

    def _scrape_site(self, site: str) -> dict:
        out: dict = {"url": site, "website": site}
        html = self.http.get_text(site)
        if not html:
            host = urlparse(site).netloc
            html = self.http.get_text(f"https://{host}") if host else ""
        if not html:
            return out

        soup = BeautifulSoup(html, "html.parser")
        self._from_jsonld(soup, out)
        self._from_meta(soup, site, out)

        if len(out.get("details", "")) < 60:
            about = self._find_internal(soup, site, ("about", "who-we-are", "company"))
            if about:
                ahtml = self.http.get_text(about)
                if ahtml:
                    asoup = BeautifulSoup(ahtml, "html.parser")
                    self._from_jsonld(asoup, out)
                    para = self._first_paragraph(asoup)
                    if para and len(para) > len(out.get("details", "")):
                        out["details"] = para
                    out.setdefault("founded", self._founded(asoup.get_text(" ", strip=True)))

        if not out.get("email"):
            contact = self._find_internal(soup, site, ("contact", "careers", "vacancies"))
            chtml = self.http.get_text(contact) if contact else ""
            blob = (chtml or html)
            m = EMAIL_RE.search(blob)
            if m and not EMAIL_BLOCKLIST.search(m.group(0)):
                out["email"] = sanitize_text(m.group(0), is_email=True)

        out["founded"] = out.get("founded") or self._founded(soup.get_text(" ", strip=True))
        return {k: v for k, v in out.items() if v}

    def _from_jsonld(self, soup, out):
        for tag in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(tag.string or "{}")
            except Exception:
                continue
            for node in (data if isinstance(data, list) else [data]):
                if not isinstance(node, dict):
                    continue
                t = str(node.get("@type", "")).lower()
                if "organization" in t or "localbusiness" in t or "corporation" in t:
                    out.setdefault("details", sanitize_text(node.get("description", "")))
                    logo = node.get("logo")
                    if isinstance(logo, dict):
                        logo = logo.get("url")
                    out.setdefault("logo", sanitize_text(logo or "", is_url=True))
                    out.setdefault("url", sanitize_text(node.get("url", ""), is_url=True))
                    fd = node.get("foundingDate", "")
                    if fd:
                        out.setdefault("founded", str(fd)[:4])
                    addr = node.get("address")
                    if isinstance(addr, dict):
                        parts = [addr.get(k, "") for k in
                                 ("streetAddress", "addressLocality", "addressRegion",
                                  "postalCode", "addressCountry")]
                        out.setdefault("address", sanitize_text(", ".join(p for p in parts if p)))
                    elif isinstance(addr, str):
                        out.setdefault("address", sanitize_text(addr))
                    ind = node.get("industry") or node.get("knowsAbout")
                    if ind:
                        out.setdefault("industry", sanitize_text(
                            ind if isinstance(ind, str) else ", ".join(ind)))

    def _from_meta(self, soup, site, out):
        if not out.get("details"):
            for sel in [("meta", {"name": "description"}),
                        ("meta", {"property": "og:description"})]:
                tag = soup.find(*sel)
                if tag and tag.get("content"):
                    out["details"] = sanitize_text(tag["content"])
                    break
        if not out.get("logo"):
            og = soup.find("meta", {"property": "og:image"})
            if og and og.get("content"):
                out["logo"] = sanitize_text(urljoin(site, og["content"]), is_url=True)
            else:
                img = soup.find("img", src=re.compile(r"logo", re.I))
                if img and img.get("src"):
                    out["logo"] = sanitize_text(urljoin(site, img["src"]), is_url=True)

    def _first_paragraph(self, soup) -> str:
        for p in soup.find_all("p"):
            txt = sanitize_text(p.get_text(" ", strip=True))
            if len(txt) > 80:
                return txt
        return ""

    def _founded(self, text: str) -> str:
        m = _FOUNDED_RE.search(text or "")
        return m.group(1) if m else ""

    def _find_internal(self, soup, site, keywords) -> str:
        host = domain_of(site)
        for a in soup.find_all("a", href=True):
            href = urljoin(site, a["href"])
            label = (a.get_text(" ", strip=True) or "").lower()
            if domain_of(href) != host:
                continue
            if any(k in href.lower() or k in label for k in keywords):
                return href
        return ""


# ════════════════════════════════════════════════════════════════════════════
# Mistral paraphraser (optional NLP extras degrade gracefully)
# ════════════════════════════════════════════════════════════════════════════
_st_model = None
try:
    from sentence_transformers import SentenceTransformer, util as _st_util
    _st_model = SentenceTransformer("all-MiniLM-L6-v2", device="cpu")
    logger.info("sentence-transformers loaded for similarity scoring")
except Exception:
    import difflib
    logger.info("sentence-transformers not available — using difflib similarity")

_grammar = None
try:
    import language_tool_python
    _grammar = language_tool_python.LanguageTool(
        "en-US", remote_server="https://api.languagetool.org")
except Exception:
    _grammar = None


def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if _st_model is not None:
        try:
            emb = _st_model.encode([a, b], convert_to_tensor=True)
            return float(_st_util.pytorch_cos_sim(emb[0], emb[1]))
        except Exception:
            pass
    import difflib
    return difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio()


def _grammar_correct(text: str) -> str:
    if not _grammar:
        return text
    try:
        return language_tool_python.utils.correct(text, _grammar.check(text))
    except Exception:
        return text


def _clean_para(text: str) -> str:
    text = fix_mojibake(text or "")
    for pat in [r"\[/?INST\]", r"</?s>",
                r"(?i)(rewritten?|rephrased?|output|paraphrase[d]?)[:\s]+",
                r"\*\*", r"###", r"---"]:
        text = re.sub(pat, "", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return _grammar_correct(text.strip())


class Paraphraser:
    def __init__(self):
        self.api_key = get_secret("MISTRAL_API_KEY")
        self.enabled = bool(self.api_key)
        if not self.enabled:
            logger.warning("MISTRAL_API_KEY not set — paraphrasing disabled (passthrough)")

    def _generate(self, prompt: str, max_tokens: int = 400, temperature: float = 0.7) -> str:
        if not self.enabled:
            return ""
        try:
            r = requests.post(
                MISTRAL_URL,
                headers={"Authorization": f"Bearer {self.api_key}",
                         "Content-Type": "application/json"},
                json={"model": MISTRAL_MODEL,
                      "messages": [{"role": "user", "content": prompt}],
                      "max_tokens": max_tokens, "temperature": temperature},
                timeout=30,
            )
            r.raise_for_status()
            return r.json()["choices"][0]["message"]["content"].strip()
        except Exception as e:
            logger.error("Mistral error: %s", e)
            return ""

    def title(self, title: str) -> str:
        clean = sanitize_text(title)
        if not clean or not self.enabled:
            return clean
        best, best_sim = None, 0.0
        for attempt in range(3):
            temp = round(0.68 + attempt * 0.06, 2)
            prompt = ("Rewrite this job title professionally using different words. "
                      "Output ONLY the rewritten title. Keep it 4-12 words.\n\n"
                      f"Job title: {clean}")
            res = _clean_para(self._generate(prompt, 50, temp)).split("\n")[0].strip().strip('"\'')
            wc = len(res.split())
            sim = _similarity(clean, res)
            if res and 4 <= wc <= 14 and sim >= 0.55 and res.lower() != clean.lower():
                if sim > best_sim:
                    best, best_sim = res, sim
            time.sleep(0.5)
        return best or clean

    def description(self, text: str) -> str:
        clean = sanitize_text(text)
        if not clean or not self.enabled:
            return clean
        paras = [p.strip() for p in clean.split("\n") if p.strip()]
        out = []
        for para in paras:
            accepted, best, best_sim = None, None, 0.0
            for attempt in range(2):
                temp = round(0.65 + attempt * 0.08, 2)
                prompt = ("Rewrite this job description paragraph professionally. "
                          "Keep ALL facts, requirements and responsibilities. "
                          "Use different sentence structure and vocabulary. "
                          "Output ONLY the rewritten paragraph.\n\n"
                          f"Original:\n{para}")
                res = _clean_para(self._generate(prompt, 500, temp))
                rw = len(res.split())
                sim = _similarity(para, res) if rw >= 5 else 0.0
                if res and rw >= 8 and sim >= 0.48:
                    accepted = res
                    break
                if res and sim > best_sim:
                    best, best_sim = res, sim
                time.sleep(0.5)
            out.append(accepted or (best if best and best_sim >= 0.40 else para))
        return "\n\n".join(out)

    def company(self, text: str) -> str:
        clean = sanitize_text(text)
        if not clean or not self.enabled:
            return clean
        prompt = ("Rewrite this company description professionally. Preserve all "
                  "facts. Use different wording. Output ONLY the rewritten text.\n\n"
                  f"Original:\n{clean}")
        res = _clean_para(self._generate(prompt, 600, 0.68))
        rw = len(res.split())
        sim = _similarity(clean, res) if rw >= 10 else 0.0
        return res if (res and rw >= 10 and sim >= 0.40) else clean


# ════════════════════════════════════════════════════════════════════════════
# WordPress client (WP Job Manager style) — same endpoints/meta keys as your
# existing pipeline.  SSL verify defaults ON (WP_VERIFY_SSL=false to disable).
# ════════════════════════════════════════════════════════════════════════════
class WordPressClient:
    def __init__(self):
        base = get_secret("WP_BASE_URL", required=True).rstrip("/")
        self.base = base
        self.jobs_url = f"{base}/job-listings"
        self.company_url = f"{base}/companies"
        self.media_url = f"{base}/media"
        self.user = get_secret("WP_USERNAME", required=True)
        self.app_pw = get_secret("WP_APP_PASSWORD", required=True)
        self.verify = get_secret("WP_VERIFY_SSL", "true").lower() != "false"
        if not self.verify:
            requests.packages.urllib3.disable_warnings()  # type: ignore

    def _headers(self):
        token = base64.b64encode(f"{self.user}:{self.app_pw}".encode()).decode()
        return {"Authorization": f"Basic {token}", "Content-Type": "application/json"}

    @staticmethod
    def _slug(name: str, limit: int = 80) -> str:
        return re.sub(r"[^a-z0-9-]", "-", name.lower().strip())[:limit].strip("-")

    def upload_logo(self, logo_url: str):
        logo_url = sanitize_text(logo_url, is_url=True)
        if not logo_url.startswith("http"):
            return None
        ext = logo_url.lower().rsplit(".", 1)[-1].split("?")[0]
        if ext not in ("png", "jpg", "jpeg", "webp", "gif", "svg"):
            ext = "jpg"
        try:
            img = requests.get(logo_url, timeout=15)
            img.raise_for_status()
            h = self._headers()
            fname = re.sub(r"[^a-zA-Z0-9._-]", "_", logo_url.split("/")[-1].split("?")[0]) or f"logo.{ext}"
            h["Content-Disposition"] = f"attachment; filename={fname}"
            h["Content-Type"] = img.headers.get("content-type", "image/jpeg")
            r = requests.post(self.media_url, headers=h, data=img.content,
                              auth=(self.user, self.app_pw), timeout=30, verify=self.verify)
            r.raise_for_status()
            return r.json().get("id")
        except Exception as e:
            logger.error("Logo upload failed (%s): %s", logo_url, e)
            return None

    def get_or_create_term(self, taxonomy_url: str, name: str):
        name = sanitize_text(name)
        if not name:
            return None
        slug = self._slug(name)
        try:
            r = requests.get(f"{taxonomy_url}?slug={slug}", headers=self._headers(),
                             timeout=10, verify=self.verify)
            terms = r.json()
            if isinstance(terms, list) and terms:
                return terms[0]["id"]
        except Exception:
            pass
        try:
            r = requests.post(taxonomy_url, json={"name": name, "slug": slug},
                              headers=self._headers(), auth=(self.user, self.app_pw),
                              timeout=10, verify=self.verify)
            return r.json().get("id")
        except Exception as e:
            logger.error("Term create '%s': %s", name, e)
            return None

    def save_company(self, rec: dict, details: str, tagline: str):
        name = sanitize_text(rec.get("Company Name", ""))
        if not name or name.lower() in ("unknown company", "nan"):
            return None, None
        slug = self._slug(name)
        try:
            r = requests.get(f"{self.company_url}?slug={slug}", headers=self._headers(),
                             timeout=10, verify=self.verify)
            posts = r.json()
            if isinstance(posts, list) and posts:
                logger.info("Company exists: %s", name)
                return posts[0]["id"], posts[0].get("link")
        except Exception:
            pass
        att = self.upload_logo(rec.get("Company Logo", ""))
        payload = {
            "title": name, "content": details or "", "status": "publish",
            "featured_media": att or 0,
            "meta": {
                "_company_name": name,
                "_company_logo": str(att) if att else "",
                "_company_industry": sanitize_text(rec.get("Company Industry", "")),
                "_company_website": sanitize_text(rec.get("Company Website", ""), is_url=True),
                "_company_address": sanitize_text(rec.get("Company Address", "")),
                "_company_founded": sanitize_text(rec.get("Company Founded", "")),
                "_company_type": sanitize_text(rec.get("Company Type", "")),
                "_company_tagline": tagline,
            },
        }
        try:
            r = requests.post(self.company_url, json=payload, headers=self._headers(),
                              auth=(self.user, self.app_pw), timeout=20, verify=self.verify)
            r.raise_for_status()
            post = r.json()
            logger.info("Company posted: %s -> ID %s", name, post.get("id"))
            return post.get("id"), post.get("link")
        except Exception as e:
            logger.error("Company post '%s': %s", name, e)
            return None, None

    def save_job(self, rec: dict, title: str, description: str):
        h = self._headers()
        location = sanitize_text(rec.get("Job Location", "")) or DEFAULT_LOCATION
        job_type = normalise_job_type(rec.get("Job Type", "Full-time"))
        application = sanitize_text(rec.get("Application", ""), is_url=True)
        deadline = sanitize_text(rec.get("Deadline", "")) or sanitize_text(rec.get("Estimated Deadline", ""))

        is_email = bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", application))
        is_url = bool(re.match(r"^https?://\S+$", application))
        if not (is_email or is_url):
            application = ""

        slug = self._slug(title)
        try:
            r = requests.get(f"{self.jobs_url}?slug={slug}", headers=h, timeout=10, verify=self.verify)
            posts = r.json()
            if isinstance(posts, list) and posts:
                logger.info("Job already on WP: %s", title)
                return posts[0]["id"], posts[0].get("link")
        except Exception:
            pass

        att = self.upload_logo(rec.get("Company Logo", ""))
        region_id = self.get_or_create_term(f"{self.base}/job_listing_region", location)
        type_id = self.get_or_create_term(f"{self.base}/job_listing_type",
                                          job_type.replace("-", " ").title())
        payload = {
            "title": title, "content": description, "status": "publish",
            "featured_media": att or 0,
            "meta": {
                "_job_title": title,
                "_job_location": location,
                "_job_type": job_type,
                "_job_description": description,
                "_application": application,
                "_job_expires": deadline,
                "_company_name": sanitize_text(rec.get("Company Name", "")),
                "_company_website": sanitize_text(rec.get("Company Website", ""), is_url=True),
                "_company_logo": str(att) if att else "",
                "_company_industry": sanitize_text(rec.get("Company Industry", "")),
                "_company_address": sanitize_text(rec.get("Company Address", "")),
                "_company_founded": sanitize_text(rec.get("Company Founded", "")),
                "_company_type": sanitize_text(rec.get("Company Type", "")),
                "_job_qualifications": sanitize_text(rec.get("Job Qualifications", "")),
                "_job_experiences": sanitize_text(rec.get("Job Experience", "")),
                "_job_field": sanitize_text(rec.get("Job Field", "")),
                "_job_source_url": sanitize_text(rec.get("Job URL", ""), is_url=True),
                "_job_salary": sanitize_text(rec.get("Salary Range", "")),
            },
        }
        if region_id:
            payload["job_listing_region"] = [region_id]
        if type_id:
            payload["job_listing_type"] = [type_id]

        for attempt in range(3):
            try:
                r = requests.post(self.jobs_url, json=payload, headers=h,
                                  auth=(self.user, self.app_pw), timeout=25, verify=self.verify)
                r.raise_for_status()
                post = r.json()
                logger.info("Job posted: '%s' -> WP ID %s", title, post.get("id"))
                return post.get("id"), post.get("link")
            except Exception as e:
                logger.error("Job post attempt %d failed: %s", attempt + 1, e)
                if attempt < 2:
                    time.sleep(2 ** attempt)
        return None, None


# ════════════════════════════════════════════════════════════════════════════
# Dedupe + status tracker (per-country CSV; committed back in CI)
# ════════════════════════════════════════════════════════════════════════════
_TRACKER_COLUMNS = ["Job ID", "Source", "Job URL", "Job Title", "Company Name",
                    "Status", "Timestamp"]


def _tracker_init():
    if not os.path.exists(TRACKER_FILE):
        with open(TRACKER_FILE, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(_TRACKER_COLUMNS)


def _tracker_rows() -> list[dict]:
    _tracker_init()
    with open(TRACKER_FILE, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _tracker_write(rows: list[dict]):
    with open(TRACKER_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=_TRACKER_COLUMNS)
        w.writeheader()
        for row in rows:
            w.writerow({c: row.get(c, "") for c in _TRACKER_COLUMNS})


def tracker_load() -> tuple[set, set]:
    rows = _tracker_rows()
    ids = {(r.get("Job ID") or "") for r in rows}
    urls = {(r.get("Job URL") or "") for r in rows}
    return ids, urls


def _tracker_upsert(job_id: str, updates: dict):
    rows = _tracker_rows()
    for r in rows:
        if str(r.get("Job ID")) == str(job_id):
            r.update(updates)
            r["Timestamp"] = datetime.now().isoformat()
            _tracker_write(rows)
            return
    row = {c: "" for c in _TRACKER_COLUMNS}
    row.update({"Job ID": job_id, "Timestamp": datetime.now().isoformat()})
    row.update(updates)
    rows.append(row)
    _tracker_write(rows)


def tracker_mark_read(job_id, source, job_url, title, company):
    _tracker_upsert(job_id, {"Source": source, "Job URL": job_url, "Job Title": title,
                             "Company Name": company, "Status": "read"})


def tracker_mark_posted(job_id, wp_id, wp_url):
    _tracker_upsert(job_id, {"Status": f"posted|wp_id={wp_id}|{wp_url}"})


def tracker_mark_failed(job_id, reason):
    _tracker_upsert(job_id, {"Status": f"failed|{str(reason)[:120]}"})


def tracker_summary():
    rows = _tracker_rows()
    if not rows:
        return
    counts: dict[str, int] = {}
    for r in rows:
        status = (r.get("Status") or "").split("|")[0] or "unknown"
        counts[status] = counts.get(status, 0) + 1
    icons = {"read": "*", "posted": "OK", "failed": "X"}
    print(f"\n{'='*48}\n TRACKER SUMMARY ({len(rows)} records)\n{'='*48}")
    for status, n in sorted(counts.items()):
        print(f" [{icons.get(status, '.')}] {status:<12} {n}")
    print("=" * 48 + "\n")


# ════════════════════════════════════════════════════════════════════════════
# Field mining + scraping helpers
# ════════════════════════════════════════════════════════════════════════════
def empty_record() -> dict:
    return {col: "" for col in APPSCRIPT_COLUMNS}


_LABELS = {
    "Job Type":           r"(?:job\s*type|employment\s*type|contract\s*type)",
    "Job Qualifications": r"(?:qualification|minimum\s*qualification|education|degree)s?",
    "Job Experience":     r"(?:experience(?:\s*(?:level|length))?|years?\s*of\s*experience)",
    "Job Location":       r"(?:location|job\s*location|city|province|region|town)",
    "Job Field":          r"(?:job\s*field|category|sector|industry|department)",
    "Salary Range":       r"(?:salary|remuneration|pay|compensation|wage)",
    "Deadline":           r"(?:deadline|closing\s*date|apply\s*by|expiry|close\s*date)",
    "Date Posted":        r"(?:date\s*posted|posted(?:\s*on)?|published|posted\s*date)",
    "Company Industry":   r"(?:company\s*industry|industry|business\s*type)",
}
_SALARY_RE = re.compile(
    r"((?:ZMW|ZK|R|ZAR|BWP|P|NAD|N\$|US\$|USD|\$|\u20ac|\u00a3)\s?[\d][\d,\. ]*\d"
    r"(?:\s?(?:-|to|\u2013)\s?(?:ZMW|ZK|R|ZAR|BWP|P|NAD|N\$|US\$|USD|\$|\u20ac|\u00a3)?\s?[\d][\d,\. ]*\d)?"
    r"(?:\s?(?:per|/)\s?(?:month|annum|year|hour|week))?)", re.I)


def mine_fields(text: str) -> dict:
    found = {}
    for field, label in _LABELS.items():
        m = re.search(rf"{label}\s*[:\-\u2013]\s*(.+)", text, re.I)
        if m:
            val = m.group(1).split("\n")[0].strip(" .;|")
            if 0 < len(val) < 160:
                found[field] = sanitize_text(val)
    if "Salary Range" not in found:
        ms = _SALARY_RE.search(text)
        if ms:
            found["Salary Range"] = sanitize_text(ms.group(1))
    return found


_NAV_PREFIXES = ("/category/", "/tag/", "/author/", "/wp-", "/page/", "/feed",
                 "/about", "/contact", "/privacy", "/dmca", "/login", "/register",
                 "/submit", "/jobs-by", "/jobs-city", "/jobs-location", "/companies",
                 "/employers", "/blog", "/pricing", "/profiles")
_JOB_HINT = re.compile(r"(job|vacanc|position|recruit|career|apply|hiring|opening)", re.I)


def _clean_title(raw: str) -> str:
    raw = sanitize_text(raw)
    raw = re.split(r"\s+[|\u2013-]\s+(?:MyJobMag|Jobweb|Jobs4BW|NaJobs|Go Zambia)", raw)[0]
    raw = re.sub(r"\s*[-\u2013]\s*Apply by .*$", "", raw, flags=re.I)
    return raw.strip()


def _company_from_title(title: str) -> str:
    m = re.search(r"\bat\s+(.+)$", title)
    if m:
        return sanitize_text(re.sub(r"\s*[-\u2013|].*$", "", m.group(1)))
    m = re.search(r"(?:positions?|vacanc(?:y|ies)|jobs?)\s+at\s+(.+)$", title, re.I)
    return sanitize_text(m.group(1)) if m else ""


def _main_content(soup: BeautifulSoup) -> str:
    candidates = []
    for sel in ["div.entry-content", "div.job_description", "div.job-description",
                "article", "div.single-job", "main", "div#content", "div.post-content",
                "div.td-post-content"]:
        for node in soup.select(sel):
            txt = node.get_text("\n", strip=True)
            if len(txt) > 120:
                candidates.append(txt)
    if candidates:
        return max(candidates, key=len)
    paras = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
    return "\n".join(p for p in paras if len(p) > 30)


def _og(soup, prop, attr="property"):
    tag = soup.find("meta", {attr: prop})
    return tag["content"].strip() if tag and tag.get("content") else ""


def _published(soup) -> str:
    for sel in [("meta", {"property": "article:published_time"}),
                ("meta", {"itemprop": "datePublished"})]:
        tag = soup.find(*sel)
        if tag and tag.get("content"):
            return tag["content"][:10]
    t = soup.find("time")
    if t:
        return t.get("datetime", t.get_text(strip=True))
    return ""


def _detail_from_listing(html: str, base: str, same_host_only=True) -> list:
    soup = BeautifulSoup(html, "html.parser")
    host = domain_of(base)
    out, seen = [], set()
    for a in soup.find_all("a", href=True):
        href = urljoin(base, a["href"].split("#")[0].split("?")[0]).rstrip("/")
        if href in seen:
            continue
        path = urlparse(href).path.lower()
        if same_host_only and domain_of(href) != host:
            continue
        if any(path.startswith(p) for p in _NAV_PREFIXES) or path in ("", "/"):
            continue
        label = a.get_text(" ", strip=True)
        slug = path.strip("/").split("/")[-1]
        looks_job = ("-" in slug and len(slug) > 18) or _JOB_HINT.search(slug) or \
                    (label and len(label.split()) >= 3 and _JOB_HINT.search(label or ""))
        if looks_job:
            seen.add(href)
            out.append(href)
    return out


def _next_page(html: str, base: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    rel = soup.find("link", {"rel": "next"})
    if rel and rel.get("href"):
        return urljoin(base, rel["href"])
    for a in soup.find_all("a", href=True):
        if re.fullmatch(r"(next|next\s*page|\u00bb|>)", a.get_text(strip=True), re.I) or \
           "next" in (a.get("class") or []) or "next" in a.get("rel", []):
            return urljoin(base, a["href"])
    return ""


def _paginate(http, index_urls, base, max_links, page_cap=4) -> list:
    links, seen = [], set()
    for start in index_urls:
        url, pages = start, 0
        while url and pages < page_cap and len(links) < max_links:
            html = http.get_text(url)
            if not html:
                break
            for link in _detail_from_listing(html, base):
                if link not in seen:
                    seen.add(link)
                    links.append(link)
            url = _next_page(html, base)
            pages += 1
    return links[:max_links]


# ════════════════════════════════════════════════════════════════════════════
# Base scraper (run loop). Country adapter below supplies iter_job_links +
# parse_detail.
# ════════════════════════════════════════════════════════════════════════════
class BaseScraper:
    source_key = COUNTRY_KEY
    base_url = BASE_URL

    def __init__(self, http: HttpClient):
        self.http = http
        self.country = COUNTRY_NAME
        self.default_location = DEFAULT_LOCATION

    def iter_job_links(self, max_jobs: int):
        raise NotImplementedError

    def parse_detail(self, html: str, url: str) -> dict:
        raise NotImplementedError

    def run(self, max_jobs: int, processed_ids: set, processed_urls: set):
        yielded = 0
        for url in self.iter_job_links(max_jobs * 2):
            if yielded >= max_jobs:
                break
            url = sanitize_text(url, is_url=True)
            if not url or url in processed_urls:
                continue
            jid = make_job_id(url)
            if jid in processed_ids:
                continue

            html = self.http.get_text(url)
            if not html:
                continue
            try:
                rec = self.parse_detail(html, url)
            except Exception as e:
                logger.warning("parse_detail failed for %s: %s", url, e)
                continue
            if not rec or not sanitize_text(rec.get("Job Title", "")):
                continue

            record = empty_record()
            record.update({k: v for k, v in rec.items() if v})
            record["Job URL"] = url

            soup = BeautifulSoup(html, "html.parser")
            mined = mine_fields(soup.get_text("\n", strip=True))
            for k, v in mined.items():
                if not sanitize_text(record.get(k, "")):
                    record[k] = v

            if not sanitize_text(record.get("Application", "")):
                record["Application"] = extract_application(html, url)

            if not record.get("Company Name"):
                record["Company Name"] = self._guess_company(record, soup)
            if not record.get("Job Location"):
                record["Job Location"] = self.default_location
            record["Job Type"] = normalise_job_type(record.get("Job Type", "")).replace("-", " ").title()
            record["Date Posted"] = parse_date(record.get("Date Posted", ""), fallback_today=True)
            record["Deadline"] = parse_date(record.get("Deadline", ""))
            record["Estimated Deadline"] = estimated_deadline(
                record["Date Posted"], record["Deadline"])

            record["_job_id"] = jid
            processed_ids.add(jid)
            processed_urls.add(url)
            yielded += 1
            yield record

    def _guess_company(self, record: dict, soup: BeautifulSoup) -> str:
        title = record.get("Job Title", "")
        m = re.search(r"\bat\s+([A-Z][\w&.,'\- ]{2,60})$", title)
        if m:
            return sanitize_text(m.group(1))
        og = soup.find("meta", {"property": "og:site_name"})
        return sanitize_text(og["content"]) if og and og.get("content") else ""


# ════════════════════════════════════════════════════════════════════════════
# Zambia — JobwebZambia (WordPress/JobRoller, detail = /jobs/<slug>/)
# ════════════════════════════════════════════════════════════════════════════
class JobwebZambia(BaseScraper):
    source_key = "zambia"
    base_url = "https://jobwebzambia.com"

    def iter_job_links(self, max_jobs):
        index = [f"{self.base_url}/", f"{self.base_url}/feed/?post_type=job_listing"]
        links = []
        for url in index:
            html = self.http.get_text(url)
            if not html:
                continue
            for href in re.findall(r"https://jobwebzambia\.com/jobs/[a-z0-9\-]+/?", html):
                href = href.rstrip("/")
                if href not in links:
                    links.append(href)
        return links[:max_jobs]

    def parse_detail(self, html, url):
        soup = BeautifulSoup(html, "html.parser")
        h1 = soup.find("h1")
        title = _clean_title(h1.get_text() if h1 else _og(soup, "og:title"))
        return {"Job Title": title, "Company Name": _company_from_title(title),
                "Company Logo": _og(soup, "og:image"),
                "Job Description": _main_content(soup),
                "Date Posted": _published(soup)}


SCRAPER_CLASS = JobwebZambia


# ════════════════════════════════════════════════════════════════════════════
# Orchestration
# ════════════════════════════════════════════════════════════════════════════
def _needs_enrichment(rec: dict) -> bool:
    if not has_application(rec):
        return True
    blanks = sum(1 for f in COMPANY_FIELDS if not sanitize_text(rec.get(f, "")))
    return blanks >= 3


def _append_csv(rec: dict):
    write_header = not os.path.exists(OUTPUT_CSV)
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=APPSCRIPT_COLUMNS)
        if write_header:
            w.writeheader()
        w.writerow({c: rec.get(c, "") for c in APPSCRIPT_COLUMNS})


def main():
    ap = argparse.ArgumentParser(
        description=f"{COUNTRY_NAME} job scraper ({SCRAPER_CLASS.base_url}) -> WordPress")
    ap.add_argument("--limit", type=int, default=10, help="max jobs this run")
    ap.add_argument("--dry-run", action="store_true", help="scrape+paraphrase but do not post")
    ap.add_argument("--no-paraphrase", action="store_true", help="post original text")
    args = ap.parse_args()
    do_paraphrase = not args.no_paraphrase

    http = HttpClient()
    enricher = CompanyEnricher(http)
    para = Paraphraser()

    wp = None
    if not args.dry_run:
        wp = WordPressClient()

    processed_ids, processed_urls = tracker_load()
    scraper = SCRAPER_CLASS(http)
    stats = {"scraped": 0, "skipped_no_app": 0, "posted": 0, "failed": 0}
    logger.info("=== %s via %s (limit %d) ===",
                COUNTRY_NAME, SCRAPER_CLASS.__name__, args.limit)

    for rec in scraper.run(args.limit, processed_ids, processed_urls):
        jid = rec.pop("_job_id")
        title = rec.get("Job Title", "")
        company = rec.get("Company Name", "")
        tracker_mark_read(jid, COUNTRY_KEY, rec.get("Job URL", ""), title, company)
        stats["scraped"] += 1

        if _needs_enrichment(rec):
            logger.info("Enriching from company site: %s", company or title)
            rec = enricher.enrich(rec)

        if not has_application(rec):
            logger.info("No application email/URL found -> skipping: %s", title)
            tracker_mark_failed(jid, "no application route")
            stats["skipped_no_app"] += 1
            _append_csv(rec)
            continue

        _append_csv(rec)

        if do_paraphrase:
            out_title = para.title(title)
            out_desc = para.description(rec.get("Job Description", ""))
            out_company = para.company(rec.get("Company Details", "")) if rec.get("Company Details") else ""
        else:
            out_title = title
            out_desc = rec.get("Job Description", "")
            out_company = rec.get("Company Details", "")

        if args.dry_run:
            logger.info("[dry-run] would post: %s", out_title)
            continue

        try:
            wp.save_company(rec, out_company, tagline="")
            wp_id, wp_url = wp.save_job(rec, out_title, out_desc)
            if wp_id:
                tracker_mark_posted(jid, wp_id, wp_url)
                stats["posted"] += 1
            else:
                tracker_mark_failed(jid, "wp post returned no id")
                stats["failed"] += 1
        except Exception as e:
            logger.error("Posting failed for %s: %s", title, e)
            tracker_mark_failed(jid, e)
            stats["failed"] += 1

    tracker_summary()
    logger.info("DONE %s | scraped=%d posted=%d skipped(no-app)=%d failed=%d",
                datetime.now().strftime("%Y-%m-%d %H:%M"),
                stats["scraped"], stats["posted"],
                stats["skipped_no_app"], stats["failed"])


if __name__ == "__main__":
    main()
