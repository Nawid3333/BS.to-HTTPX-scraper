"""
BS.TO Series Scraper -- powered by httpx (no browser needed).

Uses multiple independent httpx sessions (each logged in separately)
to work around bs.to's server-side session locking that breaks
watched status under concurrency.  Each session processes its share
of series sequentially.
"""

import asyncio
import contextlib
import difflib
import json
import logging
import os
import re
import signal
import sys
import threading
import time
from urllib.parse import urljoin, urlparse

import httpx
import lxml.etree
import lxml.html
from bs4 import BeautifulSoup

from config.config import (
    CHECKPOINT_EVERY,
    DATA_DIR,
    HTTP_REQUEST_TIMEOUT,
    NUM_WORKERS,
    PASSWORD,
    SEASON_CONCURRENCY,
    SERIES_INDEX_FILE,
    SITE_URLS,
    USERNAME,
)
from src.atomic_io import atomic_write_json

logger = logging.getLogger(__name__)


# ── HTML parsing ────────────────────────────────────────────────────────────
# lxml parses these pages roughly 1.4x faster than the stdlib parser, and the
# golden fixtures in tests/ confirm it produces identical output on every
# captured real page. The fallback keeps the scraper working on a machine
# where lxml was never installed, at the old speed.
try:
    import lxml  # noqa: F401

    _HTML_PARSER = "lxml"
except ImportError:  # pragma: no cover - depends on the install
    _HTML_PARSER = "html.parser"


def make_soup(html: str) -> BeautifulSoup:
    """Parse a page with the fastest parser available.

    Every BeautifulSoup parse in this module goes through here so the choice
    is made in exactly one place -- a parser swap must never be able to apply
    to some pages and not others.

    Season pages are the exception and no longer come through here: they are
    the hot path and go straight to lxml instead. Everything else (series
    pages, the catalogue, account pages, login) still uses this.
    """
    return BeautifulSoup(html, _HTML_PARSER)


def make_doc(html: str):
    """Parse a page into an lxml tree, or None if the body is not markup.

    The season-page parsers use this instead of make_soup: building a
    BeautifulSoup tree was most of their cost, and the whole worker pool
    shares one event loop, so that time overlapped with nothing.

    document_fromstring, not fromstring: fromstring returns a bare fragment
    root, so markup whose outermost element is the <table> itself would never
    match a .//table search and the page would read as a parse failure.
    BeautifulSoup always wraps in html/body, and this has to match it.

    Returning None rather than raising keeps the old behaviour for an empty
    or non-markup body: no episode table (a scrape failure) and no language
    dropdown (no streams), which is exactly what an empty soup produced.
    """
    try:
        return lxml.html.document_fromstring(html)
    except (lxml.etree.ParserError, ValueError):
        return None


def _stripped_text(el) -> str:
    """lxml equivalent of BeautifulSoup's get_text(strip=True).

    Not the same as text_content().strip(): BeautifulSoup strips *each* text
    node and joins them with nothing, so a title split across inline markup
    comes out glued rather than double-spaced. Joining the raw text instead
    would quietly rewrite every stored episode title.
    """
    return "".join(t.strip() for t in el.itertext())


def _hc(name: str) -> str:
    """XPath predicate matching one whitespace-delimited class token.

    `contains(@class, 'seen')` would also match `unseen`, and
    `contains(@class, 'option')` would match `optional`. Both are the kind of
    near-miss that turns into wrong data rather than an error, so every class
    test goes through this.
    """
    return f"contains(concat(' ', normalize-space(@class), ' '), ' {name} ')"


# The CSS these replace used descendant combinators, and BeautifulSoup's
# find_all/find are recursive too -- hence .// throughout, never a child axis.
_XP_ROWS_PRIMARY = f".//*[{_hc('episode-table')}]//tbody//tr[{_hc('episode-row')}]"
_XP_ROWS_ANY_TR = f".//tr[{_hc('episode-row')}]"
_XP_ROWS_ANY_EL = f".//*[{_hc('episode-row')}]"
_XP_TABLE_EPISODES = f".//table[{_hc('episodes')}]"
_XP_ANY_EPISODE_TABLE = f".//*[{_hc('episode-table')}] | .//table[{_hc('episodes')}]"
_XP_NUMBER_CELL = f".//th[{_hc('episode-number-cell')}]"
_XP_TITLE_GER = f".//*[{_hc('episode-title-ger')}]"
_XP_TITLE_ENG = f".//*[{_hc('episode-title-eng')}]"
_XP_LANGUAGE_DROPDOWN = f".//*[{_hc('series-language')}]"
_XP_LANGUAGE_OPTIONS = f".//li[{_hc('option')}]"


# ── Transient-failure handling ──────────────────────────────────────────────
# Status codes worth a second attempt. A 404 is a real answer about a real
# series and must never be retried; these are the site saying "not now".
_RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
_MAX_ATTEMPTS = 3
# How many times one run may recover from a mid-run session expiry.
_MAX_RELOGINS = 3
# A catalogue this much smaller than the local index is treated as suspect
# and reported to the user before anything is called vanished.
_CATALOGUE_WARN_RATIO = 0.95
_CATALOGUE_MIN_INDEX = 20
_BACKOFF_BASE = 0.5


class RateGuard:
    """Shared brake every worker checks before it issues a request.

    Raising concurrency is only safe if the scraper notices when the site
    pushes back. When any worker sees a 429/503, it parks the whole pool for
    a moment rather than letting the other workers keep hammering; the pool
    then eases back to full speed on its own. One instance is shared by all
    workers, so the slowdown is global, not per-connection.
    """

    def __init__(self) -> None:
        self._resume_at = 0.0
        self._penalty = 0.0

    async def wait(self) -> None:
        """Block until the pool is allowed to send again."""
        delay = self._resume_at - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)

    def penalise(self, retry_after: float | None = None) -> float:
        """Record push-back from the site and park the pool.

        Doubles on repeated push-back so a site that is genuinely
        overloaded gets progressively more room, capped so a run can never
        stall indefinitely.
        """
        if retry_after and retry_after > 0:
            pause = min(retry_after, 60.0)
        else:
            self._penalty = min(max(self._penalty * 2, 1.0), 30.0)
            pause = self._penalty
        self._resume_at = max(self._resume_at, time.monotonic() + pause)
        logger.warning("Site pushed back — pausing all workers for %.1fs", pause)
        return pause

    def reward(self) -> None:
        """A clean response: decay the accumulated penalty."""
        if self._penalty:
            self._penalty = max(0.0, self._penalty * 0.5)


def parse_season_html(html: str):
    """Parse one season page and return plain data, never a soup object.

    Goes straight to lxml rather than through BeautifulSoup, and parses the
    page once for both extractors. The tree build was most of the cost here,
    and the whole worker pool shares one event loop, so that time overlapped
    with nothing -- it was simply the ceiling the pool kept hitting. Both
    parsers were verified against the recorded output of the BeautifulSoup
    versions over 557 real pages before this switched over.

    Also deliberately not offloaded to asyncio.to_thread. That was tried and
    measured 1.8-2.7x SLOWER, both on the old parser and again at today's
    worker counts: lxml releases the GIL for the raw parse, but the dispatch
    and contention cost more than the parallelism buys. Keep this on the
    event loop unless a profile says otherwise.

    Not narrowed to the <table> subtree either. _parse_episodes accepts
    `.episode-table` on *any* element and carries generic fallbacks precisely
    so a site redesign does not break it; restricting the parse to a tag name
    throws that resilience away, and the language dropdown lives outside the
    table anyway.
    """
    doc = make_doc(html)
    return _parse_episodes(doc), _extract_season_languages(doc)


class PhaseProfiler:
    """Records where a run's wall time actually goes, per phase.

    Built because guessing was wrong three times over: offloading parsing to
    threads, restricting the parsed subtree, and raising the worker count all
    looked like clear wins on paper and all measured flat or worse. Numbers
    settle those arguments in one run instead of an afternoon.

    Off unless BS_PROFILE=1 is set, and the accounting is deliberately
    crude -- wall time per phase summed across workers, so the total exceeds
    the run's real duration whenever work overlaps. The useful figure is the
    *ratio* between phases, which tells you which one is worth attacking.
    """

    def __init__(self, enabled: bool) -> None:
        self.enabled = enabled
        self._totals: dict[str, float] = {}
        self._counts: dict[str, int] = {}

    @contextlib.contextmanager
    def phase(self, name: str):
        if not self.enabled:
            yield
            return
        start = time.perf_counter()
        try:
            yield
        finally:
            elapsed = time.perf_counter() - start
            self._totals[name] = self._totals.get(name, 0.0) + elapsed
            self._counts[name] = self._counts.get(name, 0) + 1

    def report(self, wall: float | None = None) -> None:
        if not self.enabled or not self._totals:
            return
        total = sum(self._totals.values())
        lines = ["", "Phase profile (summed across workers; overlaps, so > wall time)"]
        if wall:
            lines.append(f"  wall clock: {wall:.1f}s")
        for name, seconds in sorted(self._totals.items(), key=lambda kv: -kv[1]):
            count = self._counts[name]
            share = seconds / total * 100 if total else 0
            lines.append(
                f"  {name:<12} {seconds:8.1f}s  {share:5.1f}%  "
                f"n={count:<6} avg={seconds / count * 1000:7.1f}ms"
            )
        print("\n".join(lines))


class ProgressWriter:
    """Batches progress lines into one write instead of one write per series.

    Every console write is a synchronous syscall made from the event-loop
    thread, so with a dozen workers finishing series quickly the progress
    output itself competes with the scraping for the loop. Buffering turns a
    burst of lines into a single write.

    Nothing is dropped -- thinning the output would cost the operator the
    record of which series were processed, which is the opposite of the
    point. Lines are held for at most `interval` seconds, and `flush()` is
    called on every exit path so an interrupt cannot swallow the tail.
    """

    def __init__(self, interval: float = 0.25) -> None:
        self.interval = interval
        self._buf: list[str] = []
        self._last_flush = time.monotonic()

    def write(self, line: str) -> None:
        self._buf.append(line + "\n")
        if time.monotonic() - self._last_flush >= self.interval:
            self.flush()

    def flush(self) -> None:
        if not self._buf:
            return
        payload = "".join(self._buf)
        self._buf.clear()
        self._last_flush = time.monotonic()
        try:
            sys.stdout.write(payload)
            sys.stdout.flush()
        except Exception:  # noqa: BLE001 - a dead stdout must never kill a scrape
            logger.debug("Could not write progress output")


def _retry_after_seconds(resp) -> float | None:
    """Parse a Retry-After header, if the site sent a usable one."""
    raw = resp.headers.get("Retry-After", "")
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None



# -- Rename matching helpers -----------------------------------------

_STOPWORDS = frozenset(
    {
        "the",
        "a",
        "an",
        "and",
        "or",
        "of",
        "to",
        "in",
        "on",
        "at",
        "from",
        "with",
        "by",
        "no",
        "san",
        "chan",
        "kun",
        "sama",
    }
)


def _normalize_title_key(title: str) -> str:
    """Return a normalized title string for matching purposes."""
    if not title:
        return ""
    lowered = title.lower()
    lowered = re.sub(r"\(\d{4}\)", " ", lowered)
    lowered = re.sub(r"[^a-z0-9\s]", " ", lowered)
    tokens = [t for t in lowered.split() if t and t not in _STOPWORDS]
    return " ".join(sorted(set(tokens)))


def _slug_tokens(url_or_slug: str) -> str:
    """Return sorted significant tokens from a series slug or URL."""
    if not url_or_slug:
        return ""
    slug = url_or_slug.lower()
    slug = re.sub(r"https?://[^/]+", "", slug)
    slug = re.sub(r"/serie/", "", slug)
    slug = re.sub(r"[^a-z0-9\-]", " ", slug)
    tokens = sorted({t for t in slug.split("-") if len(t) > 2})
    return " ".join(tokens)


def _match_keys(title: str, url_or_slug: str = "") -> set[str]:
    """Return normalized match keys for a title + optional slug."""
    keys: set[str] = set()
    full = _normalize_title_key(title)
    if full:
        keys.add(full)
    slug_tokens = _slug_tokens(url_or_slug)
    if slug_tokens:
        keys.add(slug_tokens)
    return keys - {""}


def _score_rename_match(v_title: str, v_url: str, n_title: str, n_url: str) -> float:
    """Score how likely a new series is a rename of a vanished one."""
    v_keys = _match_keys(v_title, v_url)
    n_keys = _match_keys(n_title, n_url)
    if not v_keys or not n_keys:
        return 0.0
    best = 0.0
    for v_key in v_keys:
        for n_key in n_keys:
            if v_key == n_key:
                return 1.0
            v_tokens = set(v_key.split())
            n_tokens = set(n_key.split())
            if v_tokens and n_tokens:
                overlap = len(v_tokens & n_tokens) / max(len(v_tokens), len(n_tokens))
                best = max(best, overlap)
            best = max(best, difflib.SequenceMatcher(None, v_key, n_key).ratio())
    return best


def _find_vanished_renames(vanished_entries, new_entries, threshold: float = 0.75) -> set[str]:
    """Return set of new entry titles that look like renames of vanished ones.

    Advisory only -- the caller reports these, it never drops a series from
    a scrape on the strength of a guess. The threshold sits high because the
    scoring is fuzzy: at 0.35 "One Piece"/"One Punch Man" scored 0.55,
    "Death Note"/"Deadman Wonderland" 0.43 and "Bleach"/"Beelzebub" 0.40,
    so unrelated shows were being announced as renames of each other.
    """
    renames: set[str] = set()
    used_new: set[int] = set()
    new_list = list(new_entries)
    for v_title, v_url in vanished_entries:
        best_idx = -1
        best_score = 0.0
        for idx, n in enumerate(new_list):
            if idx in used_new:
                continue
            n_title = n.get("title", "")
            n_url = n.get("url", n.get("link", ""))
            score = _score_rename_match(v_title, v_url, n_title, n_url)
            if score > best_score:
                best_score = score
                best_idx = idx
        if best_idx >= 0 and best_score >= threshold:
            used_new.add(best_idx)
            renames.add(new_list[best_idx].get("title", ""))
    return renames - {""}


# -- Constants -------------------------------------------------------
# Dynamic URL helpers (use active site_url at runtime)


def _login_url(site_url: str | None = None) -> str:
    return f"{site_url or SITE_URLS[0]}/login"


def _series_list_url(site_url: str | None = None) -> str:
    return f"{site_url or SITE_URLS[0]}/andere-serien"


# Read from config like the sibling scrapers do. This was hardcoded here,
# which silently made config.HTTP_REQUEST_TIMEOUT inert -- editing it had
# no effect on any request this scraper made.
REQUEST_TIMEOUT = HTTP_REQUEST_TIMEOUT
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0"


# A login page has a password field; a stale mirror serving a placeholder does
# not. Matched on the raw markup rather than through a parser: this runs on
# every host at startup and only has to answer "is this the login page", not
# read anything out of it.
_PASSWORD_INPUT_RE = re.compile(r"<input[^>]+type\s*=\s*['\"]?password", re.IGNORECASE)


def _looks_like_login_page(html: str) -> bool:
    """True when a probe response really is this site's login page.

    The probe used to ask only whether the word "login" appeared. That is a
    single English string deciding which mirrors are usable, so a reworded or
    translated page would have taken every host down at once.

    The password field is the signal that carries the meaning and does not
    depend on wording. The text tests stay on as alternatives, which makes
    this strictly more accepting than the old check -- nothing that passes
    today can start failing because of this.
    """
    if _PASSWORD_INPUT_RE.search(html):
        return True
    lowered = html.lower()
    return "login" in lowered or "anmelden" in lowered

_SERIE_PATH_RE = re.compile(r"(/serie/[^/]+)")
_UTILITY_PAGES = {
    "alle serien",
    "andere serien",
    "beliebte serien",
    "neue serien",
    "empfehlung",
    "meistgesehen",
}

# Error page detection
_ERROR_TITLE_RE = re.compile(
    r"^(?:Error\s+)?(?P<code>\d{3})\b"
    r"(?P<phrase>\s+(?:Not Found|Forbidden|Internal Server Error|"
    r"Bad Gateway|Service Unavailable))?",
    re.IGNORECASE,
)

# Matches a plain "Error 404" style where the code follows the word Error.
_ERROR_TITLE_ALT_RE = re.compile(
    r"^Error\s+(?P<code2>\d{3})\b",
    re.IGNORECASE,
)
_SERVER_ERROR_CODES = {"429", "500", "502", "503", "504"}


def _is_logged_in(soup: BeautifulSoup) -> bool:
    """Check if the page indicates a logged-in session."""
    # Look for a logout anchor in the navigation section first.
    nav = soup.select_one("section.navigation")
    if nav and nav.find("a", href="logout") is not None:
        return True
    # Fallback: any anchor with an exact logout href.
    return soup.find("a", href="logout") is not None


def _check_error_page(soup: BeautifulSoup) -> str | None:
    """Detect HTTP error pages (404, 502, etc.) as HTML."""
    # Explicit site error box always wins over normal page content.
    error_box = soup.select_one("div.messageBox.error")
    if error_box:
        text = error_box.get_text(strip=True).lower()
        if "nicht gefunden" in text:
            return "404"
    title_tag = soup.find("title")
    if title_tag:
        title_text = title_tag.get_text(strip=True)
        m = _ERROR_TITLE_RE.search(title_text)
        if m and m.group("phrase"):
            return m.group("code")
        m2 = _ERROR_TITLE_ALT_RE.search(title_text)
        if m2:
            return m2.group("code2")
    return None


# -- HTML helpers ----------------------------------------------------


def _parse_episodes(doc) -> list[dict] | None:
    """Parse episode rows from a season page.

    Takes an lxml tree from make_doc rather than a soup: the tree build was
    most of the cost, and parse_season_html hands the same tree to
    _extract_season_languages, so the page is parsed once. Output is
    unchanged: verified against the recorded output of the BeautifulSoup
    version over 557 pages and 9,606 episodes, including the fallback
    branches that only synthetic markup reaches.

    Uses multiple fallback selectors to handle site redesigns:
      1. .episode-table tbody tr.episode-row (s.to / cine.to style)
      2. tr.episode-row
      3. .episode-row
      4. table.episodes tr (classic bs.to style -- what the site serves today)

    Note that selector 4 takes every <tr> in the table, header rows included,
    so a row that yields no usable episode number is skipped rather than
    failing the page. Every real bs.to season page depends on that.

    Returns:
        list[dict]: Parsed episodes. An empty list means the episode table
        was found but genuinely holds no rows -- a season listed in the nav
        before its episodes are uploaded. That is a real state (observed on
        the sibling s.to index) and must be stored as 0, not treated as an
        error.
        None: the episode table could not be located at all, or rows were
        found but not one of them parsed. Callers must treat None as a
        scrape failure -- storing 0 there corrupts the index and shows up
        later as a false "this series lost all its episodes" mismatch.
    """
    if doc is None:
        # An empty or non-markup body is a failed fetch, not an empty season.
        return None

    # Try modern selectors first
    rows = doc.xpath(_XP_ROWS_PRIMARY) or doc.xpath(_XP_ROWS_ANY_TR) or doc.xpath(_XP_ROWS_ANY_EL)
    if not rows:
        # Final fallback: classic bs.to table.episodes
        tables = doc.xpath(_XP_TABLE_EPISODES)
        if tables:
            rows = tables[0].xpath(".//tr")

    if not rows:
        # Tell "the table is there, it's just empty" (a real, if rare,
        # season state) apart from "this page has no episode table at all"
        # (a redesign, a truncated response, or a soft error page).
        if not doc.xpath(_XP_ANY_EPISODE_TABLE):
            return None
        return []

    episodes = []
    for idx, row in enumerate(rows, start=1):
        # Extract episode number
        num_cell = row.xpath(_XP_NUMBER_CELL)
        ep_num = _stripped_text(num_cell[0]) if num_cell else ""
        if not ep_num:
            ep_num = row.get("data-episode-season-id", "")
        if not ep_num:
            cols = row.xpath(".//td")
            if cols:
                ep_num = _stripped_text(cols[0])
        if not ep_num:
            logger.warning(
                "Could not determine episode number for row %d",
                idx,
            )
            continue

        try:
            ep_num_int = int(str(ep_num))
        except ValueError:
            logger.warning(
                "Non-numeric episode number '%s' in row %d",
                ep_num,
                idx,
            )
            continue

        # Extract titles (German and English where available)
        ger_cell = row.xpath(_XP_TITLE_GER)
        eng_cell = row.xpath(_XP_TITLE_ENG)
        title_ger = _stripped_text(ger_cell[0]) if ger_cell else ""
        title_eng = _stripped_text(eng_cell[0]) if eng_cell else ""

        # Fallback: generic title from <strong> in second column (bs.to style)
        title = ""
        if not title_ger and not title_eng:
            cols = row.xpath(".//td")
            if len(cols) >= 2:
                title_tag = cols[1].xpath(".//strong")
                title = _stripped_text(title_tag[0]) if title_tag else _stripped_text(cols[1])

        # Check if watched — s.to uses 'seen', bs.to uses 'watched'
        row_classes = (row.get("class") or "").split()
        watched = "seen" in row_classes or "watched" in row_classes

        ep = {"number": ep_num_int, "watched": watched}
        if title_ger:
            ep["title_ger"] = title_ger
        if title_eng:
            ep["title_eng"] = title_eng
        if title and not title_ger and not title_eng:
            ep["title"] = title
        episodes.append(ep)

    if not episodes:
        # Rows were present but not one of them yielded an episode.
        return None
    return episodes


def _extract_season_languages(doc) -> list[str]:
    """Return normalized language codes from the .series-language dropdown.

    bs.to shows available audio/subtitle languages at season/page level, not
    per-episode. Known data-value codes:
        de  -> german_dub
        des -> german_sub
        en  -> english_dub
        ens -> english_sub
        jp  -> japanese_sub
        ja  -> japanese_sub
    """
    bs_language_map = {
        "de": "german_dub",
        "des": "german_sub",
        "en": "english_dub",
        "ens": "english_sub",
        "jp": "japanese_sub",
        "ja": "japanese_sub",
        "jap": "japanese_sub",
    }
    languages: list[str] = []
    seen: set[str] = set()

    if doc is None:
        return ["no_streams"]

    found = doc.xpath(_XP_LANGUAGE_DROPDOWN)
    if not found:
        # No language dropdown means the season has no streams on bs.to.
        return ["no_streams"]
    dropdown = found[0]

    for option in dropdown.xpath(_XP_LANGUAGE_OPTIONS):
        value = str(option.get("data-value", "")).strip().lower()
        code = bs_language_map.get(value)
        if code and code not in seen:
            seen.add(code)
            languages.append(code)

    if not languages:
        # Dropdown exists but contains no known language option.
        return ["no_streams"]

    return languages


def _extract_season_links(
    soup: BeautifulSoup,
    base_url: str,
) -> list[tuple[str, str]]:
    """Extract season navigation links from a series page.

    Relative 'serie/...' hrefs are resolved against the site root,
    not the current series page, to avoid double paths like
    /serie/X/serie/X/1.
    """
    parsed_base = urlparse(base_url)
    root_url = f"{parsed_base.scheme}://{parsed_base.netloc}"

    links, seen = [], set()
    for a in soup.select("#seasons a"):
        label = a.get_text(strip=True)
        href = str(a.get("href", ""))
        if not label or not href:
            continue
        href = href.split("?")[0].split("#")[0]
        if href.startswith("http"):
            url = href
        elif href.startswith("serie/"):
            url = f"{root_url}/{href}"
        elif href.startswith("/"):
            url = f"{root_url}{href}"
        else:
            url = urljoin(base_url.rstrip("/") + "/", href)
        key = (label, url)
        if key not in seen:
            seen.add(key)
            links.append((label, url))
    return links


def _heading_text(el) -> str:
    """Return the flattened, normalized text of a heading element.

    Uses a separator-joined get_text() so inline sub-elements (e.g. "Harry
    Potter<small>Specials</small>") don't glue themselves to the main text.
    Does not mutate the parse tree -- safe to call on a soup shared with
    other extractors.
    """
    if not el:
        return ""
    text = " ".join(el.get_text(" ", strip=True).split())
    text = re.sub(r"\s*(?:Staffel|Season|St\.?)\s*\d+.*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*Specials\s*$", "", text, flags=re.IGNORECASE)
    return text.strip()


def _extract_title(soup: BeautifulSoup) -> str | None:
    """Extract series title from the page.

    Tries site-specific heading(s) first, then falls back to generic headings.
    Strips trailing 'Staffel N' / 'Specials' suffixes and ensures inline tags
    like <small> do not glue themselves to the main title.
    """
    # bs.to preferred heading: "Title <small>Staffel N</small>"
    for selector in ("h2",):
        el = soup.select_one(selector)
        text = _heading_text(el)
        if text:
            return text

    # Fallback headings
    for tag in ("h1",):
        el = soup.find(tag)
        text = _heading_text(el)
        if text:
            return text
    return None


def _count_seasons_from_html(soup: BeautifulSoup) -> int:
    """Return number of distinct seasons found on a bs.to series page."""
    season_links = soup.select("#seasons li a")
    if not season_links:
        # Fallback: look for staffel-N links anywhere in the page.
        season_links = soup.select("a[href*='staffel-']")
    labels = set()
    for a in season_links:
        href = str(a.get("href", ""))
        # Match both absolute (/staffel-N) and relative (staffel-N) paths.
        m = re.search(r"(?:^|/)staffel-(\d+)", href, re.IGNORECASE)
        if m:
            labels.add(int(m.group(1)))
            continue
        # Only use link text when it explicitly looks like a season label.
        text = a.get_text(strip=True)
        m = re.search(r"(?:staffel|season|st\.?)\s*(\d+)", text, re.IGNORECASE)
        if m:
            labels.add(int(m.group(1)))
    return len(labels)


# -- Exception -------------------------------------------------------


class ScrapingPausedError(Exception):
    """Raised when a pause file is detected during scraping."""


# -- BsToScraper (httpx) ---------------------------------------------


class BsToScraper:  # pylint: disable=too-many-instance-attributes
    """BS.TO series scraper powered by httpx."""

    def __init__(self):
        self.series_data: list[dict] = []
        self.all_discovered_series: list[dict] | None = None
        self.completed_links: set[str] = set()
        self.failed_links: list[dict] = []
        self.attempted_urls: set[str] = set()

        self.checkpoint_file = os.path.join(
            DATA_DIR,
            ".scrape_checkpoint.json",
        )
        self.failed_file = os.path.join(
            DATA_DIR,
            ".failed_series.json",
        )
        self.ignore_file = os.path.join(
            DATA_DIR,
            ".ignored_series.json",
        )
        self.pause_file = os.path.join(
            DATA_DIR,
            ".pause_scraping",
        )

        self._checkpoint_mode: str | None = None
        self._use_parallel: bool = True
        self._lock = threading.Lock()
        self._relogin_count = 0
        self._last_pause_check = 0.0
        self._pause_cached = False
        self.paused = False
        self.site_url = SITE_URLS[0]
        self._interrupt_requested = False
        self._rate_guard = RateGuard()
        self._progress = ProgressWriter()
        self._shared_client = None
        self._client_users = 0
        self._client_lock = asyncio.Lock()
        # Connection pool is sized from this, not from NUM_WORKERS directly,
        # so a run that overrides the worker count (a benchmark, a scoped
        # run) gets a pool that matches it. A pool smaller than the actual
        # fan-out silently serialises the requests it cannot carry.
        self.pool_workers = NUM_WORKERS
        # Filled on first lookup; see _index_items.
        self._index_items_cache: list | None = None
        self._profiler = PhaseProfiler(os.getenv("BS_PROFILE", "") == "1")

    # -- Static / class methods --------------------------------------

    @staticmethod
    def get_checkpoint_mode(data_dir):
        """Read the checkpoint mode from disk, or None."""
        cp_file = os.path.join(
            data_dir,
            ".scrape_checkpoint.json",
        )
        try:
            if os.path.exists(cp_file):
                with open(cp_file, encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    return data.get("mode")
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        return None

    # -- Checkpoint management ---------------------------------------

    def _sync_save_checkpoint(self, include_data=False):
        """Synchronous checkpoint writer; thread-safe."""
        with self._lock:
            payload = {
                "completed_links": list(self.completed_links),
                "mode": self._checkpoint_mode,
                "timestamp": time.time(),
            }
            if include_data:
                payload["series_data"] = self.series_data
            try:
                # No backup here (unlike failed/ignore lists): this is the
                # highest-frequency writer -- it can include the full
                # series_data -- and it's precisely the recovery file for
                # an unclean shutdown, so durability (fsync) matters far
                # more than keeping a history of it.
                atomic_write_json(
                    self.checkpoint_file,
                    payload,
                    indent=None,
                    backup=False,
                )
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.error(
                    "Failed to save checkpoint: %s",
                    exc,
                )

    def save_checkpoint(self, include_data=False):
        """Synchronous entry point for final/pause/error paths."""
        self._sync_save_checkpoint(include_data=include_data)

    async def asave_checkpoint(self, include_data=False):
        """Offload checkpoint I/O to a thread so the event loop stays free."""
        with self._profiler.phase("checkpoint"):
            await asyncio.to_thread(self._sync_save_checkpoint, include_data)

    def load_checkpoint(self) -> bool:
        """Load checkpoint from disk. Returns True if loaded."""
        with self._lock:
            try:
                if not os.path.exists(self.checkpoint_file):
                    return False
                with open(
                    self.checkpoint_file,
                    encoding="utf-8",
                ) as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    self.completed_links = set(data.get("completed_links", []))
                    self._checkpoint_mode = data.get("mode")
                    saved_data = data.get("series_data")
                    if saved_data:
                        self.series_data = saved_data
                elif isinstance(data, list):
                    self.completed_links = set(data)
                return bool(self.completed_links)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.error(
                    "Failed to load checkpoint: %s",
                    exc,
                )
                return False

    def clear_checkpoint(self):
        """Remove checkpoint file from disk."""
        with self._lock:
            try:
                if os.path.exists(self.checkpoint_file):
                    os.remove(self.checkpoint_file)
            except OSError:
                pass

    # -- Failed series management ------------------------------------

    def _write_failed_entries(self, entries: list):
        """Write entries to the failed-series file, or remove it if empty.

        Caller must hold self._lock.
        """
        if not entries:
            try:
                if os.path.exists(self.failed_file):
                    os.remove(self.failed_file)
            except OSError:
                pass
            return
        try:
            atomic_write_json(self.failed_file, entries, backup=False)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error(
                "Failed to save failed series: %s",
                exc,
            )

    def save_failed_series(self):
        """Merge this run's failures into the persisted failed-series file.

        Only ever adds entries -- never removes ones for series outside this
        run's scope. Used for partial/interrupted runs (pause, Ctrl+C) where
        we don't know the full set of series this run touched.
        """
        with self._lock:
            ignored_urls = {s.get("url") for s in self.load_ignored_series()}
            existing = []
            try:
                if os.path.exists(self.failed_file):
                    with open(
                        self.failed_file,
                        encoding="utf-8",
                    ) as f:
                        existing = json.load(f)
            except Exception:  # pylint: disable=broad-exception-caught
                pass
            # Drop any entries already on the ignore list
            existing = [e for e in existing if isinstance(e, dict) and e.get("url") not in ignored_urls]
            seen = {e.get("url") for e in existing if isinstance(e, dict)}
            for entry in self.failed_links:
                if isinstance(entry, dict) and entry.get("url") not in seen and entry.get("url") not in ignored_urls:
                    existing.append(entry)
                    seen.add(entry.get("url"))
            self._write_failed_entries(existing)

    def reconcile_failed_series(self):
        """Reconcile the persisted failed-series file with this run's outcome.

        Drops entries for URLs this run actually attempted (they either
        succeeded, in which case they no longer belong on the list, or
        failed again, in which case they get re-added from failed_links
        below) and leaves every other entry untouched. This matters for
        scoped runs (single URL, a specific url_list, retry-only) that
        cover a small subset of series -- e.g. the "vanished/critical
        series" integrity rescrape used to call the old replace-on-empty
        logic, which wiped out an unrelated full scrape's entire failed
        list just because its own handful of URLs all succeeded.
        """
        with self._lock:
            ignored_urls = {s.get("url") for s in self.load_ignored_series()}
            existing = []
            try:
                if os.path.exists(self.failed_file):
                    with open(
                        self.failed_file,
                        encoding="utf-8",
                    ) as f:
                        existing = json.load(f)
            except Exception:  # pylint: disable=broad-exception-caught
                pass
            kept = [
                e
                for e in existing
                if isinstance(e, dict) and e.get("url") not in self.attempted_urls and e.get("url") not in ignored_urls
            ]
            seen = {e.get("url") for e in kept}
            for entry in self.failed_links:
                if isinstance(entry, dict) and entry.get("url") not in seen and entry.get("url") not in ignored_urls:
                    kept.append(entry)
                    seen.add(entry.get("url"))
            self._write_failed_entries(kept)

    def load_failed_series(self) -> list:
        """Load previously failed series from disk."""
        with self._lock:
            try:
                if os.path.exists(self.failed_file):
                    with open(
                        self.failed_file,
                        encoding="utf-8",
                    ) as f:
                        data = json.load(f)
                    if isinstance(data, list):
                        return data
            except Exception:  # pylint: disable=broad-exception-caught
                pass
            return []

    # -- Ignore list management --------------------------------------

    def load_ignored_series(self) -> list[dict]:
        """Load the ignore list from disk."""
        try:
            if os.path.exists(self.ignore_file):
                with open(
                    self.ignore_file,
                    encoding="utf-8",
                ) as f:
                    data = json.load(f)
                if isinstance(data, list):
                    return data
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        return []

    def get_ignored_slugs(self) -> set[str]:
        """Return set of slugs from the ignore list."""
        return {self.get_series_slug_from_url(s.get("url", "")) for s in self.load_ignored_series()} - {"unknown"}

    async def _revalidate_ignored_series(
        self,
        client: httpx.AsyncClient,
    ):
        """Re-check ignored series to see if still empty/404.

        Notification-only — does not auto-remove entries.
        Prints a warning for any series that now appears to
        have content so the user can manually remove it from
        .ignored_series.json.
        """
        ignored = self.load_ignored_series()
        if not ignored:
            return

        print(f"\u2192 Re-checking {len(ignored)} ignored series...")
        now_available = 0
        still_empty = 0

        for entry in ignored:
            url = entry.get("url", "")
            title = entry.get("title", url.split("/")[-1])
            try:
                resp = await client.get(
                    url,
                    follow_redirects=True,
                )
                soup = make_soup(resp.text)
                error_code = _check_error_page(soup)
                if error_code:
                    still_empty += 1
                    print(f"  \u2713 {title}: still empty ({error_code} \u2014 ignored)")
                else:
                    has_seasons = soup.select_one("#seasons a")
                    if has_seasons:
                        now_available += 1
                        print(f"  \u26a0 {title}: now available! Consider removing from .ignored_series.json")
                    else:
                        still_empty += 1
                        print(f"  \u2713 {title}: still empty (no seasons \u2014 ignored)")
            except httpx.HTTPError as exc:
                still_empty += 1
                print(f"  \u2713 {title}: still unreachable ({exc} \u2014 ignored)")

        print(f"\u2192 Re-checked {len(ignored)} ignored: {now_available} now available, {still_empty} still empty")

    def _check_ignored_vs_catalog(self, all_series: list[dict]):
        """Compare ignored series against the fetched catalog.

        Notification-only — prints which ignored series have
        disappeared from the catalog so the user can clean up
        .ignored_series.json manually.
        """
        ignored = self.load_ignored_series()
        if not ignored:
            return

        catalog_slugs = {self.get_series_slug_from_url(s.get("link", "")) for s in all_series} - {"unknown"}

        in_catalog = []
        disappeared = []
        for entry in ignored:
            slug = self.get_series_slug_from_url(
                entry.get("url", ""),
            )
            title = entry.get("title", slug)
            if slug in catalog_slugs:
                in_catalog.append(title)
            else:
                disappeared.append(title)

        if disappeared:
            print(
                f"\n\u26a0 {len(disappeared)} ignored "
                "series no longer in catalog "
                "\u2014 consider removing from "
                ".ignored_series.json:"
            )
            for t in disappeared:
                print(f"  \u2715 {t}")
        if in_catalog:
            print(f"\u2139 {len(in_catalog)} ignored series still listed in catalog (skipped)")
        print()

    def _check_index_vs_catalog(
        self,
        all_series: list[dict],
        *,
        quiet: bool = False,
    ):
        """Detect indexed series that disappeared from the catalog.

        Notification-only by default. Set quiet=True when the caller
        will present a vanished/new comparison table later.
        """
        catalog_slugs = {self.get_series_slug_from_url(s.get("link", "")) for s in all_series} - {"unknown"}

        # Build slug→title map from the index
        index_map: dict[str, str] = {}
        try:
            if os.path.exists(SERIES_INDEX_FILE):
                items = self._index_items()
                for item in items:
                    url = item.get("url", "") or item.get("link", "")
                    slug = self.get_series_slug_from_url(
                        url,
                    )
                    if slug != "unknown":
                        index_map[slug] = item.get(
                            "title",
                            slug,
                        )
        except Exception:  # pylint: disable=broad-exception-caught
            return

        if not index_map:
            return

        vanished = [(slug, title) for slug, title in index_map.items() if slug not in catalog_slugs]

        if vanished and not quiet:
            print(
                f"\n\u26a0 {len(vanished)} indexed series "
                "no longer in catalog \u2014 may have "
                "been removed from the site:"
            )
            for slug, title in vanished[:20]:
                print(f"  \u2715 {title}  ({slug})")
            if len(vanished) > 20:
                print(f"  ... and {len(vanished) - 20} more")
            print("  \u2192 Review series_index.json and decide whether to keep or remove these entries.")
            print()

        return vanished

    # -- URL helpers -------------------------------------------------

    @staticmethod
    def get_series_slug_from_url(url):
        """Extract the series slug from a URL path."""
        try:
            path = urlparse(url).path if url.startswith("http") else url
            parts = path.split("/")
            if "serie" in parts:
                idx = parts.index("serie")
                if idx + 1 < len(parts) and parts[idx + 1]:
                    return parts[idx + 1]
            return "unknown"
        except Exception:  # pylint: disable=broad-exception-caught
            return "unknown"

    def _vanished_index_entries(self, all_series: list[dict]) -> list[tuple[str, str]]:
        """Return (title, url) for indexed series missing from the catalog."""
        catalog_slugs = {self.get_series_slug_from_url(s.get("link", "")) for s in all_series} - {"unknown"}
        entries: list[tuple[str, str]] = []
        try:
            if not os.path.exists(SERIES_INDEX_FILE):
                return entries
            items = self._index_items()
            for item in items:
                if not isinstance(item, dict):
                    continue
                # Match the catalogue side, which reads "link": comparing a
                # url-derived slug against link-derived ones would disagree the
                # moment the two fields differ, and show_vanished_series also
                # prefers "link". Both are kept as fallbacks so an entry
                # carrying only one of them still resolves.
                slug_source = item.get("link", "") or item.get("url", "")
                slug = self.get_series_slug_from_url(slug_source)
                if slug and slug != "unknown" and slug not in catalog_slugs:
                    entries.append((item.get("title", slug), item.get("url", "") or slug_source))
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        return entries

    def normalize_to_series_url(self, url):
        """Normalize a URL to the canonical series URL on the active site.

        If an absolute URL is given, its domain is swapped to the currently
        active host (self.site_url) so that dead primary domains still work
        when a fallback host has been selected.
        """
        if not url:
            return url
        url = url.split("?")[0].split("#")[0]
        m = _SERIE_PATH_RE.search(url)
        if m:
            path = m.group(1)
            return f"{self.site_url}{path}"
        return url

    # -- Pause detection ---------------------------------------------

    def _check_pause(self):
        """Check whether a pause file exists (cached 5s)."""
        now = time.time()
        if now - self._last_pause_check < 5:
            return self._pause_cached
        self._last_pause_check = now
        self._pause_cached = os.path.exists(self.pause_file)
        return self._pause_cached

    def _check_interrupt_flag(self):
        """Return True if an interrupt has been requested (Ctrl+C)."""
        return getattr(self, "_interrupt_requested", False)

    def _clear_pause_file(self):
        """Remove the pause file if present."""
        try:
            if os.path.exists(self.pause_file):
                os.remove(self.pause_file)
        except OSError:
            pass

    def _create_pause_file(self):
        """Create the pause file so workers raise ScrapingPaused."""
        try:
            with open(self.pause_file, "w", encoding="utf-8") as f:
                f.write("PAUSE")
        except OSError:
            pass

    # -- Index helpers (for new_only mode) ---------------------------

    def _confirm_catalogue_size(self, all_series) -> bool:
        """Warn when a fetched catalogue looks too short to be genuine, and ask.

        A truncated or degraded catalogue response still parses cleanly, so a
        short list is indistinguishable from a site that really did lose
        series -- except by size. Every indexed entry missing from it is later
        reported as vanished and offered for deletion, so a bad fetch can put
        thousands of good entries in front of a delete-all prompt.

        This only reports and asks; the user decides whether to go on. It is
        deliberately not an automatic abort: a site genuinely shrinking is a
        real thing, and only the user can tell the two apart.
        """
        try:
            indexed = len(self.load_existing_slugs())
        except Exception:
            return True
        fetched = len(all_series)
        if indexed < _CATALOGUE_MIN_INDEX or fetched >= indexed * _CATALOGUE_WARN_RATIO:
            return True

        missing = indexed - fetched
        pct = (fetched / indexed * 100) if indexed else 0.0
        print("\n" + "!" * 70)
        print("  [WARN] The fetched catalogue looks unusually small.")
        print(f"    indexed series : {indexed:,}")
        print(f"    fetched now    : {fetched:,}  ({pct:.1f}% of the index)")
        print(f"    would be flagged as vanished: up to {missing:,}")
        print("  A truncated or partial response looks exactly like this.")
        print("  Continuing is fine if the site really did shrink.")
        print("!" * 70)
        logger.warning(
            "Catalogue smaller than index: fetched %d vs indexed %d (%.1f%%)",
            fetched,
            indexed,
            pct,
        )
        answer = input("\nContinue with this scrape anyway? (y/n): ").strip().lower()
        if answer != "y":
            print("  -> Scrape cancelled. The index was not touched.")
            logger.info("User cancelled scrape after short-catalogue warning.")
            return False
        return True

    def _index_items(self) -> list:
        """Every entry in the index, parsed at most once per run.

        Five lookups each opened and parsed the index file for themselves, so
        a new-only run parsed the whole index four times before fetching
        anything -- and the same three lines of open/parse/normalise were
        copied into all five.

        Caching is safe because nothing in this module writes the index: the
        save goes through IndexManager after run() returns, and main.py builds
        a fresh scraper for every action, so the cache never outlives the run
        that filled it.

        Never raises. A missing or unreadable index yields an empty list,
        which is what each caller's own guard produced before.

        The list is shared between callers, so treat it as read-only. Every
        lookup here only reads; mutating it would now be visible to the rest.
        """
        if self._index_items_cache is None:
            items: list = []
            try:
                if os.path.exists(SERIES_INDEX_FILE):
                    with open(SERIES_INDEX_FILE, encoding="utf-8") as f:
                        data = json.load(f)
                    items = data if isinstance(data, list) else list(data.values())
            except Exception:  # pylint: disable=broad-exception-caught
                logger.warning("Could not read the index for lookups", exc_info=True)
                items = []
            self._index_items_cache = items
        return self._index_items_cache

    def load_existing_slugs(self) -> set[str]:
        """Load slugs of all series already in the index."""
        existing = set()
        try:
            if os.path.exists(SERIES_INDEX_FILE):
                items = self._index_items()
                for item in items:
                    url = item.get("url", "") or item.get("link", "")
                    if url:
                        existing.add(self.get_series_slug_from_url(url))
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        existing.discard("unknown")
        return existing

    # -- Async internals ---------------------------------------------

    async def _login_client(
        self,
        client: httpx.AsyncClient,
        verify: bool = True,
    ) -> None:
        """Log in an existing httpx client to bs.to.

        verify: fetch a known-good page afterwards and confirm the session
            really is logged in. Callers that immediately fetch the catalogue
            pass False: _get_all_series applies the same _is_logged_in check
            to the same response, so verifying here only downloads the page a
            second time to reach the same verdict.
        """
        site_url = self.site_url
        login_url = _login_url(site_url)
        resp = await client.get(login_url)
        soup = make_soup(resp.text)
        token_input = soup.find(
            "input",
            {"name": "security_token"},
        )
        token = token_input["value"] if token_input else ""
        if not token:
            logger.warning("CSRF security_token not found on login page")

        await client.post(
            login_url,
            data={
                "login[user]": USERNAME,
                "login[pass]": PASSWORD,
                "security_token": token,
            },
            follow_redirects=True,
        )

        # Verify on a non-login page instead of trusting the login response.
        if not verify:
            return
        series_list_url = _series_list_url(site_url)
        verify_resp = await client.get(series_list_url)
        verify_soup = make_soup(verify_resp.text)
        if not _is_logged_in(verify_soup):
            await client.aclose()
            raise RuntimeError("Login failed \u2014 check credentials")

    async def _create_logged_in_client(
        self,
        verify: bool = True,
    ) -> httpx.AsyncClient:
        """Create an httpx client and log in to bs.to."""
        client = httpx.AsyncClient(
            http2=True,
            headers={"User-Agent": UA},
            timeout=httpx.Timeout(
                REQUEST_TIMEOUT,
                connect=10.0,
            ),
            follow_redirects=True,
            limits=httpx.Limits(
                # One worker now has up to SEASON_CONCURRENCY season fetches
                # in flight at once, so a 2-connection pool would serialise
                # the fan-out straight back into the queue it was meant to
                # remove. Keepalive matches it so those connections survive
                # between series instead of re-handshaking each time.
                max_connections=self.pool_workers * SEASON_CONCURRENCY + 4,
                max_keepalive_connections=self.pool_workers * SEASON_CONCURRENCY + 4,
            ),
        )
        await self._login_client(client, verify=verify)
        return client

    async def _get_all_series(
        self,
        client: httpx.AsyncClient,
    ) -> list[dict]:
        """Fetch the full series catalogue from bs.to."""
        series_list_url = _series_list_url(self.site_url)
        resp = await self._get(client, series_list_url)
        soup = make_soup(resp.text)
        if not _is_logged_in(soup):
            raise RuntimeError("Not logged in \u2014 cannot fetch series catalogue")
        series, seen_slugs = [], set()
        for a in soup.find_all("a", href=True):
            href = str(a["href"])
            if not href.startswith("serie/"):
                continue
            raw = a.get_text(strip=True)
            if not raw or raw.lower().strip() in _UTILITY_PAGES:
                continue
            slug = href.split("/")[1] if "/" in href else href
            if not slug or slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            entry = {
                "title": raw,
                "link": f"/{href}",
                "url": f"{self.site_url}/{href}",
            }
            series.append(entry)
        return series

    async def _get(self, client, url, **kwargs):
        """GET a page, retrying only what is worth retrying.

        Before this existed a single dropped connection or one 502 failed the
        entire series: it went straight onto the failed list and its data was
        whatever the index already held until someone ran the retry menu by
        hand. Recovering in place is both faster than a second pass and more
        accurate than leaving the series stale, and it is what makes running
        more workers safe -- the pool now reacts to push-back instead of
        ignoring it.

        Raises the last httpx error if every attempt fails, so callers keep
        their existing error handling.
        """
        kwargs.setdefault("follow_redirects", True)
        delay = _BACKOFF_BASE
        last_exc: Exception | None = None
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            await self._rate_guard.wait()
            try:
                with self._profiler.phase("network"):
                    resp = await client.get(url, **kwargs)
            except httpx.HTTPError as exc:
                last_exc = exc
            else:
                if resp.status_code not in _RETRY_STATUS:
                    self._rate_guard.reward()
                    return resp
                if resp.status_code in (429, 503):
                    self._rate_guard.penalise(_retry_after_seconds(resp))
                last_exc = httpx.HTTPStatusError(
                    f"HTTP {resp.status_code}",
                    request=resp.request,
                    response=resp,
                )
            if attempt < _MAX_ATTEMPTS:
                logger.debug("Retrying %s (attempt %d/%d): %s", url, attempt, _MAX_ATTEMPTS, last_exc)
                await asyncio.sleep(delay)
                delay *= 2
        assert last_exc is not None
        raise last_exc

    async def _fetch_season_pages(self, client, season_links):
        """Fetch every season page of one series at once, in order.

        Season pages are independent GETs, but they used to be fetched one
        after another, so a series cost one round trip per season and its
        recorded scrape time scaled straight off the season count (on s.to:
        1 season 1.2s, 3 seasons 2.3s, 5 seasons 3.4s, 10 seasons 6.3s).
        Nothing in the parsing depends on the order they arrive in, only on
        the order they are *processed* in, which the caller still controls.

        The semaphore stops a 57-season series from opening 57 sockets at
        once. Results come back positionally aligned with `season_links`, a
        failed fetch as the exception object, so the caller keeps its
        original first-failure-wins behaviour exactly.
        """
        sem = asyncio.Semaphore(SEASON_CONCURRENCY)

        async def fetch(url):
            async with sem:
                resp = await self._get(client, url)
                return resp.text

        return await asyncio.gather(
            *(fetch(url) for _, url in season_links),
            return_exceptions=True,
        )

    async def _scrape_one_series(
        self,
        client: httpx.AsyncClient,
        info: dict,
    ) -> dict:  # pylint: disable=too-many-locals,too-many-return-statements
        """Scrape a single series and return its data."""
        t_start = time.perf_counter()
        url = info.get("scrape_url", info["url"])
        try:
            resp = await self._get(client, url)
        except httpx.HTTPError as exc:
            return self._error_result(info, str(exc))

        soup = make_soup(resp.text)

        error_code = _check_error_page(soup)
        if error_code:
            reason = f"{error_code} server error" if error_code in _SERVER_ERROR_CODES else f"{error_code} error page"
            logger.warning(
                "Error page detected for %s: %s",
                url,
                error_code,
            )
            return self._error_result(info, reason)

        if not _is_logged_in(soup):
            # One shared session serves the whole run, so an expiry here would
            # otherwise fail every remaining series. Re-login once and retry
            # this page; only give up if the second look is still logged out.
            if await self._relogin_shared_client(client):
                try:
                    resp = await self._get(client, url)
                except httpx.HTTPError as exc:
                    return self._error_result(info, str(exc))
                soup = make_soup(resp.text)
            if not _is_logged_in(soup):
                logger.error(
                    "Session expired while scraping %s",
                    url,
                )
                return self._error_result(
                    info,
                    "session expired \u2014 not logged in",
                )

        title = _extract_title(soup) or info["title"]
        if title.lower().strip() in _UTILITY_PAGES:
            return self._error_result(info, "utility page")

        season_links = _extract_season_links(soup, url)
        if not season_links:
            return self._error_result(
                info,
                "no seasons found",
            )

        seasons_data = []
        total_watched, total_eps = 0, 0

        season_pages = await self._fetch_season_pages(client, season_links)

        for (label, season_url), page in zip(season_links, season_pages, strict=True):
            if isinstance(page, BaseException):
                return self._error_result(
                    info,
                    f"season {label} fetch failed: {page}",
                )
            with self._profiler.phase("parse"):
                episodes, season_languages = parse_season_html(page)
            if episodes is None:
                # None means the page had no episode table at all, or had
                # rows that none parsed -- a scrape failure. An empty list
                # is different: the table is there and genuinely empty, and
                # that gets stored as 0 like before.
                return self._error_result(
                    info,
                    f"season {label}: episode table missing or unparseable",
                )
            watched_count = sum(1 for ep in episodes if ep["watched"])
            total_count = len(episodes)
            season_entry = {
                "season": label,
                "url": season_url,
                "episodes": episodes,
                "watched_episodes": watched_count,
                "total_episodes": total_count,
            }
            if season_languages:
                season_entry["languages"] = season_languages
            seasons_data.append(season_entry)
            total_watched += watched_count
            total_eps += total_count

        scrape_url = info.get("scrape_url", info["url"])
        result = {
            "title": title,
            "link": info["link"],
            "url": scrape_url,
            "total_seasons": len(seasons_data),
            "total_episodes": total_eps,
            "watched_episodes": total_watched,
            "unwatched_episodes": max(
                0,
                total_eps - total_watched,
            ),
            "seasons": seasons_data,
            "scrape_duration_seconds": time.perf_counter() - t_start,
        }
        return result

    @staticmethod
    def _error_result(info: dict, reason: str) -> dict:
        """Build a standardised error result dict."""
        return {
            "title": info.get("title", ""),
            "link": info.get("link", ""),
            "url": info.get("url", ""),
            "total_seasons": 0,
            "total_episodes": 0,
            "watched_episodes": 0,
            "unwatched_episodes": 0,
            "seasons": [],
            "_error": True,
            "_error_reason": reason,
        }

    # -- Worker ------------------------------------------------------

    async def _relogin_shared_client(self, client) -> bool:
        """Log the shared session back in after an expiry, at most once at a time.

        Every worker shares one client, so an expiry mid-run would otherwise
        fail every remaining series. Workers that hit it together must not
        each fire their own login -- that is what made a sibling site start
        refusing logins outright -- so the lock plus the counter means the
        first one re-logs in and the rest simply reuse the result.
        """
        async with self._client_lock:
            attempt = self._relogin_count
        if attempt >= _MAX_RELOGINS:
            return False
        async with self._client_lock:
            if self._relogin_count != attempt:
                return True  # someone else just refreshed it
            self._relogin_count += 1
            try:
                await self._login_client(client)
                logger.warning("Session had expired; logged back in (attempt %d)", self._relogin_count)
                return True
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.error("Re-login after session expiry failed: %s", exc)
                return False

    async def _acquire_client(self):
        """Hand out the single logged-in session that every worker shares.

        Each worker used to build its own session, so an N-worker run meant N
        logins of three requests each, every run. That is pure waste -- and it
        is not harmless: cycling worker counts during a benchmark made
        serienstream.to start refusing logins outright, because the logins,
        not the scraping, were what looked abusive.

        httpx.AsyncClient is built to be driven from many tasks at once and
        owns its connection pool, so one client sized for the whole run serves
        everybody: one login, and keep-alive connections that are reused
        across workers and across series instead of per worker.

        Reference-counted rather than opened and closed by the orchestrators,
        because several entry points spawn workers and each would otherwise
        need the same bookkeeping.
        """
        async with self._client_lock:
            if self._shared_client is None:
                self._shared_client = await self._create_logged_in_client()
            self._client_users += 1
            return self._shared_client

    async def _release_client(self) -> None:
        """Drop this worker's claim; close the session once the last one exits."""
        async with self._client_lock:
            self._client_users -= 1
            if self._client_users <= 0 and self._shared_client is not None:
                await self._shared_client.aclose()
                self._shared_client = None
                self._client_users = 0

    async def _worker(  # pylint: disable=too-many-arguments
        self,
        worker_id: int,
        queue: asyncio.Queue,
        results: list,
        progress: dict,
        total: int,
        predicted_rate: float | None = None,
    ):  # pylint: disable=too-many-positional-arguments
        """Process series from the queue using the shared session."""
        try:
            client = await self._acquire_client()
        except RuntimeError:
            logger.warning(
                "Worker %d login failed, retrying...",
                worker_id,
            )
            await asyncio.sleep(1)
            try:
                client = await self._acquire_client()
            except RuntimeError:
                logger.error(
                    "Worker %d login failed permanently",
                    worker_id,
                )
                return

        try:
            await self._worker_loop(
                worker_id,
                client,
                queue,
                results,
                progress,
                total,
                predicted_rate=predicted_rate,
            )
        finally:
            self._progress.flush()
            await self._release_client()

    async def _worker_loop(  # pylint: disable=too-many-arguments
        self,
        worker_id,
        client,
        queue,
        results,
        progress,
        total,
        predicted_rate: float | None = None,
    ):  # pylint: disable=too-many-positional-arguments
        """Inner loop for a worker — processes queue items."""
        while True:
            if self._check_pause():
                raise ScrapingPausedError("Pause file detected")

            try:
                info = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            try:
                result = await self._scrape_one_series(
                    client,
                    info,
                )
            except ScrapingPausedError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.error(
                    "Worker %d unexpected error on %s: %s",
                    worker_id,
                    info.get("url", "?"),
                    exc,
                )
                result = self._error_result(
                    info,
                    f"unexpected error: {exc}",
                )

            self._record_worker_result(
                info,
                result,
                results,
            )

            # Keep completed_links, progress, and the checkpoint snapshot
            # consistent under the lock. This prevents a crash window where
            # completed_links is ahead of the saved series_data.
            with self._lock:
                link = info.get("link", "")
                if link:
                    self.completed_links.add(link)
                if info.get("url"):
                    self.attempted_urls.add(info["url"])

                progress["done"] += 1
                done = progress["done"]
                if done % CHECKPOINT_EVERY == 0:
                    self.series_data = list(results)

            self._print_progress(
                done,
                total,
                progress,
                result,
                info,
                predicted_rate=predicted_rate,
            )

            if done % CHECKPOINT_EVERY == 0:
                await self.asave_checkpoint(include_data=False)
            if self._check_interrupt_flag():
                # Stop consuming more work; remaining tasks finish current item.
                break

    def _record_worker_result(self, info, result, results):
        """Classify result as success or failure."""
        with self._lock:
            if result.get("_error"):
                reason = result.get("_error_reason") or "scrape_error"
                self.failed_links.append(
                    {
                        "url": info["url"],
                        "title": info.get("title", ""),
                        "link": info.get("link", ""),
                        "reason": reason,
                    }
                )
                # Still append to results so checkpoint data is complete
                results.append(result)
            else:
                if result["total_episodes"] == 0:
                    self.failed_links.append(
                        {
                            "url": info["url"],
                            "title": result.get(
                                "title",
                                info.get("title", ""),
                            ),
                            "link": info.get("link", ""),
                            "reason": "empty_placeholder",
                        }
                    )
                results.append(result)

    def _print_progress(  # pylint: disable=too-many-locals
        self,
        done,
        total,
        progress,
        result,
        info,
        predicted_rate=None,
    ):
        """Print a progress bar line for one series."""
        with self._lock:
            elapsed = time.perf_counter() - progress["start"]
        rate = done / elapsed if elapsed > 0 else 0

        # Use predicted rate from per-series index timings if available
        if predicted_rate is not None and predicted_rate > 0:
            # Historical predicted rate dominates (95%+ initially),
            # current session rate gains weight very slowly.
            # Series are always the same, so last run's avg_scrape_seconds
            # is the best predictor for next scrape time.
            current_weight = min(0.05, 0.01 + (done / total) * 0.04)
            rate = rate * current_weight + predicted_rate * (1 - current_weight)

        eta = (total - done) / rate if rate > 0 else 0
        eta_mins = f"{eta / 60:.1f}"
        pct = int((done / total) * 100)
        bar_len = 30
        filled = int(bar_len * done / total)
        progress_bar = "\u2588" * filled + "\u2591" * (bar_len - filled)

        season_labels = [s.get("season", "?") for s in result.get("seasons", [])]
        season_info = f" [{','.join(season_labels)}]" if season_labels else ""

        prefix = f"[{done}/{total}] [{progress_bar}] {pct}% | ETA: {eta_mins}m"
        if result.get("_error"):
            title = info.get("title", "?")
            reason = result.get("_error_reason", "Failed")
            self._progress.write(f"{prefix} | \u26a0 {title}: {reason}")
        elif result["total_episodes"] == 0:
            self._progress.write(f"{prefix} | \u26a0 {result['title']}{season_info}: No episodes")
        else:
            w = result["watched_episodes"]
            t = result["total_episodes"]
            self._progress.write(f"{prefix} | \u2713 {result['title']}{season_info}: {w}/{t} watched")

    # -- Async scrape orchestrators ----------------------------------

    def _filter_completed(
        self,
        series_list: list[dict],
    ) -> list[dict] | None:
        """Remove already-completed series from the list."""
        if not self.completed_links:
            return series_list
        before = len(series_list)
        filtered = [s for s in series_list if s.get("link") not in self.completed_links]
        if before != len(filtered):
            print(f"  Skipping {before - len(filtered)} already-completed series")
        if not filtered:
            print("\u2713 All series already scraped (from checkpoint)")
            return None
        return filtered

    def _compute_predicted_eta(self, series_list: list[dict]) -> float | None:
        """Sum per-series avg_scrape_seconds from the index for ETA prediction."""
        try:
            if not os.path.exists(SERIES_INDEX_FILE):
                return None
            items = self._index_items()
            slug_map: dict[str, float] = {}
            for item in items:
                url = item.get("url", "") or item.get("link", "")
                slug = self.get_series_slug_from_url(url)
                avg = item.get("avg_scrape_seconds")
                if slug != "unknown" and isinstance(avg, (int, float)) and avg > 0:
                    slug_map[slug] = float(avg)

            total = 0.0
            matched = 0
            for s in series_list:
                slug = self.get_series_slug_from_url(s.get("link", ""))
                if slug in slug_map:
                    total += slug_map[slug]
                    matched += 1

            if matched >= max(3, len(series_list) * 0.1):
                return total
            return None
        except Exception:
            return None

    def _get_average_scrape_seconds(self) -> float | None:
        """Compute the average avg_scrape_seconds across all indexed series.

        Used as a fallback estimate when per-series slug matching fails
        (e.g. first scrape with no historical data for specific series).
        """
        try:
            if not os.path.exists(SERIES_INDEX_FILE):
                return None
            items = self._index_items()
            values = [
                float(item["avg_scrape_seconds"])
                for item in items
                if isinstance(item.get("avg_scrape_seconds"), (int, float)) and item["avg_scrape_seconds"] > 0
            ]
            if len(values) >= 3:
                return sum(values) / len(values)
            return None
        except Exception:
            return None

    async def _scrape_list(
        self,
        series_list: list[dict],
        num_workers: int | None = None,
    ):
        """Scrape a list of series with multi-session workers."""
        filtered = self._filter_completed(series_list)
        if filtered is None:
            return

        queue: asyncio.Queue = asyncio.Queue()
        for s in filtered:
            queue.put_nowait(s)

        results: list[dict] = list(self.series_data)
        n = min(num_workers or NUM_WORKERS, len(filtered))
        n = max(1, n)
        progress = {
            "done": 0,
            "start": time.perf_counter(),
        }

        # Pre-compute predicted ETA from per-series historical timings
        predicted_seconds = self._compute_predicted_eta(filtered)
        if predicted_seconds:
            # Divide by workers: _compute_predicted_eta returns sequential total,
            # but we scrape in parallel so wall-clock time is roughly total / workers.
            predicted_seconds = predicted_seconds / n
            predicted_rate = len(filtered) / predicted_seconds
            print(
                f"\u2192 Scraping {len(filtered)} series "
                f"with {n} session(s)"
                f" (predicted ~{predicted_seconds / 60:.1f}m)..."
            )
        else:
            # Fallback: use average of existing per-series timings from the index
            avg_seconds = self._get_average_scrape_seconds()
            estimated_seconds_per_series = avg_seconds or 1.0
            predicted_seconds = len(filtered) * estimated_seconds_per_series / n
            predicted_rate = len(filtered) / predicted_seconds
            print(
                f"\u2192 Scraping {len(filtered)} series "
                f"with {n} session(s)"
                f" (estimated ~{predicted_seconds / 60:.1f}m)..."
            )

        tasks = [
            asyncio.create_task(
                self._worker(
                    i,
                    queue,
                    results,
                    progress,
                    len(filtered),
                    predicted_rate=predicted_rate,
                )
            )
            for i in range(n)
        ]
        try:
            await asyncio.gather(*tasks)
        except ScrapingPausedError:
            for t in tasks:
                t.cancel()
            await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )
            with self._lock:
                self.series_data = results
            raise
        except BaseException:
            # Any other failure (a parser bug, Ctrl+C, a cancelled task) used
            # to skip the assignment below, so every series scraped since the
            # last checkpoint was thrown away and the siblings were left
            # running detached. Keep the work and stop the pool, exactly as
            # the pause path does, then let the error travel on.
            for t in tasks:
                t.cancel()
            await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )
            with self._lock:
                self.series_data = results
            raise

        with self._lock:
            self.series_data = results

    async def _async_run(
        self,
        single_url=None,
        url_list=None,
        new_only=False,
        retry_failed=False,
    ):
        """Async core of run()."""
        tmp = await self._create_logged_in_client()
        try:
            print("\u2713 Logged in to bs.to")
            await self._async_run_inner(
                tmp,
                single_url=single_url,
                url_list=url_list,
                new_only=new_only,
                retry_failed=retry_failed,
            )
        finally:
            if not tmp.is_closed:
                await tmp.aclose()

    async def _async_run_inner(
        self,
        tmp,
        single_url=None,
        url_list=None,
        new_only=False,
        retry_failed=False,
    ):  # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Dispatch to the correct scraping strategy."""
        if single_url:
            await self._run_single(tmp, single_url)
            return

        if url_list:
            self._checkpoint_mode = self._checkpoint_mode or "batch"
            await self._run_url_list(tmp, url_list)
            return

        if retry_failed:
            await self._run_retry(tmp)
            return

        if new_only:
            await self._run_new_only(tmp)
            return

        await self._run_all(tmp)

    async def _run_single(self, tmp, single_url):
        """Scrape a single series by URL."""
        self._checkpoint_mode = "single"
        main_url = self.normalize_to_series_url(single_url)
        m = _SERIE_PATH_RE.search(main_url)
        link = m.group(1) if m else main_url
        info = {
            "title": main_url.split("/")[-1],
            "link": link,
            "url": main_url,
            "scrape_url": main_url,
        }
        print(f"\u2192 Scraping single series: {main_url}")
        self.attempted_urls.add(info["url"])
        # tmp is closed by _async_run's finally block.
        # Go through the worker pool with a single worker instead of calling
        # _scrape_one_series directly. Done directly, a one-series run
        # finished silently: no progress line, no episode count, and none of
        # the empty-page or episode-0 warnings the pool raises. One worker
        # costs one extra login and makes this mode report exactly like every
        # other one.
        await self._scrape_list([info], num_workers=1)
        # Single-series runs have no partial resume state to preserve.
        self.clear_checkpoint()

    async def _run_url_list(self, tmp, url_list):
        """Scrape a batch of series from a URL list."""
        if self._checkpoint_mode is None:
            self._checkpoint_mode = "batch"
        series_list = []
        for u in url_list:
            main_url = self.normalize_to_series_url(u)
            m = _SERIE_PATH_RE.search(main_url)
            link = m.group(1) if m else main_url
            series_list.append(
                {
                    "title": main_url.split("/")[-1],
                    "link": link,
                    "url": main_url,
                    "scrape_url": main_url,
                }
            )
        # tmp is closed by _async_run's finally block.
        n = NUM_WORKERS if self._use_parallel and len(series_list) > 1 else 1
        await self._scrape_list(
            series_list,
            num_workers=n,
        )
        print(f"  Successfully scraped: {len(self.series_data)}/{len(url_list)} series")

    async def _run_retry(self, tmp):
        """Retry previously failed series."""
        self._checkpoint_mode = "retry"
        failed_list = self.load_failed_series()
        if not failed_list:
            print("\u2713 No failed series found")
            return
        print(f"\u2713 Found {len(failed_list)} failed series \u2014 retrying in sequential mode")
        await self._scrape_list(failed_list, num_workers=1)

    async def _run_new_only(self, tmp):
        """Scrape only new (not-yet-indexed) series."""
        self._checkpoint_mode = "new_only"
        await self._revalidate_ignored_series(tmp)
        print("\u2192 Fetching series list...")
        all_series = await self._get_all_series(tmp)
        if not self._confirm_catalogue_size(all_series):
            return
        self.all_discovered_series = all_series
        self._check_ignored_vs_catalog(all_series)
        self._check_index_vs_catalog(all_series, quiet=True)
        existing_slugs = self.load_existing_slugs()
        ignored_slugs = self.get_ignored_slugs()
        new_list = [
            s
            for s in all_series
            if (
                self.get_series_slug_from_url(
                    s.get("link", ""),
                )
                not in existing_slugs
                and self.get_series_slug_from_url(
                    s.get("link", ""),
                )
                not in ignored_slugs
            )
        ]

        # Identify renames of vanished series so they don't appear as brand-new.
        vanished_entries = self._vanished_index_entries(all_series)
        rename_titles = _find_vanished_renames(
            [(t, u) for t, u in vanished_entries],
            new_list,
        )
        # Flagged renames are still scraped: the score is a fuzzy guess and
        # dropping a series here means it never enters the index at all.
        truly_new = list(new_list)

        total = len(all_series)
        print(f"\u2192 New series to scrape: {len(truly_new)} (out of {total})")
        if rename_titles:
            print(
                f"  ({len(rename_titles)} possible rename(s) of vanished series "
                f"flagged below; all are still scraped)"
            )
        if not truly_new:
            if rename_titles:
                print("\u2713 No brand-new series detected \u2014 only renames of vanished entries.")
            else:
                print("\u2713 No new series detected \u2014 nothing to scrape")
            self.series_data = []
            return
        if len(truly_new) <= 50:
            for s in truly_new:
                print(f"  + {s['title']}")
        await self._scrape_list(truly_new, num_workers=1)
        # Ensure matched rename data is available for the post-scrape table.
        self.series_data.extend([s for s in new_list if s.get("title") in rename_titles])

    async def _run_all(self, tmp):
        """Scrape all series (default mode)."""
        self._checkpoint_mode = "all_series"
        await self._revalidate_ignored_series(tmp)
        print("\u2192 Fetching series list...")
        all_series = await self._get_all_series(tmp)
        if not self._confirm_catalogue_size(all_series):
            return
        self.all_discovered_series = all_series
        ignored_slugs = self.get_ignored_slugs()
        print(f"\u2713 Found {len(all_series)} series")
        self._check_ignored_vs_catalog(all_series)
        self._check_index_vs_catalog(all_series, quiet=True)

        existing_slugs = self.load_existing_slugs()
        new_entries = [
            s
            for s in all_series
            if (
                self.get_series_slug_from_url(
                    s.get("link", ""),
                )
                not in existing_slugs
                and self.get_series_slug_from_url(
                    s.get("link", ""),
                )
                not in ignored_slugs
            )
        ]

        # Separate renames of vanished series from genuine new titles.
        vanished_entries = self._vanished_index_entries(all_series)
        rename_titles = _find_vanished_renames(
            [(t, u) for t, u in vanished_entries],
            new_entries,
        )
        # Renames are reported, not withheld -- see the note above.
        truly_new_titles = [s["title"] for s in new_entries]

        if truly_new_titles:
            print(f"\n\u2139 {len(truly_new_titles)} new series detected:")
            for t in truly_new_titles[:10]:
                print(f"  + {t}")
            if len(truly_new_titles) > 10:
                print(f"  ... and {len(truly_new_titles) - 10} more")
            print()
        elif rename_titles:
            print(
                f"\n\u2139 {len(rename_titles)} "
                "indexed series renames detected "
                "(review table will appear after scraping)."
            )
            print()

        if ignored_slugs:
            all_series = [
                s
                for s in all_series
                if self.get_series_slug_from_url(
                    s.get("link", ""),
                )
                not in ignored_slugs
            ]
            skipped = len(self.all_discovered_series) - len(all_series)
            if skipped:
                print(f"  Skipping {skipped} ignored series")
        n = NUM_WORKERS if self._use_parallel else 1
        await self._scrape_list(all_series, num_workers=n)
        print(f"\n\u2713 Successfully scraped {len(self.series_data)} series")

    # -- Domain checking / probing -----------------------------------

    async def _probe_one_site(
        self,
        site_url: str,
    ) -> dict:
        """Return probe result for a single site URL."""
        client = None
        try:
            client = httpx.AsyncClient(
            http2=True,
                headers={"User-Agent": UA},
                timeout=httpx.Timeout(REQUEST_TIMEOUT, connect=10.0),
                follow_redirects=True,
                limits=httpx.Limits(
                # One worker now has up to SEASON_CONCURRENCY season fetches
                # in flight at once, so a 2-connection pool would serialise
                # the fan-out straight back into the queue it was meant to
                # remove. Keepalive matches it so those connections survive
                # between series instead of re-handshaking each time.
                max_connections=self.pool_workers * SEASON_CONCURRENCY + 4,
                max_keepalive_connections=self.pool_workers * SEASON_CONCURRENCY + 4,
            ),
            )
            resp = await client.get(_login_url(site_url))
            ok = resp.status_code < 500 and _looks_like_login_page(resp.text)
            return {
                "site_url": site_url,
                "ok": ok,
                "status_code": resp.status_code,
                "reason": "reachable" if ok else "unexpected response",
            }
        except Exception as exc:
            return {
                "site_url": site_url,
                "ok": False,
                "status_code": None,
                "reason": str(exc),
            }
        finally:
            if client and not client.is_closed:
                await client.aclose()

    async def probe_sites(
        self,
        site_urls: list[str] | None = None,
    ) -> list[dict]:
        """Probe multiple site URLs in parallel and report availability."""
        urls = site_urls or SITE_URLS
        return list(await asyncio.gather(*[self._probe_one_site(u) for u in urls]))

    async def get_catalogue_info_for_site(
        self,
        site_url: str,
    ) -> tuple[int | None, set[str]]:
        """Login once, fetch the catalogue once, and return (count, slugs).

        This avoids the duplicate login + full-page download that happened
        when count and slugs were fetched in separate calls.
        """
        previous_site_url = self.site_url
        try:
            self.site_url = site_url
            # _get_all_series fetches the catalogue and applies the same
            # logged-in check the login's verify step would, so verifying
            # here only downloaded the same large page a second time.
            client = await self._create_logged_in_client(verify=False)
            try:
                series = await self._get_all_series(client)
            finally:
                await client.aclose()
            slugs = set()
            for s in series:
                slug = self.get_series_slug_from_url(s.get("link", ""))
                if slug and slug != "unknown":
                    slugs.add(slug)
            return len(series), slugs
        except Exception as exc:
            logger.error(
                "Error fetching catalogue info from %s: %s",
                site_url,
                exc,
            )
            return None, set()
        finally:
            self.site_url = previous_site_url

    # -- Public API --------------------------------------------------

    def run(
        self,
        single_url=None,
        url_list=None,
        new_only=False,
        resume_only=False,
        retry_failed=False,
        parallel=None,
        checkpoint_mode=None,
    ):  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-branches  # noqa: E501
        """Main entry point: login, scrape, save checkpoint."""
        if parallel is not None:
            self._use_parallel = parallel
            mode_name = "multi-session" if parallel else "single-session"
            print(f"\u2192 Using {mode_name} mode")
        else:
            self._use_parallel = True

        if checkpoint_mode is not None:
            self._checkpoint_mode = checkpoint_mode

        self._clear_pause_file()

        # Register graceful Ctrl+C pause: signal handler creates the
        # pause file so workers finish their current series and then raise
        # ScrapingPaused at the next checkpoint.
        def _signal_handler(signum, _frame):
            logger.info("Received signal %d — graceful pause requested", signum)
            print("\n⚠ Pause requested (Ctrl+C). Finishing current series...")
            self._create_pause_file()

        try:
            signal.signal(signal.SIGINT, _signal_handler)
            if hasattr(signal, "SIGTERM"):
                signal.signal(signal.SIGTERM, _signal_handler)
        except ValueError:
            # Not the main thread or signal not supported; ignore.
            pass

        try:
            if resume_only:
                if self.load_checkpoint():
                    count = len(self.completed_links)
                    print(f"\u2192 Resuming from checkpoint ({count} series already done)")
                else:
                    print("\u26a0 No checkpoint found. Starting fresh...")

            asyncio.run(
                self._async_run(
                    single_url=single_url,
                    url_list=url_list,
                    new_only=new_only,
                    retry_failed=retry_failed,
                )
            )

            empty = [s for s in self.series_data if s.get("total_episodes", 0) == 0]
            if empty:
                print(f"\n\u26a0 {len(empty)} series with 0 episodes:")
                for s in empty:
                    print(f"  \u2022 {s['title']} \u2192 {s['url']}")

            self.save_checkpoint(include_data=True)
            self.reconcile_failed_series()

        except ScrapingPausedError:
            self.paused = True
            self._clear_pause_file()
            self.save_checkpoint(include_data=True)
            if self.failed_links:
                self.save_failed_series()
        except (KeyboardInterrupt, SystemExit):
            self.save_checkpoint(include_data=True)
            if self.failed_links:
                self.save_failed_series()
            raise
        except Exception:  # pylint: disable=broad-exception-caught
            # An unexpected failure is exactly when the partial work matters
            # most: without this the run's scraped series and failed list were
            # both discarded and the next run started over from the last
            # checkpoint interval. Persist, then re-raise unchanged.
            logger.exception("Unexpected error during scrape — saving partial progress")
            self.save_checkpoint(include_data=True)
            if self.failed_links:
                self.save_failed_series()
            raise

    async def verify_series_url(
        self,
        client: httpx.AsyncClient,
        url: str,
    ) -> dict:
        """Fetch a series URL and return current title/availability info.

        Used to verify vanished/rename candidates without doing a full scrape.
        Returns a dict with keys: url, reachable, title, season_count, error.
        """
        result = {
            "url": url,
            "reachable": False,
            "title": None,
            "season_count": 0,
            "error": None,
        }
        try:
            resp = await client.get(url, follow_redirects=True)
            soup = make_soup(resp.text)
            error_code = _check_error_page(soup)
            if error_code:
                result["error"] = f"error_page_{error_code}"
                return result
            title = _extract_title(soup)
            season_count = _count_seasons_from_html(soup)
            result["reachable"] = True
            result["title"] = title
            result["season_count"] = season_count
        except httpx.HTTPError as exc:
            result["error"] = f"http_error_{exc}"
        except Exception as exc:  # pylint: disable=broad-exception-caught
            result["error"] = f"exception_{exc}"
        return result

    async def verify_vanished_and_candidates(
        self,
        vanished_entries: list[tuple[str, str]],
        candidate_entries: list[dict],
    ) -> tuple[list[tuple[str, str]], list[dict]]:
        """Re-fetch vanished URLs and rename candidates to verify accuracy.

        Args:
            vanished_entries: list of (title, url) tuples for vanished series.
            candidate_entries: list of new-entry dicts that might be renames.

        Returns:
            Tuple of (verified_vanished, verified_candidates) with updated
            titles and reachability flags stored in dict metadata.
        """
        client = httpx.AsyncClient(
            http2=True,
            headers={"User-Agent": UA},
            timeout=httpx.Timeout(REQUEST_TIMEOUT, connect=10.0),
            follow_redirects=True,
            limits=httpx.Limits(
                # One worker now has up to SEASON_CONCURRENCY season fetches
                # in flight at once, so a 2-connection pool would serialise
                # the fan-out straight back into the queue it was meant to
                # remove. Keepalive matches it so those connections survive
                # between series instead of re-handshaking each time.
                max_connections=self.pool_workers * SEASON_CONCURRENCY + 4,
                max_keepalive_connections=self.pool_workers * SEASON_CONCURRENCY + 4,
            ),
        )
        try:
            await self._login_client(client)
            all_urls = [url for _, url in vanished_entries if url]
            all_urls.extend(e.get("url", e.get("link", "")) for e in candidate_entries if e.get("url") or e.get("link"))
            unique_urls = sorted(set(all_urls))
            if not unique_urls:
                return vanished_entries, candidate_entries

            print(f"\n→ Verifying {len(unique_urls)} vanished/rename URL(s) with fresh scrape...")
            results = await asyncio.gather(
                *[self.verify_series_url(client, url) for url in unique_urls],
                return_exceptions=True,
            )
            info_by_url: dict[str, dict] = {}
            for res in results:
                if isinstance(res, Exception):
                    continue
                if isinstance(res, dict):
                    info_by_url[res["url"]] = res

            verified_vanished = []
            for title, url in vanished_entries:
                info = info_by_url.get(url, {})
                if info.get("reachable"):
                    # Page still exists — probably not vanished.
                    verified_vanished.append((info.get("title") or title, url))
                else:
                    verified_vanished.append((title, url))

            verified_candidates = []
            for entry in candidate_entries:
                url = entry.get("url", entry.get("link", ""))
                info = info_by_url.get(url, {})
                new_entry = dict(entry)
                if info.get("reachable") and info.get("title"):
                    new_entry["title"] = info["title"]
                    new_entry["verified_season_count"] = info.get("season_count", 0)
                new_entry["_verified_reachable"] = info.get("reachable", False)
                new_entry["_verified_error"] = info.get("error")
                verified_candidates.append(new_entry)

            return verified_vanished, verified_candidates
        finally:
            if client and not client.is_closed:
                await client.aclose()
