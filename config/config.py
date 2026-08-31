"""
Configuration for the BS.TO series scraper.

bs.to is dead; bs.cine.to is the current primary mirror.
"""

import contextlib
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv


def configure_console() -> None:
    """Make arrow/box-drawing output safe on any code page.

    A redirected pipe or a legacy Windows code page falls back to cp1252,
    which cannot encode "→" or "─" -- printing the very first status
    line would kill the run with a UnicodeEncodeError. ``errors="replace"``
    guarantees no crash even where UTF-8 itself is refused.

    Called at import time because this module is the earliest one every
    entry point (main.py, the test suite) pulls in, and it prints on import.
    """
    for stream in (sys.stdout, sys.stderr):
        with contextlib.suppress(Exception):
            stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]


configure_console()

# Load environment variables from .env file at import time so every module
# that imports from this config sees the correct values immediately.
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))


def _validate_and_normalize_url(url: str) -> str:
    """Validate and normalize a URL, raising ValueError for invalid URLs."""
    if not url:
        raise ValueError("URL cannot be empty")

    # Ensure URL has a scheme
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    # Parse and validate
    try:
        parsed = urlparse(url)
        if not parsed.netloc:
            raise ValueError(f"Invalid URL: {url}")
        return url.rstrip("/")
    except Exception as e:
        raise ValueError(f"Invalid URL '{url}': {e}") from e


# Site configuration (edit here, not in .env)
# bs.to is dead; bs.cine.to is the current primary.
_BS_SITE_URLS = [
    "https://burningseries.ac",
    "https://burningseries.cx",
    "https://bs.cine.to",
]
SITE_URLS = []
_seen = set()
for _url in _BS_SITE_URLS:
    try:
        _normalized = _validate_and_normalize_url(_url)
        if _normalized not in _seen:
            _seen.add(_normalized)
            SITE_URLS.append(_normalized)
    except ValueError:
        print(f"⚠ Warning: Invalid site URL skipped: {_url}")

# Backwards-compatible alias: the first configured URL is the canonical primary.
SITE_URL = SITE_URLS[0] if SITE_URLS else ""

# Compute valid series hosts from SITE_URLS for URL validation
_VALID_HOSTS = set()
for _url in SITE_URLS:
    try:
        _parsed = urlparse(_url)
        if _parsed.netloc:
            _VALID_HOSTS.add(_parsed.netloc)
    except Exception:
        pass
VALID_SERIES_HOSTS = frozenset(_VALID_HOSTS)

# ==================== CREDENTIALS ====================
USERNAME = os.getenv("BS_USERNAME", "")
PASSWORD = os.getenv("BS_PASSWORD", "")

# ==================== DIRECTORIES ====================
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
LOGS_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")

Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)

# ==================== FILE PATHS ====================
SERIES_INDEX_FILE = os.path.join(DATA_DIR, "series_index.json")

# Default batch file for batch URL import
# Edit DEFAULT_BATCH_FILE_PATH below to change the default batch file
DEFAULT_BATCH_FILE_PATH = os.path.join(os.path.dirname(__file__), "..", "series_urls.txt")
DEFAULT_BATCH_FILE = os.path.abspath(DEFAULT_BATCH_FILE_PATH)

# ==================== SCRAPING SETTINGS ====================
# Measured, not guessed. Two sweeps on one shared login, series shuffled
# per pass and worker counts run in random order:
#
#   250 series x2   4: 11.78   6: 15.78   8: 17.69  12: 18.18  16: 19.32
#   300 series x3                        12: 17.14  16: 16.70  20: 16.30
#                                        24: 16.55  32: 16.61
#
# Throughput climbs steeply to 8, flattens by 12, and is indistinguishable
# from 12 to 32 -- the two sweeps disagree on whether 16 beats 12, which is
# itself the answer: past 12 the differences are session noise, not signal.
# 12 was fastest in the longer sweep and the steadiest there (spread 0.33
# vs 3.12 at 24), so it is the last setting that buys anything real.
#
# The earlier note here recommended 4 on stability grounds from a sweep
# that stopped at 12; the wider sweep shows 4 costs ~35% throughput for
# no benefit. Zero 429/503 was seen anywhere up to 32 workers, so this
# plateau is local saturation, not the site pushing back -- raising it
# further only adds load.
# Past the peak the season fan-out already keeps pool_workers *
# SEASON_CONCURRENCY requests in flight, so more workers only add load.
#
# Where the time actually goes, measured with the built-in PhaseProfiler
# over 300 series x2 shuffled passes at these settings:
#   network 99.5%   parse 0.4%   checkpoint <0.1%
# Parsing costs 7% of ONE core across the run, so the scrape is bound by
# the network and not by this process. Offloading parse off the event loop was
# already measured 2-2.7x SLOWER (see parse_season_html), and the lxml parser
# cut per-page parse time another 3.8x on top, so there is nothing left to
# win here. Do not reopen this without a fresh profile showing otherwise.
NUM_WORKERS = int(os.getenv("BS_MAX_WORKERS", "12"))

# Season pages of one series are independent GETs. Fetching them one after
# another made a series' scrape time scale linearly with its season count,
# so they are fanned out this many at a time instead. Total requests in
# flight is NUM_WORKERS * SEASON_CONCURRENCY -- raise either with care, and
# only alongside the RateGuard that reacts to the site pushing back.
SEASON_CONCURRENCY = int(os.getenv("BS_SEASON_CONCURRENCY", "4"))


# Checkpoint frequency: serialize resume state every N completed series.
# Large index (≈48 MB) → less frequent to avoid event-loop blocking.
CHECKPOINT_EVERY = int(os.getenv("BS_CHECKPOINT_EVERY", "50"))

# ==================== TIMEOUTS ====================
HTTP_REQUEST_TIMEOUT = 20.0

# ==================== LOGGING ====================
LOG_FILE = os.path.join(LOGS_DIR, "bs_to_backup.log")

print(f"✓ Config loaded (DATA_DIR: {os.path.abspath(DATA_DIR)})")
