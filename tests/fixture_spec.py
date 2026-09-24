"""Which parser outputs the golden fixtures pin, for this site.

Each project defines the same names so `capture_fixtures.py`,
`test_golden_parse.py` and `site_check.py` stay identical across the three
scrapers; only this adapter differs.
"""

from src.scraper import (  # noqa: E402
    _check_error_page,
    _extract_season_languages,
    _extract_season_links,
    _extract_title,
    _is_logged_in,
    _login_url,
    _parse_episodes,
    make_doc,
)

SCRAPER_CLASS_NAME = "BsToScraper"
SLUG_RE = r"/serie/([^/?#]+)"
SERIES_PATH = "/serie/{slug}"
CATALOGUE_PATH = "/andere-serien"

# ── Live site check (tests/site_check.py) ──────────────────────────────────
# The login form fields _login_client posts. It sends security_token even
# when the page has none, so a missing token field is a login about to fail.
LOGIN_FIELDS = ("login[user]", "login[pass]", "security_token")
# Read by config.config; set as repository secrets for the monthly workflow.
CREDENTIAL_VARS = ("BS_USERNAME", "BS_PASSWORD")
# Long-running series that should outlive any one check. A slug that has
# gone is skipped, and series linked from the home page are tried after these.
PROBE_SLUGS = ("Breaking-Bad", "Better-Call-Saul", "Die-Simpsons")
# The index held 10,628 series in September 2026. Far below that, the
# catalogue parse is losing series rather than the site shrinking.
MIN_CATALOGUE = 5000
# bs.to has no subscribe or watchlist buttons to read.
HAS_ACCOUNT_BUTTONS = False


def login_url(site_url: str) -> str:
    return _login_url(site_url)


def parse_all(html: str, slug: str, base_url: str) -> dict:
    """Run every parser this scraper applies to a page, as a plain dict.

    One tree now serves all six, where the series-page readers used to need a
    BeautifulSoup of their own. The recorded golden file predates that move
    and was left untouched across it, so these tests re-parse every captured
    page with the lxml readers and compare against what the soup ones
    produced.
    """
    doc = make_doc(html)
    return {
        "is_logged_in": _is_logged_in(doc),
        "error_page": _check_error_page(doc),
        "title": _extract_title(doc),
        "season_languages": _extract_season_languages(doc),
        "season_links": [list(x) for x in _extract_season_links(doc, base_url)],
        "episodes": _parse_episodes(doc),
    }
