"""Which parser outputs the golden fixtures pin, for this site.

Each project defines the same two names so `capture_fixtures.py` and
`test_golden_parse.py` stay identical across the three scrapers; only this
adapter differs.
"""

from src.scraper import (  # noqa: E402
    _check_error_page,
    _extract_season_languages,
    _extract_season_links,
    _extract_title,
    _is_logged_in,
    _parse_episodes,
    make_doc,
    make_soup,
)

SCRAPER_CLASS_NAME = "BsToScraper"
SLUG_RE = r"/serie/([^/?#]+)"
SERIES_PATH = "/serie/{slug}"
CATALOGUE_PATH = "/andere-serien"


def parse_all(html: str, slug: str, base_url: str) -> dict:
    """Run every parser this scraper applies to a page, as a plain dict."""
    soup = make_soup(html)
    doc = make_doc(html)
    return {
        "is_logged_in": _is_logged_in(soup),
        "error_page": _check_error_page(soup),
        "title": _extract_title(soup),
        "season_languages": _extract_season_languages(doc),
        "season_links": [list(x) for x in _extract_season_links(soup, base_url)],
        "episodes": _parse_episodes(doc),
    }
