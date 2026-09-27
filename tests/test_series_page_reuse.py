"""The series page is read as the season it embeds, not fetched a second time.

Every series page already carries one season's full episode table and its
language dropdown. Live, on 300 random series (#4), both were identical to
that season's own page, episode for episode including the watched flags, 300
times out of 300 -- so fetching that season again was one request in every
3.00 a series cost.

What these tests pin: the result is exactly what fetching every season gives,
languages included; the season is read off the page's episode links, never
assumed to be the first (47 of 300 real series list season 0 first); and
anything short of certainty -- no marker, a marker naming several seasons or
none, no table, an empty one, or a table that fails the season-page login
check -- fetches every season as before. Failures and re-logins behave as
they always did.
"""

from __future__ import annotations

import asyncio
import unittest
from unittest import mock

import src.scraper as sc

SCRAPER_CLS = sc.BsToScraper
BASE = "https://burningseries.ac"
SLUG = "test-series"
SERIES_URL = f"{BASE}/serie/{SLUG}"

# Season 0 is listed first, as on 47 of 300 real series; the page shows season 1.
SEASONS = {"0": (False,), "1": (True, False, True), "2": (False, False)}
LANGUAGES = {"0": ("de",), "1": ("de", "des"), "2": ("ens",)}


def season_url(season: str) -> str:
    return f"{SERIES_URL}/{season}"


def episode_href(season: str, n: int) -> str:
    return f"serie/{SLUG}/{season}/{1000 + n}-Episode-{n}/de"


def table(watched: tuple[bool, ...], links_to: str | None, languages=("de",)) -> str:
    """An episode table linking to episodes of season `links_to` (None: no links), with its language dropdown."""
    rows = []
    for n, seen in enumerate(watched, start=1):
        title = f"Episode {n}"
        if links_to is not None:
            title = f'<a href="{episode_href(links_to, n)}">{title}</a>'
        rows.append(
            f"<tr{' class=watched' if seen else ''}>"
            f'<th class="episode-number-cell">{n}</th><td class="episode-title-ger">{title}</td></tr>'
        )
    options = "".join(f'<li class="option" data-value="{code}"></li>' for code in languages)
    return f'<div class="series-language"><ul>{options}</ul></div><table class="episodes">{"".join(rows)}</table>'


def page(body: str, account: str = "alice", logged_in: bool = True) -> str:
    nav = f'<section class="navigation"><a href="logout">Logout</a><strong>{account}</strong></section>'
    return f"<html><body>{nav if logged_in else ''}{body}</body></html>"


def series_page(embedded: str, seasons=tuple(SEASONS), **kwargs) -> str:
    nav = "".join(f'<li><a href="serie/{SLUG}/{n}">{n}</a></li>' for n in seasons)
    return page(f'<h2>Test Series</h2><div id="seasons"><ul>{nav}</ul></div>{embedded}', **kwargs)


def shown(season: str, **kwargs) -> str:
    """The series page's embedded table for `season`, exactly as its own page renders it."""
    return table(SEASONS[season], links_to=season, languages=LANGUAGES[season], **kwargs)


def season_pages(seasons=None, languages=None) -> dict:
    seasons, languages = seasons or SEASONS, languages or LANGUAGES
    return {n: page(table(w, links_to=n, languages=languages.get(n, ("de",)))) for n, w in seasons.items()}


class _Response:
    def __init__(self, text: str) -> None:
        self.text = text
        self.status_code = 200
        self.headers: dict = {}


class _Client:
    """Serves the series page and each season's page (a body may be an exception), counting requests."""

    def __init__(self, series: str, seasons: dict) -> None:
        self.series = series
        self.seasons = seasons
        self.requests: dict[str, int] = {}

    async def get(self, url, **_kwargs):
        self.requests[url] = self.requests.get(url, 0) + 1
        if url == SERIES_URL:
            return _Response(self.series)
        body = self.seasons[url.rstrip("/").rsplit("/", 1)[1]]
        if isinstance(body, BaseException):
            raise body
        return _Response(body)

    def fetched(self, season: str) -> int:
        return self.requests.get(season_url(season), 0)


def _scrape(client, account=None, reuse=True, relogin=None):
    scraper = SCRAPER_CLS()
    scraper._account_name = account
    if relogin is not None:
        scraper._relogin_shared_client = relogin  # type: ignore[method-assign]
    info = {"url": SERIES_URL, "link": f"/serie/{SLUG}", "title": "Test Series"}
    with (
        mock.patch.object(sc, "_ANON_REREAD_DELAY", (0.0, 0.0)),
        mock.patch.object(sc, "_shown_season", sc._shown_season if reuse else lambda *_: None),
    ):
        result = asyncio.run(scraper._scrape_one_series(client, info))  # type: ignore[arg-type]
    result.pop("scrape_duration_seconds", None)
    return result


class TestTheEmbeddedSeasonIsNotFetchedAgain(unittest.TestCase):
    def test_the_result_is_identical_to_fetching_every_season(self):
        series = series_page(shown("1"))
        reused = _Client(series, season_pages())
        fetched = _Client(series, season_pages())
        result = _scrape(reused)
        self.assertFalse(result.get("_error"), result)
        self.assertEqual(result, _scrape(fetched, reuse=False))
        self.assertEqual(sum(reused.requests.values()), sum(fetched.requests.values()) - 1)
        self.assertEqual(reused.fetched("1"), 0)

    def test_the_languages_are_the_ones_the_season_page_shows(self):
        result = _scrape(_Client(series_page(shown("1")), season_pages()))
        self.assertEqual(
            [s["languages"] for s in result["seasons"]],
            [["german_dub"], ["german_dub", "german_sub"], ["english_sub"]],
        )

    def test_season_0_listed_first_still_matches_season_1(self):
        client = _Client(series_page(shown("1")), season_pages())
        result = _scrape(client)
        self.assertEqual([s["season"] for s in result["seasons"]], ["0", "1", "2"])
        self.assertEqual([client.fetched(n) for n in SEASONS], [1, 0, 1])
        one = result["seasons"][1]
        self.assertEqual(one["url"], season_url("1"), "the stored url stays the season link")
        self.assertEqual(one["watched_episodes"], 2)

    def test_whichever_season_the_page_shows_is_the_one_skipped(self):
        for season in ("0", "2"):
            with self.subTest(shown=season):
                client = _Client(series_page(shown(season)), season_pages())
                result = _scrape(client)
                self.assertEqual([client.fetched(n) for n in SEASONS], [int(n != season) for n in SEASONS])
                self.assertEqual(result, _scrape(_Client(client.series, season_pages()), reuse=False))

    def test_identical_seasons_are_told_apart_by_the_links(self):
        twins = {"1": (True, False), "2": (True, False)}
        same = {"1": ("de",), "2": ("de",)}
        client = _Client(series_page(table(twins["2"], links_to="2"), seasons=twins), season_pages(twins, same))
        result = _scrape(client)
        self.assertEqual((client.fetched("1"), client.fetched("2")), (1, 0))
        self.assertEqual(result, _scrape(_Client(client.series, season_pages(twins, same)), reuse=False))

    def test_a_single_season_series_costs_only_the_series_page(self):
        one = {"1": (True, True)}
        client = _Client(series_page(table(one["1"], links_to="1"), seasons=one), season_pages(one))
        result = _scrape(client)
        self.assertEqual(client.requests, {SERIES_URL: 1})
        self.assertEqual(result["watched_episodes"], 2)


class TestAnythingUnsureFetchesEverySeason(unittest.TestCase):
    def assert_fetched_every_season(self, client, account=None):
        result = _scrape(client, account=account)
        self.assertFalse(result.get("_error"), result)
        self.assertEqual([client.fetched(n) for n in SEASONS], [1] * len(SEASONS))
        self.assertEqual(result["watched_episodes"], 2, "the season pages' own data is what gets stored")

    def test_no_episode_links(self):
        self.assert_fetched_every_season(_Client(series_page(table(SEASONS["1"], links_to=None)), season_pages()))

    def test_links_naming_two_seasons(self):
        mixed = shown("1") + f'<a href="{episode_href("2", 1)}">next</a>'
        self.assert_fetched_every_season(_Client(series_page(mixed), season_pages()))

    def test_links_naming_a_season_the_nav_does_not_list(self):
        self.assert_fetched_every_season(_Client(series_page(table(SEASONS["1"], links_to="9")), season_pages()))

    def test_a_table_that_fails_the_season_login_check(self):
        # The series page reads as logged in, but greets someone else: every
        # season page would be refused for that, so the series page's table
        # is refused too.
        series = series_page(table((False, False, False), links_to="1"), account="bob")
        self.assert_fetched_every_season(_Client(series, season_pages()), account="alice")

    def test_that_table_is_refused_up_front_not_left_to_the_logged_out_screen(self):
        # The screen after the fetch would catch it too, but only through a
        # re-read with a 1-2 s pause; the table must never get that far.
        series = series_page(table((False, False, False), links_to="1"), account="bob")
        client = _Client(series, season_pages())
        scraper = SCRAPER_CLS()
        scraper._account_name = "alice"
        doc = sc.make_doc(series)
        links = sc._extract_season_links(doc, SERIES_URL)
        pages = asyncio.run(scraper._read_season_pages(client, doc, links))
        self.assertEqual([client.fetched(n) for n in SEASONS], [1] * len(SEASONS))
        self.assertFalse(scraper._any_season_logged_out(pages))

    def test_an_empty_table(self):
        empty = '<table class="episodes"></table>' + f'<a href="{episode_href("1", 1)}">x</a>'
        self.assert_fetched_every_season(_Client(series_page(empty), season_pages()))

    def test_no_table_at_all(self):
        marker_only = f'<a href="{episode_href("1", 1)}">continue</a>'
        self.assert_fetched_every_season(_Client(series_page(marker_only), season_pages()))


class TestFailuresAndReloginsAreUnchanged(unittest.TestCase):
    def test_a_failed_fetch_of_another_season_still_names_that_season(self):
        seasons = season_pages()
        seasons["0"] = RuntimeError("connection dropped")
        result = _scrape(_Client(series_page(shown("1")), seasons))
        self.assertTrue(result.get("_error"))
        self.assertIn("season 0 fetch failed", result["_error_reason"])

    def test_a_relogin_refetches_every_season_including_the_reused_one(self):
        seasons = season_pages()
        seasons["2"] = page(table(SEASONS["2"], links_to="2"), logged_in=False)
        client = _Client(series_page(shown("1")), seasons)

        async def relogin(_client):
            client.seasons["1"] = page(table((True, True, True), links_to="1"))
            client.seasons["2"] = page(table(SEASONS["2"], links_to="2"))
            return True

        result = _scrape(client, relogin=relogin)
        self.assertFalse(result.get("_error"), result)
        self.assertEqual(client.fetched("1"), 1, "fetched once, by the refetch after the re-login")
        self.assertEqual(client.fetched("2"), 3, "first fetch, the no-login re-read, the refetch")
        self.assertEqual(result["seasons"][1]["watched_episodes"], 3, "the refetched season 1 is what gets stored")


class TestShownSeason(unittest.TestCase):
    LINKS = [("0", season_url("0")), ("1", season_url("1")), ("11", season_url("11") + "/de")]

    def shown(self, hrefs: list[str], links=None):
        doc = sc.make_doc("<html><body>" + "".join(f'<a href="{h}">x</a>' for h in hrefs) + "</body></html>")
        return sc._shown_season(doc, links or self.LINKS)

    def test_season_1_is_not_read_as_season_11_or_back(self):
        self.assertEqual(self.shown([episode_href("1", 3)]), 1)
        self.assertEqual(self.shown([episode_href("11", 3)]), 2, "a season link with a language suffix still counts")

    def test_rooted_links_count_the_same(self):
        self.assertEqual(self.shown(["/" + episode_href("0", 1)]), 0)

    def test_season_links_that_are_not_episode_links_are_ignored(self):
        self.assertEqual(self.shown([f"serie/{SLUG}/11", f"serie/{SLUG}/2/de", episode_href("1", 1)]), 1)

    def test_unsure_is_none(self):
        self.assertIsNone(self.shown([]))
        self.assertIsNone(self.shown([episode_href("1", 1), episode_href("0", 1)]))
        self.assertIsNone(self.shown([episode_href("5", 1)]))
        twice = [("1", season_url("1")), ("1", season_url("1") + "/en")]
        self.assertIsNone(self.shown([episode_href("1", 1)], links=twice), "two links for one season")


if __name__ == "__main__":
    unittest.main()
