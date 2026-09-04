"""Regression test for duplicate lines in a batch URL file.

A batch file names series, not pages. "/serie/one-piece" and
"/serie/one-piece/staffel-23" are two ways of writing one line item,
and normalize_to_series_url turns both into the same series URL. Before this
was collapsed the series was fetched twice, printed twice in the progress
output, and merged twice -- visible in a real run as the same title appearing
under two different indices.

Run with:  python -m unittest discover -s tests
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import src.scraper as sc  # noqa: E402

SCRAPER_CLS = sc.BsToScraper


class BatchUrlDedupTests(unittest.TestCase):
    """One entry per series, whatever spelling the file used."""

    def setUp(self):
        self.scraper = SCRAPER_CLS()

    def _links(self, urls):
        return [entry["link"] for entry in self.scraper._series_list_from_urls(urls)]

    def test_a_season_url_does_not_re_add_a_series_already_listed(self):
        links = self._links(["/serie/one-piece", "/serie/one-piece/staffel-23"])
        self.assertEqual(links, ["/serie/one-piece"])

    def test_an_episode_url_does_not_re_add_a_series_already_listed(self):
        links = self._links(["/serie/one-piece", "/serie/one-piece/staffel-23/episode-4"])
        self.assertEqual(links, ["/serie/one-piece"])

    def test_two_spellings_of_one_slug_are_one_entry(self):
        """Case and percent-encoding are spellings, not identities."""
        links = self._links(["/serie/one-piece", "/serie/One-Piece"])
        self.assertEqual(links, ["/serie/one-piece"])

    def test_the_first_line_wins_so_file_order_is_preserved(self):
        links = self._links(["/serie/one-piece/staffel-23", "/serie/one-piece", "/serie/dark-matter"])
        self.assertEqual(links, ["/serie/one-piece", "/serie/dark-matter"])

    def test_distinct_series_are_all_kept(self):
        urls = ["/serie/one-piece", "/serie/dark-matter", "/serie/scrubs"]
        self.assertEqual(self._links(urls), urls)

    def test_an_empty_batch_yields_nothing(self):
        self.assertEqual(self._links([]), [])


if __name__ == "__main__":
    unittest.main()
