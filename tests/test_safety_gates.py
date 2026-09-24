"""The decisions themselves: which answer opens which gate, and what gets removed.

Most suites mock the approval prompts away to test what happens *after* a
decision. That left the prompts untested, and they are where a wrong mapping
would do the most damage: one mixed-up key and an answer of "n" to deleting
episodes becomes a yes. The same goes for the catalogue parse, which decides
what a new-only scrape treats as new, and for the prompts that delete entries.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import main
from src import index_manager as im
from src.scraper import BsToScraper
from tests._support import FakeResponse, captured_output, series, write_index

ALL_GATES = ("new_series", "new_episodes", "watched", "unwatched", "episode_remove", "season_remove")

# change category -> (sample entry, words from its prompt, gate it opens)
CATEGORIES = {
    "new_series": ("A", "Add these new series", "new_series"),
    "new_episodes": (("A", "Season 1", 1), "Add these new episodes", "new_episodes"),
    "newly_watched": (("A", "Season 1", 1), "marked as WATCHED", "watched"),
    "newly_unwatched": (("A", "Season 1", 1), "marked as UNWATCHED", "unwatched"),
    "removed_episodes": (("A", "Season 1", 1), "DELETE these episodes", "episode_remove"),
    "removed_seasons": (("A", "Season 1"), "DELETE these whole seasons", "season_remove"),
}


def _changes(*present):
    changes = {category: [] for category in CATEGORIES}
    for category in present:
        changes[category] = [CATEGORIES[category][0]]
    return changes


class _Answers:
    """Answer "y" to prompts containing any of *yes_to*, and *default* otherwise."""

    def __init__(self, *yes_to, default="n"):
        self.yes_to = yes_to
        self.default = default
        self.asked: list[str] = []

    def __call__(self, prompt=""):
        self.asked.append(prompt)
        return "y" if any(words in prompt for words in self.yes_to) else self.default


def _confirm(changes, answers):
    with mock.patch("builtins.input", answers), captured_output():
        return im._prompt_watch_status_changes(changes, {"A": series("A")})


class ChangeConfirmationTests(unittest.TestCase):
    def test_yes_opens_exactly_its_own_gate(self):
        for category, (_sample, words, gate) in CATEGORIES.items():
            with self.subTest(category):
                allowed = _confirm(_changes(category), _Answers(words))
                self.assertTrue(allowed[gate])
                self.assertEqual([g for g in ALL_GATES if allowed[g]], [gate])

    def test_anything_but_yes_keeps_every_gate_closed(self):
        # "j" is the German yes; it is not accepted, and that is the safe way
        # round -- a refused change is simply offered again next scrape.
        for answer in ("", "n", "no", "j", "ja", "yes please"):
            with self.subTest(answer=answer):
                allowed = _confirm(_changes(*CATEGORIES), _Answers(default=answer))
                self.assertEqual([g for g in ALL_GATES if allowed[g]], [])

    def test_a_declined_new_series_is_not_asked_about_again(self):
        answers = _Answers()
        _confirm(_changes("new_series", "newly_watched"), answers)
        self.assertTrue(any("new series" in p for p in answers.asked))
        self.assertFalse(any("WATCHED" in p for p in answers.asked), "watch state of a refused series was asked")


def _mismatch(severity, title="A"):
    return {
        "title": title,
        "severity": severity,
        "issues": [{"type": "total_episode_count", "old": 12, "new": 10, "diff": -2, "percent_diff": -16.7}],
    }


class EpisodeMismatchDialogTests(unittest.TestCase):
    """The integrity dialog, including the path that deletes and re-scrapes."""

    def setUp(self):
        # The dialog appends to logs/integrity_check.log next to the package;
        # point that at a temp dir so tests never write into the real log.
        tmp = tempfile.mkdtemp()
        patcher = mock.patch.object(im, "__file__", str(Path(tmp) / "src" / "index_manager.py"))
        patcher.start()
        self.addCleanup(patcher.stop)
        entry = series("A", slug="A")
        entry["url"] = entry["link"] = "/serie/A"
        self.old_data = {"A": entry}

    def _run(self, mismatches, *answers):
        with mock.patch("builtins.input", side_effect=list(answers)), captured_output():
            return im._prompt_episode_mismatches(mismatches, self.old_data, active_site_url="https://mirror.example/")

    def test_info_only_is_approved_without_asking(self):
        self.assertEqual(self._run([_mismatch("info")]), (True, None))

    def test_warnings_need_an_explicit_yes(self):
        self.assertEqual(self._run([_mismatch("warning")], "y"), (True, None))
        for answer in ("", "n", "j"):
            with self.subTest(answer=answer):
                self.assertEqual(self._run([_mismatch("warning")], answer), (False, None))

    def test_cancel_discards_the_merge(self):
        self.assertEqual(self._run([_mismatch("critical")], "3"), (False, None))

    def test_rescrape_hands_back_the_entry_and_an_absolute_url(self):
        proceed, data = self._run([_mismatch("critical")], "2")
        self.assertFalse(proceed)
        assert data is not None
        self.assertEqual(data["urls"], ["https://mirror.example/serie/A"])
        self.assertEqual(data["titles"], ["A"])
        self.assertIs(data["series"]["A"], self.old_data["A"])

    def test_rescrape_of_an_entry_that_cannot_be_found_gives_up(self):
        self.assertEqual(self._run([_mismatch("critical", title="Unknown")], "2"), (False, None))

    def test_enter_proceeds_and_leaves_every_deletion_to_its_own_prompt(self):
        # Proceeding is not destructive: the merge still asks separately before
        # removing any episode or season, and those prompts default to keep.
        self.assertEqual(self._run([_mismatch("critical")], ""), (True, None))


def _index_with(*entries):
    path = write_index(list(entries))
    return path, im.IndexManager(path)


def _titles_on_disk(path):
    with open(path, encoding="utf-8") as f:
        return sorted(e["title"] for e in json.load(f))


class DuplicateSlugPromptTests(unittest.TestCase):
    """main._remove_duplicate_index_entries deletes; only what the user picked may go."""

    def setUp(self):
        self.path, self.manager = _index_with(
            series("Old Name", slug="Shared", watched=5),
            series("New Name", slug="Shared"),
            series("Other", slug="Other", watched=2),
        )

    def _resolve(self, *answers):
        _slugs, duplicates, _missing = main._collect_index_slugs(self.manager)
        with mock.patch("builtins.input", side_effect=list(answers)), captured_output():
            main._remove_duplicate_index_entries(self.manager, duplicates)

    def test_keeping_one_copy_removes_only_its_siblings(self):
        self._resolve("2")  # listed alphabetically: 1 = New Name, 2 = Old Name
        self.assertEqual(_titles_on_disk(self.path), ["Old Name", "Other"])

    def test_skip_abort_and_unknown_answers_keep_every_copy(self):
        for answer in ("", "s", "a", "3", "x"):
            with self.subTest(answer=answer):
                self._resolve(answer)
                self.assertEqual(_titles_on_disk(self.path), ["New Name", "Old Name", "Other"])

    def test_abort_stops_before_later_slugs(self):
        path, manager = _index_with(
            series("A1", slug="A"), series("A2", slug="A"), series("B1", slug="B"), series("B2", slug="B")
        )
        _slugs, duplicates, _missing = main._collect_index_slugs(manager)
        with mock.patch("builtins.input", side_effect=["a", "1"]), captured_output():
            main._remove_duplicate_index_entries(manager, duplicates)
        self.assertEqual(_titles_on_disk(path), ["A1", "A2", "B1", "B2"])


CATALOGUE = """<html><body><section class="navigation"><a href="logout">Logout</a></section><ul>
<li><a href="serie/Waldern">Wäldern</a></li>
<li><a href="serie/Wldern">Wäldern</a></li>
<li><a href="serie/Some-Show">Some Show</a></li>
<li><a href="serie/some-show">Some Show</a></li>
<li><a href="serie/Neue-Serien">Neue Serien</a></li>
<li><a href="andere-seite">Other Page</a></li>
</ul></body></html>"""


def _parse_catalogue(html):
    scraper = BsToScraper()

    async def fake_get(client, url, *args, **kwargs):
        return FakeResponse(200, html)

    scraper._get = fake_get  # type: ignore[method-assign]
    return asyncio.run(scraper._get_all_series(object()))  # type: ignore[arg-type]


class CatalogueParseTests(unittest.TestCase):
    """_get_all_series decides what a new-only scrape treats as new."""

    def setUp(self):
        self.catalogue = _parse_catalogue(CATALOGUE)
        self.links = [s["link"] for s in self.catalogue]

    def test_two_series_sharing_a_title_are_both_listed(self):
        self.assertIn("/serie/Waldern", self.links)
        self.assertIn("/serie/Wldern", self.links)

    def test_one_series_spelled_two_ways_is_listed_once(self):
        self.assertEqual([link for link in self.links if "how" in link], ["/serie/Some-Show"])

    def test_navigation_links_are_not_series(self):
        self.assertEqual(len(self.catalogue), 3)

    def test_a_logged_out_page_is_refused_rather_than_read_as_empty(self):
        with self.assertRaises(RuntimeError):
            _parse_catalogue(CATALOGUE.replace('<a href="logout">Logout</a>', ""))


if __name__ == "__main__":
    unittest.main()
