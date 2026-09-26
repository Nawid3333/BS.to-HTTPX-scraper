"""Choosing "delete & rescrape" must not discard the confirmations already given.

The integrity check runs at the very end of confirm_and_save_changes, after
the user has answered every approval prompt for the whole run. When one series
trips a critical mismatch and the user picks "delete index & rescrape", that
decision concerns *that series only* -- every other approval in the run (new
episodes, newly watched, removed seasons) was still given and still belongs in
the index.

Observed on the sibling s.to project: a full scrape read a series as 12/12
watched, the user approved it, another series tripped the critical check, the
user chose to rescrape it -- and the approved series stayed at 7/12. The next
two scrapes re-detected and re-prompted for the same change, because it had
never been saved. This project returned the rescrape request the same way, so
it carried the same defect.

Run with:  python -m unittest discover -s tests
"""

import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import index_manager as im  # noqa: E402


def _series(title, season, watched_flags):
    """One index entry with a single season and the given watched flags."""
    episodes = [
        {"number": i + 1, "watched": w, "title_ger": f"E{i + 1}", "title_eng": ""} for i, w in enumerate(watched_flags)
    ]
    watched = sum(1 for w in watched_flags if w)
    return {
        "url": f"https://burningseries.ac/serie/{title}",
        "link": f"/serie/{title}",
        "title": title,
        "title_ger": title,
        "title_eng": "",
        "subscribed": True,
        "watchlist": False,
        "total_seasons": 1,
        "total_episodes": len(episodes),
        "watched_episodes": watched,
        "unwatched_episodes": len(episodes) - watched,
        "seasons": [
            {
                "season": season,
                "url": f"https://burningseries.ac/serie/{title}/{season}",
                "episodes": episodes,
                "watched_episodes": watched,
                "total_episodes": len(episodes),
            }
        ],
    }


ALLOW_EVERYTHING = {
    "new_series": True,
    "new_episodes": True,
    "watched": True,
    "unwatched": True,
    "subscribe": True,
    "unsubscribe": True,
    "watchlist_add": True,
    "watchlist_remove": True,
    "title_ger": True,
    "title_eng": True,
    "episode_remove": True,
    "season_remove": True,
}

RESCRAPE = {
    "urls": ["https://burningseries.ac/serie/Critical"],
    "titles": ["Critical"],
    "series": {"Critical": _series("Critical", 1, [True, True])},
}


class TestApprovalsSurviveCriticalRescrape(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.index_file = str(Path(self.dir.name) / "series_index.json")

        # "Kept" is an ordinary series the user approves a watch change for.
        # "Critical" loses an episode, which is what trips the integrity check.
        self.manager = im.IndexManager(self.index_file)
        self.manager.series_index = {
            "Kept": _series("Kept", 1, [True] * 7 + [False] * 5),
            "Critical": _series("Critical", 1, [True, True]),
        }
        self.manager.save_index()

        self.new_data = {
            "Kept": _series("Kept", 1, [True] * 12),
            "Critical": _series("Critical", 1, [True]),
        }

    def _saved(self, title):
        reloaded = im.IndexManager(self.index_file)
        return reloaded.series_index[title]

    def _run(self, answer="y"):
        """Approve everything, then choose 'delete & rescrape' for Critical."""
        manager = im.IndexManager(self.index_file)
        with (
            mock.patch.object(im, "_prompt_watch_status_changes", return_value=dict(ALLOW_EVERYTHING)),
            mock.patch.object(im, "_prompt_episode_mismatches", return_value=(False, RESCRAPE)),
            mock.patch("builtins.input", return_value=answer),
        ):
            return im.confirm_and_save_changes(self.new_data, "test run", index_manager=manager)

    def test_rescrape_is_still_requested(self):
        action, _ = self._run()
        self.assertIsInstance(action, dict)
        self.assertEqual(action.get("action"), "rescrape")
        self.assertEqual(action["titles"], ["Critical"])
        # main.py deletes by these entries, so they must be handed back too.
        self.assertEqual([s["title"] for s in action["series"]], ["Critical"])

    def test_approved_watch_change_is_saved(self):
        """The regression: approvals for every other series must persist."""
        self._run()
        self.assertEqual(
            self._saved("Kept")["watched_episodes"],
            12,
            "approved watch change was discarded when a critical rescrape was chosen",
        )

    def test_declining_the_save_also_cancels_the_rescrape(self):
        """Declining the final save must not still delete and rescrape.

        main.py acts on the returned action by deleting those series from the
        index. Handing it back after the user answered "n" to "Save these
        changes?" would destroy data on the strength of a prompt they had just
        refused, so the refusal has to cancel both halves.
        """
        saved, _ = self._run(answer="n")
        self.assertIs(saved, False)
        self.assertEqual(self._saved("Kept")["watched_episodes"], 7)


class TestRescrapeListsStayInStep(unittest.TestCase):
    """main.py zips "urls" and "titles" into the retry list, pair by pair.

    A critical series the index holds no URL for used to add its title but no
    URL, so every title after it landed on the wrong series' URL.
    """

    def test_a_series_without_a_url_is_left_out_of_both_lists(self):
        old = {
            "No Url": {"title": "No Url"},
            "Has Url": {"title": "Has Url", "url": "https://example.test/x/has-url"},
        }
        mismatches = [{"title": "No Url", "severity": "critical"}, {"title": "Has Url", "severity": "critical"}]
        with self.assertLogs(im.logger, "WARNING"):
            result = im._extract_critical_series_for_rescrape(mismatches, old)
        self.assertEqual(result["titles"], ["Has Url"])
        self.assertEqual(result["urls"], ["https://example.test/x/has-url"])
        self.assertEqual(list(result["series"]), ["Has Url"])


if __name__ == "__main__":
    unittest.main()
