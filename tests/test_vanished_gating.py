"""Which vanished-entry decision _run_scrape_and_save offers, and when.

Two things can happen to the startup mismatch report at the end of a run:

  * a catalogued run has already put every one of its entries through
    show_vanished_series' decision table, so asking again asks the same
    question twice -- it must stay quiet;
  * a run that fetched no catalogue at all has no evidence to delete on, so
    it may only report what is flagged.

bs.to tracks no subscription or watchlist state, so its scope is always
"all" or "new_only" -- both of which prompt in the table above. The
report-driven prompt is therefore dormant here; it is kept because the
sibling scrapers reach it through their account scopes. That dormancy is
pinned below rather than left as a comment, so a future account scope that
re-activates the branch shows up as a failing test rather than a surprise.

The assertions are on which branch the function chose, not on what the
collaborators did with it -- that is control flow this function owns, so
stubbing the collaborators does not hollow the test out.

Run with:  python -m unittest discover -s tests
"""

import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main  # noqa: E402

SLUG_PREFIX = "/serie"
HOST = "https://burningseries.ac"


def entry(slug):
    """A catalogue/scrape row, carrying only the fields the gating reads."""
    return {"title": slug, "link": f"{SLUG_PREFIX}/{slug}", "url": f"{HOST}{SLUG_PREFIX}/{slug}"}


class FakeScraper:
    """The whole surface _run_scrape_and_save touches."""

    def __init__(self, series_data, all_discovered_series):
        self.series_data = series_data
        self.all_discovered_series = all_discovered_series
        self.failed_links = []
        self.paused = False
        self.site_url = HOST
        self.run_kwargs = None

    def run(self, **kwargs):
        self.run_kwargs = kwargs

    def clear_checkpoint(self):
        pass

    def get_series_slug_from_url(self, url):
        if not url:
            return "unknown"
        return url.rstrip("/").split("/")[-1] or "unknown"


class FakeIndexManager:
    def __init__(self, _path=None):
        self.series_index = {}

    def load_index(self):
        pass


class _GatingTest(unittest.TestCase):
    def setUp(self):
        self.scraped = [entry("alpha")]
        self.catalogue = [entry("alpha"), entry("beta")]

        self.show_vanished = mock.MagicMock(return_value=[])
        self.prompt_clean = mock.MagicMock(return_value=False)
        self.notify = mock.MagicMock()

        patches = {
            "IndexManager": FakeIndexManager,
            "show_vanished_series": self.show_vanished,
            "_prompt_clean_vanished": self.prompt_clean,
            "_notify_vanished_at_startup": self.notify,
            # bs.to's confirm step returns (saved, changes), not a single value.
            "confirm_and_save_changes": mock.MagicMock(return_value=(True, {})),
            "print_scraped_series_status": mock.MagicMock(),
            "ACTIVE_SITE_URL": HOST,
        }
        for name, value in patches.items():
            patcher = mock.patch.object(main, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def run_scrape(self, *, catalogue=True, run_kwargs=None):
        """Drive one run and return the fake scraper it used."""
        scraper = FakeScraper(self.scraped, self.catalogue if catalogue else None)
        with mock.patch.object(main, "BsToScraper", lambda: scraper), redirect_stdout(StringIO()):
            main._run_scrape_and_save(
                run_kwargs=run_kwargs or {},
                description="test run",
                success_msg="done",
                no_data_msg="nothing",
            )
        return scraper


class FullCatalogueRunTests(_GatingTest):
    """scope "all": the scrape-time table already asked. Do not ask again."""

    def test_the_scrape_time_table_runs(self):
        self.run_scrape()
        self.show_vanished.assert_called_once()

    def test_the_report_driven_prompt_does_not_run_again(self):
        """The whole point: one vanished decision per run, not two."""
        self.run_scrape()
        self.prompt_clean.assert_not_called()

    def test_no_notification_either(self):
        self.run_scrape()
        self.notify.assert_not_called()

    def test_the_table_is_given_the_catalogue_not_the_scraped_subset(self):
        """Anything indexed but absent from the catalogue is what "vanished" means."""
        self.run_scrape()
        self.assertEqual(self.show_vanished.call_args.args[1], {"alpha", "beta"})

    def test_the_scope_is_all(self):
        self.run_scrape()
        self.assertEqual(self.show_vanished.call_args.args[2], "all")


class NewOnlyRunTests(_GatingTest):
    """scope "new_only" prompts in the same table, so it is equally covered."""

    def test_the_scope_follows_the_run_kind(self):
        self.run_scrape(run_kwargs={"new_only": True})
        self.assertEqual(self.show_vanished.call_args.args[2], "new_only")

    def test_the_report_driven_prompt_does_not_run_again(self):
        self.run_scrape(run_kwargs={"new_only": True})
        self.prompt_clean.assert_not_called()


class TargetedRunTests(_GatingTest):
    """No catalogue was fetched, so there is no evidence to delete on."""

    def test_the_scrape_time_table_is_skipped(self):
        self.run_scrape(catalogue=False)
        self.show_vanished.assert_not_called()

    def test_the_user_is_notified_rather_than_prompted(self):
        self.run_scrape(catalogue=False, run_kwargs={"url_list": [f"{HOST}{SLUG_PREFIX}/alpha"]})
        self.notify.assert_called_once()
        self.prompt_clean.assert_not_called()

    def test_what_the_run_just_scraped_counts_as_alive(self):
        """A freshly scraped entry must not be reported vanished by a stale report."""
        self.run_scrape(catalogue=False)
        self.assertEqual(self.notify.call_args.kwargs["seen_slugs"], {"alpha"})


class DormantPromptTests(_GatingTest):
    """bs.to has no account scope, so the report-driven prompt never fires.

    If bs.to ever gains a scope whose table is informational only, this test
    starts failing -- which is the point. It says "currently unreachable",
    not "must stay unreachable".
    """

    def test_no_run_shape_reaches_the_report_driven_prompt(self):
        for kwargs in ({}, {"new_only": True}, {"url_list": ["x"]}, {"retry_failed": True}):
            for catalogue in (True, False):
                with self.subTest(run_kwargs=kwargs, catalogue=catalogue):
                    self.prompt_clean.reset_mock()
                    self.run_scrape(catalogue=catalogue, run_kwargs=kwargs)
                    self.prompt_clean.assert_not_called()


if __name__ == "__main__":
    unittest.main()
