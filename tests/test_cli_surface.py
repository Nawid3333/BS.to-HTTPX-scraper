"""The menu-level guards in main.py: checkpoints, credentials, disk, slugs.

These decide whether a run starts at all and whether it resumes, so a wrong
answer here either destroys a checkpoint the user wanted or silently resumes
one they did not. They are also pure decision logic once ``input()`` is
scripted, which makes them cheap to cover -- they were simply never reached
by a test before.

Style note for future edits
---------------------------
``scripted_input`` answers prompts in order and returns ``default`` once it
runs out. Set ``default`` to the *safe* answer ("n") so a prompt added later
cannot make an old test silently start approving something.
"""

from __future__ import annotations

import json
import os
import re

import pytest

import main
from tests._support import captured_output, scripted_input, series


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    """Point main.py and the scraper class at a throwaway data directory."""
    monkeypatch.setattr(main, "DATA_DIR", str(tmp_path))
    return tmp_path


def _write_checkpoint(directory, mode: str) -> str:
    path = os.path.join(str(directory), ".scrape_checkpoint.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"completed_links": ["/serie/a"], "mode": mode}, fh)
    return path


class TestCheckpointPrompt:
    def test_no_checkpoint_starts_a_fresh_run_without_asking(self, data_dir):
        with captured_output(), scripted_input(default="n"):
            assert main._check_checkpoint("all") == {"ok": True, "resume": False}

    def test_matching_mode_can_be_resumed(self, data_dir):
        _write_checkpoint(data_dir, "all")
        with captured_output(), scripted_input("y", default="n"):
            assert main._check_checkpoint("all") == {"ok": True, "resume": True}

    def test_declining_the_resume_then_discarding_starts_fresh(self, data_dir):
        path = _write_checkpoint(data_dir, "all")
        with captured_output(), scripted_input("n", "y", default="n"):
            assert main._check_checkpoint("all") == {"ok": True, "resume": False}
        assert not os.path.exists(path), "discarding must actually remove the checkpoint"

    def test_declining_both_cancels_rather_than_guessing(self, data_dir):
        """Neither resuming nor discarding is a real answer -- so do nothing."""
        path = _write_checkpoint(data_dir, "all")
        with captured_output(), scripted_input("n", "n", default="n"):
            assert main._check_checkpoint("all") == {"ok": False, "resume": False}
        assert os.path.exists(path), "cancelling must leave the checkpoint alone"

    def test_a_checkpoint_from_another_mode_is_never_silently_resumed(self, data_dir):
        """Resuming a 'new only' checkpoint into a full scrape would skip real work."""
        _write_checkpoint(data_dir, "new_only")
        with captured_output() as out, scripted_input("n", default="n"):
            result = main._check_checkpoint("all")
        assert result == {"ok": False, "resume": False}
        assert "different mode" in out.getvalue()

    def test_a_mismatched_checkpoint_can_be_discarded_to_continue(self, data_dir):
        path = _write_checkpoint(data_dir, "new_only")
        with captured_output(), scripted_input("y", default="n"):
            assert main._check_checkpoint("all") == {"ok": True, "resume": False}
        assert not os.path.exists(path)

    def test_an_unremovable_checkpoint_does_not_crash_the_run(self, data_dir, monkeypatch):
        _write_checkpoint(data_dir, "all")

        def refuse(_path):
            raise OSError("locked")

        monkeypatch.setattr(main.os, "remove", refuse)
        with captured_output(), scripted_input("n", "y", default="n"):
            assert main._check_checkpoint("all")["ok"] is True


class TestCredentialValidation:
    def test_missing_credentials_are_refused_with_instructions(self, monkeypatch):
        monkeypatch.setattr(main, "USERNAME", "")
        monkeypatch.setattr(main, "PASSWORD", "")
        with captured_output() as out:
            assert main.validate_credentials() is False
        printed = out.getvalue()
        assert ".env" in printed, "the error must say where to put the credentials"

    def test_a_password_without_an_email_is_still_refused(self, monkeypatch):
        monkeypatch.setattr(main, "USERNAME", "")
        monkeypatch.setattr(main, "PASSWORD", "secret")
        with captured_output():
            assert main.validate_credentials() is False

    def test_both_present_passes(self, monkeypatch):
        monkeypatch.setattr(main, "USERNAME", "someone")
        monkeypatch.setattr(main, "PASSWORD", "secret")
        assert main.validate_credentials() is True


class TestDiskSpaceCheck:
    def test_plenty_of_space_passes(self, monkeypatch):
        monkeypatch.setattr(main.shutil, "disk_usage", lambda _p: _Usage(free=5 * 1024**3))
        assert main.check_disk_space(min_mb=100) is True

    def test_low_space_is_reported_and_refused(self, monkeypatch):
        monkeypatch.setattr(main.shutil, "disk_usage", lambda _p: _Usage(free=10 * 1024**2))
        with captured_output() as out:
            assert main.check_disk_space(min_mb=100) is False
        assert "Low disk space" in out.getvalue()

    def test_an_unreadable_filesystem_does_not_block_the_run(self, monkeypatch):
        """A failed check is not evidence of a full disk, so it must not stop work."""

        def boom(_path):
            raise OSError("no such device")

        monkeypatch.setattr(main.shutil, "disk_usage", boom)
        assert main.check_disk_space() is True


class _Usage:
    """Minimal stand-in for shutil.disk_usage's named tuple."""

    def __init__(self, free: int):
        self.total = free * 2
        self.used = free
        self.free = free


class TestSlugExtraction:
    def test_link_is_preferred_over_url(self):
        entry = {"link": "https://x/serie/from-link", "url": "https://x/serie/from-url"}
        assert main._extract_slug(entry) == "from-link"

    def test_url_is_used_when_link_is_empty(self):
        """Only the returned slug is asserted.

        Whether the fallback is announced on stdout or only logged differs
        between the three repos, and pinning it here would make one sibling's
        cosmetic choice a failure in another. What must hold everywhere is
        that the entry still resolves rather than being treated as slugless.
        """
        entry = {"link": "", "url": "https://x/serie/from-url", "title": "T"}
        with captured_output():
            assert main._extract_slug(entry) == "from-url"

    def test_an_entry_with_neither_yields_none(self):
        assert main._extract_slug({"title": "T"}) is None

    def test_a_non_dict_yields_none(self):
        assert main._extract_slug("not an entry") is None


class TestShowMenu:
    """The menu is derived from the output, not hardcoded.

    The three repos offer different numbers of options -- bs.to has no
    subscribed/watchlist scrape, for instance -- and the list grows over time.
    Reading the numbers back out of the printed menu means these tests keep
    working when an option is added, and still catch the two things that are
    always wrong: a gap in the numbering, and a missing exit.
    """

    @staticmethod
    def _options() -> set[int]:
        with captured_output() as out:
            main.show_menu()
        return {int(match) for match in re.findall(r"^\s*(\d+)\.", out.getvalue(), re.MULTILINE)}

    def test_the_menu_offers_options(self):
        assert self._options(), "show_menu printed nothing that looks like an option"

    def test_there_is_always_a_way_out(self):
        assert 0 in self._options(), "option 0 (exit) must always be offered"

    def test_the_numbering_has_no_gaps(self):
        """A gap means an option was removed and the rest never renumbered."""
        options = self._options()
        assert options == set(range(max(options) + 1)), f"menu numbering is not contiguous: {sorted(options)}"


class _RecordingScraper:
    """Stands in for the scraper class _run_scrape_and_save builds; records each run."""

    made: list = []

    def __init__(self):
        self.series_data = []
        self.all_discovered_series = None
        self.failed_links = []
        self.paused = False
        self.checkpointing = True
        self.site_url = ""
        self.kwargs = None
        type(self).made.append(self)

    def run(self, **kwargs):
        self.kwargs = kwargs
        self.checkpointing = kwargs.get("checkpoint", True) and not kwargs.get("single_url")
        if len(type(self).made) == 1:
            self.series_data = [series("Outer")]

    @staticmethod
    def get_series_slug_from_url(url):
        return url.rstrip("/").rsplit("/", 1)[-1]

    def clear_checkpoint(self):
        pass


class TestNestedRunsLeaveTheCheckpointAlone:
    """A rescrape started from inside a run used to own the checkpoint file too.

    The integrity dialog's rescrape runs as a url_list scrape inside
    _run_scrape_and_save. It checkpointed as a "batch" run and main.py cleared
    the file afterwards, so a paused run's checkpoint vanished while the outer
    call went on to report it preserved.
    """

    @pytest.fixture(autouse=True)
    def _fakes(self, monkeypatch):
        _RecordingScraper.made = []
        rescrape = {"action": "rescrape", "urls": ["https://x.test/v"], "titles": ["V"], "series": [series("V")]}
        answers = [(rescrape, {})]
        self.replaced = []
        for name, value in (
            ("BsToScraper", _RecordingScraper),
            ("ACTIVE_SITE_URL", "https://x.test"),
            ("confirm_and_save_changes", lambda *_a, **_k: answers.pop(0) if answers else (True, {})),
            ("replace_critical_series", lambda *args: self.replaced.append(args) or (0, 0)),
            ("print_scraped_series_status", lambda *_a, **_k: None),
            ("_notify_vanished_at_startup", lambda *_a, **_k: None),
        ):
            monkeypatch.setattr(main, name, value)

    def test_the_integrity_rescrape_keeps_no_checkpoint(self):
        with captured_output():
            main._run_scrape_and_save(run_kwargs={"url_list": ["u"]}, description="d", success_msg="s", no_data_msg="n")
        nested = _RecordingScraper.made[1]
        assert nested.kwargs["checkpoint"] is False

    def test_what_the_integrity_rescrape_reads_is_offered_as_a_replacement(self, monkeypatch):
        """The critical series used to be deleted before the rescrape even ran."""

        class Rereads(_RecordingScraper):
            def run(self, **kwargs):
                super().run(**kwargs)
                if len(type(self).made) == 2:
                    self.series_data = [series("V")]

        monkeypatch.setattr(main, "BsToScraper", Rereads)
        with captured_output():
            main._run_scrape_and_save(run_kwargs={"url_list": ["u"]}, description="d", success_msg="s", no_data_msg="n")
        assert len(self.replaced) == 1
        entries, fresh, _path = self.replaced[0]
        assert [entry["title"] for entry in entries] == ["V"]
        assert [entry["title"] for entry in fresh] == ["V"]

    def test_an_integrity_rescrape_that_reads_nothing_replaces_nothing(self):
        with captured_output() as out:
            main._run_scrape_and_save(run_kwargs={"url_list": ["u"]}, description="d", success_msg="s", no_data_msg="n")
        assert self.replaced == []
        assert "stay in the index unchanged" in out.getvalue()

    def test_a_run_of_nothing_but_failures_is_not_saved_as_a_success(self, monkeypatch):
        """The S.to sibling gated on the raw list, so a run whose every series failed still saved and said so."""
        saved = []

        class OnlyFailures(_RecordingScraper):
            def run(self, **kwargs):
                super().run(**kwargs)
                self.series_data = [{**series("Failed"), "_error": True}]

        monkeypatch.setattr(main, "BsToScraper", OnlyFailures)
        monkeypatch.setattr(main, "confirm_and_save_changes", lambda *args, **_k: saved.append(args) or (True, {}))
        with captured_output() as out:
            main._run_scrape_and_save(run_kwargs={"url_list": ["u"]}, description="d", success_msg="s", no_data_msg="n")
        assert saved == []
        assert "✓ s" not in out.getvalue()

    def test_a_paused_run_that_kept_no_checkpoint_does_not_claim_one(self, monkeypatch):
        class Paused(_RecordingScraper):
            def run(self, **kwargs):
                super().run(**kwargs)
                self.series_data = []
                self.paused = True

        monkeypatch.setattr(main, "BsToScraper", Paused)
        with captured_output() as out:
            main._run_scrape_and_save(
                run_kwargs={"single_url": "https://x.test/one"}, description="d", success_msg="s", no_data_msg="n"
            )
        assert "keeps no checkpoint" in out.getvalue()
        assert "checkpoint preserved" not in out.getvalue()
