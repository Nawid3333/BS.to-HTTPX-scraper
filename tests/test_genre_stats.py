"""Tests for the genre statistics feature (menu option 7).

Every parser case here comes from a page that really exists, captured in
`tests/fixtures/pages/`, or -- for shapes those 18 captures never happen to
show (a 7-genre series, a genre field that is genuinely empty) -- an inline
HTML string built to match the real markup exactly. bs.to's own headline trap
is not truncation (it has none) but div.infos: five of its six child <div>s
share the identical shape as the genre one, so a parser that is not scoped to
the single <div> labelled "Genres" reads a director's name or a production
year as a genre.
"""

import gzip
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import genre_stats  # noqa: E402
from src.genre_stats import (  # noqa: E402
    build_snapshot,
    diff_snapshots,
    extract_genres,
    load_genres,
    normalize_genre_key,
    save_genres,
)
from src.scraper import _extract_title, _stripped_text, make_doc  # noqa: E402

PAGE_DIR = Path(__file__).resolve().parent / "fixtures" / "pages"


def load_page(name):
    path = PAGE_DIR / f"series__{name}.html.gz"
    if not path.exists():
        return None
    return make_doc(gzip.decompress(path.read_bytes()).decode("utf-8"))


def keys_of(doc):
    return [key for key, _ in extract_genres(doc)]


def infos_page(genre_html: str, *, extra: str = ""):
    """Build a div.infos block shaped like a real bs.to series page.

    Always includes the same sibling blocks (Produktionsjahre, Hauptdarsteller,
    Regisseure) that a naive "div.infos span" selector would misread as
    genres, so every test using this helper exercises the scoping trap.
    """
    html = f"""
    <html><body>
    <div class="infos">
      <div><span>Genres</span><p>{genre_html}</p></div>
      <div><span>Produktionsjahre</span><p><em>1996 - <i>Unbekannt</i></em></p></div>
      <div><span>Hauptdarsteller</span><p>
        <span>Bear Grylls,</span><span>Jake Gyllenhaal,</span><span>Will Ferrell</span>
      </p></div>
      <div><span>Regisseure</span><p><span>Kenji Kodama,</span><span>Kojin Ochi</span></p></div>
      {extra}
    </div>
    </body></html>
    """
    return make_doc(html)


def series_entry(total, watched, slug):
    """Minimal index entry: only what get_episode_counts and the slug join read."""
    episodes = [{"number": n + 1, "watched": n < watched} for n in range(total)]
    return {
        "title": slug,
        "link": f"/serie/{slug}",
        "url": f"https://bs.to/serie/{slug}",
        "seasons": [{"season": "1", "episodes": episodes}],
    }


class FakeIndex:
    def __init__(self, entries):
        self.series_index = {e["title"]: e for e in entries}


@unittest.skipUnless(PAGE_DIR.exists(), "no fixtures captured yet")
class TestGenreScoping(unittest.TestCase):
    """The single most likely way this feature ships subtly wrong: div.infos
    has six near-identical child divs, and only one of them is genres."""

    def test_a_real_page_yields_only_its_genres(self):
        doc = load_page("Detektiv-Conan-DC-Case-Closed")
        if doc is None:
            self.skipTest("fixture not captured")
        self.assertEqual(keys_of(doc), ["anime", "comedy", "krimi", "mystery"])

    def test_director_and_actor_names_never_leak_into_genres(self):
        doc = load_page("Detektiv-Conan-DC-Case-Closed")
        if doc is None:
            self.skipTest("fixture not captured")
        labels = [label for _, label in extract_genres(doc)]
        for leaked in ("Kenji Kodama", "Kenji Kodama,", "TMS Entertainment", "Gosho Aoyama"):
            self.assertNotIn(leaked, labels)

    def test_production_years_never_leak_into_genres(self):
        doc = infos_page("<span>Krimi</span>")
        self.assertEqual(keys_of(doc), ["krimi"])

    def test_a_naive_whole_block_selector_would_have_failed_this(self):
        """Regression guard: div.infos span (unscoped) picks up every sibling
        block's spans too, which is exactly the bug this parser must avoid."""
        doc = infos_page("<span>Krimi</span>")
        naive = [_stripped_text(s) for s in doc.xpath(".//div[@class='infos']//span")]
        self.assertGreater(len(naive), 1, "fixture helper stopped exercising the trap")
        self.assertEqual(keys_of(doc), ["krimi"])


class TestNoEntryAndEmpty(unittest.TestCase):
    def test_keine_angabe_parses_as_no_genres(self):
        doc = infos_page('<span class="no-entry"><i>Keine Angabe</i></span>')
        self.assertEqual(extract_genres(doc), [])

    def test_a_page_with_no_infos_block_returns_no_genres(self):
        doc = make_doc("<html><body><p>nothing here</p></body></html>")
        self.assertEqual(extract_genres(doc), [])

    def test_a_page_with_no_genres_div_returns_no_genres(self):
        doc = make_doc(
            """<html><body><div class="infos">
                 <div><span>Produktionsjahre</span><p><em>2020</em></p></div>
               </div></body></html>"""
        )
        self.assertEqual(extract_genres(doc), [])


class TestNoUpperBound(unittest.TestCase):
    """Fixtures sample the site; they do not specify it. A live check found
    Naruto Shippuden carrying 7 genres -- nothing here may assume a max."""

    def test_seven_genres_all_parse(self):
        genre_html = "".join(
            f"<span>{g}</span>" for g in ("Anime", "Action", "Abenteuer", "Comedy", "Drama", "Fantasy", "Shounen")
        )
        doc = infos_page(genre_html)
        self.assertEqual(len(keys_of(doc)), 7)

    def test_no_fixture_or_inline_case_is_truncated_by_a_hardcoded_limit(self):
        counts = {}
        for path in sorted(PAGE_DIR.glob("series__*.html.gz")) if PAGE_DIR.exists() else []:
            doc = make_doc(gzip.decompress(path.read_bytes()).decode("utf-8"))
            genres = extract_genres(doc)
            if genres:
                counts[path.name] = len(genres)
        doc = infos_page("".join(f"<span>g{i}</span>" for i in range(9)))
        self.assertEqual(len(keys_of(doc)), 9)


class TestBoldIsIgnored(unittest.TestCase):
    """A genre is a genre -- the bold style some pages use is not meaningful."""

    def test_bold_and_plain_genres_are_both_returned(self):
        doc = infos_page('<span style="font-weight: bold;">Anime</span><span>Comedy</span>')
        self.assertEqual(keys_of(doc), ["anime", "comedy"])


class TestSeriesTitles(unittest.TestCase):
    """Displayed names must be real titles, never slugs or raw URL text."""

    def test_title_is_read_from_the_heading_not_the_url_slug(self):
        for name, expected in (
            ("Detektiv-Conan-DC-Case-Closed", "Detektiv Conan | DC | Case Closed"),
            ("Tatort", "Tatort"),
        ):
            doc = load_page(name)
            if doc is None:
                self.skipTest(f"{name} fixture not captured")
            with self.subTest(page=name):
                self.assertEqual(_extract_title(doc), expected)

    def test_a_page_with_no_usable_heading_falls_back_to_the_slug(self):
        doc = make_doc("<html><body><p>no heading here</p></body></html>")
        self.assertIsNone(_extract_title(doc))


class TestGenreIdentity(unittest.TestCase):
    def test_display_text_and_a_normalized_form_agree(self):
        self.assertEqual(normalize_genre_key("K-Drama"), normalize_genre_key("k-drama"))

    def test_multiword_genres_collapse_whitespace(self):
        self.assertEqual(normalize_genre_key("Science Fiction"), "science-fiction")
        self.assertEqual(normalize_genre_key("Reality TV"), "reality-tv")

    def test_junk_is_empty_not_an_exception(self):
        for value in (None, "", "   ", 42, []):
            with self.subTest(value=value):
                self.assertEqual(normalize_genre_key(value), "")

    def test_a_trailing_comma_is_stripped(self):
        self.assertEqual(normalize_genre_key("Krimi,"), "krimi")


class TestSlugJoin(unittest.TestCase):
    """Trap: bs.to slugs carry capitals, and a case mismatch on either side
    of the join zeroes every number without raising anything."""

    def test_index_entries_are_found_by_catalogue_slug(self):
        entries = [series_entry(12, 12, "Tatort"), series_entry(10, 3, "Lindenstrasse")]
        by_slug = genre_stats._index_by_slug(FakeIndex(entries))
        self.assertEqual(set(by_slug), {"Tatort", "Lindenstrasse"})

    def test_a_capitalised_slug_is_preserved_not_lowercased(self):
        entry = series_entry(1, 1, "2012-Das-Jahr-Null")
        by_slug = genre_stats._index_by_slug(FakeIndex([entry]))
        self.assertIn("2012-Das-Jahr-Null", by_slug)
        self.assertNotIn("2012-das-jahr-null", by_slug)

    def test_a_full_url_and_a_bare_path_resolve_to_the_same_slug(self):
        entry = series_entry(1, 1, "Tatort")
        only_url = {k: v for k, v in entry.items() if k != "link"}
        by_slug = genre_stats._index_by_slug(FakeIndex([only_url]))
        self.assertIn("Tatort", by_slug)

    def test_entries_without_a_usable_slug_are_dropped_not_crashed_on(self):
        broken = {"title": "x", "link": "", "url": "", "seasons": []}
        self.assertEqual(genre_stats._index_by_slug(FakeIndex([broken])), {})


class TestSnapshot(unittest.TestCase):
    def setUp(self):
        self.entries = [
            series_entry(10, 10, "done-one"),
            series_entry(10, 4, "partial"),
            series_entry(0, 0, "empty"),
        ]
        self.by_slug = genre_stats._index_by_slug(FakeIndex(self.entries))
        self.data = {
            "series": {
                "done-one": ["krimi", "drama"],
                "partial": ["krimi"],
                "empty": ["drama"],
                "not-in-index": ["krimi"],
            },
            "labels": {"krimi": "Krimi", "drama": "Drama"},
            "catalogue_total": 4,
            "scraped_count": 4,
        }

    def test_a_series_counts_in_every_one_of_its_genres(self):
        cats = build_snapshot(self.data, self.by_slug)["categories"]
        self.assertEqual(cats["krimi"], {"done": 1, "indexed": 2})
        self.assertEqual(cats["drama"], {"done": 1, "indexed": 2})

    def test_column_totals_exceed_the_series_count_by_design(self):
        snap = build_snapshot(self.data, self.by_slug)
        indexed_sum = sum(c["indexed"] for c in snap["categories"].values())
        self.assertGreater(indexed_sum, snap["indexed_series"])

    def test_a_zero_episode_series_is_not_counted_as_done(self):
        cats = build_snapshot(self.data, self.by_slug)["categories"]
        self.assertEqual(cats["drama"]["done"], 1)

    def test_series_not_in_the_index_do_not_inflate_category_counts(self):
        cats = build_snapshot(self.data, self.by_slug)["categories"]
        self.assertEqual(cats["krimi"]["indexed"], 2)

    def test_series_with_no_genres_are_reported_not_silently_dropped(self):
        self.data["series"]["genreless"] = []
        snap = build_snapshot(self.data, self.by_slug)
        self.assertEqual(snap["without_genres"], ["genreless"])
        self.assertNotIn("", snap["categories"])

    def test_indexed_series_missing_from_the_genre_data_are_reported(self):
        del self.data["series"]["partial"]
        snap = build_snapshot(self.data, self.by_slug)
        self.assertIn("partial", snap["indexed_without_genre_data"])


class TestDiff(unittest.TestCase):
    def test_a_new_series_is_reported_with_its_genres(self):
        d = diff_snapshots({}, {"tatort": ["krimi"]})
        self.assertEqual(d["new_series"], [("tatort", ["krimi"])])

    def test_gained_and_lost_genres_are_both_reported(self):
        d = diff_snapshots({"x": ["krimi", "drama"]}, {"x": ["krimi", "fantasy"]})
        self.assertEqual(d["changed"], [("x", ["fantasy"], ["drama"])])

    def test_an_unchanged_series_produces_no_change(self):
        d = diff_snapshots({"x": ["krimi"]}, {"x": ["krimi"]})
        self.assertEqual(d["changed"], [])
        self.assertEqual(d["new_series"], [])

    def test_genre_order_alone_is_not_a_change(self):
        d = diff_snapshots({"x": ["krimi", "drama"]}, {"x": ["drama", "krimi"]})
        self.assertEqual(d["changed"], [])

    def test_new_and_vanished_categories_are_reported(self):
        d = diff_snapshots({"x": ["old"]}, {"x": ["new"]})
        self.assertEqual(d["new_categories"], ["new"])
        self.assertEqual(d["gone_categories"], ["old"])


class TestChangeLines(unittest.TestCase):
    """Change-list lines show real titles, never bare slugs."""

    def test_a_new_series_is_shown_by_title(self):
        data = {"labels": {"krimi": "Krimi"}, "titles": {"tatort": "Tatort"}}
        changes = diff_snapshots({}, {"tatort": ["krimi"]})
        lines = genre_stats._change_lines(data, changes)
        self.assertIn("Tatort is new in Krimi", lines[0])
        self.assertNotIn("tatort ", lines[0])

    def test_a_slug_with_no_known_title_falls_back_to_the_slug(self):
        data = {"labels": {}, "titles": {}}
        changes = diff_snapshots({}, {"some-slug": ["krimi"]})
        lines = genre_stats._change_lines(data, changes)
        self.assertIn("some-slug", lines[0])


class TestRowOrdering(unittest.TestCase):
    def test_most_complete_first_then_largest_among_ties(self):
        cats = {
            "done": {"done": 1, "indexed": 1},
            "small": {"done": 0, "indexed": 3},
            "big": {"done": 0, "indexed": 40},
            "half": {"done": 5, "indexed": 10},
        }
        labels = {k: k for k in cats}
        order = [r[0] for r in genre_stats._sorted_rows(cats, labels)]
        self.assertEqual(order, ["done", "half", "big", "small"])

    def test_an_empty_category_does_not_divide_by_zero(self):
        cats = {"none": {"done": 0, "indexed": 0}}
        rows = genre_stats._sorted_rows(cats, {"none": "None"})
        self.assertEqual(rows, [("None", 0, 0)])
        self.assertTrue(genre_stats._table_lines(rows))


class TestBar(unittest.TestCase):
    def test_bar_endpoints_and_width(self):
        self.assertEqual(genre_stats._bar(0, 10), "░" * 10)
        self.assertEqual(genre_stats._bar(100, 10), "█" * 10)
        self.assertEqual(len(genre_stats._bar(37.4, 10)), 10)

    def test_out_of_range_percentages_are_clamped(self):
        self.assertEqual(len(genre_stats._bar(-5, 10)), 10)
        self.assertEqual(genre_stats._bar(150, 10), "█" * 10)


class TestStorage(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "genre_index.json")
        self.patch = mock.patch.object(genre_stats, "GENRE_INDEX_FILE", self.path)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()
        self.tmp.cleanup()

    def test_a_missing_file_is_an_empty_skeleton_not_an_error(self):
        data = load_genres()
        self.assertEqual(data["series"], {})
        self.assertEqual(data["version"], genre_stats.SCHEMA_VERSION)

    def test_round_trip_preserves_the_data(self):
        data = load_genres()
        data["series"] = {"tatort": ["krimi"]}
        data["labels"] = {"krimi": "Krimi"}
        data["titles"] = {"tatort": "Tatort"}
        data["host"] = "https://bs.to"
        save_genres(data)
        reloaded = load_genres()
        self.assertEqual(reloaded["series"], {"tatort": ["krimi"]})
        self.assertEqual(reloaded["titles"], {"tatort": "Tatort"})

    def test_keys_are_written_sorted_for_clean_diffs(self):
        data = load_genres()
        data["series"] = {"zzz": ["b"], "aaa": ["a"]}
        data["labels"] = {"b": "B", "a": "A"}
        save_genres(data)
        with open(self.path, encoding="utf-8") as fh:
            written = json.load(fh)
        self.assertEqual(list(written["series"]), ["aaa", "zzz"])

    def test_no_backup_file_is_left_behind(self):
        save_genres(load_genres())
        self.assertEqual(list(Path(self.tmp.name).glob("*.bak*")), [])

    def test_corrupt_json_yields_an_empty_skeleton(self):
        Path(self.path).write_text("{not json", encoding="utf-8")
        self.assertEqual(load_genres()["series"], {})

    def test_an_unknown_schema_version_is_discarded_not_guessed_at(self):
        Path(self.path).write_text(json.dumps({"version": 99, "series": {"x": ["y"]}}), encoding="utf-8")
        self.assertEqual(load_genres()["series"], {})

    def test_a_wrongly_typed_series_map_is_rejected(self):
        Path(self.path).write_text(
            json.dumps({"version": genre_stats.SCHEMA_VERSION, "series": [], "labels": {}}),
            encoding="utf-8",
        )
        self.assertEqual(load_genres()["series"], {})

    def test_a_wrongly_typed_titles_map_is_rejected(self):
        Path(self.path).write_text(
            json.dumps({"version": genre_stats.SCHEMA_VERSION, "series": {}, "labels": {}, "titles": []}),
            encoding="utf-8",
        )
        self.assertEqual(load_genres()["series"], {})

    def test_a_file_saved_before_titles_existed_still_loads(self):
        """Additive field: an older genre_index.json simply has no titles yet."""
        Path(self.path).write_text(
            json.dumps({"version": genre_stats.SCHEMA_VERSION, "series": {"tatort": ["krimi"]}, "labels": {}}),
            encoding="utf-8",
        )
        data = load_genres()
        self.assertEqual(data["series"], {"tatort": ["krimi"]})
        self.assertEqual(data["titles"], {})


class TestChangeListConsumption(unittest.TestCase):
    """Viewing the change list must mark it as seen, or it repeats forever."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "genre_index.json")
        self.patch = mock.patch.object(genre_stats, "GENRE_INDEX_FILE", self.path)
        self.patch.start()
        data = genre_stats.load_genres()
        data["catalogue_total"] = 1
        data["scraped_count"] = 1
        data["labels"] = {"krimi": "Krimi"}
        data["previous_series"] = {}
        data["series"] = {"tatort": ["krimi"]}
        save_genres(data)

    def tearDown(self):
        self.patch.stop()
        self.tmp.cleanup()

    def test_a_second_view_with_no_new_scrape_shows_no_changes(self):
        entry = series_entry(1, 1, "tatort")
        with mock.patch.object(genre_stats, "IndexManager", lambda *a, **k: FakeIndex([entry])):
            first = diff_snapshots(load_genres().get("previous_series") or {}, load_genres()["series"])
            self.assertTrue(first["new_series"])
            genre_stats.show_stats()
            second = diff_snapshots(load_genres().get("previous_series") or {}, load_genres()["series"])
        self.assertEqual(second["new_series"], [])
        self.assertEqual(second["changed"], [])

    def test_viewing_stats_does_not_touch_series_data_itself(self):
        entry = series_entry(1, 1, "tatort")
        with mock.patch.object(genre_stats, "IndexManager", lambda *a, **k: FakeIndex([entry])):
            genre_stats.show_stats()
        self.assertEqual(load_genres()["series"], {"tatort": ["krimi"]})


class TestExportReport(unittest.TestCase):
    """Regression pin: title and genres must live together per series, not
    as a separate top-level "titles" map a consumer has to cross-reference."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.index_path = str(Path(self.tmp.name) / "genre_index.json")
        self.report_path = str(Path(self.tmp.name) / "genre_report.json")
        self.patches = [
            mock.patch.object(genre_stats, "GENRE_INDEX_FILE", self.index_path),
            mock.patch.object(genre_stats, "GENRE_REPORT_FILE", self.report_path),
        ]
        for p in self.patches:
            p.start()
        data = genre_stats.load_genres()
        data["catalogue_total"] = 1
        data["scraped_count"] = 1
        data["labels"] = {"krimi": "Krimi"}
        data["titles"] = {"tatort": "Tatort"}
        data["series"] = {"tatort": ["krimi"]}
        save_genres(data)

    def tearDown(self):
        for p in self.patches:
            p.stop()
        self.tmp.cleanup()

    def test_title_and_genres_live_together_per_series(self):
        entry = series_entry(1, 1, "tatort")
        with mock.patch.object(genre_stats, "IndexManager", lambda *a, **k: FakeIndex([entry])):
            genre_stats.export_report()
        with open(self.report_path, encoding="utf-8") as fh:
            report = json.load(fh)
        self.assertNotIn("titles", report, "a separate top-level titles map defeats the point of merging")
        self.assertEqual(report["series"], {"tatort": {"title": "Tatort", "genres": ["krimi"]}})

    def test_a_slug_with_no_known_title_falls_back_to_the_slug(self):
        data = genre_stats.load_genres()
        data["series"]["some-slug"] = ["krimi"]
        save_genres(data)
        entry = series_entry(1, 1, "tatort")
        with mock.patch.object(genre_stats, "IndexManager", lambda *a, **k: FakeIndex([entry])):
            genre_stats.export_report()
        with open(self.report_path, encoding="utf-8") as fh:
            report = json.load(fh)
        self.assertEqual(report["series"]["some-slug"]["title"], "some-slug")


class TestUnwatchedByGenre(unittest.TestCase):
    """Option 7 sub menu 4: list unwatched series by genre with a back option."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "genre_index.json")
        self.index_path = str(Path(self.tmp.name) / "series_index.json")
        self.patch = mock.patch.object(genre_stats, "GENRE_INDEX_FILE", self.path)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()
        self.tmp.cleanup()

    def _write_data(self, labels=None, series=None, titles=None):
        data = genre_stats.load_genres()
        data["labels"] = labels or {"action": "Action", "comedy": "Comedy"}
        data["series"] = series or {"bleach": ["action"], "naruto": ["comedy"]}
        data["titles"] = titles or {"bleach": "Bleach", "naruto": "Naruto"}
        save_genres(data)

    @mock.patch.object(genre_stats, "_prompt_genre_choice", return_value="__back__")
    def test_back_option_returns_early(self, _mock_picker):
        """Choosing 0/Back from the picker must not print unwatched series."""
        self._write_data()
        entry = series_entry(12, 0, "bleach")
        with mock.patch.object(genre_stats, "IndexManager", lambda *a, **k: FakeIndex([entry])):
            captured = io.StringIO()
            with mock.patch("sys.stdout", new=captured):
                genre_stats.list_unwatched_by_genre()
        out = captured.getvalue()
        self.assertNotIn("Unwatched series", out)
        self.assertNotIn("Bleach", out)

    @mock.patch.object(genre_stats, "_prompt_genre_choice", return_value="action")
    def test_selected_genre_filters_unwatched(self, _mock_picker):
        """Picking a genre only lists unwatched series tagged with that genre."""
        self._write_data()
        entries = [
            series_entry(12, 0, "bleach"),
            series_entry(12, 0, "naruto"),
        ]
        with mock.patch.object(genre_stats, "IndexManager", lambda *a, **k: FakeIndex(entries)):
            captured = io.StringIO()
            with mock.patch("sys.stdout", new=captured):
                genre_stats.list_unwatched_by_genre()
        out = captured.getvalue()
        self.assertIn("Unwatched series", out)
        self.assertIn("bleach", out)
        self.assertNotIn("naruto", out)

    def test_picker_resolves_back_input(self):
        """The picker returns __back__ when the user types 0."""
        choices = {"all": "All genres / no filter", "action": "Action"}
        with mock.patch("sys.stdout.isatty", return_value=False), mock.patch("builtins.input", return_value="0"):
            self.assertEqual(genre_stats._prompt_genre_choice(choices), "__back__")

    def test_picker_resolves_label_input(self):
        """The picker returns the matching key for a typed genre label."""
        choices = {"all": "All genres / no filter", "action": "Action"}
        with mock.patch("sys.stdout.isatty", return_value=False), mock.patch("builtins.input", return_value="Action"):
            self.assertEqual(genre_stats._prompt_genre_choice(choices), "action")

    def test_picker_rejects_unknown_input_and_retries(self):
        """Unknown input loops back to retry instead of returning a default."""
        choices = {"all": "All genres / no filter", "action": "Action"}
        with (
            mock.patch("sys.stdout.isatty", return_value=False),
            mock.patch("builtins.input", side_effect=["nonsense", "Action"]),
        ):
            self.assertEqual(genre_stats._prompt_genre_choice(choices), "action")


if __name__ == "__main__":
    unittest.main()
