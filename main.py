#!/usr/bin/env python3
"""
BS.TO Series Scraper & Index Manager.

Scrapes watched TV series from bs.to and maintains a local JSON index.
Uses httpx (no browser needed) with multi-session architecture.
Supports checkpoint resume, batch URL import, and interactive
change confirmation.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import logging.handlers
import os
import random
import re
import shutil
import sys
import time
from collections import Counter
from datetime import datetime
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from src.index_manager import IndexManager

from config.config import (
    DATA_DIR,
    DEFAULT_BATCH_FILE,
    LOG_FILE,
    NUM_WORKERS,
    PASSWORD,
    SERIES_INDEX_FILE,
    SITE_URLS,
    USERNAME,
    VALID_SERIES_HOSTS,
    configure_console,
)
from src import genre_stats  # noqa: E402
from src.index_manager import (  # noqa: E402
    IndexManager,
    _extract_slug,
    confirm_and_save_changes,
    get_episode_counts,
    remove_series_from_index,
    show_vanished_series,
)
from src.scraper import (  # noqa: E402
    BsToScraper,
    ScrapingPausedError,
)

# Global active site URL (set by domain probing)
ACTIVE_SITE_URL: str | None = None

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.handlers.RotatingFileHandler(
            LOG_FILE,
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        ),
        logging.StreamHandler(),
    ],
)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

_SERIE_URL_RE = re.compile(r"/serie/[^/]+")

_MODE_LABELS = {
    "all_series": "Scrape all series",
    "new_only": "Scrape new series only",
    "unwatched": "Scrape unwatched series",
    "batch": "Batch add from file",
    "retry": "Retry failed series",
    "subscribed": "Subscribed series",
    "watchlist": "Watchlist series",
    "both": "Subscribed+Watchlist series",
}


def _host_label(site_url):
    """Return the hostname part of a URL for display."""
    return urlparse(site_url).netloc


def _format_host_rows(hosts):
    """Return a list of table-formatted host status lines.

    hosts is a list of (label, status, count, idx_count, compare_txt) tuples.
    """
    if not hosts:
        return []

    term_w = max(shutil.get_terminal_size().columns, 80)
    arrow_gap = "  "

    labels = ["Host", "Status", "Series", "Index", "Compare"]
    cols = {
        "host": max([len(str(label)) for label, *_ in hosts] + [len(labels[0])]),
        "status": max([len("OK" if status else "FAILED") for _, status, *_ in hosts] + [len(labels[1])]),
        "series": max([len(f"{count:,}") if count is not None else 1 for _, _, count, *_ in hosts] + [len(labels[2])]),
        "index": max(
            [len(f"{idx_count:,}") if idx_count is not None else 1 for _, _, _, idx_count, *_ in hosts]
            + [len(labels[3])]
        ),
        "compare": max(
            [len(str(compare_txt)) if compare_txt is not None else 1 for *_, compare_txt in hosts] + [len(labels[4])]
        ),
    }

    total = sum(cols.values()) + len(labels) * len(arrow_gap)
    if total > term_w:
        # Trim the widest columns first (host and compare) while keeping a minimum
        excess = total - term_w
        trimmable = cols["host"] - len(labels[0]) + cols["compare"] - len(labels[4])
        if trimmable > 0:
            factor = min(excess / trimmable, 1.0)
            cols["host"] = max(
                len(labels[0]),
                int(cols["host"] - (cols["host"] - len(labels[0])) * factor),
            )
            cols["compare"] = max(
                len(labels[4]),
                int(cols["compare"] - (cols["compare"] - len(labels[4])) * factor),
            )

    def _trunc(text, width):
        text = str(text)
        return text if len(text) <= width else text[: width - 1] + "…"

    sep_parts = ["─" * cols["host"]] + ["─" * cols[key] for key in ["status", "series", "index", "compare"]]

    lines = [
        arrow_gap
        + "  ".join(
            [
                f"{_trunc(labels[0], cols['host']):<{cols['host']}}",
                f"{labels[1]:<{cols['status']}}",
                f"{labels[2]:<{cols['series']}}",
                f"{labels[3]:<{cols['index']}}",
                f"{labels[4]:<{cols['compare']}}",
            ]
        ),
        arrow_gap + "  ".join(sep_parts),
    ]

    for label, status, count, idx_count, compare_txt in hosts:
        status_txt = "OK" if status else "FAILED"
        count_txt = f"{count:,}" if count is not None else "-"
        idx_txt = f"{idx_count:,}" if idx_count is not None else "-"
        cmp_txt = compare_txt if compare_txt is not None else "-"
        lines.append(
            arrow_gap
            + "  ".join(
                [
                    f"{_trunc(label, cols['host']):<{cols['host']}}",
                    f"{status_txt:<{cols['status']}}",
                    f"{count_txt:<{cols['series']}}",
                    f"{idx_txt:<{cols['index']}}",
                    f"{_trunc(cmp_txt, cols['compare']):<{cols['compare']}}",
                ]
            )
        )
    return lines


def _probe_hosts(scraper, site_urls):
    """Return probe results; on failure, print the error and return an empty list."""
    try:
        return asyncio.run(scraper.probe_sites(site_urls))
    except Exception as exc:
        print(f"  ✗ Probe failed: {exc}")
        logger.exception("Host probe failed")
        return []


def _fetch_catalogue_info_for_hosts(scraper, site_urls):
    """Fetch every host's catalogue at once; return {site_url: (count, slugs)}.

    The hosts are independent servers, so fetching them one after another was
    time spent for no reason: three sequential multi-megabyte catalogue
    downloads were most of the wait between launch and the menu.

    Each host gets its own scraper instance. get_catalogue_info_for_site sets
    self.site_url for the duration of the call, so sharing one scraper across
    concurrent hosts would let them overwrite each other's target -- and a
    count cross-checked against a different host's slug set is exactly the
    kind of wrong-but-plausible result that goes unnoticed. The instances do
    no I/O in __init__, so an extra one per host costs nothing.

    A host that fails still yields (None, set()) and does not affect the
    others, which is what the one-host-at-a-time version did.
    """

    async def one(site_url):
        try:
            return await type(scraper)().get_catalogue_info_for_site(site_url)
        except Exception as exc:
            logger.warning("Could not fetch catalogue info for %s: %s", site_url, exc)
            return None, set()

    async def gather_all():
        return await asyncio.gather(*(one(url) for url in site_urls))

    if not site_urls:
        return {}
    try:
        return dict(zip(site_urls, asyncio.run(gather_all()), strict=True))
    except Exception as exc:
        logger.warning("Could not fetch catalogue info: %s", exc)
        return {}


def _collect_index_slugs(idx_mgr):
    """Collect slugs from the local index, including entries without a slug."""
    index_slugs_list = []
    index_entries_without_slug = []
    for title, s in idx_mgr.series_index.items():
        slug = _extract_slug(s)
        if slug:
            index_slugs_list.append(slug)
        else:
            index_entries_without_slug.append(
                {
                    "title": s.get("title", title),
                    "link": s.get("link", ""),
                    "url": s.get("url", ""),
                }
            )

    index_duplicates = {slug: n for slug, n in Counter(index_slugs_list).items() if n > 1}
    return set(index_slugs_list), index_duplicates, index_entries_without_slug


def _remove_duplicate_index_entries(idx_mgr, index_duplicates):
    """Prompt to remove duplicate index entries and save if confirmed."""
    dup_extra = sum(index_duplicates.values()) - len(index_duplicates)
    print(f"    ⚠ Found {len(index_duplicates)} duplicate slug(s) in index (extra count: {dup_extra})")
    print("    Duplicate slugs:")
    for dup_slug in sorted(index_duplicates):
        print(f"      - {dup_slug}")

    choice = input("\nDelete duplicate index entries? (y/n): ").strip().lower()
    if choice != "y":
        return

    removed_titles = []
    for title, series in list(idx_mgr.series_index.items()):
        slug = _extract_slug(series)
        if slug in index_duplicates:
            removed_titles.append(title)
            del idx_mgr.series_index[title]
    idx_mgr.save_index()
    print(f"    🗑 Removed {len(removed_titles)} duplicate entries. Run 'Fetch new series' to rescrape them.")
    logger.info(
        "Removed %d duplicate index entries: %s",
        len(removed_titles),
        sorted(index_duplicates),
    )


def _cross_check_index(scraper, site_url, count, idx_mgr=None, site_slugs=None):
    """Compare site slugs against the local index for one host.

    idx_mgr can be passed in to avoid reloading the index repeatedly.
    site_slugs can be passed in if already fetched by the caller.

    Returns a tuple (idx_count, compare_txt, report_entry):
      - idx_count: number of entries in the local index, or None.
      - compare_txt: short status string for the host table.
      - report_entry: dict with per-host mismatch details (or None if skipped).
    """
    if idx_mgr is None:
        idx_mgr = IndexManager()
    idx_count = len(idx_mgr.series_index)
    if idx_count == 0:
        return None, None, None

    diff = idx_count - count
    if diff == 0:
        return (
            idx_count,
            "match",
            {
                "host": site_url,
                "site_count": count,
                "site_unique_slugs": count,
                "only_in_index": [],
                "only_on_site": [],
                "compare": "match",
            },
        )

    sign = "+" if diff > 0 else ""
    compare_txt = f"mismatch ({sign}{diff})"

    if site_slugs is None:
        logger.warning("Cannot compare slugs because site slug list is unavailable.")
        return (
            idx_count,
            compare_txt,
            {
                "host": site_url,
                "site_count": count,
                "site_unique_slugs": None,
                "only_in_index": [],
                "only_on_site": [],
                "compare": compare_txt,
            },
        )

    index_slugs, index_duplicates, index_entries_without_slug = _collect_index_slugs(idx_mgr)
    only_in_index = sorted(index_slugs - site_slugs)
    only_on_site = sorted(site_slugs - index_slugs)

    report_entry = {
        "host": site_url,
        "site_count": count,
        "site_unique_slugs": len(site_slugs),
        "only_in_index": only_in_index,
        "only_on_site": only_on_site,
        "compare": compare_txt,
    }

    return idx_count, compare_txt, report_entry


def _save_combined_mismatch_report(report_path, idx_mgr, host_reports):
    """Write a single combined mismatch report covering all probed hosts."""
    index_slugs, index_duplicates, index_entries_without_slug = _collect_index_slugs(idx_mgr)
    report = {
        "generated": datetime.now().isoformat(),
        "index_count": len(idx_mgr.series_index),
        "index_unique_slugs": len(index_slugs),
        "index_entries_without_slug_count": len(index_entries_without_slug),
        "index_entries_without_slug": index_entries_without_slug,
        "index_duplicates": index_duplicates,
        "hosts": host_reports,
    }
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    in_only_set = set().union(*(set(h.get("only_in_index", [])) for h in host_reports))
    on_only_set = set().union(*(set(h.get("only_on_site", [])) for h in host_reports))
    dup_count = len(index_duplicates)
    has_mismatch = in_only_set or on_only_set or dup_count
    if has_mismatch:
        print(
            f"\n  Mismatch report: {len(host_reports)} host | in-index: {len(in_only_set)} | "
            f"on-site: {len(on_only_set)} | dups: {dup_count}"
        )
    logger.debug(
        "Combined mismatch report saved: %d hosts, %d only-in-index unique, "
        "%d only-on-site unique, %d index duplicates",
        len(host_reports),
        len(in_only_set),
        len(on_only_set),
        dup_count,
    )


def _probe_sites_before_scrape(scraper, idx_mgr=None):
    """Probe configured hosts, show OK/FAILED, and auto-select the first working one.

    This function is always called from a synchronous context (the main menu loop),
    so asyncio.run() is the correct way to execute async coroutines here.
    """
    global ACTIVE_SITE_URL  # pylint: disable=global-statement
    if not SITE_URLS:
        print("\u2717 No configured site URLs; cannot probe hosts.")
        return None
    site_urls = SITE_URLS

    # Load the index once and reuse it for every host cross-check.
    if idx_mgr is None:
        idx_mgr = IndexManager()

    print("\n→ Checking host availability...\n")
    results = _probe_hosts(scraper, site_urls)

    ok_hosts = [entry["site_url"] for entry in results if entry.get("ok")]
    # Every reachable host's catalogue in one concurrent round. Unreachable
    # hosts are left out, so a dead mirror still costs only its probe rather
    # than a second full timeout.
    catalogue = _fetch_catalogue_info_for_hosts(scraper, ok_hosts)

    host_counts = {}
    table_rows = []
    host_reports = []

    for entry in results:
        site_url = entry["site_url"]
        label = _host_label(site_url)
        ok = bool(entry.get("ok"))
        count = None
        idx_count = None
        compare_txt = None

        if ok:
            count, site_slugs = catalogue.get(site_url, (None, set()))
            host_counts[site_url] = count
            if count is not None:
                idx_count, compare_txt, report_entry = _cross_check_index(
                    scraper, site_url, count, idx_mgr=idx_mgr, site_slugs=site_slugs
                )
                if report_entry:
                    host_reports.append(report_entry)

        table_rows.append((label, ok, count, idx_count, compare_txt))

    for line in _format_host_rows(table_rows):
        print(line)

    # Write a single combined mismatch report covering all reachable hosts.
    if host_reports:
        report_path = os.path.join(DATA_DIR, "mismatch_report.json")
        _save_combined_mismatch_report(report_path, idx_mgr, host_reports)

    # Handle duplicate index entries once after all hosts have been checked.
    _, index_duplicates, _ = _collect_index_slugs(idx_mgr)
    if index_duplicates:
        _remove_duplicate_index_entries(idx_mgr, index_duplicates)

    print()
    # Prefer a host that actually served its catalogue. A host can answer the
    # reachability probe and still fail the catalogue fetch, and making that
    # one active means the scrape runs against a host already known not to be
    # serving -- at best a failed run, at worst a truncated catalogue, and a
    # truncated catalogue makes every indexed series look vanished. The counts
    # are already in hand by this point, so reachability alone is the wrong
    # test. Falls back to the probe order when no host served, which is what
    # this did before.
    serving_hosts = [url for url in ok_hosts if host_counts.get(url) is not None]
    preferred = serving_hosts or ok_hosts

    scraper.site_url = preferred[0] if preferred else SITE_URLS[0]
    suffix = "" if ok_hosts else " (default)"
    print(f"→ Active host: {scraper.site_url}{suffix}")
    if scraper.site_url.startswith("http://"):
        print("  ⚠ WARNING: Active host is unencrypted (HTTP) — credentials sent in cleartext.")

    if len(ok_hosts) >= 2:
        counts = [host_counts.get(host) for host in ok_hosts if host_counts.get(host) is not None]
        if len(counts) == len(ok_hosts) and counts:
            match = all(count == counts[0] for count in counts[1:])
            print(f"→ Cross-host counts: match = {match}")
        else:
            print("→ Cross-host counts: match = False")

    ACTIVE_SITE_URL = scraper.site_url
    return scraper.site_url


def _load_ignored_vanished():
    """Load slugs the user has chosen not to delete."""
    path = os.path.join(DATA_DIR, "ignored_vanished.json")
    if not os.path.exists(path):
        return set()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return set(data)
        if isinstance(data, dict):
            return set(data.get("slugs", []))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Could not read ignored vanished file: %s", exc)
    return set()


def _save_ignored_vanished(slugs):
    """Persist slugs the user has chosen not to delete."""
    path = os.path.join(DATA_DIR, "ignored_vanished.json")
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"slugs": sorted(slugs)}, f, indent=2, ensure_ascii=False)
    except OSError as exc:
        logger.warning("Could not save ignored vanished file: %s", exc)


def _find_vanished_to_clean(idx_mgr=None, ignored=None):
    """Return title->slug mapping for vanished entries that can be cleaned.

    Reads the most recent mismatch report. Only slugs reported as
    'only_in_index' by every reachable host are considered vanished.
    Slugs in the ignored set are skipped.
    """
    if ignored is None:
        ignored = _load_ignored_vanished()
    report_path = os.path.join(DATA_DIR, "mismatch_report.json")
    if not os.path.exists(report_path):
        return {}
    try:
        with open(report_path, encoding="utf-8") as f:
            report = json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Could not read mismatch report: %s", exc)
        return {}

    host_reports = report.get("hosts", [])
    if not host_reports:
        return {}

    in_only_sets = [set(h.get("only_in_index", [])) for h in host_reports]
    if not in_only_sets or not any(in_only_sets):
        return {}

    vanished_slugs = set.intersection(*in_only_sets) - ignored
    if not vanished_slugs:
        return {}

    if idx_mgr is None:
        idx_mgr = IndexManager()

    title_by_slug = {}
    for title, series in idx_mgr.series_index.items():
        slug = _extract_slug(series)
        if slug in vanished_slugs:
            title_by_slug[slug] = title

    return title_by_slug


def _notify_vanished_at_startup(idx_mgr=None):
    """Print a notification when vanished entries exist, without prompting."""
    title_by_slug = _find_vanished_to_clean(idx_mgr)
    if not title_by_slug:
        return
    print(f"\n  ⚠ {len(title_by_slug)} series in index are not on any reachable host.")
    print("  → Run option 1 or 2 to verify, then choose whether to remove them.")


def _prompt_clean_vanished(idx_mgr: IndexManager | None = None):
    """Ask to delete vanished entries and update the ignored list.

    Returns True if anything was removed.
    """
    title_by_slug = _find_vanished_to_clean(idx_mgr)
    if not title_by_slug:
        return False

    titles = sorted(title_by_slug.values())
    print(f"\n⚠ {len(titles)} series found in index but not on site:")
    for title in titles:
        print(f"    - {title}")

    choice = input("\nDelete these vanished entries from the index? (y/n/ignore): ").strip().lower()
    if choice == "ignore":
        ignored = _load_ignored_vanished()
        ignored.update(title_by_slug)
        _save_ignored_vanished(ignored)
        print(f"  Ignored {len(title_by_slug)} vanished slug(s) — will not prompt again.")
        return False
    if choice != "y":
        print("  Cancelled — no changes made.")
        return False

    if idx_mgr is None:
        idx_mgr = IndexManager()

    removed = 0
    for title in titles:
        if title in idx_mgr.series_index:
            del idx_mgr.series_index[title]
            removed += 1
    idx_mgr.save_index()
    print(f"\n✓ Removed {removed} vanished series from index.")
    logger.info("Removed %d vanished series from index after scrape: %s", removed, titles[:10])
    return True


def _suggest_something_to_watch(idx_mgr: IndexManager | None = None):
    """Suggest unwatched series from the index, optionally filtered by genre.

    Loads the genre index and presents a list of unwatched series. The user
    can filter by genre or pick from all unwatched series. Five random
    suggestions are shown (or fewer if not enough exist).
    """
    if idx_mgr is None:
        idx_mgr = IndexManager()

    genre_path = os.path.join(DATA_DIR, "genre_index.json")
    genre_labels = {}
    series_genres = {}
    if os.path.exists(genre_path):
        try:
            with open(genre_path, encoding="utf-8") as f:
                genre_data = json.load(f)
            genre_labels = genre_data.get("labels", {})
            series_genres = genre_data.get("series", {})
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Could not read genre index: %s", exc)

    choices = {"all": "All genres / no filter"}
    for key, label in sorted(genre_labels.items()):
        choices[key] = label

    print("\nSuggest something to watch")
    print("Available filters:")
    for key, label in choices.items():
        print(f"  {key}: {label}")

    selected = input("\nEnter genre key (or 'all'): ").strip().lower()
    if selected not in choices:
        print("\u2717 Invalid genre key. Using 'all'.")
        selected = "all"

    candidates = []
    for title, series in idx_mgr.series_index.items():
        if not isinstance(series, dict):
            continue
        watched = series.get("watched_episodes", 0)
        total = series.get("total_episodes", 0)
        if watched == 0 and total > 0:
            if selected != "all":
                genres = series_genres.get(title, [])
                if selected not in genres:
                    continue
            candidates.append(series)

    if not candidates:
        suffix = f" for genre '{choices[selected]}'" if selected != "all" else ""
        print(f"\n\u2713 No unwatched series found{suffix}.")
        return

    random.shuffle(candidates)
    sample = candidates[: max(5, len(candidates))]

    print(f"\n\ud83c\udfb2 {len(sample)} suggestion(s) from {len(candidates)} unwatched series:")
    for i, series in enumerate(sample, 1):
        title = series.get("title", "Unknown")
        link = series.get("url") or series.get("link", "")
        total = series.get("total_episodes", 0)
        genres = series_genres.get(title, [])
        genre_str = ", ".join(genre_labels.get(g, g) for g in genres) if genres else "\u2014"
        print(f"\n  {i}. {title}")
        print(f"     Link: {link}")
        print(f"     Episodes: {total}")
        print(f"     Genres: {genre_str}")
    print("\nCopy a link and open it in your browser to check it out.")


def print_header():
    """Print the application header banner."""
    print("=" * 60)
    print("  BS.TO SERIES SCRAPER & INDEX MANAGER  (httpx)")
    print("=" * 60 + "\n")


def print_scraped_series_status(changes=None):
    """Print episode counts for the most recently updated series."""
    try:
        index_manager = IndexManager()

        if not index_manager.series_index:
            return

        series_list = list(index_manager.series_index.values())
        if not series_list:
            return

        sorted_series = sorted(
            series_list,
            key=lambda s: s.get("last_updated", s.get("added_date", "")),
            reverse=True,
        )

        # Build per-title new episode counts from changes
        new_ep_counts = {}
        if changes and changes.get("new_episodes"):
            for title, _season, _ep in changes["new_episodes"]:
                new_ep_counts[title] = new_ep_counts.get(title, 0) + 1

        display_count = min(5, len(sorted_series))
        if display_count > 0:
            print("\n" + "-" * 70)
            print("EPISODE STATUS (from merged index):")
            print("-" * 70)
            for s in sorted_series[:display_count]:
                watched = s.get("watched_episodes", 0)
                total = s.get("total_episodes", 0)
                season_labels = [str(sn.get("season", "?")) for sn in s.get("seasons", [])]
                season_info = f" [{','.join(season_labels)}]" if season_labels else ""
                title = s.get("title", "")
                new_count = new_ep_counts.get(title, 0)
                new_info = f" ({new_count} new episodes)" if new_count > 0 else ""
                print(f"  \u2022 {title}{season_info}: {watched}/{total}{new_info}")
    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("Error printing series status")


def validate_credentials():
    """Check that username and password are configured."""
    if not USERNAME or not PASSWORD:
        print("\u2717 ERROR: Credentials not configured!")
        print("\nPlease follow these steps:")
        print("1. Copy '.env.example' to '.env'")
        print("2. Add your bs.to username and password")
        print("3. Save the file and try again\n")
        return False
    return True


def check_disk_space(min_mb=100):
    """Check if enough disk space is available."""
    try:
        stat = shutil.disk_usage(DATA_DIR)
        available_mb = stat.free / (1024 * 1024)
        if available_mb < min_mb:
            print("\n✗ WARNING: Low disk space!")
            print(f"  Available: {available_mb:.1f} MB (minimum needed: {min_mb} MB)")
            print("  Please free up disk space before scraping.\n")
            return False
        return True
    except Exception as e:
        logger.warning("Could not check disk space: %s", e)
        return True


def show_menu():
    """Display the interactive main menu."""
    print("\nOptions:")
    print("  1. Scrape all series")
    print("  2. Scrape only NEW series")
    print("  3. Scrape unwatched series")
    print("  4. Generate full report")
    print("  5. Single link / batch add")
    print("  6. Retry failed series from last run")
    print("  7. Watch Stats of Categories")
    print("  8. Suggest something to watch")
    print("  0. Exit\n")


def _check_checkpoint(expected_mode=None):
    """Check for existing checkpoint, prompt to resume or discard.

    When expected_mode is provided, also handles mode mismatches
    (e.g. checkpoint from 'all_series' when user wants 'new_only').

    Returns dict with 'ok' (proceed?) and 'resume' (resume?).
    """
    saved_mode = BsToScraper.get_checkpoint_mode(DATA_DIR)
    if saved_mode is None:
        return {"ok": True, "resume": False}

    saved_label = _MODE_LABELS.get(saved_mode, saved_mode)
    cp_file = os.path.join(DATA_DIR, ".scrape_checkpoint.json")

    if expected_mode is None or saved_mode == expected_mode:
        print(f'\n\u26a0 Checkpoint found from a previous "{saved_label}" run!\n')
        choice = input("Resume from checkpoint? (y/n): ").strip().lower()
        if choice == "y":
            return {"ok": True, "resume": True}
        discard = input("Discard old checkpoint and start fresh? (y/n): ").strip().lower()
        if discard == "y":
            with contextlib.suppress(OSError):
                os.remove(cp_file)
            return {"ok": True, "resume": False}
        return {"ok": False, "resume": False}

    expected_label = _MODE_LABELS.get(expected_mode, expected_mode)
    print(f'\n\u26a0 A checkpoint exists from a different mode: "{saved_label}"')
    print(f'   You are about to run: "{expected_label}"\n')
    discard = input("Discard the old checkpoint and continue? (y/n): ").strip().lower()
    if discard == "y":
        with contextlib.suppress(OSError):
            os.remove(cp_file)
        return {"ok": True, "resume": False}
    return {"ok": False, "resume": False}


def _run_scrape_and_save(
    run_kwargs,
    description,
    success_msg,
    no_data_msg,
    *,
    post_scrape_allow_rescrape=True,
):
    """Create scraper, run, confirm & save.

    Args:
        post_scrape_allow_rescrape: If False, suppress the status
            alert rescrape prompt (used during recursive rescrapes).

    Returns the scraper instance or None on error.
    """
    global ACTIVE_SITE_URL  # pylint: disable=global-statement
    scraper = None
    t_start = time.perf_counter()
    active_site_url = ACTIVE_SITE_URL
    try:
        scraper = BsToScraper()
        if active_site_url:
            scraper.site_url = active_site_url
        else:
            _probe_sites_before_scrape(scraper)
            active_site_url = scraper.site_url
        scraper.run(**run_kwargs)

        # scraper.series_data may include "_error" placeholder entries (kept
        # so checkpoint data stays complete across pauses/resumes) alongside
        # genuine results, so gate on entries that actually succeeded rather
        # than on the raw list -- otherwise a run where every series failed
        # (e.g. all retries failing again) is misreported as a success below.
        successful_data = [s for s in scraper.series_data if isinstance(s, dict) and not s.get("_error")]

        if successful_data:
            if scraper.all_discovered_series is not None:
                all_slugs = set()
                for s in scraper.all_discovered_series:
                    slug = scraper.get_series_slug_from_url(s.get("link", ""))
                    if slug and slug != "unknown":
                        all_slugs.add(slug)
                scope = "new_only" if run_kwargs.get("new_only") else "all"
                idx_mgr = IndexManager()
                show_vanished_series(
                    idx_mgr.series_index,
                    all_slugs,
                    scope,
                    index_file=SERIES_INDEX_FILE,
                    new_data=scraper.series_data,
                    scraper=scraper,
                )
                # Always reload: show_vanished_series may have deleted entries from disk
                idx_mgr.load_index()

            saved, changes = confirm_and_save_changes(
                scraper.series_data,
                description,
                active_site_url=active_site_url,
            )
            result = saved if isinstance(saved, dict) else {}
            if result.get("action") == "rescrape":
                # User already confirmed deletion in the integrity dialog — proceed directly
                n = len(result["urls"])
                print(f"\n→ Deleting {n} critical series from index before rescraping...")
                remove_series_from_index(SERIES_INDEX_FILE, result["titles"])
                print(f"\n→ Rescraping {n} critical series...\n")
                return _run_scrape_and_save(
                    run_kwargs={"url_list": result["urls"], "parallel": False},
                    description=f"Rescrape critical series ({n})",
                    success_msg=f"Critical series rescraping completed! {n} series updated.",
                    no_data_msg="No data scraped for critical series",
                    post_scrape_allow_rescrape=True,
                )
            elif saved:
                print(f"\n✓ {success_msg}")
                print_scraped_series_status(changes)
                if not post_scrape_allow_rescrape:
                    return scraper
                logger.info(success_msg)
                # Final cross-check: scraped count vs index count
                # Only meaningful for full scrapes, not for new_only/batch/retry/single modes.
                if scraper.all_discovered_series is not None and not run_kwargs.get("new_only"):
                    scraped_count = len(successful_data)
                    idx_mgr2 = IndexManager()
                    idx_count = len(idx_mgr2.series_index)
                    if scraped_count == idx_count:
                        print(f"  Index count: {idx_count}  →  match = True")
                    else:
                        diff = scraped_count - idx_count
                        sign = "+" if diff > 0 else ""
                        print(f"  Index count: {idx_count}  →  match = False ({sign}{diff} difference)")
                        print("  → Vanished/renamed series were already checked above.")
                # After full or new-only scrape, offer to clean vanished entries.
                if run_kwargs.get("new_only") or not run_kwargs.get("new_only"):
                    _prompt_clean_vanished(idx_mgr2)
        else:
            if run_kwargs.get("retry_failed") and scraper.failed_links:
                n = len(scraper.failed_links)
                print(f"\n\u2717 All {n} retried series failed again:")
                for entry in scraper.failed_links:
                    title = entry.get("title") or entry.get("url", "?")
                    reason = entry.get("reason", "unknown error")
                    print(f"  \u2022 {title}  \u2192  {reason}")
                print("\n\u2192 Failed list preserved. Use option 6 to retry again.")
                logger.warning(
                    "All %d retried series failed again",
                    n,
                )
            else:
                print(f"\n\u26a0 {no_data_msg}")
                logger.warning(no_data_msg)

        if not scraper.paused:
            scraper.clear_checkpoint()
        else:
            print("\n\u26a0 Scraping was paused \u2014 checkpoint preserved for resume.")

        if scraper.failed_links:
            print(f"\n\u26a0 {len(scraper.failed_links)} series failed during scraping.")
            print("\u2192 Use option 6 (Retry failed series) to rescrape these later.")

        t_elapsed = time.perf_counter() - t_start
        print(f"\n\u23f1 Scrape duration: {t_elapsed / 60:.1f}m ({t_elapsed:.1f}s)")

        return scraper
    except ScrapingPausedError:
        print("\n⚠ Scraping was paused — checkpoint preserved for resume.")
        logger.info("%s paused — returning to menu", description)
        if scraper is not None and scraper.failed_links:
            print(f"\n⚠ {len(scraper.failed_links)} series failed.")
            print("→ Use option 6 (Retry failed series) to rescrape these later.")
        return scraper
    except (KeyboardInterrupt, SystemExit):
        print("\n⚠ Scraping interrupted by Ctrl+C")
        if scraper is not None and scraper.series_data:
            scraper.save_checkpoint(include_data=True)
            scraper.save_failed_series()
            print("✓ Checkpoint saved — resume with option 1/2/3/5/6.")
        if scraper is not None and scraper.failed_links:
            print(f"\n⚠ {len(scraper.failed_links)} series failed.")
            print("→ Use option 6 (Retry failed series) to rescrape these later.")
        return scraper
    except OSError as exc:
        print(f"\n✗ Network error occurred: {exc}")
        logger.error(
            "Network error in %s: %s",
            description,
            exc,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        print(f"\n✗ Unexpected error: {exc}")
        logger.error(
            "Unexpected error in %s: %s",
            description,
            exc,
        )
    return None


def scrape_series():
    """Scrape all series from bs.to."""
    print("\n\u2192 Starting BS.TO scraper (httpx)...\n")

    chk = _check_checkpoint("all_series")
    if not chk["ok"]:
        print("\u2717 Cancelled")
        return
    resume = chk["resume"]

    print("\nScraping mode:")
    print("  1. Single session (slower, but most reliable)")
    print(f"  2. Multi-session (faster, {NUM_WORKERS} parallel sessions)")
    print("  0. Back\n")
    mode_choice = input("Choose mode (0-2) [default: 2]: ").strip() or "2"

    if mode_choice == "0":
        return
    if mode_choice not in ("1", "2"):
        print("\u26a0 Invalid choice, using default (multi-session)")
        use_parallel = True
    else:
        use_parallel = mode_choice == "2"

    _run_scrape_and_save(
        run_kwargs={
            "resume_only": resume,
            "parallel": use_parallel,
        },
        description="Scraped data",
        success_msg="Scraping completed successfully!",
        no_data_msg="No data scraped",
    )


def scrape_new_series():
    """Scrape only new (not-yet-indexed) series."""
    print("\n\u2192 Starting BS.TO scraper \u2014 NEW series only (httpx)...\n")

    chk = _check_checkpoint("new_only")
    if not chk["ok"]:
        print("\u2717 Cancelled")
        return

    _run_scrape_and_save(
        run_kwargs={
            "new_only": True,
            "resume_only": chk["resume"],
        },
        description="New series data",
        success_msg="New series scraping completed successfully!",
        no_data_msg="No new series found",
    )


def scrape_unwatched():
    """Scrape only unwatched/ongoing series from the index."""
    print("\n\u2192 Scrape unwatched series (skipping fully watched)...\n")

    index_manager = IndexManager()
    if not index_manager.series_index:
        print("\u2717 No series in index. Run a full scrape first (option 1).")
        return

    unwatched_urls = []
    skipped = 0
    for series in index_manager.series_index.values():
        total, watched = get_episode_counts(series)
        if 0 < total <= watched:
            skipped += 1
        else:
            url = series.get("url")
            if url:
                unwatched_urls.append(url)

    if not unwatched_urls:
        print("\u2713 All series are fully watched! Nothing to scrape.")
        return

    print(f"  Found {len(unwatched_urls)} unwatched/ongoing series (skipping {skipped} fully watched)\n")

    chk = _check_checkpoint("unwatched")
    if not chk["ok"]:
        print("\u2717 Cancelled")
        return
    resume = chk["resume"]

    print("\nScraping mode:")
    print("  1. Single session (slower, but most reliable)")
    print(f"  2. Multi-session (faster, {NUM_WORKERS} parallel sessions)")
    print("  0. Back\n")
    mode_choice = input("Choose mode (0-2) [default: 2]: ").strip() or "2"

    if mode_choice == "0":
        return
    if mode_choice not in ("1", "2"):
        print("\u26a0 Invalid choice, using default (multi-session)")
        use_parallel = True
    else:
        use_parallel = mode_choice == "2"

    _run_scrape_and_save(
        run_kwargs={
            "url_list": unwatched_urls,
            "resume_only": resume,
            "parallel": use_parallel,
            "checkpoint_mode": "unwatched",
        },
        description=(f"Unwatched series scrape ({len(unwatched_urls)} series)"),
        success_msg=(f"Unwatched series scraping completed! ({len(unwatched_urls)} series)"),
        no_data_msg="No data scraped",
    )


def single_or_batch_add():
    """Add single series by URL or batch from file with auto-detect"""
    default_file = DEFAULT_BATCH_FILE
    print("\n\u2192 Add single link / batch from file")
    print("  \u2022 Paste URL \u2192 scrapes single series")
    print("  \u2022 Enter filename \u2192 uses that file for batch")
    print(f"  \u2022 Press Enter \u2192 uses default ({default_file})")
    print("  \u2022 Type 0   \u2192 back to main menu\n")

    user_input = input(f"Enter [default: {default_file}]: ").strip()

    if user_input == "0":
        return
    if not user_input:
        user_input = default_file

    if user_input.startswith(("http://", "https://")):
        _add_single_series_by_url(user_input)
    else:
        _batch_add_from_file(user_input)


def _add_single_series_by_url(url):
    """Validate a single bs.to series URL and scrape it, retrying on bad input."""
    example_host = (ACTIVE_SITE_URL or SITE_URLS[0]).rstrip("/")

    while True:
        if not url or url == "0":
            return
        if not url.startswith(("http://", "https://")):
            print("\u2717 Invalid URL (must start with http:// or https://)")
            url = input("Enter series URL (or 0 to cancel): ").strip()
            continue
        try:
            parsed_url = urlparse(url)
            if not parsed_url.netloc or parsed_url.netloc not in VALID_SERIES_HOSTS:
                print("\u2717 Invalid series host URL")
                url = input("Enter series URL (or 0 to cancel): ").strip()
                continue
            if not _SERIE_URL_RE.search(parsed_url.path):
                print(f"\u2717 URL must be a valid series page (e.g. {example_host}/serie/Breaking-Bad)")
                url = input("Enter series URL (or 0 to cancel): ").strip()
                continue
        except ValueError:
            print("\u2717 Invalid URL format")
            logger.error("Invalid URL format: %s", url)
            url = input("Enter series URL (or 0 to cancel): ").strip()
            continue
        break

    print("\n\u2192 Scraping single series...\n")

    scraper = _run_scrape_and_save(
        run_kwargs={"single_url": url},
        description="Series data",
        success_msg="Series added/updated successfully!",
        no_data_msg=f"No data scraped for URL: {url}",
    )

    if scraper and scraper.series_data:
        _print_single_series_status(scraper.series_data, url)


def _print_single_series_status(series_data, url):
    """Print watched/total episode counts for one series."""
    series = None
    if isinstance(series_data, list):
        series = next(
            (s for s in series_data if s.get("url") == url or s.get("link") == url),
            series_data[0] if len(series_data) == 1 else None,
        )
    elif isinstance(series_data, dict):
        series = next(
            (s for s in series_data.values() if s.get("url") == url or s.get("link") == url),
            next(iter(series_data.values()), None),
        )

    if not series:
        return

    index_manager = IndexManager()
    source = next(
        (
            s
            for s in index_manager.series_index.values()
            if (s.get("title") == series.get("title") or s.get("link") == series.get("link"))
        ),
        series,
    )

    watched = source.get("watched_episodes", 0)
    total = source.get("total_episodes", 0)
    pct = round((watched / total * 100), 1) if total else 0
    title = source.get("title", url)
    print(f"\nStatus for '{title}': {watched}/{total} episodes watched ({pct}%)")


def generate_report():  # pylint: disable=too-many-locals,too-many-branches
    """Generate and display a comprehensive series report."""
    manager = IndexManager()
    report = manager.get_full_report()

    report_file = os.path.join(DATA_DIR, "series_report.json")

    try:  # pylint: disable=too-many-nested-blocks
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"\n\u2713 Report saved to: {report_file}")

        meta = report["metadata"]
        stats = meta["statistics"]
        term_w = max(shutil.get_terminal_size().columns, 80)

        ongoing_count = report["categories"]["ongoing"]["count"]
        not_started_count = report["categories"]["not_started"]["count"]
        completed = stats.get("completed_count", stats["watched"])
        ongoing_stat = stats.get("ongoing_count", ongoing_count)
        ns_stat = stats.get("not_started_count", not_started_count)
        waiting_count = report["categories"].get("waiting_for_new_episodes", {}).get("count", 0)

        metrics = [
            ("Total series", str(stats["total_series"])),
            ("Completed (100%)", str(completed)),
            ("Ongoing (started)", str(ongoing_stat)),
        ]
        if waiting_count > 0:
            metrics.append(("Waiting for new eps", str(waiting_count)))
        metrics.extend(
            [
                ("Not started (0%)", str(ns_stat)),
                ("Total episodes", str(stats.get("total_episodes", 0))),
                ("Watched episodes", str(stats.get("watched_episodes", 0))),
                ("Unwatched episodes", str(stats.get("unwatched_episodes", 0))),
                (
                    "Avg episodes/series",
                    str(stats.get("average_episodes_per_series", 0)),
                ),
                ("Average completion", f"{stats.get('average_completion', 0):.1f}%"),
                ("Generated", str(meta["generated"])),
            ]
        )

        label_w = min(max((len(m[0]) for m in metrics), default=0), term_w // 2 - 4)
        value_w = min(max((len(m[1]) for m in metrics), default=0), term_w // 2 - 4)
        table_w = label_w + value_w + 3
        sep = "─" * table_w

        def _trunc(text, width):
            return text if len(text) <= width else text[: width - 1] + "…"

        print("\n" + sep)
        print("  STATISTICS")
        print("  " + "─" * (table_w - 2))
        for label, value in metrics:
            line = f"  {_trunc(label, label_w):<{label_w}}  {_trunc(value, value_w):<{value_w}}"
            print(line.rstrip())
        print(sep)

        dist = stats.get("completion_distribution", {})
        if dist:
            print("\n  COMPLETION BREAKDOWN")
            bucket_w = max((len(k) for k in dist), default=0)
            count_w = max((len(str(v)) for v in dist.values()), default=0)
            print(f"    {'Bucket':<{bucket_w}}  {'Count':<{count_w}}")
            print(f"    {'─' * bucket_w}  {'─' * count_w}")
            for bucket, count in dist.items():
                row = f"    {bucket:<{bucket_w}}  {str(count):<{count_w}}"
                print(row.rstrip())

        ongoing_titles = report["categories"]["ongoing"]["titles"]
        if ongoing_count > 0:
            print("\n  ONGOING SERIES")
            idx_w = len(str(min(ongoing_count, 10)))
            title_w = max((len(t) for t in ongoing_titles[:10]), default=0)
            print(f"    {'#':<{idx_w}}  {'Title':<{title_w}}")
            print(f"    {'─' * idx_w}  {'─' * title_w}")
            for i, title in enumerate(ongoing_titles[:10], 1):
                row = f"    {i:<{idx_w}}  {title:<{title_w}}"
                print(row.rstrip())
            if ongoing_count > 10:
                print(f"    ... and {ongoing_count - 10} more")

            # Exported automatically: appending is additive and de-duplicated,
            # so there is nothing to lose by doing it, and the old prompt only
            # stood between the report and an up-to-date batch file.
            _export_ongoing_urls(manager, ongoing_titles, active_site_url=ACTIVE_SITE_URL)

        print("\n  SAVED TO")
        print(f"    {report_file}")
        print(sep + "\n")

    except Exception as exc:  # pylint: disable=broad-exception-caught
        print(f"\n\u2717 Failed to generate report: {exc}")
        logger.error("Failed to generate report: %s", exc)


def _report_batch_export(added, skipped, urls_file, noun):
    """Print what the automatic batch-file export actually did.

    The export runs without asking now, so the terminal has to say plainly
    what changed -- which URLs were added, and how many were already there.
    """
    if added:
        print(f"\n✓ Added {len(added)} {noun} URL(s) to {urls_file}:")
        for url in added[:10]:
            print(f"    + {url}")
        if len(added) > 10:
            print(f"    ... and {len(added) - 10} more")
    else:
        print(f"\n✓ {urls_file} already lists every ongoing {noun} — nothing added")
    if skipped:
        print(f"  ({skipped} already listed)")
    print(f"  → Use option 5 (Single link / batch add) to rescrape these {noun}")
    print("  (existing entries are kept — delete the file first for a clean replace)")


def _append_urls_to_batch_file(urls_file, urls):
    """Add URLs to the batch file without discarding what is already there.

    Exporting used to open the file in "w" mode, which replaced the whole
    file -- a hand-curated list, comments and all, vanished the moment
    someone answered yes to the export prompt. Appending keeps that work.

    A URL already present is skipped, including one that is commented out:
    commenting a line was a deliberate decision to skip that series, and an
    export should not quietly undo it. To start clean, delete the file and
    export again.

    Returns (added_urls, skipped_count).
    """
    existing_lines = []
    known = set()
    if os.path.exists(urls_file):
        with open(urls_file, encoding="utf-8") as fh:
            existing_lines = fh.read().splitlines()
        for line in existing_lines:
            stripped = line.strip().lstrip("#").strip()
            if stripped:
                known.add(stripped)

    fresh = [u for u in urls if u not in known]
    if fresh:
        body = list(existing_lines)
        while body and not body[-1].strip():
            body.pop()
        body.extend(fresh)
        with open(urls_file, "w", encoding="utf-8") as fh:
            fh.write("\n".join(body) + "\n")
    return fresh, len(urls) - len(fresh)


def _export_ongoing_urls(manager, ongoing_titles, active_site_url=None):
    """Export ongoing series URLs to series_urls.txt."""
    try:
        urls = []
        base_url = (active_site_url or ACTIVE_SITE_URL or SITE_URLS[0]).rstrip("/")
        for title in ongoing_titles:
            series_data = manager.series_index.get(title, {})
            url = series_data.get("url") or series_data.get("link")
            if url:
                if not url.startswith("http"):
                    url = f"{base_url}{url}"
                urls.append(url)
        if urls:
            urls_file = DEFAULT_BATCH_FILE
            added, skipped = _append_urls_to_batch_file(urls_file, urls)
            _report_batch_export(added, skipped, urls_file, "series")
            logger.info(
                "Appended %d URLs to %s (%d already present)",
                len(added),
                urls_file,
                skipped,
            )
        else:
            print("\n\u26a0 Could not extract URLs from ongoing series")
            logger.warning("Could not extract URLs from ongoing series")
    except Exception as exc:  # pylint: disable=broad-exception-caught
        print(f"\n\u2717 Failed to export URLs: {exc}")
        logger.error("Failed to export URLs: %s", exc)


def _batch_add_from_file(file_path):
    """Read series URLs from a text file and scrape them."""
    if not os.path.exists(file_path):
        print(f"\u2717 File not found: {file_path}")
        return

    try:
        with open(file_path, encoding="utf-8") as f:
            raw_lines = [line.strip() for line in f if line.strip()]
            urls = []
            skipped = []
            for line in raw_lines:
                if not line.startswith(("http://", "https://")):
                    skipped.append(line)
                    continue
                parsed = urlparse(line)
                if parsed.netloc not in VALID_SERIES_HOSTS:
                    skipped.append(line)
                    continue
                if not _SERIE_URL_RE.search(parsed.path):
                    skipped.append(line)
                    continue
                urls.append(line)
    except OSError as exc:
        print(f"\u2717 Failed to read file: {exc}")
        logger.error(
            "Failed to read file %s: %s",
            file_path,
            exc,
        )
        return

    if skipped:
        print(f"\u26a0 Skipped {len(skipped)} line(s) that were not valid series URLs")
        logger.warning("Skipped %d non-URL lines in %s", len(skipped), file_path)

    if not urls:
        print("\u2717 No valid URLs found in file")
        return

    print(f"\n\u2713 Found {len(urls)} URL(s) in file")
    print("\nURLs to process:")
    for url in urls:
        print(f"  \u2022 {url}")

    confirm = input("\nProceed with batch add? (y/n): ").strip().lower()
    if confirm != "y":
        print("\u2717 Cancelled")
        return

    print("\n\u2192 Starting batch scraper...\n")

    chk = _check_checkpoint("batch")
    if not chk["ok"]:
        print("\u2717 Cancelled")
        return

    _run_scrape_and_save(
        run_kwargs={
            "url_list": urls,
            "resume_only": chk["resume"],
        },
        description=f"Batch data ({len(urls)} series)",
        success_msg=(f"Batch add completed! {len(urls)} series processed."),
        no_data_msg="No data scraped",
    )


def retry_failed_series():
    """Load and retry previously failed series."""
    print("\n\u2192 Retry failed series from last run\n")

    temp_scraper = BsToScraper()
    failed_list = temp_scraper.load_failed_series()
    if not failed_list:
        print("\u2713 No failed series found. Nothing to retry.")
        return
    print(f"\u2713 Found {len(failed_list)} failed series from last run")
    print("\u2192 Starting retry in sequential mode (for reliability)...")

    chk = _check_checkpoint("retry")
    if not chk["ok"]:
        print("\u2717 Cancelled")
        return

    _run_scrape_and_save(
        run_kwargs={
            "retry_failed": True,
            "parallel": False,
            "resume_only": chk["resume"],
        },
        description="Retry data",
        success_msg="Retry completed successfully!",
        no_data_msg="No data to retry",
    )


def main():
    """Application entry point — run interactive menu loop."""
    # Already done at config import time; repeated here so the entry point
    # does not depend on that import side effect.
    configure_console()

    idx_mgr = IndexManager()
    index_count = len(idx_mgr.series_index)
    print(f"✓ Index loaded ({os.path.abspath(SERIES_INDEX_FILE)}) ({index_count:,} entries)\n")

    print_header()
    if not validate_credentials():
        sys.exit(1)

    print(f"\u2713 Credentials found for user: {USERNAME}\n")

    if not check_disk_space():
        response = input("Continue anyway? (y/n): ").strip().lower()
        if response != "y":
            sys.exit(1)

    scraper = BsToScraper()
    _probe_sites_before_scrape(scraper, idx_mgr=idx_mgr)
    _notify_vanished_at_startup(idx_mgr)

    while True:
        show_menu()
        choice = input("Enter your choice (0-8): ").strip()
        if not choice.isdigit() or not 0 <= int(choice) <= 8:
            print("\u2717 Invalid choice. Please enter a number between 0 and 8.")
            continue

        if choice in ["1", "2", "3", "5", "6", "7", "8"] and not check_disk_space():
            print("\u26a0 Aborting due to low disk space.")
            continue

        if choice == "1":
            scrape_series()
        elif choice == "2":
            scrape_new_series()
        elif choice == "3":
            scrape_unwatched()
        elif choice == "4":
            generate_report()
        elif choice == "5":
            single_or_batch_add()
        elif choice == "6":
            retry_failed_series()
        elif choice == "7":
            genre_stats.menu(ACTIVE_SITE_URL)
        elif choice == "8":
            _suggest_something_to_watch(idx_mgr)
        elif choice == "0":
            print("\n\u2713 Goodbye!\n")
            break


def _run_cli() -> int:
    """Run main() and return a process exit code.

    Separate from main() so tests and packaging entry points can call it.
    """
    try:
        main()
    except KeyboardInterrupt:
        print("\n  interrupted.")
        return 130
    except SystemExit as exc:
        if exc.code is None:
            return 0
        return exc.code if isinstance(exc.code, int) else 1
    return 0


if __name__ == "__main__":
    sys.exit(_run_cli())
