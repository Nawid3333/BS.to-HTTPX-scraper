#!/usr/bin/env python3
"""
BS.TO Series Scraper & Index Manager.

Scrapes watched TV series from bs.to and maintains a local JSON index.
Uses httpx (no browser needed) with multi-session architecture.
Supports checkpoint resume, batch URL import, and interactive
change confirmation.
"""

import json
import logging
import logging.handlers
import os
import re
import sys
from urllib.parse import urlparse

# Ensure project root is on sys.path so imports work from any cwd
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# pylint: disable=wrong-import-position
from config.config import (  # noqa: E402
    USERNAME, PASSWORD, DATA_DIR, LOG_FILE, NUM_WORKERS,
)
from src.scraper import (  # noqa: E402
    BsToScraper,
)
from src.index_manager import (  # noqa: E402
    IndexManager, confirm_and_save_changes,
    show_vanished_series, get_episode_counts,
)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler(
            LOG_FILE, maxBytes=10*1024*1024, backupCount=5,
        ),
        logging.StreamHandler()
    ]
)
logging.getLogger('urllib3').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

_SERIE_URL_RE = re.compile(r'/serie/[^/]+')

_MODE_LABELS = {
    'all_series': 'Scrape all series',
    'new_only': 'Scrape new series only',
    'unwatched': 'Scrape unwatched series',
    'single': 'Add single series by URL',
    'batch': 'Batch add from file',
    'retry': 'Retry failed series',
}


def print_header():
    """Print the application header banner."""
    print("\n" + "="*60)
    print("  BS.TO SERIES SCRAPER & INDEX MANAGER  (httpx)")
    print("="*60 + "\n")


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
            key=lambda s: s.get(
                'last_updated', s.get('added_date', '')
            ),
            reverse=True
        )

        # Build per-title new episode counts from changes
        new_ep_counts = {}
        if changes and changes.get('new_episodes'):
            for title, _season, _ep in changes['new_episodes']:
                new_ep_counts[title] = (
                    new_ep_counts.get(title, 0) + 1
                )

        display_count = min(5, len(sorted_series))
        if display_count > 0:
            print("\n" + "-"*70)
            print("EPISODE STATUS (from merged index):")
            print("-"*70)
            for s in sorted_series[:display_count]:
                watched = s.get('watched_episodes', 0)
                total = s.get('total_episodes', 0)
                season_labels = [
                    str(sn.get('season', '?'))
                    for sn in s.get('seasons', [])
                ]
                season_info = (
                    f" [{','.join(season_labels)}]"
                    if season_labels else ""
                )
                title = s.get('title', '')
                new_count = new_ep_counts.get(title, 0)
                new_info = (
                    f" ({new_count} new episodes)"
                    if new_count > 0 else ""
                )
                print(
                    f"  \u2022 {title}{season_info}:"
                    f" {watched}/{total}{new_info}"
                )
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


def show_menu():
    """Display the interactive main menu."""
    print("\nOptions:")
    print("  1. Scrape series from bs.to (requires login)")
    print("  2. Scrape only NEW series (faster)")
    print("  3. Scrape unwatched series")
    print("  4. Generate full report")
    print("  5. Batch add series from text file")
    print("  6. Retry failed series from last run")
    print("  7. Add single series by URL")
    print("  8. Pause current scraping (in another terminal)")
    print("  9. Exit\n")


def _check_checkpoint(expected_mode):
    """Check for existing checkpoint, prompt to resume or discard.

    Returns dict with 'ok' (proceed?) and 'resume' (resume?).
    """
    saved_mode = BsToScraper.get_checkpoint_mode(DATA_DIR)
    if saved_mode is None:
        return {'ok': True, 'resume': False}

    saved_label = _MODE_LABELS.get(saved_mode, saved_mode)
    expected_label = _MODE_LABELS.get(expected_mode, expected_mode)
    cp_file = os.path.join(DATA_DIR, '.scrape_checkpoint.json')

    if saved_mode == expected_mode:
        print(
            f'\n\u26a0 Checkpoint found from a previous '
            f'"{saved_label}" run!\n'
        )
        choice = input(
            "Resume from checkpoint? (y/n): "
        ).strip().lower()
        if choice == 'y':
            return {'ok': True, 'resume': True}
        discard = input(
            "Discard old checkpoint and start fresh? (y/n): "
        ).strip().lower()
        if discard == 'y':
            try:
                os.remove(cp_file)
            except OSError:
                pass
            return {'ok': True, 'resume': False}
        return {'ok': False, 'resume': False}

    print(
        f'\n\u26a0 A checkpoint exists from a different '
        f'mode: "{saved_label}"'
    )
    print(f'   You are about to run: "{expected_label}"\n')
    discard = input(
        "Discard the old checkpoint and continue? (y/n): "
    ).strip().lower()
    if discard == 'y':
        try:
            os.remove(cp_file)
        except OSError:
            pass
        return {'ok': True, 'resume': False}
    return {'ok': False, 'resume': False}


def _run_scrape_and_save(  # pylint: disable=too-many-branches
    run_kwargs, description, success_msg, no_data_msg,
):
    """Create scraper, run, confirm & save.

    Returns the scraper instance or None on error.
    """
    scraper = None
    try:
        scraper = BsToScraper()
        scraper.run(**run_kwargs)

        if scraper.series_data:
            if scraper.all_discovered_series is not None:
                all_slugs = set()
                for s in scraper.all_discovered_series:
                    slug = scraper.get_series_slug_from_url(
                        s.get('link', '')
                    )
                    if slug and slug != 'unknown':
                        all_slugs.add(slug)
                scope = (
                    'new_only'
                    if run_kwargs.get('new_only')
                    else 'all'
                )
                idx_mgr = IndexManager()
                show_vanished_series(
                    idx_mgr.series_index, all_slugs, scope,
                )

            saved, changes = confirm_and_save_changes(
                scraper.series_data, description,
            )
            if saved:
                print(f"\n\u2713 {success_msg}")
                print_scraped_series_status(changes)
                logger.info(success_msg)
        else:
            print(f"\n\u26a0 {no_data_msg}")
            logger.warning(no_data_msg)

        if not scraper.paused:
            scraper.clear_checkpoint()
        else:
            print(
                "\n\u26a0 Scraping was paused "
                "\u2014 checkpoint preserved for resume."
            )

        if scraper.failed_links:
            print(
                f"\n\u26a0 {len(scraper.failed_links)} series "
                "failed during scraping."
            )
            print(
                "\u2192 Use option 6 (Retry failed series) "
                "to rescrape these later."
            )

        return scraper
    except (KeyboardInterrupt, SystemExit):
        print("\n\u26a0 Scraping interrupted by Ctrl+C")
        if scraper is not None and scraper.series_data:
            saved, changes = confirm_and_save_changes(
                scraper.series_data, description,
            )
            if saved:
                count = len(scraper.series_data)
                print(
                    f"\n\u2713 Partial data saved ({count} series)"
                )
                logger.info(
                    "%s interrupted \u2014 partial data saved",
                    description,
                )
        if scraper is not None and scraper.failed_links:
            print(
                f"\n\u26a0 {len(scraper.failed_links)} "
                "series failed."
            )
            print(
                "\u2192 Use option 6 (Retry failed series) "
                "to rescrape these later."
            )
        return scraper
    except OSError as exc:
        print(f"\n\u2717 Network error occurred: {exc}")
        logger.error(
            "Network error in %s: %s", description, exc,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        print(f"\n\u2717 Unexpected error: {exc}")
        logger.error(
            "Unexpected error in %s: %s", description, exc,
        )
    return None


def scrape_series():
    """Scrape all series from bs.to."""
    print("\n\u2192 Starting BS.TO scraper (httpx)...\n")

    chk = _check_checkpoint('all_series')
    if not chk['ok']:
        print("\u2717 Cancelled")
        return
    resume = chk['resume']

    print("\nScraping mode:")
    print("  1. Single session (slower, but most reliable)")
    print(
        f"  2. Multi-session (faster, "
        f"{NUM_WORKERS} parallel sessions)"
    )
    print("  0. Back\n")
    mode_choice = (
        input("Choose mode (0-2) [default: 2]: ").strip() or '2'
    )

    if mode_choice == '0':
        return
    if mode_choice not in ('1', '2'):
        print("\u26a0 Invalid choice, using default (multi-session)")
        use_parallel = True
    else:
        use_parallel = mode_choice == '2'

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
    print(
        "\n\u2192 Starting BS.TO scraper "
        "\u2014 NEW series only (httpx)...\n"
    )

    chk = _check_checkpoint('new_only')
    if not chk['ok']:
        print("\u2717 Cancelled")
        return

    _run_scrape_and_save(
        run_kwargs={
            "new_only": True,
            "resume_only": chk['resume'],
        },
        description="New series data",
        success_msg="New series scraping completed successfully!",
        no_data_msg="No new series found",
    )


def scrape_unwatched():
    """Scrape only unwatched/ongoing series from the index."""
    print(
        "\n\u2192 Scrape unwatched series "
        "(skipping fully watched)...\n"
    )

    index_manager = IndexManager()
    if not index_manager.series_index:
        print(
            "\u2717 No series in index. "
            "Run a full scrape first (option 1)."
        )
        return

    unwatched_urls = []
    skipped = 0
    for series in index_manager.series_index.values():
        total, watched = get_episode_counts(series)
        if 0 < total <= watched:
            skipped += 1
        else:
            url = series.get('url')
            if url:
                unwatched_urls.append(url)

    if not unwatched_urls:
        print(
            "\u2713 All series are fully watched! "
            "Nothing to scrape."
        )
        return

    print(
        f"  Found {len(unwatched_urls)} unwatched/ongoing "
        f"series (skipping {skipped} fully watched)\n"
    )

    chk = _check_checkpoint('unwatched')
    if not chk['ok']:
        print("\u2717 Cancelled")
        return
    resume = chk['resume']

    print("\nScraping mode:")
    print("  1. Single session (slower, but most reliable)")
    print(
        f"  2. Multi-session (faster, "
        f"{NUM_WORKERS} parallel sessions)"
    )
    print("  0. Back\n")
    mode_choice = (
        input("Choose mode (0-2) [default: 2]: ").strip() or '2'
    )

    if mode_choice == '0':
        return
    if mode_choice not in ('1', '2'):
        print("\u26a0 Invalid choice, using default (multi-session)")
        use_parallel = True
    else:
        use_parallel = mode_choice == '2'

    _run_scrape_and_save(
        run_kwargs={
            "url_list": unwatched_urls,
            "resume_only": resume,
            "parallel": use_parallel,
        },
        description=(
            f"Unwatched series scrape "
            f"({len(unwatched_urls)} series)"
        ),
        success_msg=(
            "Unwatched series scraping completed! "
            f"({len(unwatched_urls)} series)"
        ),
        no_data_msg="No data scraped",
    )


def add_series_by_url():
    """Prompt for a single bs.to series URL and scrape it."""
    print("\n\u2192 Add single series by URL")
    print("  Example: https://bs.to/serie/Breaking-Bad")
    print("  0. Back\n")

    while True:
        url = input("Enter series URL: ").strip()
        if not url or url == '0':
            return
        if not url.startswith(("http://", "https://")):
            print(
                "\u2717 Invalid URL "
                "(must start with http:// or https://)"
            )
            continue
        try:
            parsed_url = urlparse(url)
            if (
                not parsed_url.netloc
                or 'bs.to' not in parsed_url.netloc
            ):
                print("\u2717 Invalid bs.to URL")
                continue
            if not _SERIE_URL_RE.search(parsed_url.path):
                print(
                    "\u2717 URL must be a valid bs.to series page "
                    "(e.g. https://bs.to/serie/Breaking-Bad)"
                )
                continue
        except ValueError:
            print("\u2717 Invalid URL format")
            logger.error("Invalid URL format: %s", url)
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
            (
                s for s in series_data
                if s.get('url') == url or s.get('link') == url
            ),
            series_data[0] if len(series_data) == 1 else None,
        )
    elif isinstance(series_data, dict):
        series = next(
            (
                s for s in series_data.values()
                if s.get('url') == url or s.get('link') == url
            ),
            next(iter(series_data.values()), None),
        )

    if not series:
        return

    index_manager = IndexManager()
    source = next(
        (
            s for s in index_manager.series_index.values()
            if (
                s.get('title') == series.get('title')
                or s.get('link') == series.get('link')
            )
        ),
        series,
    )

    watched = source.get('watched_episodes', 0)
    total = source.get('total_episodes', 0)
    pct = round((watched / total * 100), 1) if total else 0
    title = source.get('title', url)
    print(
        f"\nStatus for '{title}': "
        f"{watched}/{total} episodes watched ({pct}%)"
    )


def generate_report():  # pylint: disable=too-many-locals,too-many-branches
    """Generate and display a comprehensive series report."""
    manager = IndexManager()
    report = manager.get_full_report()

    report_file = os.path.join(DATA_DIR, 'series_report.json')

    try:  # pylint: disable=too-many-nested-blocks
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"\n\u2713 Report saved to: {report_file}")

        meta = report['metadata']
        stats = meta['statistics']
        print(f"\n  Total series:       {stats['total_series']}")
        completed = stats.get(
            'completed_count', stats['watched']
        )
        print(f"  Completed (100%):   {completed}")

        ongoing_count = report['categories']['ongoing']['count']
        not_started_count = (
            report['categories']['not_started']['count']
        )
        ongoing_stat = stats.get('ongoing_count', ongoing_count)
        ns_stat = stats.get('not_started_count', not_started_count)
        print(f"  Ongoing (started):  {ongoing_stat}")
        print(f"  Not started (0%):   {ns_stat}")
        print(f"  Generated:          {meta['generated']}")

        if ongoing_count > 0:
            print(
                f"\n\U0001f4fa ONGOING SERIES ({ongoing_count}):"
            )
            ongoing_titles = (
                report['categories']['ongoing']['titles']
            )
            for title in ongoing_titles[:10]:
                print(f"  \u2022 {title}")
            if ongoing_count > 10:
                print(
                    f"  ... and {ongoing_count - 10} more\n"
                )

            prompt = (
                f"\nExport {ongoing_count} ongoing series "
                "URLs to series_urls.txt? (y/n): "
            )
            export = input(prompt).strip().lower()
            if export == 'y':
                _export_ongoing_urls(manager, ongoing_titles)

    except Exception as exc:  # pylint: disable=broad-exception-caught
        print(f"\n\u2717 Failed to generate report: {exc}")
        logger.error("Failed to generate report: %s", exc)


def _export_ongoing_urls(manager, ongoing_titles):
    """Export ongoing series URLs to series_urls.txt."""
    try:
        urls = []
        for title in ongoing_titles:
            series_data = manager.series_index.get(title, {})
            url = (
                series_data.get('url')
                or series_data.get('link')
            )
            if url:
                if not url.startswith('http'):
                    url = f"https://bs.to{url}"
                urls.append(url)

        if urls:
            urls_file = os.path.join(
                os.path.dirname(__file__), 'series_urls.txt',
            )
            with open(urls_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(urls) + '\n')
            print(
                f"\n\u2713 Exported {len(urls)} URLs "
                "to series_urls.txt"
            )
            print(
                "  \u2192 Use option 5 (Batch add) "
                "to rescrape these series"
            )
            logger.info(
                "Exported %d URLs to series_urls.txt",
                len(urls),
            )
        else:
            print(
                "\n\u26a0 Could not extract URLs "
                "from ongoing series"
            )
            logger.warning(
                "Could not extract URLs from ongoing series"
            )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        print(f"\n\u2717 Failed to export URLs: {exc}")
        logger.error("Failed to export URLs: %s", exc)


def batch_add_series_from_file():
    """Read series URLs from a text file and scrape them."""
    print("\n\u2192 Batch add series from text file")
    print("  The file should contain one URL per line")
    print("  Example format:")
    print("    https://bs.to/serie/Breaking-Bad")
    print("  (type 0 to go back)")

    default_file = os.path.join(
        os.path.dirname(__file__), 'series_urls.txt',
    )
    file_path = input(
        "Enter file path [default: series_urls.txt]: "
    ).strip().strip("\"'")
    if file_path == '0':
        return
    if not file_path:
        file_path = default_file

    if not os.path.exists(file_path):
        print(f"\u2717 File not found: {file_path}")
        return

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            urls = [
                line.strip() for line in f
                if line.strip() and line.strip().startswith('http')
            ]
    except OSError as exc:
        print(f"\u2717 Failed to read file: {exc}")
        logger.error(
            "Failed to read file %s: %s", file_path, exc,
        )
        return

    if not urls:
        print("\u2717 No valid URLs found in file")
        return

    print(f"\n\u2713 Found {len(urls)} URL(s) in file")
    print("\nURLs to process:")
    for url in urls:
        print(f"  \u2022 {url}")

    confirm = input(
        "\nProceed with batch add? (y/n): "
    ).strip().lower()
    if confirm != 'y':
        print("\u2717 Cancelled")
        return

    print("\n\u2192 Starting batch scraper...\n")

    chk = _check_checkpoint('batch')
    if not chk['ok']:
        print("\u2717 Cancelled")
        return

    _run_scrape_and_save(
        run_kwargs={
            "url_list": urls,
            "resume_only": chk['resume'],
        },
        description=f"Batch data ({len(urls)} series)",
        success_msg=(
            f"Batch add completed! {len(urls)} series processed."
        ),
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
    print(
        f"\u2713 Found {len(failed_list)} failed series "
        "from last run"
    )
    print(
        "\u2192 Starting retry in sequential mode "
        "(for reliability)..."
    )

    chk = _check_checkpoint('retry')
    if not chk['ok']:
        print("\u2717 Cancelled")
        return

    _run_scrape_and_save(
        run_kwargs={
            "retry_failed": True,
            "parallel": False,
            "resume_only": chk['resume'],
        },
        description="Retry data",
        success_msg="Retry completed successfully!",
        no_data_msg="No data to retry",
    )


def pause_scraping():
    """Create a pause file to signal workers to stop."""
    pause_file = os.path.join(DATA_DIR, '.pause_scraping')
    try:
        with open(pause_file, 'w', encoding='utf-8') as f:
            f.write('PAUSE')
        print(
            f"\n\u2713 Pause file created: {pause_file}\n"
            "Workers will pause at next checkpoint."
        )
        logger.info("Pause file created: %s", pause_file)
    except OSError as exc:
        print(f"\n\u2717 Failed to create pause file: {exc}")
        logger.error(
            "Failed to create pause file %s: %s",
            pause_file, exc,
        )


def main():
    """Application entry point — run interactive menu loop."""
    print_header()
    if not validate_credentials():
        sys.exit(1)

    print(f"\u2713 Credentials found for user: {USERNAME}\n")

    while True:
        show_menu()
        choice = input("Enter your choice (1-9): ").strip()
        if (
            not choice.isdigit()
            or not 1 <= int(choice) <= 9
        ):
            print(
                "\u2717 Invalid choice. "
                "Please enter a number between 1 and 9."
            )
            continue
        if choice == '1':
            scrape_series()
        elif choice == '2':
            scrape_new_series()
        elif choice == '3':
            scrape_unwatched()
        elif choice == '4':
            generate_report()
        elif choice == '5':
            batch_add_series_from_file()
        elif choice == '6':
            retry_failed_series()
        elif choice == '7':
            add_series_by_url()
        elif choice == '8':
            pause_scraping()
        elif choice == '9':
            print("\n\u2713 Goodbye!\n")
            break


if __name__ == "__main__":
    main()
