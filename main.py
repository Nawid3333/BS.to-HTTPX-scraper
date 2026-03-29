#!/usr/bin/env python3
"""
BS.TO Series Scraper & Index Manager

Scrapes watched TV series from bs.to and maintains a local JSON index.
Uses httpx (no browser needed) with multi-session architecture.
Supports checkpoint resume, batch URL import, and interactive change confirmation.
"""

import json
import logging
import logging.handlers
import os
import re
import subprocess
import sys
from urllib.parse import urlparse

# Ensure project root is on sys.path so imports work from any working directory
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from config.config import USERNAME, PASSWORD, DATA_DIR, LOG_FILE
from src.scraper import BsToScraper
from src.index_manager import IndexManager, confirm_and_save_changes, show_vanished_series

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=10*1024*1024, backupCount=5),
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
    'single': 'Add single series by URL',
    'batch': 'Batch add from file',
    'retry': 'Retry failed series',
}


def print_header():
    print("\n" + "="*60)
    print("  BS.TO SERIES SCRAPER & INDEX MANAGER  (httpx)")
    print("="*60 + "\n")


def print_scraped_series_status():
    """Print episode counts for the most recently updated series."""
    try:
        index_manager = IndexManager()
        
        if not index_manager.series_index:
            return
        
        # Show top updated/new series with their complete episode counts
        series_list = list(index_manager.series_index.values())
        if not series_list:
            return
        
        # Sort by last updated (most recent first)
        sorted_series = sorted(
            series_list,
            key=lambda s: s.get('last_updated', s.get('added_date', '')),
            reverse=True
        )
        
        # Show first 5 updated series
        display_count = min(5, len(sorted_series))
        if display_count > 0:
            print("\n" + "-"*70)
            print("EPISODE STATUS (from merged index):")
            print("-"*70)
            for s in sorted_series[:display_count]:
                watched = s.get('watched_episodes', 0)
                total = s.get('total_episodes', 0)
                percent = round((watched / total * 100), 1) if total else 0
                season_labels = [str(sn.get('season', '?')) for sn in s.get('seasons', [])]
                season_info = f" [{','.join(season_labels)}]" if season_labels else ""
                print(f"  • {s.get('title')}{season_info}: {watched}/{total} episodes ({percent}%)")
    except Exception as e:
        logger.error(f"Error printing series status: {e}")


def validate_credentials():
    if not USERNAME or not PASSWORD:
        print("✗ ERROR: Credentials not configured!")
        print("\nPlease follow these steps:")
        print("1. Copy '.env.example' to '.env'")
        print("2. Add your bs.to username and password to the .env file")
        print("3. Save the file and try again\n")
        return False
    return True


def show_menu():
    print("\nOptions:")
    print("  1. Scrape series from bs.to (requires login)")
    print("  2. Scrape only NEW series (faster)")
    print("  3. Add single series by URL")
    print("  4. Generate full report")
    print("  5. Batch add series from text file")
    print("  6. Retry failed series from last run")
    print("  7. Pause current scraping (in another terminal)")
    print("  8. Exit\n")


def _check_checkpoint(expected_mode):
    """Check for an existing checkpoint and prompt the user to resume or discard.

    Returns dict with 'ok' (proceed?) and 'resume' (resume from checkpoint?).
    """
    saved_mode = BsToScraper.get_checkpoint_mode(DATA_DIR)
    if saved_mode is None:
        return {'ok': True, 'resume': False}

    saved_label = _MODE_LABELS.get(saved_mode, saved_mode)
    expected_label = _MODE_LABELS.get(expected_mode, expected_mode)

    checkpoint_file = os.path.join(DATA_DIR, '.scrape_checkpoint.json')

    if saved_mode == expected_mode:
        print(f"\n⚠ Checkpoint found from a previous \"{saved_label}\" run!\n")
        choice = input("Resume from checkpoint? (y/n): ").strip().lower()
        if choice == 'y':
            return {'ok': True, 'resume': True}
        # User declined resume — ask whether to discard
        discard = input("Discard old checkpoint and start fresh? (y/n): ").strip().lower()
        if discard == 'y':
            try:
                os.remove(checkpoint_file)
            except OSError:
                pass
            return {'ok': True, 'resume': False}
        return {'ok': False, 'resume': False}
    else:
        print(f"\n⚠ A checkpoint exists from a different mode: \"{saved_label}\"")
        print(f"   You are about to run: \"{expected_label}\"\n")
        discard = input("Discard the old checkpoint and continue? (y/n): ").strip().lower()
        if discard == 'y':
            try:
                os.remove(checkpoint_file)
            except OSError:
                pass
            return {'ok': True, 'resume': False}
        return {'ok': False, 'resume': False}


def _run_scrape_and_save(run_kwargs, description, success_msg, no_data_msg):
    """Create scraper, run, confirm & save. Returns the scraper or None on error."""
    try:
        scraper = BsToScraper()
        scraper.run(**run_kwargs)

        if scraper.series_data:
            # Show vanished-series notification if full catalogue was fetched
            if scraper.all_discovered_series is not None:
                all_slugs = set()
                for s in scraper.all_discovered_series:
                    slug = scraper.get_series_slug_from_url(s.get('link', ''))
                    if slug and slug != 'unknown':
                        all_slugs.add(slug)
                scope = 'new_only' if run_kwargs.get('new_only') else 'all'
                index_manager = IndexManager()
                show_vanished_series(index_manager.series_index, all_slugs, scope)

            if confirm_and_save_changes(scraper.series_data, description):
                print(f"\n✓ {success_msg}")
                print_scraped_series_status()
                logger.info(success_msg)
        else:
            print(f"\n⚠ {no_data_msg}")
            logger.warning(no_data_msg)

        # Scraping completed normally — safe to clear checkpoint now that user has confirmed/declined
        scraper.clear_checkpoint()

        if scraper.failed_links:
            print(f"\n⚠ {len(scraper.failed_links)} series failed during scraping.")
            print("→ Use option 6 (Retry failed series) to rescrape these later.")

        return scraper
    except (KeyboardInterrupt, SystemExit):
        print(f"\n⚠ Scraping interrupted by Ctrl+C")
        if 'scraper' in locals() and scraper.series_data:
            if confirm_and_save_changes(scraper.series_data, description):
                print(f"\n✓ Partial data saved ({len(scraper.series_data)} series)")
                logger.info(f"{description} interrupted — partial data saved")
        if 'scraper' in locals() and scraper.failed_links:
            print(f"\n⚠ {len(scraper.failed_links)} series failed.")
            print("→ Use option 6 (Retry failed series) to rescrape these later.")
        return scraper if 'scraper' in locals() else None
    except OSError as e:
        print(f"\n✗ Network error occurred: {str(e)}")
        logger.error(f"Network error in {description}: {e}")
    except Exception as e:
        print(f"\n✗ Unexpected error: {str(e)}")
        logger.error(f"Unexpected error in {description}: {e}")
    return None


def scrape_series():
    print("\n→ Starting BS.TO scraper (httpx)...\n")

    # Check checkpoint mode
    chk = _check_checkpoint('all_series')
    if not chk['ok']:
        print("✗ Cancelled")
        return
    resume = chk['resume']

    print("\nScraping mode:")
    print("  1. Single session (slower, but most reliable)")
    print("  2. Multi-session (faster, 12 parallel sessions)")
    print("  0. Back\n")
    mode_choice = input("Choose mode (0-2) [default: 2]: ").strip() or '2'

    if mode_choice == '0':
        return
    if mode_choice not in ['1', '2']:
        print("⚠ Invalid choice, using default (multi-session)")
        use_parallel = True
    else:
        use_parallel = mode_choice == '2'

    _run_scrape_and_save(
        run_kwargs=dict(resume_only=resume, parallel=use_parallel),
        description="Scraped data",
        success_msg="Scraping completed successfully!",
        no_data_msg="No data scraped",
    )


def scrape_new_series():
    print("\n→ Starting BS.TO scraper — NEW series only (httpx)...\n")

    chk = _check_checkpoint('new_only')
    if not chk['ok']:
        print("✗ Cancelled")
        return

    _run_scrape_and_save(
        run_kwargs=dict(new_only=True, resume_only=chk['resume']),
        description="New series data",
        success_msg="New series scraping completed successfully!",
        no_data_msg="No new series found",
    )


def add_series_by_url():
    print("\n→ Add single series by URL")
    print("  Example: https://bs.to/serie/Breaking-Bad")
    print("  0. Back\n")
    
    while True:
        url = input("Enter series URL: ").strip()
        # Validate URL format
        if not url or url == '0':
            return
        if not url.startswith(("http://", "https://")):
            print("✗ Invalid URL (must start with http:// or https://)")
            continue
        try:
            parsed_url = urlparse(url)
            if not parsed_url.netloc or 'bs.to' not in parsed_url.netloc:
                print("✗ Invalid bs.to URL")
                continue
            # Require /serie/ in path
            if not _SERIE_URL_RE.search(parsed_url.path):
                print("✗ URL must be a valid bs.to series page (e.g. https://bs.to/serie/Breaking-Bad)")
                continue
        except Exception as e:
            print("✗ Invalid URL format")
            logger.error(f"Invalid URL format: {url}, error: {e}")
            continue
        break
    
    print("\n→ Scraping single series...\n")

    scraper = _run_scrape_and_save(
        run_kwargs=dict(single_url=url),
        description="Series data",
        success_msg="Series added/updated successfully!",
        no_data_msg=f"No data scraped for URL: {url}",
    )

    if scraper and scraper.series_data:
        _print_single_series_status(scraper.series_data, url)


def _print_single_series_status(series_data, url):
    """Print watched/total episode counts for one series."""
    # Find the series in scraped data
    series = None
    if isinstance(series_data, list):
        series = next(
            (s for s in series_data if s.get('url') == url or s.get('link') == url),
            series_data[0] if len(series_data) == 1 else None,
        )
    elif isinstance(series_data, dict):
        series = next(
            (s for s in series_data.values() if s.get('url') == url or s.get('link') == url),
            next(iter(series_data.values()), None),
        )

    if not series:
        return

    # Prefer merged index data over raw scraped data
    index_manager = IndexManager()
    source = next(
        (s for s in index_manager.series_index.values()
         if s.get('title') == series.get('title') or s.get('link') == series.get('link')),
        series,
    )

    watched = source.get('watched_episodes', 0)
    total = source.get('total_episodes', 0)
    percent = round((watched / total * 100), 1) if total else 0
    print(f"\nStatus for '{source.get('title', url)}': {watched}/{total} episodes watched ({percent}%)")


def generate_report():
    manager = IndexManager()
    report = manager.get_full_report()
    
    report_file = os.path.join(DATA_DIR, 'series_report.json')
    
    try:
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"\n✓ Report saved to: {report_file}")
        
        # Display summary
        meta = report['metadata']
        stats = meta['statistics']
        print(f"\n  Total series:       {stats['total_series']}")
        print(f"  Completed (100%):   {stats.get('completed_count', stats['watched'])}")
        
        ongoing_count = report['categories']['ongoing']['count']
        not_started_count = report['categories']['not_started']['count']
        print(f"  Ongoing (started):  {stats.get('ongoing_count', ongoing_count)}")
        print(f"  Not started (0%):   {stats.get('not_started_count', not_started_count)}")
        print(f"  Generated:          {meta['generated']}")
        
        # Show ongoing series (started but incomplete)
        if ongoing_count > 0:
            print(f"\n📺 ONGOING SERIES ({ongoing_count}):")
            ongoing_titles = report['categories']['ongoing']['titles']
            for title in ongoing_titles[:10]:
                print(f"  • {title}")
            if ongoing_count > 10:
                print(f"  ... and {ongoing_count - 10} more\n")
            
            # Offer to export ongoing series URLs to series_urls.txt
            export = input(f"\nExport {ongoing_count} ongoing series URLs to series_urls.txt? (y/n): ").strip().lower()
            if export == 'y':
                try:
                    # Get URLs for ongoing series
                    urls = []
                    ongoing_titles = report['categories']['ongoing']['titles']
                    for title in ongoing_titles:
                        series_data = manager.series_index.get(title, {})
                        url = series_data.get('url') or series_data.get('link')
                        if url:
                            # Full URL if needed
                            if not url.startswith('http'):
                                url = f"https://bs.to{url}"
                            urls.append(url)
                    
                    if urls:
                        # Write to series_urls.txt
                        urls_file = os.path.join(os.path.dirname(__file__), 'series_urls.txt')
                        with open(urls_file, 'w', encoding='utf-8') as f:
                            f.write('\n'.join(urls) + '\n')
                        print(f"\n✓ Exported {len(urls)} URLs to series_urls.txt")
                        print(f"  → Use option 5 (Batch add) to rescrape these series")
                        logger.info(f"Exported {len(urls)} URLs to series_urls.txt")
                    else:
                        print("\n⚠ Could not extract URLs from ongoing series")
                        logger.warning("Could not extract URLs from ongoing series for export")
                except Exception as e:
                    print(f"\n✗ Failed to export URLs: {str(e)}")
                    logger.error(f"Failed to export URLs: {e}")
        
    except Exception as e:
        print(f"\n✗ Failed to generate report: {str(e)}")
        logger.error(f"Failed to generate report: {e}")


def batch_add_series_from_file():
    print("\n→ Batch add series from text file")
    print("  The file should contain one URL per line")
    print("  Example format:")
    print("    https://bs.to/serie/Breaking-Bad")
    print("  (type 0 to go back)")
    
    default_file = os.path.join(os.path.dirname(__file__), 'series_urls.txt')
    file_path = input(f"Enter file path [default: series_urls.txt]: ").strip().strip("\"'")    
    if file_path == '0':
        return
    if not file_path:
        file_path = default_file
    
    if not os.path.exists(file_path):
        print(f"✗ File not found: {file_path}")
        return
    
    # Read URLs from file
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f if line.strip() and line.strip().startswith('http')]
    except Exception as e:
        print(f"✗ Failed to read file: {str(e)}")
        logger.error(f"Failed to read file {file_path}: {e}")
        return
    
    if not urls:
        print("✗ No valid URLs found in file")
        return
    
    print(f"\n✓ Found {len(urls)} URL(s) in file")
    print("\nURLs to process:")
    for url in urls:
        print(f"  • {url}")
    
    confirm = input("\nProceed with batch add? (y/n): ").strip().lower()
    if confirm != 'y':
        print("✗ Cancelled")
        return
    
    print("\n→ Starting batch scraper...\n")

    chk = _check_checkpoint('batch')
    if not chk['ok']:
        print("✗ Cancelled")
        return

    _run_scrape_and_save(
        run_kwargs=dict(url_list=urls, resume_only=chk['resume']),
        description=f"Batch data ({len(urls)} series)",
        success_msg=f"Batch add completed! {len(urls)} series processed.",
        no_data_msg="No data scraped",
    )

def retry_failed_series():
    print("\n→ Retry failed series from last run\n")

    temp_scraper = BsToScraper()
    failed_list = temp_scraper.load_failed_series()
    if not failed_list:
        print("✓ No failed series found. Nothing to retry.")
        return
    print(f"✓ Found {len(failed_list)} failed series from last run")
    print("→ Starting retry in sequential mode (for reliability)...")

    chk = _check_checkpoint('retry')
    if not chk['ok']:
        print("✗ Cancelled")
        return

    _run_scrape_and_save(
        run_kwargs=dict(retry_failed=True, parallel=False, resume_only=chk['resume']),
        description="Retry data",
        success_msg="Retry completed successfully!",
        no_data_msg="No data to retry",
    )


def pause_scraping():
    pause_file = os.path.join(DATA_DIR, '.pause_scraping')
    try:
        with open(pause_file, 'w', encoding='utf-8') as f:
            f.write('PAUSE')
        print(f"\n✓ Pause file created: {pause_file}\nWorkers will pause at next checkpoint.")
        logger.info(f"Pause file created: {pause_file}")
    except Exception as e:
        print(f"\n✗ Failed to create pause file: {str(e)}")
        logger.error(f"Failed to create pause file {pause_file}: {e}")


def main():
    print_header()
    if not validate_credentials():
        sys.exit(1)
        
    print(f"✓ Credentials found for user: {USERNAME}\n")
    
    while True:
        show_menu()
        choice = input("Enter your choice (1-8): ").strip()
        if not choice.isdigit() or not (1 <= int(choice) <= 8):
            print("✗ Invalid choice. Please enter a number between 1 and 8.")
            continue
        if choice == '1':
            scrape_series()
        elif choice == '2':
            scrape_new_series()
        elif choice == '3':
            add_series_by_url()
        elif choice == '4':
            generate_report()
        elif choice == '5':
            batch_add_series_from_file()
        elif choice == '6':
            retry_failed_series()
        elif choice == '7':
            pause_scraping()
        elif choice == '8':
            print("\n✓ Goodbye!\n")
            break


if __name__ == "__main__":
    main()
