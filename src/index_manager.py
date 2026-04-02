"""Index manager for the BS.TO series scraper.

Handles series index storage, change detection, merging,
and reporting for the local JSON-based series database.
"""

import json
import logging
import os
import re
import shutil
import tempfile
from collections import defaultdict
from datetime import datetime

from config.config import SERIES_INDEX_FILE, DATA_DIR
from src.scraper import BsToScraper

logger = logging.getLogger(__name__)


def _create_file_backup(filepath):
    """Create a backup of a file (up to 3 generations kept)."""
    if not os.path.exists(filepath):
        return
    try:
        backup_dir = os.path.dirname(filepath)
        filename = os.path.basename(filepath)

        # Remove oldest backup if 3 already exist
        for i in range(3, 10):
            old_backup = os.path.join(
                backup_dir, f"{filename}.bak{i}",
            )
            if os.path.exists(old_backup):
                try:
                    os.remove(old_backup)
                except OSError:
                    pass

        # Shift existing backups
        for i in range(2, 0, -1):
            src = os.path.join(
                backup_dir, f"{filename}.bak{i}",
            )
            dst = os.path.join(
                backup_dir, f"{filename}.bak{i+1}",
            )
            if os.path.exists(src):
                try:
                    shutil.move(src, dst)
                except OSError:
                    pass

        # Create new backup
        backup_path = os.path.join(
            backup_dir, f"{filename}.bak1",
        )
        shutil.copy2(filepath, backup_path)
        logger.debug("Created backup: %s", backup_path)
    except Exception:  # pylint: disable=broad-exception-caught
        logger.warning(
            "Could not create backup of %s", filepath,
        )


def _atomic_write_json(filepath, data):
    """Write JSON to file atomically via temp file + os.replace.

    Creates backup before writing to prevent data loss on corruption.
    Prevents corrupted files if the process is killed mid-write.
    """
    dirpath = os.path.dirname(filepath)
    os.makedirs(dirpath, exist_ok=True)

    # Create backup of existing file before overwriting
    if os.path.exists(filepath):
        _create_file_backup(filepath)

    fd, tmp_path = tempfile.mkstemp(dir=dirpath, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, filepath)
    except Exception:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise


_SEASON_NUMBER_RE = re.compile(
    r'(staffel|season|s)\s*(\d+)', re.IGNORECASE,
)


def _validate_series_entry(series, title=''):
    """Validate series entry structure. Returns True if valid."""
    if not isinstance(series, dict):
        logger.warning(
            "Skipping invalid series entry (not dict): %s",
            title,
        )
        return False
    if not series.get('url'):
        logger.warning(
            "Skipping series '%s' - missing 'url' field", title,
        )
        return False
    seasons = series.get('seasons')
    if seasons is not None and not isinstance(seasons, list):
        logger.warning(
            "Skipping series '%s' - 'seasons' must be list, "
            "got %s", title, type(seasons),
        )
        return False
    # Validate episode structure within seasons
    for season in (seasons or []):
        if not isinstance(season, dict):
            continue
        episodes = season.get('episodes')
        if episodes is not None and not isinstance(episodes, list):
            logger.warning(
                "Series '%s' season '%s' has invalid "
                "episodes type",
                title, season.get('season', '?'),
            )
            season['episodes'] = []
    return True


def _find_series(new_data, title):
    """Look up a series by title in either a dict or list."""
    if isinstance(new_data, dict):
        return new_data.get(title)
    if isinstance(new_data, list):
        return next(
            (s for s in new_data if s.get('title') == title),
            None,
        )
    return None


def _get_season_stats(series, season_label):
    """Get (total_episodes, watched_episodes) for a season."""
    if not series:
        return 0, 0
    for s in series.get('seasons', []):
        if s.get('season') == season_label:
            eps = s.get('episodes', [])
            return len(eps), sum(
                1 for ep in eps if ep.get('watched', False)
            )
    return 0, 0


def get_episode_counts(series):
    """Get (total, watched) episode counts across all seasons."""
    total = 0
    watched = 0
    for season in series.get('seasons', []):
        eps = season.get('episodes', [])
        total += len(eps)
        watched += sum(
            1 for ep in eps if ep.get('watched', False)
        )
    return total, watched


def _order_series_entry(series):
    """Return a stable series dict with metadata before seasons."""
    ordered = {
        'title': series.get('title', ''),
        'link': series.get('link', ''),
        'url': series.get('url', ''),
        'total_seasons': series.get(
            'total_seasons', len(series.get('seasons', [])),
        ),
        'total_episodes': series.get('total_episodes', 0),
        'watched_episodes': series.get('watched_episodes', 0),
        'unwatched_episodes': series.get(
            'unwatched_episodes',
            (series.get('total_episodes', 0)
             - series.get('watched_episodes', 0)),
        ),
        'seasons': series.get('seasons', []),
    }
    if 'added_date' in series:
        ordered['added_date'] = series['added_date']
    if 'last_updated' in series:
        ordered['last_updated'] = series['last_updated']
    return ordered


def paginate_list(items, formatter, page_size=50):
    """Print items with pagination; Enter=next page, q=skip."""
    if not items:
        return
    total = len(items)
    idx = 0
    while idx < total:
        end = min(idx + page_size, total)
        for item in items[idx:end]:
            print(formatter(item))
        idx = end
        if idx < total:
            choice = input(
                f"  ({idx}/{total}) Enter = more, q = skip: "
            ).strip().lower()
            if choice == 'q':
                print(
                    f"  ... skipped {total - idx} remaining"
                )
                break


def format_season_ep(season_label, ep_num):
    """Format season/episode for display (e.g. S1E5)."""
    match = _SEASON_NUMBER_RE.search(str(season_label))
    if match:
        return f"S{match.group(2)}E{ep_num}"
    if str(season_label).strip().isdigit():
        return f"S{season_label}E{ep_num}"
    return f"[{season_label}] Ep {ep_num}"


def group_episodes_by_season(
    episode_list, new_data, prefix='[+]',
):
    """Group (title, season, ep_num) tuples by season for display."""
    grouped = defaultdict(list)

    for item in episode_list:
        title, season, ep_num = item[0], item[1], item[2]
        grouped[(title, season)].append(ep_num)

    # Convert to dict for new_data lookup
    if isinstance(new_data, list):
        new_data_dict = {s.get('title'): s for s in new_data}
    elif isinstance(new_data, dict):
        new_data_dict = new_data
    else:
        new_data_dict = {}

    result = []
    for (title, season), ep_nums in sorted(grouped.items()):
        series = new_data_dict.get(title, {})
        total_in_season, watched_in_season = (
            _get_season_stats(series, season)
        )
        if total_in_season > 0:
            result.append(
                f"  {prefix} {title} [{season}]: "
                f"{watched_in_season}/{total_in_season} episodes"
            )
        else:
            for ep_num in sorted(ep_nums):
                result.append(
                    f"  {prefix} {title} "
                    f"{format_season_ep(season, ep_num)}"
                )

    return result


def _extract_slug(entry):
    """Extract series slug from an index entry's link or url.

    Delegates to BsToScraper.get_series_slug_from_url.
    Returns None if extraction fails.
    """
    if not isinstance(entry, dict):
        return None
    for field in ('link', 'url'):
        value = entry.get(field, '')
        if not value or not isinstance(value, str):
            continue
        slug = BsToScraper.get_series_slug_from_url(value)
        if slug and slug != 'unknown':
            return slug
    return None


def show_vanished_series(
    old_data, all_discovered_slugs, scrape_scope,
):
    """Show notification about indexed series not found in scrape.

    Purely informational - no index changes are made.

    Args:
        old_data: dict of old series index (title -> series)
        all_discovered_slugs: set of slugs from current scrape
        scrape_scope: 'all'/'new_only' for full catalogue,
            None/other suppresses notification

    Returns:
        list of vanished series titles, or empty list
    """
    if scrape_scope not in ('all', 'new_only'):
        return []

    vanished = []
    corrupt_entries = []

    for title, entry in old_data.items():
        slug = _extract_slug(entry)
        if slug is None:
            corrupt_entries.append(title)
            continue

        if slug not in all_discovered_slugs:
            vanished.append(title)

    if corrupt_entries:
        count = len(corrupt_entries)
        print(
            f"\n\u26a0 {count} index entry(s) have "
            "corrupt/missing URL data:"
        )
        for t in corrupt_entries[:10]:
            print(f"  \u2022 {t}")
        if count > 10:
            print(f"  ... and {count - 10} more")
        print(
            "  These entries were skipped during "
            "vanished-series detection."
        )
        logger.warning(
            "Corrupt URL data in %d index entries: %s",
            count, corrupt_entries[:5],
        )

    if vanished:
        separator = '\u2500' * 70
        print(f"\n{separator}")
        print(
            f"  [INFO] {len(vanished)} previously indexed "
            "series NOT found in current scrape:"
        )
        print(separator)
        for title in vanished[:20]:
            print(f"  \u2022 {title}  (not found on bs.to)")
        if len(vanished) > 20:
            print(f"  ... and {len(vanished) - 20} more")
        print(separator)
        print(
            "  These series are preserved unchanged "
            "in the index."
        )
        logger.info(
            "Vanished series notification: %d series "
            "not found in scrape scope '%s'",
            len(vanished), scrape_scope,
        )

    return vanished


def detect_changes(  # pylint: disable=too-many-branches
    old_data, new_data,
):
    """Detect changes between old and new data.

    Returns dict of change lists.
    Does not track 'removed series' because partial scrapes would
    incorrectly show all non-scraped series as removed.
    Handles missing/None fields safely.
    """
    changes = {
        "new_series": [],
        "new_episodes": [],
        "newly_watched": [],
        "newly_unwatched": [],
    }

    # Handle empty or invalid data
    if not old_data:
        old_data = []
    if not new_data:
        new_data = []

    old_titles = (
        set(old_data.keys())
        if isinstance(old_data, dict)
        else {
            s.get('title') for s in (old_data or [])
            if s and s.get('title')
        }
    )
    new_titles = (
        set(new_data.keys())
        if isinstance(new_data, dict)
        else {
            s.get('title') for s in (new_data or [])
            if s and s.get('title')
        }
    )

    # Convert to dicts if needed
    if isinstance(old_data, list):
        old_data = {
            s.get('title'): s for s in (old_data or [])
            if s and s.get('title')
        }
    if isinstance(new_data, list):
        new_data = {
            s.get('title'): s for s in (new_data or [])
            if s and s.get('title')
        }

    # New series (in scraped data but not in existing index)
    for title in new_titles - old_titles:
        if title:
            changes["new_series"].append(title)

    # Episode changes for existing series
    for title in old_titles & new_titles:
        _detect_episode_changes(
            title, old_data, new_data, changes,
        )

    return changes


def _detect_episode_changes(  # pylint: disable=too-many-branches
    title, old_data, new_data, changes,
):
    """Detect episode-level changes for a single series."""
    try:
        old_series = old_data.get(title, {})
        new_series = new_data.get(title, {})

        if not old_series or not isinstance(old_series, dict):
            return
        if not new_series or not isinstance(new_series, dict):
            return

        # Build old episode map
        old_eps = {}
        for season in old_series.get('seasons', []):
            if not season or not isinstance(season, dict):
                continue
            s_label = season.get('season', '')
            for ep in season.get('episodes', []):
                if not ep or not isinstance(ep, dict):
                    continue
                ep_num = ep.get('number')
                if ep_num is not None:
                    old_eps[(s_label, str(ep_num))] = bool(
                        ep.get('watched', False)
                    )

        # Check new episodes and watch status changes
        for season in new_series.get('seasons', []):
            if not season or not isinstance(season, dict):
                continue
            s_label = season.get('season', '')
            for ep in season.get('episodes', []):
                if not ep or not isinstance(ep, dict):
                    continue
                ep_num = ep.get('number')
                if ep_num is None:
                    continue
                ep_key = (s_label, str(ep_num))
                new_watched = bool(ep.get('watched', False))

                if ep_key not in old_eps:
                    changes["new_episodes"].append(
                        (title, s_label, ep_num),
                    )
                elif old_eps[ep_key] != new_watched:
                    if not old_eps[ep_key] and new_watched:
                        changes["newly_watched"].append(
                            (title, s_label, ep_num),
                        )
                    elif old_eps[ep_key] and not new_watched:
                        changes["newly_unwatched"].append(
                            (title, s_label, ep_num),
                        )
    except Exception:  # pylint: disable=broad-exception-caught
        logger.debug(
            "Error detecting changes for '%s'", title,
            exc_info=True,
        )


def show_changes(
    changes,
    include_unwatched=True,
    include_watched=True,
    new_data=None,
):
    """Print formatted change summary with pagination."""
    total = 0
    for k, v in changes.items():
        if k == 'newly_unwatched' and not include_unwatched:
            continue
        if k == 'newly_watched' and not include_watched:
            continue
        total += len(v)
    if total == 0:
        return 0

    print("\n" + "="*70)
    print("  CHANGES DETECTED")
    print("="*70)

    if changes["new_series"]:
        count = len(changes['new_series'])
        print(f"\n[NEW SERIES] ({count})")

        def format_new_series(title):
            """Format a new series entry for display."""
            if not new_data:
                return f"  + {title}"
            series = _find_series(new_data, title)
            if not series:
                return f"  + {title}"
            watched = series.get('watched_episodes', 0)
            total_ep = series.get('total_episodes', 0)
            return (
                f"  + {title}: "
                f"{watched}/{total_ep} watched"
            )

        paginate_list(
            changes["new_series"], format_new_series,
        )

    if changes["new_episodes"]:
        ep_count = len(changes['new_episodes'])
        if new_data:
            grouped_lines = group_episodes_by_season(
                [
                    (x[0], x[1], x[2])
                    for x in changes["new_episodes"]
                ],
                new_data,
            )
            print(f"\n[NEW EPISODES] ({ep_count})")
            paginate_list(grouped_lines, lambda line: line)
        else:
            print(
                f"\n[NEW EPISODES] ({ep_count}) "
                "[ungrouped fallback]"
            )
            paginate_list(
                changes["new_episodes"],
                lambda x: f"  + {x[0]} [{x[1]}] Ep {x[2]}",
            )

    if changes["newly_watched"] and include_watched:
        count = len(changes['newly_watched'])
        print(f"\n[NEWLY WATCHED] ({count} episodes)")
        watched_lines = group_episodes_by_season(
            changes["newly_watched"], new_data,
        )
        paginate_list(watched_lines, lambda line: line)

    if changes.get("newly_unwatched") and include_unwatched:
        count = len(changes['newly_unwatched'])
        print(
            f"\n[SITE REPORTS UNWATCHED] ({count} episodes)"
        )
        unwatched_lines = group_episodes_by_season(
            changes["newly_unwatched"], new_data,
            prefix='[!]',
        )
        paginate_list(unwatched_lines, lambda line: line)

    print("\n" + "="*70)
    return total


def _read_index_json():
    """Read and parse series_index.json from disk.

    Returns the raw parsed data (list or dict), or None on error.
    Handles missing file, corrupt JSON, and I/O errors.
    """
    if not os.path.exists(SERIES_INDEX_FILE):
        logger.info(
            "No existing index found at %s",
            SERIES_INDEX_FILE,
        )
        return None
    try:
        with open(
            SERIES_INDEX_FILE, 'r', encoding='utf-8',
        ) as f:
            data = json.load(f)
        if not isinstance(data, (list, dict)):
            print(
                "\u26a0 Index file is not a valid list "
                "or dict, ignoring."
            )
            logger.error(
                "Index file is not a valid list or dict."
            )
            return None
        logger.info(
            "Loaded index from %s (%d entries)",
            SERIES_INDEX_FILE, len(data),
        )
        return data
    except json.JSONDecodeError as exc:
        print(f"[ERROR] Index file corrupted: {exc}")
        logger.error("Index file corrupted: %s", exc)
        return None
    except OSError as exc:
        print(f"[ERROR] Cannot read index file: {exc}")
        logger.error("Cannot read index file: %s", exc)
        return None


def _load_existing_index():
    """Load current series index from disk (list or empty)."""
    data = _read_index_json()
    return data if data is not None else []


def _prompt_watch_status_changes(  # pylint: disable=too-many-branches
    changes, new_dict,
):
    """Prompt user to confirm watched/unwatched flips.

    Returns (allow_watched, allow_unwatched).
    """
    allow_watched = False
    allow_unwatched = False

    if changes["newly_watched"]:
        count = len(changes['newly_watched'])
        logger.info(
            "Prompting user to confirm marking "
            "%d episodes as watched.", count,
        )
        print(
            f"\n[OK] {count} episode(s) would change "
            "from UNWATCHED to WATCHED"
        )
        print(
            "   (manual confirmation required "
            "for all watched changes)"
        )
        print("\n" + "-"*70)
        grouped = defaultdict(list)
        for x in changes["newly_watched"]:
            grouped[(x[0], x[1])].append(x[2])
        for (title, season), ep_nums in grouped.items():
            series = new_dict.get(title)
            total_in_season, watched_in_season = (
                _get_season_stats(series, season)
            )
            if total_in_season > 0:
                print(
                    f"  [+] {title} [{season}]: "
                    f"{watched_in_season}/{total_in_season} "
                    "episodes"
                )
            else:
                print(
                    f"  [+] {title} [{season}]: "
                    f"{len(ep_nums)} episode(s)"
                )
        print("-"*70)
        answer = input(
            "\nAllow these episodes to be marked "
            "as WATCHED? (y/n): "
        ).strip().lower()
        if answer == 'y':
            allow_watched = True
            logger.info("User allowed watched changes.")
        else:
            print(
                "  \u2192 Watched changes will be ignored "
                "(episodes stay unwatched)"
            )
            logger.info("User denied watched changes.")

    if changes["newly_unwatched"]:
        count = len(changes['newly_unwatched'])
        logger.info(
            "Prompting user to confirm marking "
            "%d episodes as unwatched.", count,
        )
        print(
            f"\n[WARN] {count} episode(s) would change "
            "from WATCHED to UNWATCHED"
        )
        print(
            "   (manual confirmation required "
            "for all unwatched changes)"
        )
        print("\n" + "-"*70)
        grouped = defaultdict(list)
        for x in changes["newly_unwatched"]:
            grouped[(x[0], x[1])].append(x[2])
        for (title, season), ep_nums in grouped.items():
            series = new_dict.get(title)
            total_in_season, watched_in_season = (
                _get_season_stats(series, season)
            )
            if total_in_season > 0:
                print(
                    f"  [!] {title} [{season}]: "
                    f"{watched_in_season}/{total_in_season} "
                    "episodes"
                )
            else:
                print(
                    f"  [!] {title} [{season}]: "
                    f"{len(ep_nums)} episode(s)"
                )
        print("-"*70)
        answer = input(
            "\nAllow these episodes to be marked "
            "as UNWATCHED? (y/n): "
        ).strip().lower()
        if answer == 'y':
            allow_unwatched = True
            logger.info("User allowed unwatched changes.")
        else:
            print(
                "  \u2192 Unwatched changes will be ignored "
                "(episodes stay watched)"
            )
            logger.info("User denied unwatched changes.")

    return allow_watched, allow_unwatched


def _merge_series_data(
    old_data, new_dict, allow_watched, allow_unwatched,
):
    """Merge new scraped data into the existing index.

    Preserves all existing series and only applies watched/unwatched
    flips when the corresponding flag is True.
    Returns merged dict {title: series}.
    """
    if isinstance(old_data, list):
        merged = {s.get('title'): s for s in old_data}
    else:
        merged = dict(old_data)

    for title, new_entry in new_dict.items():
        if title not in merged:
            new_entry['added_date'] = (
                datetime.now().isoformat()
            )
            merged[title] = _order_series_entry(new_entry)
            continue

        _merge_existing_series(
            merged, title, new_entry,
            allow_watched, allow_unwatched,
        )

    return merged


def _merge_existing_series(  # pylint: disable=too-many-locals
    merged, title, new_entry, allow_watched, allow_unwatched,
):
    """Merge a single new series entry into existing data."""
    old_entry = merged[title]
    old_seasons = {
        s.get('season'): s
        for s in old_entry.get('seasons', [])
    }

    for new_season in new_entry.get('seasons', []):
        season_label = new_season.get('season')
        if season_label in old_seasons:
            old_eps = {
                str(ep.get('number')): ep
                for ep in old_seasons[season_label].get(
                    'episodes', [],
                )
            }
            for new_ep in new_season.get('episodes', []):
                ep_num = str(new_ep.get('number'))
                if ep_num in old_eps:
                    old_w = old_eps[ep_num].get(
                        'watched', False,
                    )
                    new_w = new_ep.get('watched', False)
                    if allow_watched and not old_w and new_w:
                        new_ep['watched'] = True
                    elif (
                        allow_unwatched
                        and old_w
                        and not new_w
                    ):
                        new_ep['watched'] = False
                    else:
                        new_ep['watched'] = old_w
                old_eps[ep_num] = new_ep
            old_seasons[season_label]['episodes'] = sorted(
                old_eps.values(),
                key=lambda e: e.get('number', 0),
            )
        else:
            old_seasons[season_label] = new_season

    old_entry['seasons'] = list(old_seasons.values())
    old_entry['total_seasons'] = len(old_entry['seasons'])
    total_eps, watched_eps = get_episode_counts(old_entry)
    old_entry['watched_episodes'] = watched_eps
    old_entry['total_episodes'] = total_eps
    old_entry['unwatched_episodes'] = (
        old_entry['total_episodes']
        - old_entry['watched_episodes']
    )
    old_entry['url'] = new_entry.get(
        'url', old_entry.get('url'),
    )
    old_entry['last_updated'] = datetime.now().isoformat()
    merged[title] = _order_series_entry(old_entry)


def confirm_and_save_changes(new_data, description="data"):
    """Show changes, prompt, merge, and save.

    Returns (saved: bool, changes: dict | None).
    """
    old_data = _load_existing_index()

    if isinstance(new_data, list):
        new_dict = {s.get('title'): s for s in new_data}
    else:
        new_dict = dict(new_data)

    changes = detect_changes(old_data, new_dict)
    logger.info(
        "Detected changes: %s",
        {k: len(v) for k, v in changes.items()},
    )

    total_detected = sum(len(v) for v in changes.values())
    if total_detected == 0:
        print(
            f"\n\u2713 {description} already up to date."
        )
        logger.info(
            "No changes to save for %s.", description,
        )
        return True, changes

    show_changes(
        changes, include_unwatched=True,
        include_watched=True, new_data=new_dict,
    )

    allow_watched, allow_unwatched = (
        _prompt_watch_status_changes(changes, new_dict)
    )

    if not allow_watched:
        changes["newly_watched"] = []
    if not allow_unwatched:
        changes["newly_unwatched"] = []

    merged = _merge_series_data(
        old_data, new_dict, allow_watched, allow_unwatched,
    )

    main_changes = sum(
        len(v) for k, v in changes.items()
        if k != 'newly_unwatched'
    )
    if allow_unwatched:
        main_changes += len(changes['newly_unwatched'])

    if main_changes == 0:
        print(
            f"\n\u2713 {description} already up to date."
        )
        logger.info(
            "No changes to save for %s.", description,
        )
        return True, changes

    answer = input(
        "\nSave these changes? (y/n): "
    ).strip().lower()
    if answer != 'y':
        print(
            "\u2717 Changes discarded. Nothing saved."
        )
        logger.info(
            "User discarded changes. Nothing saved."
        )
        return False, None

    try:
        series_list = [
            _order_series_entry(series)
            for series in merged.values()
        ]
        _atomic_write_json(SERIES_INDEX_FILE, series_list)
        print(
            f"\u2713 Saved {len(series_list)} series to index"
        )
        logger.info(
            "Saved %d series to %s",
            len(series_list), SERIES_INDEX_FILE,
        )
        return True, changes
    except Exception as exc:  # pylint: disable=broad-exception-caught
        print(f"\u2717 Failed to save: {exc}")
        logger.error("Failed to save index: %s", exc)
        return False, None


class IndexManager:
    """Manages the local series index file."""

    def __init__(self):
        self.series_index = {}
        self.ensure_data_dir()
        self.load_index()

    def ensure_data_dir(self):
        """Create the data directory if it does not exist."""
        os.makedirs(DATA_DIR, exist_ok=True)

    def load_index(self):
        """Load series index from JSON with corruption detection.

        Converts both list and dict formats to dict format.
        Validates loaded data for consistency.
        """
        self.series_index = {}
        data = _read_index_json()
        if data is None:
            return
        try:
            if isinstance(data, list):
                self.series_index = {
                    item.get("title"): item
                    for item in data if item.get("title")
                }
            elif isinstance(data, dict):
                first_item = next(iter(data.values()), None)
                if (
                    first_item
                    and isinstance(first_item, dict)
                    and first_item.get('title')
                ):
                    self.series_index = data
                else:
                    self.series_index = {
                        item.get("title"): item
                        for item in data.values()
                        if isinstance(item, dict)
                        and item.get("title")
                    }
            else:
                self.series_index = {}

            validated = {}
            for title, series in self.series_index.items():
                if _validate_series_entry(series, title):
                    validated[title] = series
            self.series_index = validated

            print(
                f"[OK] Loaded {len(self.series_index)} "
                "series from index"
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            print(f"[WARN] Error loading index: {exc}")
            logger.error("Error loading index: %s", exc)
            self.series_index = {}

    def get_statistics(  # pylint: disable=too-many-locals
        self,
    ):
        """Return detailed analytics about the series index."""
        series_with_progress = self.get_series_with_progress()
        total = len(series_with_progress)

        if total == 0:
            return {
                "total_series": 0,
                "watched": 0,
                "unwatched": 0,
                "watched_percentage": 0.0,
                "empty_series": 0,
            }

        watched = sum(
            1 for s in series_with_progress
            if not s['is_incomplete']
        )
        unwatched = total - watched
        empty_count = len([
            s for s in self.series_index.values()
            if s.get('empty', False)
        ])

        completion_pcts = [
            s['completion'] for s in series_with_progress
        ]
        avg_completion = round(
            sum(completion_pcts) / total, 2,
        )

        total_episodes = sum(
            s['total_episodes'] for s in series_with_progress
        )
        watched_episodes = sum(
            s['watched_episodes']
            for s in series_with_progress
        )
        avg_eps = (
            round(total_episodes / total, 1)
            if total > 0 else 0
        )

        completion_ranges = {
            "0-25%": sum(
                1 for p in completion_pcts if 0 <= p < 25
            ),
            "25-50%": sum(
                1 for p in completion_pcts if 25 <= p < 50
            ),
            "50-75%": sum(
                1 for p in completion_pcts if 50 <= p < 75
            ),
            "75-99%": sum(
                1 for p in completion_pcts if 75 <= p < 100
            ),
            "100%": sum(
                1 for p in completion_pcts if p == 100
            ),
        }

        ongoing_only = [
            s for s in series_with_progress
            if 0 < s['completion'] < 100
        ]
        sorted_ongoing = sorted(
            ongoing_only,
            key=lambda x: x['completion'],
            reverse=True,
        )
        most_completed = sorted_ongoing[:5]
        least_completed = (
            sorted_ongoing[-5:] if sorted_ongoing else []
        )

        completed_count = watched
        ongoing_count = len(ongoing_only)
        not_started_count = sum(
            1 for s in series_with_progress
            if s['watched_episodes'] == 0
        )

        def _progress_entry(s):
            return {
                "title": s['title'],
                "completion": s['completion'],
                "progress": (
                    f"{s['watched_episodes']}"
                    f"/{s['total_episodes']}"
                ),
            }

        return {
            "total_series": total,
            "watched": watched,
            "unwatched": unwatched,
            "watched_percentage": round(
                (watched / total * 100), 2,
            ),
            "empty_series": empty_count,
            "completed_count": completed_count,
            "ongoing_count": ongoing_count,
            "not_started_count": not_started_count,
            "average_completion": avg_completion,
            "total_episodes": total_episodes,
            "watched_episodes": watched_episodes,
            "unwatched_episodes": (
                total_episodes - watched_episodes
            ),
            "average_episodes_per_series": avg_eps,
            "completion_distribution": completion_ranges,
            "most_completed_series": [
                _progress_entry(s) for s in most_completed
            ],
            "least_completed_series": [
                _progress_entry(s) for s in least_completed
            ],
        }

    def get_full_report(  # pylint: disable=too-many-locals
        self,
    ):
        """Generate a comprehensive report with categories."""
        series_progress = self.get_series_with_progress()
        stats = self.get_statistics()

        watched_series = [
            s for s in series_progress
            if not s['is_incomplete']
        ]
        ongoing_series = [
            s for s in series_progress
            if s['is_incomplete'] and s['watched_episodes'] > 0
        ]
        not_started_series = [
            s for s in series_progress
            if s['is_incomplete']
            and s['watched_episodes'] == 0
        ]

        ongoing_sorted = sorted(
            ongoing_series,
            key=lambda x: x['completion'],
            reverse=True,
        )
        ongoing_titles = [s['title'] for s in ongoing_sorted]

        not_started_titles = sorted(
            [s['title'] for s in not_started_series]
        )

        episode_ranges = {
            "short_series": [
                s['title'] for s in series_progress
                if s['total_episodes'] <= 5
            ],
            "medium_series": [
                s['title'] for s in series_progress
                if 6 <= s['total_episodes'] <= 25
            ],
            "long_series": [
                s['title'] for s in series_progress
                if s['total_episodes'] > 25
            ],
        }

        near_completion = [
            s['title'] for s in ongoing_sorted
            if 80 <= s['completion'] < 100
        ][:10]
        stalled = [
            s['title'] for s in ongoing_sorted
            if s['completion'] < 25
        ][:10]

        def _detail_entry(s):
            return {
                "title": s['title'],
                "completion": s['completion'],
                "progress": (
                    f"{s['watched_episodes']}"
                    f"/{s['total_episodes']}"
                ),
            }

        report = {
            "metadata": {
                "generated": datetime.now().isoformat(),
                "total_series_in_index": len(
                    self.series_index
                ),
                "active_series": len(series_progress),
                "statistics": stats,
            },
            "categories": {
                "watched": {
                    "count": len(watched_series),
                    "titles": sorted(
                        [s['title'] for s in watched_series]
                    ),
                },
                "ongoing": {
                    "count": len(ongoing_series),
                    "titles": ongoing_titles,
                    "details": [
                        _detail_entry(s)
                        for s in ongoing_sorted[:20]
                    ],
                },
                "not_started": {
                    "count": len(not_started_series),
                    "titles": not_started_titles,
                },
            },
            "insights": {
                "completion_distribution": stats.get(
                    "completion_distribution", {},
                ),
                "episode_ranges": episode_ranges,
                "near_completion": near_completion,
                "stalled_series": stalled,
                "most_completed": stats.get(
                    "most_completed_series", [],
                )[:10],
                "least_completed": stats.get(
                    "least_completed_series", [],
                )[:10],
            },
            "raw_data": {
                "all_series": self.series_index,
                "series_progress": series_progress,
            },
        }
        return report

    def get_series_with_progress(
        self, sort_by='completion', reverse=False,
    ):
        """Return series list with progress and completion %."""
        series_list = []
        for s in self.series_index.values():
            total_eps = 0
            watched_eps = 0
            for season in s.get('seasons', []):
                eps = season.get('episodes', [])
                total_eps += len(eps)
                watched_eps += sum(
                    1 for ep in eps
                    if ep.get('watched', False)
                )
            is_incomplete = (
                total_eps == 0 or watched_eps < total_eps
            )
            completion = (
                round((watched_eps / total_eps) * 100, 2)
                if total_eps > 0 else 0.0
            )
            series_list.append({
                'title': s.get('title', ''),
                'watched_episodes': watched_eps,
                'total_episodes': total_eps,
                'is_incomplete': is_incomplete,
                'completion': completion,
                'empty': s.get('empty', False),
            })
        if sort_by:
            series_list.sort(
                key=lambda x: x.get(sort_by, 0),
                reverse=reverse,
            )
        return series_list
