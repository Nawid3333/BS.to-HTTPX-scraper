# BS.TO Series Scraper & Index Manager (httpx)

[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](./LICENSE)

Scrapes watched TV series from **bs.to** and maintains a local JSON index.
Uses **httpx** (no browser needed) with a multi-session architecture for fast, parallel scraping.

## Features

- **Multi-session parallel scraping** — 12 concurrent httpx sessions by default (configurable in `config/config.py`)
- **Host probing** — checks all configured hosts before scraping, compares site series count with the local index, and writes a `mismatch_report.json` when differences or duplicate slugs are detected
- **Duplicate slug detection** — finds duplicate slugs in the index and offers to delete them before continuing
- **Smart per-series ETA estimation** — each series stores its own `avg_scrape_seconds` (exponential moving average for ETA prediction) and `scrape_duration_seconds` (actual duration of the most recent scrape) in the index. ETA is predicted by summing those per-series averages for the remaining work, then blended with the live session rate (historical 85%→45% as progress increases). Because the database is stable, per-series history is the best predictor.
- **Checkpoint & resume** — automatically saves progress every 50 series; resume after interruptions (Ctrl+C safe)
- **New series detection** — detects newly added series on your account and lists them before scraping
- **Vanished series detection** — alerts when series disappear from your account
- **Ignored series** — skip specific series via `.ignored_series.json`
- **Batch URL import** — import series from a text file
- **Failed series retry** — automatically tracks failures for later bulk retry
- **Pause/resume** — create a `.pause_scraping` file to gracefully pause workers
- **Report generation** — full statistics with ongoing series export
- **Genre completion stats** — option 7 scrapes every series page for its genres into a separate
  `data/genre_index.json`, then reports watched/total per genre. Self-contained: it never writes
  `series_index.json`
- **Data integrity checks** — detects episode count drops, season removals, watched-status corruption, and title changes before merging; offers to delete & rescrape critical series
- **Atomic file writes** — all JSON writes use temp file + replace to prevent corruption
- **Disk space check** — warns before scraping if free space is below 100 MB
- **Rotating log files** — 10 MB per file, 5 backups

## Requirements

- Python 3.10+ — developed and tested on 3.14. The 3.10 floor comes from
  `zip(strict=True)` and PEP 604 `X | None` annotations evaluated at runtime;
  versions between 3.10 and 3.13 are expected to work but are not tested.
- Dependencies: `httpx`, `beautifulsoup4`, `lxml`, `h2`, `python-dotenv`

`lxml` and `h2` are what make the scraper fast: pages parse ~1.4x quicker than with
the stdlib parser, and HTTP/2 lets one connection carry many requests. Both fall
back gracefully if unavailable, at the old speed.

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file inside the `config/` directory (see `config/.env.example`):

```
BS_USERNAME=yourusername
BS_PASSWORD=yourpassword
```

`.env` is used **only for credentials**. All other settings (site URLs, fallback domains, workers, batch file paths) live in `config/config.py`.

The default batch file is `series_urls.txt` next to `main.py`. To change it, edit `DEFAULT_BATCH_FILE` in `config/config.py`.

Site URL and fallback domains are also defined in `config/config.py`:

```python
SITE_URL = "https://bs.to"
BS_FALLBACK_SITE_URL = "https://bs.cine.to"
```

Built-in fallback hosts: `bs.cine.to`, `burningseries.ac`, `burningseries.cx`.

Scraping parallelism can be adjusted in `config/config.py`:

```python
NUM_WORKERS = 12  # Number of parallel httpx sessions
```

## Tuning

All optional, with sensible defaults. Set them in `config/.env`.

| Variable | Default | What it does |
| --- | --- | --- |
| `BS_MAX_WORKERS` | `12` | Concurrent scraping sessions. Measured, not guessed: throughput climbs steeply to 8, flattens by 12, and is indistinguishable from 12 up to 32 — past 12 only adds load. |
| `BS_SEASON_CONCURRENCY` | `4` | Season pages fetched at once per series. Total requests in flight is workers x this. |
| `BS_CHECKPOINT_EVERY` | `50` | Save resume state every N series. |
| `BS_PROFILE` | unset | Set to `1` to print where a run's time actually went (network vs parse vs disk). |

## Usage

```bash
python main.py
```

### Menu Options

| #   | Option                              | Description                                                                |
| --- | ----------------------------------- | -------------------------------------------------------------------------- |
| 1   | **Scrape series from bs.to**        | Full scrape of all watched series. Choose single-session or multi-session. |
| 2   | **Scrape only NEW series**          | Scrapes only series not yet in the index (faster).                         |
| 3   | **Scrape unwatched series**         | Skips fully watched series; focuses on ongoing/partial.                    |
| 4   | **Generate full report**            | Statistics report saved to JSON with ongoing series export.                |
| 5   | **Single link / batch add**         | Add one series by pasting its bs.to URL, or batch-import from a text file. |
| 6   | **Retry failed series**             | Bulk retry all series that failed in previous runs.                        |
| 7   | **Watch Stats of Categories**       | Genre completion stats: scrape genres, show watched/total per genre, export. |
| 0   | **Exit**                            | Clean exit.                                                                |

> **Pausing scraping:** there is no dedicated menu option. To gracefully pause workers, create a `.pause_scraping` file in the `data/` directory (see [Pause/resume](#pauseresume) below).

### Scraping Modes (Option 1)

1. **Single session** — one httpx client, sequential (most reliable)
2. **Multi-session** — 12 parallel workers (default, faster)

### Batch File Format (Option 5)

One URL per line:

```
https://bs.to/serie/Breaking-Bad
https://bs.to/serie/Better-Call-Saul
```

### Reports (Option 4)

Reports include:

- Total series, completed, ongoing, not started counts
- Episode counts and completion percentages
- Most recently updated series status
- Ongoing series list

After report generation, you can export ongoing series URLs back to the default batch file (`series_urls.txt` by default).

## Pause/resume

There is no menu option for pausing. To gracefully pause a running scrape, create an empty `.pause_scraping` file in the `data/` directory:

```bash
# from the project folder
touch data/.pause_scraping          # Linux / macOS
New-Item data\.pause_scraping -ItemType File   # PowerShell
```

Active workers check for this file periodically and finish their current series before stopping. The checkpoint is saved so you can resume the run later. Delete the file to allow new scrapes to run.

## Ignored Series

Some series pages are empty, return a 404/502 error, or exist in the catalog with no real seasons. These make every scrape fail or show phantom unwatched entries.

The file `data/.ignored_series.json` lists series to skip entirely during scraping. It is **not** created automatically; create it manually when needed:

```json
[
  {
    "url": "https://bs.to/serie/empty-series",
    "title": "Empty Series"
  }
]
```

Only the `url` field is required for matching; `title` is optional and informational. The slug is extracted from the URL automatically.

**Behavior during scraping:**

| Scenario                                               | Behavior                                                                                 |
| ------------------------------------------------------ | ---------------------------------------------------------------------------------------- |
| Series is in ignore list                               | Skipped entirely; not fetched, not counted, not included in reports                      |
| Ignored series page is still empty / 404 / unreachable | Printed as `✓ {title}: still empty` during re-validation                                 |
| Ignored series now has real season content             | Warning printed: `⚠ {title}: now available! Consider removing from .ignored_series.json` |
| Ignored series no longer appears in the catalog        | Warning printed: consider removing the stale entry                                       |

The scraper re-validates ignored series at the start of every run and checks them against the fetched catalog. It **does not** auto-add or auto-remove entries — all changes to `.ignored_series.json` are manual.

## Project Structure

```
├── .gitignore
├── LICENSE                  # GNU GPL v3.0
├── README.md                # This file
├── main.py                  # Entry point & interactive menu
├── requirements.txt         # Python dependencies
├── ruff.toml                # Lint/format configuration
├── config/
│   ├── .env.example         # Template for your credentials
│   └── config.py            # Settings (credentials, workers, paths)
├── src/
│   ├── atomic_io.py         # Durable atomic JSON writes (shared by every writer)
│   ├── genre_stats.py       # Genre completion stats (option 7), self-contained
│   ├── index_manager.py     # Merge, change detection, stats, reports
│   └── scraper.py           # httpx scraping engine
└── tests/
    ├── __init__.py
    ├── capture_fixtures.py  # Regenerates test fixtures from the live site
    ├── fixture_spec.py      # Which parser outputs the fixtures pin
    ├── test_genre_stats.py  # Genre parser, snapshot, diff and storage tests
    ├── test_golden_parse.py # Parser output pinned against real captured pages
    └── test_scraper.py      # Unit + regression tests
```

Directories created at runtime (`data/`, `logs/`) and your `.env` are not part of
the repository. Test fixtures live in `tests/fixtures/` and are generated locally
with `python tests/capture_fixtures.py`.

## Author

Nawid Salehie

## License

GNU General Public License v3.0 — see [LICENSE](LICENSE) for details.
