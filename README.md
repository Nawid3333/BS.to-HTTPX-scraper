# BS.TO Series Scraper & Index Manager (httpx)

[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
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
  `data/genre_index.json`, then reports watched/total per genre, exports a full JSON report, and lists
  unwatched series by genre. Self-contained: it never writes `series_index.json`
- **Smart genre picker** — used by option 7 and 8: type a few characters to filter the genre list,
  press Tab to cycle matches, Enter to confirm, or type `0`/`back` to return. No scroll menu, no external
  prompt toolkit required
- **Data integrity checks** — detects episode count drops, season removals, watched-status corruption, and title changes before merging; offers to delete & rescrape critical series
- **Atomic file writes** — all JSON writes use temp file + replace to prevent corruption
- **Disk space check** — warns before scraping if free space is below 100 MB
- **Rotating log files** — 10 MB per file, 5 backups

## Requirements

- **Python 3.11+** — developed and tested on 3.14. `requires-python` in
  `pyproject.toml` enforces 3.11, so pip will refuse to install on anything
  older. The code itself uses nothing newer than 3.10 features
  (`zip(strict=True)`, PEP 604 `X | None` annotations evaluated at runtime), so
  3.10 would very likely work — it is simply not tested, so it is not offered.
- Dependencies: `httpx`, `beautifulsoup4`, `lxml`, `h2`, `python-dotenv`

`lxml` and `h2` are what make the scraper fast: pages parse ~1.4x quicker than with
the stdlib parser, and HTTP/2 lets one connection carry many requests. Both fall
back gracefully if unavailable, at the old speed.

## Installation

There are two ways to run this. **Clone it** unless you have a reason not to —
that is how the program is designed to be used, and it keeps your credentials,
your watch list and your scraped index together in one folder you control.

### Run from a clone (recommended)

```bash
git clone https://github.com/Nawid3333/BS.to-HTTPX-scraper.git
cd BS.to-HTTPX-scraper

python -m venv .venv
source .venv/bin/activate        # Linux / macOS
.venv\Scripts\Activate.ps1       # Windows (PowerShell)

pip install -r requirements.txt

cp .env.example .env                 # then edit it — see Configuration below
python main.py
```

Everything the program reads or writes — `.env`, `data/`, `logs/` and the
default batch file — stays inside that folder. Nothing is written anywhere else
on your machine.

### Install it as a command

Building a wheel puts a `bs-to-scraper` command on your PATH:

```bash
pip install build
python -m build
pip install dist/bs_to_scraper-2.0.0-py3-none-any.whl
```

Two things are worth knowing before you do.

**Give each program its own virtual environment.** Every project in this family
ships its code as the top-level modules `main`, `src` and `config`. Install two
of them into the same environment and the second overwrites the first — the
command still exists, but it silently runs the other program. `pipx` creates an
isolated environment per application and avoids this entirely:

```bash
pipx install .
```

**Tell it where to keep your files.** Once installed, the package lives inside
`site-packages`, which is no place to keep a `.env` you have to edit by hand.
Point `BS_HOME` at a folder you own, and `.env`, `data/`, `logs/` and the
default batch file all move there:

```bash
export BS_HOME=~/bs-scraper                  # Linux / macOS
$env:BS_HOME = "$HOME\bs-scraper"            # Windows (PowerShell)

mkdir -p ~/bs-scraper
cp .env.example ~/bs-scraper/.env
```

If you skip that copy, the first run writes the template there for you and
says where it put it -- so an installed copy never leaves you hunting for a
file inside `site-packages`.

`BS_HOME` has to be a real environment variable. It cannot be set inside
`.env`, because it is what tells the program where to find that file in
the first place. Left unset it resolves to the checkout, which is why running
from a clone needs no configuration at all.

## Configuration

Create a `.env` file in the project root (see `.env.example`). Running the
program once without one writes that template for you:

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

All optional, with sensible defaults. Set them in `.env`.

| Variable                | Default | What it does                                                                                                                                                             |
| ----------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `BS_MAX_WORKERS`        | `12`    | Concurrent scraping sessions. Measured, not guessed: throughput climbs steeply to 8, flattens by 12, and is indistinguishable from 12 up to 32 — past 12 only adds load. |
| `BS_SEASON_CONCURRENCY` | `4`     | Season pages fetched at once per series. Total requests in flight is workers x this.                                                                                     |
| `BS_CHECKPOINT_EVERY`   | `50`    | Save resume state every N series.                                                                                                                                        |
| `BS_PROFILE`            | unset   | Set to `1` to print where a run's time actually went (network vs parse vs disk).                                                                                         |
| `BS_HOME` | unset | Where `.env`, `data/`, `logs/` and the default batch file live. Unset, that is this checkout. Set it when you install the package, so they do not land in site-packages. Must be a real environment variable — it cannot be set inside `.env`, because it is what locates that file. |

## Usage

```bash
python main.py
```

### Menu Options

| #   | Option                         | Description                                                                                           |
| --- | ------------------------------ | ----------------------------------------------------------------------------------------------------- |
| 1   | **Scrape series from bs.to**   | Full scrape of all watched series. Choose single-session or multi-session.                            |
| 2   | **Scrape only NEW series**     | Scrapes only series not yet in the index (faster).                                                    |
| 3   | **Scrape unwatched series**    | Skips fully watched series; focuses on ongoing/partial.                                               |
| 4   | **Generate full report**       | Statistics report saved to JSON with ongoing series export.                                           |
| 5   | **Single link / batch add**    | Add one series by pasting its bs.to URL, or batch-import from a text file.                            |
| 6   | **Retry failed series**        | Bulk retry all series that failed in previous runs.                                                   |
| 7   | **Watch Stats of Categories**  | Genre completion stats: scrape genres, show watched/total, export report, or list unwatched by genre. |
| 8   | **Suggest something to watch** | Pick a genre (or all genres) and get up to 10 random unwatched series suggestions.                    |
| 0   | **Exit**                       | Clean exit.                                                                                           |

> **Pausing scraping:** there is no dedicated menu option. To gracefully pause workers, create a `.pause_scraping` file in the `data/` directory (see [Pause/resume](#pauseresume) below).

### Option 7 — Watch Stats of Categories

Opens its own sub-menu:

1. **Scrape genres** — fetches every series page once, extracts all genres, and writes `data/genre_index.json`. Resumes if interrupted.
2. **Show stats** — prints a watched/total table per genre, joined against the local series index, plus change notices since the last check.
3. **Export genre report** — writes the same breakdown to `data/genre_report.json`.
4. **Show unwatched by genre** — uses the interactive genre picker to filter series that still have unwatched episodes.

The genre picker prints the full list once, then keeps a single prompt line:

- **Type** to filter the list; matches are shown as `→ genre name`.
- **Tab** cycles through matching genres.
- **Enter** confirms the highlighted match.
- **0** or **back** returns to the previous menu.
- Non-interactive terminals fall back to plain text input.

### Option 8 — Suggest something to watch

Shows up to 10 random unwatched series. You can filter by a specific genre via the same picker, or choose **All genres / no filter** to pick across everything. The list is shuffled every time.

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

## Development

```bash
pip install -e ".[dev]"     # pytest + ruff
```

| Command                                                  | What it does                                              |
| -------------------------------------------------------- | --------------------------------------------------------- |
| `python -m pytest`                                       | The suite. Benchmarks are excluded, so it stays fast.     |
| `python -m pytest --cov`                                 | With a branch-coverage report.                            |
| `python -m pytest --benchmark`                           | Adds the timing benchmarks.                               |
| `python -m pytest --benchmark -m benchmark --benchmark-update` | Re-records the timing baseline.                     |
| `ruff check . && ruff format --check .`                  | Lint and formatting.                                      |

Benchmarks compare against `tests/benchmark_baseline.json` and fail only when a
result exceeds the recorded time by more than 60%. That tolerance is deliberately
loose: it is there to catch an algorithmic regression — a loop that turned
quadratic, a parse that started running twice — not to police a few percent of
drift between machines. The baseline is machine-specific, so treat a failure on
hardware that did not record it as "go look", not as a hard gate.

Fixtures under `tests/fixtures/` are captured from the live site and are not in
git. Regenerate them with `python tests/capture_fixtures.py` (needs working
credentials); the tests that use them skip when they are absent.

## Project Structure

```
├── .env.example            # Template for your credentials
├── .gitignore
├── LICENSE                  # GNU GPL v3.0
├── MANIFEST.in              # What a source archive ships
├── README.md                # This file
├── main.py                  # Entry point & interactive menu
├── pyproject.toml           # Package metadata, pytest and coverage settings
├── requirements.txt         # Runtime dependencies
├── ruff.toml                # Lint/format configuration
├── config/
│   ├── __init__.py
│   └── config.py            # Settings, paths, and the project-home override
├── src/
│   ├── __init__.py
│   ├── atomic_io.py         # Durable atomic JSON writes (shared by every writer)
│   ├── genre_stats.py       # Option 7: genre completion stats (own data file)
│   ├── index_manager.py     # Merge, change detection, stats, reports
│   └── scraper.py           # httpx scraping engine
└── tests/
    ├── _support.py              # Builders and fakes shared across the suite
    ├── bench.py                 # Timing harness and regression tolerance
    ├── capture_fixtures.py      # Regenerates fixtures from the live site
    ├── conftest.py              # sys.path, --benchmark flag, shared fixtures
    ├── fixture_spec.py          # Which parser outputs the fixtures pin
    └── test_*.py                # The suite itself (see Development)
```

Directories created at runtime (`data/`, `logs/`), your `.env`, and your
`series_urls.txt` are not part of the repository. Test fixtures live in
`tests/fixtures/` and are generated locally.

## Author

Nawid Salehie

## License

GNU General Public License v3.0 — see [LICENSE](LICENSE) for details.
