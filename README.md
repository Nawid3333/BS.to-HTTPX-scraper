# BS.TO Series Scraper & Index Manager (httpx)

Scrapes watched TV series from **bs.to** and maintains a local JSON index.
Uses **httpx** (no browser needed) with a multi-session architecture for fast, parallel scraping.

## Features

- **Multi-session parallel scraping** — 24 concurrent httpx sessions by default (configurable)
- **Checkpoint & resume** — automatically saves progress; resume after interruptions (Ctrl+C safe)
- **New series detection** — detects newly added series on your account and lists them before scraping
- **Vanished series detection** — alerts when series disappear from your account
- **Batch URL import** — import series from a text file
- **Failed series retry** — automatically tracks failures for later bulk retry
- **Pause/resume** — create a `.pause_scraping` file to gracefully pause workers
- **Report generation** — full statistics with ongoing series export
- **Disk space check** — warns before scraping if free space is below 100 MB
- **Rotating log files** — 10 MB per file, 5 backups

## Requirements

- Python 3.8+
- Dependencies: `httpx`, `beautifulsoup4`, `python-dotenv`

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file inside the `config/` directory:

```
BS_USERNAME=yourusername
BS_PASSWORD=yourpassword
```

Scraping parallelism can be adjusted in `config/config.py`:

```python
NUM_WORKERS = 24  # Number of parallel httpx sessions
```

## Usage

```bash
python main.py
```

### Menu Options

| # | Option | Description |
|---|--------|-------------|
| 1 | **Scrape series from bs.to** | Full scrape of all watched series. Choose single-session or multi-session mode. |
| 2 | **Scrape NEW series only** | Scrapes only series not yet in the index (faster). |
| 3 | **Add single series by URL** | Add one series by pasting its bs.to URL. |
| 4 | **Generate full report** | Statistics report saved to JSON with ongoing series export. |
| 5 | **Batch add from file** | Import multiple series from a text file. |
| 6 | **Retry failed series** | Bulk retry all series that failed in previous runs. |
| 7 | **Pause scraping** | Creates `.pause_scraping` flag file for graceful worker pause. |
| 8 | **Exit** | Clean exit. |

### Scraping Modes (Option 1)

1. **Single session** — one httpx client, sequential (most reliable)
2. **Multi-session** — 12+ parallel workers (default, faster)

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

After report generation, you can export ongoing series URLs back to `series_urls.txt`.

## Project Structure

```
├── main.py                     # Entry point & interactive menu
├── requirements.txt
├── series_urls.txt             # Optional batch URL file
├── config/
│   ├── config.py               # Settings (credentials, workers, paths)
│   └── .env                    # Credentials (not committed)
├── data/
│   ├── series_index.json       # Main series database
│   ├── series_report.json      # Generated report
│   ├── .scrape_checkpoint.json # Resume checkpoint (auto-managed)
│   ├── .failed_series.json     # Failed series list (auto-managed)
│   └── .pause_scraping         # Pause flag file (auto-managed)
├── src/
│   ├── scraper.py              # BsToScraper — httpx scraping engine
│   └── index_manager.py        # IndexManager — merge, stats, reports
└── logs/
    └── bs_to_backup.log        # Rotating log file
```

## License

Private project — not licensed for redistribution.
