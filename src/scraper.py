"""
BS.TO Series Scraper -- powered by httpx (no browser needed).

Uses multiple independent httpx sessions (each logged in separately)
to work around bs.to's server-side session locking that breaks
watched status under concurrency.  Each session processes its share
of series sequentially.
"""

import asyncio
import json
import logging
import os
import re
import threading
import time
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from config.config import (
    USERNAME, PASSWORD, DATA_DIR, SERIES_INDEX_FILE,
    NUM_WORKERS,
)

logger = logging.getLogger(__name__)

# -- Constants -------------------------------------------------------
SITE_URL = "https://bs.to"
LOGIN_URL = f"{SITE_URL}/login"
SERIES_LIST_URL = f"{SITE_URL}/andere-serien"
CHECKPOINT_EVERY = 10
REQUEST_TIMEOUT = 20.0
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) "
    "Gecko/20100101 Firefox/128.0"
)

_SERIE_PATH_RE = re.compile(r'(/serie/[^/]+)')
_UTILITY_PAGES = {
    'alle serien', 'andere serien', 'beliebte serien',
    'neue serien', 'empfehlung', 'meistgesehen',
}

# Error page detection
_ERROR_TITLE_RE = re.compile(
    r'^(?:Error\s+)?(?P<code>\d{3})\b'
    r'|\b(?:Error|Fehler)\s+(?P<code2>\d{3})\b',
    re.IGNORECASE,
)
_SERVER_ERROR_CODES = {'429', '500', '502', '503', '504'}


def _is_logged_in(html: str) -> bool:
    """Check if the page indicates a logged-in session."""
    soup = BeautifulSoup(html, "html.parser")
    nav = soup.select_one("section.navigation")
    return (
        nav is not None
        and nav.find("a", href="logout") is not None
    )


def _check_error_page(html: str) -> str | None:
    """Detect HTTP error pages (404, 502, etc.) as HTML."""
    soup = BeautifulSoup(html, "html.parser")
    if soup.select_one("#seasons a"):
        return None
    error_box = soup.select_one("div.messageBox.error")
    if error_box:
        text = error_box.get_text(strip=True).lower()
        if 'nicht gefunden' in text:
            return '404'
    title_tag = soup.find("title")
    if title_tag:
        title_text = title_tag.get_text(strip=True)
        m = _ERROR_TITLE_RE.search(title_text)
        if m:
            return m.group("code") or m.group("code2")
    return None


# -- HTML helpers ----------------------------------------------------

def _parse_episodes(html: str) -> list[dict]:
    """Parse episode table from a season page."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.episodes")
    if not table:
        return []
    episodes = []
    for idx, row in enumerate(table.select("tr"), start=1):
        cols = row.find_all("td")
        if len(cols) < 2:
            continue
        ep_num = cols[0].get_text(strip=True)
        if not ep_num:
            ep_num = row.get("data-episode-season-id", "")
        if not ep_num:
            logger.warning(
                "Could not determine episode number "
                "for row %d", idx,
            )
            return None
        try:
            ep_num_int = int(ep_num)
        except ValueError:
            logger.warning(
                "Non-numeric episode number '%s' "
                "in row %d", ep_num, idx,
            )
            return None
        title_tag = cols[1].find("strong")
        title = (
            title_tag.get_text(strip=True)
            if title_tag
            else cols[1].get_text(strip=True)
        )
        watched = "watched" in (row.get("class") or [])
        episodes.append({
            "number": ep_num_int,
            "title": title,
            "watched": watched,
        })
    return episodes


def _extract_season_links(
    html: str, base_url: str,
) -> list[tuple[str, str]]:
    """Extract season navigation links from a series page."""
    soup = BeautifulSoup(html, "html.parser")
    links, seen = [], set()
    for a in soup.select("#seasons a"):
        label = a.get_text(strip=True)
        href = a.get("href", "")
        if not label or not href:
            continue
        href = href.split("?")[0].split("#")[0]
        if href.startswith("http"):
            url = href
        elif href.startswith("serie/"):
            url = f"{SITE_URL}/{href}"
        else:
            url = urljoin(base_url.rstrip("/") + "/", href)
        key = (label, url)
        if key not in seen:
            seen.add(key)
            links.append((label, url))
    return links


def _extract_title(html: str) -> str | None:
    """Extract the series title from the page."""
    soup = BeautifulSoup(html, "html.parser")
    h2 = soup.find("h2")
    if h2:
        text = h2.get_text(strip=True)
        text = re.sub(r'\s*Staffel\s*\d+.*$', '', text)
        return text or None
    return None


# -- Exception -------------------------------------------------------

class ScrapingPaused(Exception):
    """Raised when a pause file is detected during scraping."""


# -- BsToScraper (httpx) ---------------------------------------------

class BsToScraper:  # pylint: disable=too-many-instance-attributes
    """BS.TO series scraper powered by httpx."""

    def __init__(self):
        self.series_data: list[dict] = []
        self.all_discovered_series: list[dict] | None = None
        self.completed_links: set[str] = set()
        self.failed_links: list[dict] = []

        self.checkpoint_file = os.path.join(
            DATA_DIR, '.scrape_checkpoint.json',
        )
        self.failed_file = os.path.join(
            DATA_DIR, '.failed_series.json',
        )
        self.ignore_file = os.path.join(
            DATA_DIR, '.ignored_series.json',
        )
        self.pause_file = os.path.join(
            DATA_DIR, '.pause_scraping',
        )

        self._checkpoint_mode: str | None = None
        self._use_parallel: bool = True
        self._lock = threading.Lock()
        self._last_pause_check = 0.0
        self._pause_cached = False
        self.paused = False

    # -- Static / class methods --------------------------------------

    @staticmethod
    def get_checkpoint_mode(data_dir):
        """Read the checkpoint mode from disk, or None."""
        cp_file = os.path.join(
            data_dir, '.scrape_checkpoint.json',
        )
        try:
            if os.path.exists(cp_file):
                with open(cp_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    return data.get('mode')
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        return None

    # -- Checkpoint management ---------------------------------------

    def save_checkpoint(self, include_data=False):
        """Persist current scrape progress to disk."""
        with self._lock:
            payload = {
                'completed_links': list(
                    self.completed_links
                ),
                'mode': self._checkpoint_mode,
                'timestamp': time.time(),
            }
            if include_data:
                payload['series_data'] = self.series_data
            tmp = self.checkpoint_file + '.tmp'
            try:
                with open(tmp, 'w', encoding='utf-8') as f:
                    json.dump(
                        payload, f, ensure_ascii=False,
                    )
                os.replace(tmp, self.checkpoint_file)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.error(
                    "Failed to save checkpoint: %s", exc,
                )

    def load_checkpoint(self) -> bool:
        """Load checkpoint from disk. Returns True if loaded."""
        with self._lock:
            try:
                if not os.path.exists(self.checkpoint_file):
                    return False
                with open(
                    self.checkpoint_file, 'r',
                    encoding='utf-8',
                ) as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    self.completed_links = set(
                        data.get('completed_links', [])
                    )
                    self._checkpoint_mode = data.get('mode')
                    saved_data = data.get('series_data')
                    if saved_data:
                        self.series_data = saved_data
                elif isinstance(data, list):
                    self.completed_links = set(data)
                return bool(self.completed_links)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.error(
                    "Failed to load checkpoint: %s", exc,
                )
                return False

    def clear_checkpoint(self):
        """Remove checkpoint file from disk."""
        with self._lock:
            try:
                if os.path.exists(self.checkpoint_file):
                    os.remove(self.checkpoint_file)
            except OSError:
                pass

    # -- Failed series management ------------------------------------

    def save_failed_series(self):
        """Persist failed series list to disk."""
        with self._lock:
            existing = []
            try:
                if os.path.exists(self.failed_file):
                    with open(
                        self.failed_file, 'r',
                        encoding='utf-8',
                    ) as f:
                        existing = json.load(f)
            except Exception:  # pylint: disable=broad-exception-caught
                pass
            seen = {
                e.get('url') for e in existing
                if isinstance(e, dict)
            }
            for entry in self.failed_links:
                if (
                    isinstance(entry, dict)
                    and entry.get('url') not in seen
                ):
                    existing.append(entry)
                    seen.add(entry.get('url'))
            tmp = self.failed_file + '.tmp'
            try:
                with open(tmp, 'w', encoding='utf-8') as fout:
                    json.dump(
                        existing, fout,
                        indent=2, ensure_ascii=False,
                    )
                os.replace(tmp, self.failed_file)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.error(
                    "Failed to save failed series: %s", exc,
                )

    def load_failed_series(self) -> list:
        """Load previously failed series from disk."""
        with self._lock:
            try:
                if os.path.exists(self.failed_file):
                    with open(
                        self.failed_file, 'r',
                        encoding='utf-8',
                    ) as f:
                        data = json.load(f)
                    if isinstance(data, list):
                        return data
            except Exception:  # pylint: disable=broad-exception-caught
                pass
            return []

    def clear_failed_series(self):
        """Remove failed series file from disk."""
        with self._lock:
            try:
                if os.path.exists(self.failed_file):
                    os.remove(self.failed_file)
            except OSError:
                pass

    # -- Ignore list management --------------------------------------

    def load_ignored_series(self) -> list[dict]:
        """Load the ignore list from disk."""
        try:
            if os.path.exists(self.ignore_file):
                with open(
                    self.ignore_file, 'r', encoding='utf-8',
                ) as f:
                    data = json.load(f)
                if isinstance(data, list):
                    return data
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        return []

    def save_ignored_series(self, ignored: list[dict]):
        """Persist the ignore list to disk."""
        tmp = self.ignore_file + '.tmp'
        try:
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(
                    ignored, f, indent=2, ensure_ascii=False,
                )
            os.replace(tmp, self.ignore_file)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error(
                "Failed to save ignored series: %s", exc,
            )

    def get_ignored_slugs(self) -> set[str]:
        """Return set of slugs from the ignore list."""
        return {
            self.get_series_slug_from_url(s.get('url', ''))
            for s in self.load_ignored_series()
        } - {'unknown'}

    # -- URL helpers -------------------------------------------------

    @staticmethod
    def get_series_slug_from_url(url):
        """Extract the series slug from a URL path."""
        try:
            path = (
                urlparse(url).path
                if url.startswith('http') else url
            )
            parts = path.split('/')
            if 'serie' in parts:
                idx = parts.index('serie')
                if idx + 1 < len(parts) and parts[idx + 1]:
                    return parts[idx + 1]
            return 'unknown'
        except Exception:  # pylint: disable=broad-exception-caught
            return 'unknown'

    @staticmethod
    def normalize_to_series_url(url):
        """Normalize a URL to the canonical series URL."""
        if not url:
            return url
        url = url.split('?')[0].split('#')[0]
        m = _SERIE_PATH_RE.search(url)
        if m:
            return f"{SITE_URL}{m.group(1)}"
        return url

    # -- Pause detection ---------------------------------------------

    def _check_pause(self):
        """Check whether a pause file exists (cached 5s)."""
        now = time.time()
        if now - self._last_pause_check < 5:
            return self._pause_cached
        self._last_pause_check = now
        self._pause_cached = os.path.exists(self.pause_file)
        return self._pause_cached

    def _clear_pause_file(self):
        """Remove the pause file if present."""
        try:
            if os.path.exists(self.pause_file):
                os.remove(self.pause_file)
        except OSError:
            pass

    # -- Index helpers (for new_only mode) ---------------------------

    def load_existing_slugs(self) -> set[str]:
        """Load slugs of all series already in the index."""
        existing = set()
        try:
            if os.path.exists(SERIES_INDEX_FILE):
                with open(
                    SERIES_INDEX_FILE, 'r', encoding='utf-8',
                ) as f:
                    data = json.load(f)
                items = (
                    data if isinstance(data, list)
                    else list(data.values())
                )
                for item in items:
                    url = (
                        item.get('url', '')
                        or item.get('link', '')
                    )
                    if url:
                        existing.add(
                            self.get_series_slug_from_url(url)
                        )
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        existing.discard('unknown')
        return existing

    # -- Async internals ---------------------------------------------

    async def _create_logged_in_client(
        self,
    ) -> httpx.AsyncClient:
        """Create an httpx client and log in to bs.to."""
        client = httpx.AsyncClient(
            headers={"User-Agent": UA},
            timeout=httpx.Timeout(
                REQUEST_TIMEOUT, connect=10.0,
            ),
            follow_redirects=True,
            limits=httpx.Limits(
                max_connections=2,
                max_keepalive_connections=1,
            ),
        )
        resp = await client.get(LOGIN_URL)
        soup = BeautifulSoup(resp.text, "html.parser")
        token_input = soup.find(
            "input", {"name": "security_token"},
        )
        token = (
            token_input["value"] if token_input else ""
        )
        if not token:
            logger.warning(
                "CSRF security_token not found on login page"
            )

        login_resp = await client.post(LOGIN_URL, data={
            "login[user]": USERNAME,
            "login[pass]": PASSWORD,
            "security_token": token,
        }, follow_redirects=True)

        if "logout" not in login_resp.text.lower():
            await client.aclose()
            raise RuntimeError(
                "Login failed \u2014 check credentials"
            )

        return client

    async def _get_all_series(
        self, client: httpx.AsyncClient,
    ) -> list[dict]:
        """Fetch the full series catalogue from bs.to."""
        resp = await client.get(SERIES_LIST_URL)
        if not _is_logged_in(resp.text):
            raise RuntimeError(
                "Not logged in \u2014 "
                "cannot fetch series catalogue"
            )
        soup = BeautifulSoup(resp.text, "html.parser")
        series, seen_slugs = [], set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not href.startswith("serie/"):
                continue
            title = a.get_text(strip=True)
            if (
                not title
                or title.lower().strip() in _UTILITY_PAGES
            ):
                continue
            slug = (
                href.split("/")[1] if "/" in href else href
            )
            if not slug or slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            series.append({
                "title": title,
                "link": f"/{href}",
                "url": f"{SITE_URL}/{href}",
            })
        return series

    async def _scrape_one_series(
        self, client: httpx.AsyncClient, info: dict,
    ) -> dict:  # pylint: disable=too-many-locals,too-many-return-statements
        """Scrape a single series and return its data."""
        url = info["url"]
        try:
            resp = await client.get(
                url, follow_redirects=True,
            )
        except httpx.HTTPError as exc:
            return self._error_result(info, str(exc))

        html = resp.text

        error_code = _check_error_page(html)
        if error_code:
            reason = (
                f"{error_code} server error"
                if error_code in _SERVER_ERROR_CODES
                else f"{error_code} error page"
            )
            logger.warning(
                "Error page detected for %s: %s",
                url, error_code,
            )
            return self._error_result(info, reason)

        if not _is_logged_in(html):
            logger.error(
                "Session expired while scraping %s", url,
            )
            return self._error_result(
                info, "session expired \u2014 not logged in",
            )

        title = _extract_title(html) or info["title"]
        if title.lower().strip() in _UTILITY_PAGES:
            return self._error_result(info, "utility page")

        season_links = _extract_season_links(html, url)
        if not season_links:
            return self._error_result(
                info, "no seasons found",
            )

        seasons_data = []
        total_watched, total_eps = 0, 0

        for label, season_url in season_links:
            try:
                sr = await client.get(
                    season_url, follow_redirects=True,
                )
            except httpx.HTTPError as exc:
                return self._error_result(
                    info,
                    f"season {label} fetch failed: {exc}",
                )
            episodes = _parse_episodes(sr.text)
            if episodes is None:
                return self._error_result(
                    info,
                    f"season {label} episode parse failed",
                )
            watched_count = sum(
                1 for ep in episodes if ep["watched"]
            )
            total_count = len(episodes)
            seasons_data.append({
                "season": label,
                "url": season_url,
                "episodes": episodes,
                "watched_episodes": watched_count,
                "total_episodes": total_count,
            })
            total_watched += watched_count
            total_eps += total_count

        return {
            "title": title,
            "link": info["link"],
            "url": info["url"],
            "total_seasons": len(seasons_data),
            "total_episodes": total_eps,
            "watched_episodes": total_watched,
            "unwatched_episodes": max(
                0, total_eps - total_watched,
            ),
            "seasons": seasons_data,
        }

    @staticmethod
    def _error_result(info: dict, reason: str) -> dict:
        """Build a standardised error result dict."""
        return {
            "title": f"[ERROR: {reason}]",
            "link": info.get("link", ""),
            "url": info.get("url", ""),
            "total_seasons": 0,
            "total_episodes": 0,
            "watched_episodes": 0,
            "unwatched_episodes": 0,
            "seasons": [],
        }

    # -- Worker ------------------------------------------------------

    async def _worker(  # pylint: disable=too-many-arguments
        self, worker_id: int, queue: asyncio.Queue,
        results: list, progress: dict, total: int,
    ):  # pylint: disable=too-many-positional-arguments
        """Process series from the queue using its own session."""
        try:
            client = await self._create_logged_in_client()
        except RuntimeError:
            logger.warning(
                "Worker %d login failed, retrying...",
                worker_id,
            )
            await asyncio.sleep(1)
            try:
                client = await self._create_logged_in_client()
            except RuntimeError:
                logger.error(
                    "Worker %d login failed permanently",
                    worker_id,
                )
                return

        try:
            await self._worker_loop(
                worker_id, client, queue,
                results, progress, total,
            )
        finally:
            await client.aclose()

    async def _worker_loop(  # pylint: disable=too-many-arguments
        self, worker_id, client, queue,
        results, progress, total,
    ):  # pylint: disable=too-many-positional-arguments
        """Inner loop for a worker — processes queue items."""
        while True:
            if self._check_pause():
                raise ScrapingPaused("Pause file detected")

            try:
                info = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            try:
                result = await self._scrape_one_series(
                    client, info,
                )
            except ScrapingPaused:
                raise
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.error(
                    "Worker %d unexpected error on %s: %s",
                    worker_id, info.get('url', '?'), exc,
                )
                self.failed_links.append({
                    "url": info["url"],
                    "title": info.get("title", ""),
                    "link": info.get("link", ""),
                    "reason": f"unexpected_error: {exc}",
                })
                progress["done"] += 1
                continue

            self._record_worker_result(
                info, result, results,
            )

            link = info.get("link", "")
            if link:
                self.completed_links.add(link)

            progress["done"] += 1
            done = progress["done"]

            self._print_progress(
                done, total, progress, result, info,
            )

            if done % CHECKPOINT_EVERY == 0:
                self.series_data = list(results)
                self.save_checkpoint(include_data=True)

    def _record_worker_result(self, info, result, results):
        """Classify result as success or failure."""
        if result["title"].startswith("[ERROR"):
            self.failed_links.append({
                "url": info["url"],
                "title": info.get("title", ""),
                "link": info.get("link", ""),
                "reason": result["title"],
            })
        else:
            if result["total_episodes"] == 0:
                self.failed_links.append({
                    "url": info["url"],
                    "title": result.get(
                        "title", info.get("title", ""),
                    ),
                    "link": info.get("link", ""),
                    "reason": "zero_episodes",
                })
            results.append(result)

    @staticmethod
    def _print_progress(  # pylint: disable=too-many-locals
        done, total, progress, result, info,
    ):
        """Print a progress bar line for one series."""
        elapsed = time.perf_counter() - progress["start"]
        rate = done / elapsed if elapsed > 0 else 0
        eta = (total - done) / rate if rate > 0 else 0
        eta_mins = f"{eta / 60:.1f}"
        pct = int((done / total) * 100)
        bar_len = 30
        filled = int(bar_len * done / total)
        progress_bar = (
            '\u2588' * filled + '\u2591' * (bar_len - filled)
        )

        season_labels = [
            s.get('season', '?')
            for s in result.get('seasons', [])
        ]
        season_info = (
            f" [{','.join(season_labels)}]"
            if season_labels else ""
        )

        prefix = (
            f"[{done}/{total}] [{progress_bar}] "
            f"{pct}% | ETA: {eta_mins}m"
        )
        if result["title"].startswith("[ERROR"):
            title = info.get('title', '?')
            print(
                f"{prefix} | \u26a0 {title}: Failed"
            )
        elif result["total_episodes"] == 0:
            print(
                f"{prefix} | \u26a0 "
                f"{result['title']}{season_info}: "
                "No episodes"
            )
        else:
            w = result['watched_episodes']
            t = result['total_episodes']
            print(
                f"{prefix} | \u2713 "
                f"{result['title']}{season_info}: "
                f"{w}/{t} watched"
            )

    # -- Async scrape orchestrators ----------------------------------

    def _filter_completed(
        self, series_list: list[dict],
    ) -> list[dict] | None:
        """Remove already-completed series from the list."""
        if not self.completed_links:
            return series_list
        before = len(series_list)
        filtered = [
            s for s in series_list
            if s.get('link') not in self.completed_links
        ]
        if before != len(filtered):
            print(
                f"  Skipping {before - len(filtered)} "
                "already-completed series"
            )
        if not filtered:
            print(
                "\u2713 All series already scraped "
                "(from checkpoint)"
            )
            return None
        return filtered

    async def _scrape_list(
        self,
        series_list: list[dict],
        num_workers: int | None = None,
    ):
        """Scrape a list of series with multi-session workers."""
        filtered = self._filter_completed(series_list)
        if filtered is None:
            return

        queue: asyncio.Queue = asyncio.Queue()
        for s in filtered:
            queue.put_nowait(s)

        results: list[dict] = list(self.series_data)
        n = min(num_workers or NUM_WORKERS, len(filtered))
        progress = {
            "done": 0, "start": time.perf_counter(),
        }

        print(
            f"\u2192 Scraping {len(filtered)} series "
            f"with {n} session(s)..."
        )

        tasks = [
            asyncio.create_task(
                self._worker(
                    i, queue, results, progress,
                    len(filtered),
                )
            )
            for i in range(n)
        ]
        try:
            await asyncio.gather(*tasks)
        except ScrapingPaused:
            for t in tasks:
                t.cancel()
            await asyncio.gather(
                *tasks, return_exceptions=True,
            )
            self.series_data = results
            raise

        self.series_data = results

    async def _async_run(
        self, single_url=None, url_list=None,
        new_only=False, retry_failed=False,
    ):
        """Async core of run()."""
        tmp = await self._create_logged_in_client()
        try:
            print("\u2713 Logged in to bs.to")
            await self._async_run_inner(
                tmp, single_url=single_url,
                url_list=url_list, new_only=new_only,
                retry_failed=retry_failed,
            )
        finally:
            if not tmp.is_closed:
                await tmp.aclose()

    async def _async_run_inner(
        self, tmp, single_url=None, url_list=None,
        new_only=False, retry_failed=False,
    ):  # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Dispatch to the correct scraping strategy."""
        if single_url:
            await self._run_single(tmp, single_url)
            return

        if url_list:
            await self._run_url_list(tmp, url_list)
            return

        if retry_failed:
            await self._run_retry(tmp)
            return

        if new_only:
            await self._run_new_only(tmp)
            return

        await self._run_all(tmp)

    async def _run_single(self, tmp, single_url):
        """Scrape a single series by URL."""
        self._checkpoint_mode = 'single'
        main_url = self.normalize_to_series_url(single_url)
        m = _SERIE_PATH_RE.search(main_url)
        link = m.group(1) if m else main_url
        info = {
            "title": main_url.split("/")[-1],
            "link": link, "url": main_url,
        }
        print(
            f"\u2192 Scraping single series: {main_url}"
        )
        result = await self._scrape_one_series(tmp, info)
        await tmp.aclose()
        if result["title"].startswith("[ERROR"):
            self.failed_links.append(info)
        self.series_data = [result]

    async def _run_url_list(self, tmp, url_list):
        """Scrape a batch of series from a URL list."""
        self._checkpoint_mode = 'batch'
        series_list = []
        for u in url_list:
            main_url = self.normalize_to_series_url(u)
            m = _SERIE_PATH_RE.search(main_url)
            link = m.group(1) if m else main_url
            series_list.append({
                "title": main_url.split("/")[-1],
                "link": link, "url": main_url,
            })
        await tmp.aclose()
        n = (
            NUM_WORKERS
            if self._use_parallel and len(series_list) > 1
            else 1
        )
        await self._scrape_list(
            series_list, num_workers=n,
        )
        print(
            f"  Successfully scraped: "
            f"{len(self.series_data)}/{len(url_list)} series"
        )

    async def _run_retry(self, tmp):
        """Retry previously failed series."""
        self._checkpoint_mode = 'retry'
        failed_list = self.load_failed_series()
        await tmp.aclose()
        if not failed_list:
            print("\u2713 No failed series found")
            return
        print(
            f"\u2713 Found {len(failed_list)} failed series "
            "\u2014 retrying in sequential mode"
        )
        await self._scrape_list(failed_list, num_workers=1)

    async def _run_new_only(self, tmp):
        """Scrape only new (not-yet-indexed) series."""
        self._checkpoint_mode = 'new_only'
        print("\u2192 Fetching series list...")
        all_series = await self._get_all_series(tmp)
        await tmp.aclose()
        self.all_discovered_series = all_series
        existing_slugs = self.load_existing_slugs()
        ignored_slugs = self.get_ignored_slugs()
        new_list = [
            s for s in all_series
            if (
                self.get_series_slug_from_url(
                    s.get('link', ''),
                ) not in existing_slugs
                and self.get_series_slug_from_url(
                    s.get('link', ''),
                ) not in ignored_slugs
            )
        ]
        total = len(all_series)
        print(
            f"\u2192 New series to scrape: "
            f"{len(new_list)} (out of {total})"
        )
        if not new_list:
            print(
                "\u2713 No new series detected "
                "\u2014 nothing to scrape"
            )
            return
        if len(new_list) <= 50:
            for s in new_list:
                print(f"  + {s['title']}")
        n = (
            NUM_WORKERS if self._use_parallel else 1
        )
        await self._scrape_list(
            new_list, num_workers=n,
        )

    async def _run_all(self, tmp):
        """Scrape all series (default mode)."""
        self._checkpoint_mode = 'all_series'
        print("\u2192 Fetching series list...")
        all_series = await self._get_all_series(tmp)
        await tmp.aclose()
        self.all_discovered_series = all_series
        ignored_slugs = self.get_ignored_slugs()
        print(
            f"\u2713 Found {len(all_series)} series"
        )

        existing_slugs = self.load_existing_slugs()
        new_titles = [
            s["title"] for s in all_series
            if (
                self.get_series_slug_from_url(
                    s.get('link', ''),
                ) not in existing_slugs
                and self.get_series_slug_from_url(
                    s.get('link', ''),
                ) not in ignored_slugs
            )
        ]
        if new_titles:
            print(
                f"\n\u2139 {len(new_titles)} "
                "new series detected:"
            )
            for t in new_titles[:10]:
                print(f"  + {t}")
            if len(new_titles) > 10:
                print(
                    f"  ... and {len(new_titles) - 10} more"
                )
            print()

        if ignored_slugs:
            all_series = [
                s for s in all_series
                if self.get_series_slug_from_url(
                    s.get('link', ''),
                ) not in ignored_slugs
            ]
            skipped = (
                len(self.all_discovered_series)
                - len(all_series)
            )
            if skipped:
                print(
                    f"  Skipping {skipped} ignored series"
                )
        n = NUM_WORKERS if self._use_parallel else 1
        await self._scrape_list(all_series, num_workers=n)
        print(
            f"\n\u2713 Successfully scraped "
            f"{len(self.series_data)} series"
        )

    # -- Public API --------------------------------------------------

    def run(
        self, single_url=None, url_list=None,
        new_only=False, resume_only=False,
        retry_failed=False, parallel=None,
    ):  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-branches  # noqa: E501
        """Main entry point: login, scrape, save checkpoint."""
        if parallel is not None:
            self._use_parallel = parallel
            mode_name = (
                'multi-session' if parallel
                else 'single-session'
            )
            print(f"\u2192 Using {mode_name} mode")
        else:
            self._use_parallel = True

        self._clear_pause_file()

        try:
            if resume_only:
                if self.load_checkpoint():
                    count = len(self.completed_links)
                    print(
                        "\u2192 Resuming from checkpoint "
                        f"({count} series already done)"
                    )
                else:
                    print(
                        "\u26a0 No checkpoint found. "
                        "Starting fresh..."
                    )

            asyncio.run(self._async_run(
                single_url=single_url,
                url_list=url_list,
                new_only=new_only,
                retry_failed=retry_failed,
            ))

            empty = [
                s for s in self.series_data
                if s.get('total_episodes', 0) == 0
            ]
            if empty:
                print(
                    f"\n\u26a0 {len(empty)} series "
                    "with 0 episodes:"
                )
                for s in empty:
                    print(
                        f"  \u2022 {s['title']} "
                        f"\u2192 {s['url']}"
                    )

            self.save_checkpoint(include_data=True)
            if not self.failed_links:
                self.clear_failed_series()
            else:
                self.save_failed_series()

        except ScrapingPaused:
            self.paused = True
            self._clear_pause_file()
            self.save_checkpoint(include_data=True)
            if self.failed_links:
                self.save_failed_series()
        except BaseException:
            self.save_checkpoint(include_data=True)
            if self.failed_links:
                self.save_failed_series()
            raise
