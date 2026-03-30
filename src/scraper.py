"""
BS.TO Series Scraper — powered by httpx (no browser needed).

Uses multiple independent httpx sessions (each logged in separately) to work
around bs.to's server-side session locking that breaks watched status under
concurrency.  Each session processes its share of series sequentially.
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
    USERNAME, PASSWORD, DATA_DIR, SERIES_INDEX_FILE, NUM_WORKERS,
)

logger = logging.getLogger(__name__)

# ── Constants ───────────────────────────────────────────────────────────────
SITE_URL = "https://bs.to"
LOGIN_URL = f"{SITE_URL}/login"
SERIES_LIST_URL = f"{SITE_URL}/andere-serien"
CHECKPOINT_EVERY = 10
REQUEST_TIMEOUT = 20.0
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0"

_SERIE_PATH_RE = re.compile(r'(/serie/[^/]+)')
_UTILITY_PAGES = {'alle serien', 'andere serien', 'beliebte serien',
                  'neue serien', 'empfehlung', 'meistgesehen'}


# ── HTML helpers ────────────────────────────────────────────────────────────

def _parse_episodes(html: str) -> list[dict]:
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
            ep_num = row.get("data-episode-season-id", "") or str(idx)
        try:
            ep_num_int = int(ep_num)
        except ValueError:
            ep_num_int = idx
        title_tag = cols[1].find("strong")
        title = title_tag.get_text(strip=True) if title_tag else cols[1].get_text(strip=True)
        watched = "watched" in (row.get("class") or [])
        episodes.append({"number": ep_num_int, "title": title, "watched": watched})
    return episodes


def _extract_season_links(html: str, base_url: str) -> list[tuple[str, str]]:
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
    soup = BeautifulSoup(html, "html.parser")
    h2 = soup.find("h2")
    if h2:
        text = h2.get_text(strip=True)
        text = re.sub(r'\s*Staffel\s*\d+.*$', '', text)
        return text or None
    return None


# ── Exception ───────────────────────────────────────────────────────────────

class ScrapingPaused(Exception):
    pass


# ── BsToScraper (httpx) ────────────────────────────────────────────────────

class BsToScraper:
    """BS.TO series scraper powered by httpx (no browser needed)."""

    def __init__(self):
        self.series_data: list[dict] = []
        self.all_discovered_series: list[dict] | None = None
        self.completed_links: set[str] = set()
        self.failed_links: list[dict] = []

        self.checkpoint_file = os.path.join(DATA_DIR, '.scrape_checkpoint.json')
        self.failed_file = os.path.join(DATA_DIR, '.failed_series.json')
        self.ignore_file = os.path.join(DATA_DIR, '.ignored_series.json')
        self.pause_file = os.path.join(DATA_DIR, '.pause_scraping')

        self._checkpoint_mode: str | None = None
        self._use_parallel: bool = True
        self._lock = threading.Lock()
        self._last_pause_check = 0.0
        self._pause_cached = False
        self.paused = False

    # ── Static / class methods ──────────────────────────────────────────────

    @staticmethod
    def get_checkpoint_mode(data_dir):
        cp_file = os.path.join(data_dir, '.scrape_checkpoint.json')
        try:
            if os.path.exists(cp_file):
                with open(cp_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    return data.get('mode')
        except Exception:
            pass
        return None

    # ── Checkpoint management ───────────────────────────────────────────────

    def save_checkpoint(self, include_data=False):
        with self._lock:
            payload = {
                'completed_links': list(self.completed_links),
                'mode': self._checkpoint_mode,
                'timestamp': time.time(),
            }
            if include_data:
                payload['series_data'] = self.series_data
            tmp = self.checkpoint_file + '.tmp'
            try:
                with open(tmp, 'w', encoding='utf-8') as f:
                    json.dump(payload, f, ensure_ascii=False)
                os.replace(tmp, self.checkpoint_file)
            except Exception as e:
                logger.error(f"Failed to save checkpoint: {e}")

    def load_checkpoint(self) -> bool:
        with self._lock:
            try:
                if not os.path.exists(self.checkpoint_file):
                    return False
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    self.completed_links = set(data.get('completed_links', []))
                    self._checkpoint_mode = data.get('mode')
                    saved_data = data.get('series_data')
                    if saved_data:
                        self.series_data = saved_data
                elif isinstance(data, list):
                    self.completed_links = set(data)
                return bool(self.completed_links)
            except Exception as e:
                logger.error(f"Failed to load checkpoint: {e}")
                return False

    def clear_checkpoint(self):
        with self._lock:
            try:
                if os.path.exists(self.checkpoint_file):
                    os.remove(self.checkpoint_file)
            except OSError:
                pass

    # ── Failed series management ────────────────────────────────────────────

    def save_failed_series(self):
        with self._lock:
            existing = []
            try:
                if os.path.exists(self.failed_file):
                    with open(self.failed_file, 'r', encoding='utf-8') as f:
                        existing = json.load(f)
            except Exception:
                pass
            seen = {e.get('url') for e in existing if isinstance(e, dict)}
            for f in self.failed_links:
                if isinstance(f, dict) and f.get('url') not in seen:
                    existing.append(f)
                    seen.add(f.get('url'))
            tmp = self.failed_file + '.tmp'
            try:
                with open(tmp, 'w', encoding='utf-8') as f_out:
                    json.dump(existing, f_out, indent=2, ensure_ascii=False)
                os.replace(tmp, self.failed_file)
            except Exception as e:
                logger.error(f"Failed to save failed series: {e}")

    def load_failed_series(self) -> list:
        with self._lock:
            try:
                if os.path.exists(self.failed_file):
                    with open(self.failed_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    if isinstance(data, list):
                        return data
            except Exception:
                pass
            return []

    def clear_failed_series(self):
        with self._lock:
            try:
                if os.path.exists(self.failed_file):
                    os.remove(self.failed_file)
            except OSError:
                pass

    # ── Ignore list management ──────────────────────────────────────────────

    def load_ignored_series(self) -> list[dict]:
        try:
            if os.path.exists(self.ignore_file):
                with open(self.ignore_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, list):
                    return data
        except Exception:
            pass
        return []

    def save_ignored_series(self, ignored: list[dict]):
        tmp = self.ignore_file + '.tmp'
        try:
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(ignored, f, indent=2, ensure_ascii=False)
            os.replace(tmp, self.ignore_file)
        except Exception as e:
            logger.error(f"Failed to save ignored series: {e}")

    def get_ignored_slugs(self) -> set[str]:
        return {self.get_series_slug_from_url(s.get('url', '')) for s in self.load_ignored_series()} - {'unknown'}

    # ── URL helpers ─────────────────────────────────────────────────────────

    def get_series_slug_from_url(self, url):
        try:
            path = urlparse(url).path if url.startswith('http') else url
            parts = path.split('/')
            if 'serie' in parts:
                idx = parts.index('serie')
                if idx + 1 < len(parts) and parts[idx + 1]:
                    return parts[idx + 1]
            return 'unknown'
        except Exception:
            return 'unknown'

    def normalize_to_series_url(self, url):
        if not url:
            return url
        url = url.split('?')[0].split('#')[0]
        m = _SERIE_PATH_RE.search(url)
        if m:
            return f"{SITE_URL}{m.group(1)}"
        return url

    # ── Pause detection ─────────────────────────────────────────────────────

    def _check_pause(self):
        now = time.time()
        if now - self._last_pause_check < 5:
            return self._pause_cached
        self._last_pause_check = now
        self._pause_cached = os.path.exists(self.pause_file)
        return self._pause_cached

    def _clear_pause_file(self):
        try:
            if os.path.exists(self.pause_file):
                os.remove(self.pause_file)
        except OSError:
            pass

    # ── Index helpers (for new_only mode) ───────────────────────────────────

    def load_existing_slugs(self) -> set[str]:
        existing = set()
        try:
            if os.path.exists(SERIES_INDEX_FILE):
                with open(SERIES_INDEX_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                items = data if isinstance(data, list) else list(data.values())
                for item in items:
                    url = item.get('url', '') or item.get('link', '')
                    if url:
                        existing.add(self.get_series_slug_from_url(url))
        except Exception:
            pass
        existing.discard('unknown')
        return existing

    # ── Async internals ─────────────────────────────────────────────────────

    async def _create_logged_in_client(self) -> httpx.AsyncClient:
        client = httpx.AsyncClient(
            headers={"User-Agent": UA},
            timeout=httpx.Timeout(REQUEST_TIMEOUT, connect=10.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=2, max_keepalive_connections=1),
        )
        resp = await client.get(LOGIN_URL)
        soup = BeautifulSoup(resp.text, "html.parser")
        token_input = soup.find("input", {"name": "security_token"})
        token = token_input["value"] if token_input else ""

        login_resp = await client.post(LOGIN_URL, data={
            "login[user]": USERNAME,
            "login[pass]": PASSWORD,
            "security_token": token,
        }, follow_redirects=True)

        if "logout" not in login_resp.text.lower():
            await client.aclose()
            raise RuntimeError("Login failed — check credentials")

        return client

    async def _get_all_series(self, client: httpx.AsyncClient) -> list[dict]:
        resp = await client.get(SERIES_LIST_URL)
        soup = BeautifulSoup(resp.text, "html.parser")
        series, seen_slugs = [], set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not href.startswith("serie/"):
                continue
            title = a.get_text(strip=True)
            if not title or title.lower().strip() in _UTILITY_PAGES:
                continue
            slug = href.split("/")[1] if "/" in href else href
            if not slug or slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            series.append({
                "title": title,
                "link": f"/{href}",
                "url": f"{SITE_URL}/{href}",
            })
        return series

    async def _scrape_one_series(self, client: httpx.AsyncClient, info: dict) -> dict:
        url = info["url"]
        try:
            resp = await client.get(url, follow_redirects=True)
        except httpx.HTTPError as e:
            return self._error_result(info, str(e))

        html = resp.text
        title = _extract_title(html) or info["title"]
        if title.lower().strip() in _UTILITY_PAGES:
            return self._error_result(info, "utility page")

        season_links = _extract_season_links(html, url)
        if not season_links:
            season_links = [("1", url)]

        seasons_data = []
        total_watched, total_eps = 0, 0

        for label, season_url in season_links:
            try:
                sr = await client.get(season_url, follow_redirects=True)
            except httpx.HTTPError:
                continue
            episodes = _parse_episodes(sr.text)
            watched_count = sum(1 for ep in episodes if ep["watched"])
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
            "unwatched_episodes": max(0, total_eps - total_watched),
            "seasons": seasons_data,
        }

    @staticmethod
    def _error_result(info: dict, reason: str) -> dict:
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

    # ── Worker ──────────────────────────────────────────────────────────────

    async def _worker(self, worker_id: int, queue: asyncio.Queue,
                      results: list, progress: dict, total: int):
        try:
            client = await self._create_logged_in_client()
        except RuntimeError:
            logger.warning(f"Worker {worker_id} login failed, retrying...")
            await asyncio.sleep(1)
            try:
                client = await self._create_logged_in_client()
            except RuntimeError:
                logger.error(f"Worker {worker_id} login failed permanently")
                return

        try:
            while True:
                if self._check_pause():
                    raise ScrapingPaused("Pause file detected")

                try:
                    info = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                result = await self._scrape_one_series(client, info)

                if result["title"].startswith("[ERROR"):
                    self.failed_links.append({
                        "url": info["url"],
                        "title": info.get("title", ""),
                        "link": info.get("link", ""),
                    })
                else:
                    if result["total_episodes"] == 0:
                        self.failed_links.append({
                            "url": info["url"],
                            "title": result.get("title", info.get("title", "")),
                            "link": info.get("link", ""),
                            "reason": "zero_episodes",
                        })
                    results.append(result)

                link = info.get("link", "")
                if link:
                    self.completed_links.add(link)

                progress["done"] += 1
                done = progress["done"]

                # Progress bar + ETA
                elapsed = time.perf_counter() - progress["start"]
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                eta_mins = f"{eta / 60:.1f}"
                pct = int((done / total) * 100)
                bar_len = 30
                filled = int(bar_len * done / total)
                bar = '█' * filled + '░' * (bar_len - filled)

                season_labels = [s.get('season', '?') for s in result.get('seasons', [])]
                season_info = f" [{','.join(season_labels)}]" if season_labels else ""

                if result["title"].startswith("[ERROR"):
                    print(f"[{done}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m | ⚠ {info.get('title', '?')}: Failed")
                elif result["total_episodes"] == 0:
                    print(f"[{done}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m | ⚠ {result['title']}{season_info}: No episodes")
                else:
                    print(f"[{done}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m | ✓ {result['title']}{season_info}: {result['watched_episodes']}/{result['total_episodes']} watched")

                if done % CHECKPOINT_EVERY == 0:
                    self.series_data = list(results)
                    self.save_checkpoint(include_data=True)
        finally:
            await client.aclose()

    # ── Async scrape orchestrators ──────────────────────────────────────────

    def _filter_completed(self, series_list: list[dict]) -> list[dict] | None:
        if not self.completed_links:
            return series_list
        before = len(series_list)
        filtered = [s for s in series_list if s.get('link') not in self.completed_links]
        if before != len(filtered):
            print(f"  Skipping {before - len(filtered)} already-completed series")
        if not filtered:
            print("✓ All series already scraped (from checkpoint)")
            return None
        return filtered

    async def _scrape_list(self, series_list: list[dict], num_workers: int | None = None):
        """Scrape a list of series using multi-session workers."""
        filtered = self._filter_completed(series_list)
        if filtered is None:
            return

        queue: asyncio.Queue = asyncio.Queue()
        for s in filtered:
            queue.put_nowait(s)

        results: list[dict] = list(self.series_data)  # keep checkpoint data
        n = min(num_workers or NUM_WORKERS, len(filtered))
        progress = {"done": 0, "start": time.perf_counter()}

        print(f"→ Scraping {len(filtered)} series with {n} session(s)...")

        tasks = [
            self._worker(i, queue, results, progress, len(filtered))
            for i in range(n)
        ]
        await asyncio.gather(*tasks)

        self.series_data = results

    async def _async_run(self, single_url=None, url_list=None,
                         new_only=False, retry_failed=False):
        """Async core of run()."""
        # Use a temp client for discovery, then close it
        tmp = await self._create_logged_in_client()
        try:
            print("✓ Logged in to bs.to")
            await self._async_run_inner(tmp, single_url=single_url,
                                        url_list=url_list, new_only=new_only,
                                        retry_failed=retry_failed)
        finally:
            if not tmp.is_closed:
                await tmp.aclose()

    async def _async_run_inner(self, tmp, single_url=None, url_list=None,
                               new_only=False, retry_failed=False):

        if single_url:
            self._checkpoint_mode = 'single'
            main_url = self.normalize_to_series_url(single_url)
            m = _SERIE_PATH_RE.search(main_url)
            link = m.group(1) if m else main_url
            info = {"title": main_url.split("/")[-1], "link": link, "url": main_url}
            print(f"→ Scraping single series: {main_url}")
            result = await self._scrape_one_series(tmp, info)
            await tmp.aclose()
            if result["title"].startswith("[ERROR"):
                self.failed_links.append(info)
            self.series_data = [result]
            return

        if url_list:
            self._checkpoint_mode = 'batch'
            series_list = []
            for u in url_list:
                main_url = self.normalize_to_series_url(u)
                m = _SERIE_PATH_RE.search(main_url)
                link = m.group(1) if m else main_url
                series_list.append({"title": main_url.split("/")[-1], "link": link, "url": main_url})
            await tmp.aclose()
            n = NUM_WORKERS if self._use_parallel and len(series_list) > 1 else 1
            await self._scrape_list(series_list, num_workers=n)
            print(f"  Successfully scraped: {len(self.series_data)}/{len(url_list)} series")
            return

        if retry_failed:
            self._checkpoint_mode = 'retry'
            failed_list = self.load_failed_series()
            await tmp.aclose()
            if not failed_list:
                print("✓ No failed series found")
                return
            print(f"✓ Found {len(failed_list)} failed series — retrying in sequential mode")
            await self._scrape_list(failed_list, num_workers=1)
            return

        if new_only:
            self._checkpoint_mode = 'new_only'
            print("→ Fetching series list...")
            all_series = await self._get_all_series(tmp)
            await tmp.aclose()
            self.all_discovered_series = all_series
            existing_slugs = self.load_existing_slugs()
            ignored_slugs = self.get_ignored_slugs()
            new_list = [s for s in all_series
                        if self.get_series_slug_from_url(s.get('link', '')) not in existing_slugs
                        and self.get_series_slug_from_url(s.get('link', '')) not in ignored_slugs]
            print(f"→ New series to scrape: {len(new_list)} (out of {len(all_series)})")
            if not new_list:
                print("✓ No new series detected — nothing to scrape")
                return
            if len(new_list) <= 50:
                for s in new_list:
                    print(f"  + {s['title']}")
            await self._scrape_list(new_list, num_workers=1)
            return

        # Default: scrape all
        self._checkpoint_mode = 'all_series'
        print("→ Fetching series list...")
        all_series = await self._get_all_series(tmp)
        await tmp.aclose()
        self.all_discovered_series = all_series
        ignored_slugs = self.get_ignored_slugs()
        print(f"✓ Found {len(all_series)} series")

        # New series detection
        existing_slugs = self.load_existing_slugs()
        new_titles = [s["title"] for s in all_series
                      if self.get_series_slug_from_url(s.get('link', '')) not in existing_slugs
                      and self.get_series_slug_from_url(s.get('link', '')) not in ignored_slugs]
        if new_titles:
            print(f"\nℹ {len(new_titles)} new series detected:")
            for t in new_titles:
                print(f"  + {t}")
            print()

        if ignored_slugs:
            all_series = [s for s in all_series
                          if self.get_series_slug_from_url(s.get('link', '')) not in ignored_slugs]
            skipped = len(self.all_discovered_series) - len(all_series)
            if skipped:
                print(f"  Skipping {skipped} ignored series")
        n = NUM_WORKERS if self._use_parallel else 1
        await self._scrape_list(all_series, num_workers=n)
        print(f"\n✓ Successfully scraped {len(self.series_data)} series")

    # ── Public API ───────────────────────────────────────────────────────────

    def run(self, single_url=None, url_list=None, new_only=False,
            resume_only=False, retry_failed=False, parallel=None):
        """Main entry point: login, scrape, save checkpoint."""
        if parallel is not None:
            self._use_parallel = parallel
            print(f"→ Using {'multi-session' if parallel else 'single-session'} mode")
        else:
            self._use_parallel = True

        # Clear any stale pause file from a previous run
        self._clear_pause_file()

        try:
            if resume_only:
                if self.load_checkpoint():
                    print(f"→ Resuming from checkpoint ({len(self.completed_links)} series already done)")
                else:
                    print("⚠ No checkpoint found. Starting fresh...")

            asyncio.run(self._async_run(
                single_url=single_url,
                url_list=url_list,
                new_only=new_only,
                retry_failed=retry_failed,
            ))

            # Alert for empty series (0 episodes)
            empty = [s for s in self.series_data if s.get('total_episodes', 0) == 0]
            if empty:
                print(f"\n⚠ {len(empty)} series with 0 episodes:")
                for s in empty:
                    print(f"  • {s['title']} → {s['url']}")

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


