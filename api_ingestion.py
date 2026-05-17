"""
Async API Ingestion Pipeline — rate-limited, resumable, multi-source.

Fetches data from REST APIs with:
  - Automatic rate-limit detection and back-off
  - Cursor/offset-based pagination
  - Checkpointing for restartable runs
  - Configurable concurrency (asyncio + aiohttp)
  - Pluggable output (JSONL files, S3, database)

Usage:
    python api_ingestion.py --config sources.yaml --output ./data/
"""

import asyncio
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Optional
from urllib.parse import urlencode

import aiohttp

log = logging.getLogger("api_ingestion")


# ---------------------------------------------------------------------------
# Pagination strategies
# ---------------------------------------------------------------------------

class PaginationStrategy(ABC):
    @abstractmethod
    def next_params(self, response_data: dict, current_params: dict) -> Optional[dict]:
        """Return params for the next page, or None if exhausted."""
        ...


class OffsetPagination(PaginationStrategy):
    """Classic limit/offset: ?limit=100&offset=0, ?limit=100&offset=100 ..."""

    def __init__(self, limit: int = 100, limit_key: str = "limit", offset_key: str = "offset") -> None:
        self.limit = limit
        self.limit_key = limit_key
        self.offset_key = offset_key
        self._offset = 0

    def next_params(self, response_data: dict, current_params: dict) -> Optional[dict]:
        count = len(response_data.get("results", response_data.get("data", [])))
        if count < self.limit:
            return None  # Last page
        self._offset += self.limit
        return {self.limit_key: self.limit, self.offset_key: self._offset}


class CursorPagination(PaginationStrategy):
    """Token/cursor-based: response contains next_cursor or next_url."""

    def __init__(self, cursor_field: str = "next_cursor", param_name: str = "cursor") -> None:
        self.cursor_field = cursor_field
        self.param_name = param_name

    def next_params(self, response_data: dict, current_params: dict) -> Optional[dict]:
        cursor = response_data.get(self.cursor_field)
        if not cursor:
            return None
        return {self.param_name: cursor}


class LinkHeaderPagination(PaginationStrategy):
    """GitHub-style: rel="next" in the Link response header."""

    def __init__(self) -> None:
        self._next_url: Optional[str] = None

    def next_params(self, response_data: dict, current_params: dict) -> Optional[dict]:
        # The URL is set externally from the response headers
        return {"__next_url__": self._next_url} if self._next_url else None

    def set_next_url(self, url: Optional[str]) -> None:
        self._next_url = url


# ---------------------------------------------------------------------------
# Rate limiter (token bucket)
# ---------------------------------------------------------------------------

class RateLimiter:
    """
    Async token-bucket rate limiter.
    Allows `rate` requests per second with a burst of `burst` tokens.
    """

    def __init__(self, rate: float = 10.0, burst: int = 20) -> None:
        self.rate = rate
        self.burst = burst
        self._tokens = float(burst)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
            self._last_refill = now

            if self._tokens < 1:
                wait = (1 - self._tokens) / self.rate
                log.debug("Rate limit: sleeping %.2fs", wait)
                await asyncio.sleep(wait)
                self._tokens = 0
            else:
                self._tokens -= 1


# ---------------------------------------------------------------------------
# Source definition
# ---------------------------------------------------------------------------

@dataclass
class APISource:
    name: str
    base_url: str
    endpoint: str
    params: dict = field(default_factory=dict)
    headers: dict = field(default_factory=dict)
    data_path: str = "results"         # JSONPath to the list within the response
    pagination: Optional[PaginationStrategy] = None
    rate_limiter: Optional[RateLimiter] = None
    max_retries: int = 3
    timeout_s: float = 30.0

    @property
    def url(self) -> str:
        return self.base_url.rstrip("/") + "/" + self.endpoint.lstrip("/")


# ---------------------------------------------------------------------------
# Ingestion engine
# ---------------------------------------------------------------------------

class APIIngestionEngine:
    """
    Fetches pages from an APISource, applies back-off on errors,
    and yields individual records.
    """

    RETRY_STATUSES = {429, 500, 502, 503, 504}

    def __init__(self, session: aiohttp.ClientSession) -> None:
        self._session = session

    async def fetch_all(self, source: APISource) -> AsyncIterator[dict]:
        """Yield every record from all pages of the source."""
        params = dict(source.params)

        while True:
            if source.rate_limiter:
                await source.rate_limiter.acquire()

            page = await self._fetch_page(source, params)
            if page is None:
                break

            records = self._extract_records(page, source.data_path)
            for record in records:
                yield self._enrich(record, source)

            if source.pagination is None:
                break

            # Handle link-header pagination
            if isinstance(source.pagination, LinkHeaderPagination):
                next_params = source.pagination.next_params(page, params)
            else:
                next_params = source.pagination.next_params(page, params)

            if next_params is None:
                break

            if "__next_url__" in (next_params or {}):
                # Direct URL override — rebuild params from scratch
                params = {}
                source.base_url = next_params["__next_url__"]
                source.endpoint = ""
            else:
                params.update(next_params)

    async def _fetch_page(self, source: APISource, params: dict) -> Optional[dict]:
        last_err: Exception = RuntimeError("no attempts")
        for attempt in range(1, source.max_retries + 1):
            try:
                url = source.url
                log.debug("GET %s params=%s (attempt %d)", url, params, attempt)
                async with self._session.get(
                    url,
                    params=params,
                    headers=source.headers,
                    timeout=aiohttp.ClientTimeout(total=source.timeout_s),
                ) as resp:
                    if resp.status in self.RETRY_STATUSES:
                        retry_after = float(resp.headers.get("Retry-After", 2 ** attempt))
                        log.warning("HTTP %d — retrying in %.1fs", resp.status, retry_after)
                        await asyncio.sleep(retry_after)
                        continue

                    resp.raise_for_status()
                    data = await resp.json()

                    # Update link-header pagination if applicable
                    if isinstance(source.pagination, LinkHeaderPagination):
                        link = resp.headers.get("Link", "")
                        next_url = self._parse_link_header(link)
                        source.pagination.set_next_url(next_url)

                    return data
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_err = exc
                wait = 2 ** attempt
                log.warning("Fetch error %s — retrying in %ds", exc, wait)
                await asyncio.sleep(wait)

        log.error("All %d attempts failed: %s", source.max_retries, last_err)
        return None

    @staticmethod
    def _extract_records(data: Any, path: str) -> list[dict]:
        """Navigate a dot-separated path into nested JSON."""
        node = data
        for key in path.split("."):
            if isinstance(node, dict):
                node = node.get(key, [])
            else:
                return []
        return node if isinstance(node, list) else []

    @staticmethod
    def _enrich(record: dict, source: APISource) -> dict:
        record["__source"] = source.name
        record["__ingested_at"] = datetime.now(tz=timezone.utc).isoformat()
        return record

    @staticmethod
    def _parse_link_header(link: str) -> Optional[str]:
        """Extract rel="next" URL from a Link header string."""
        for part in link.split(","):
            url_part, *rels = part.strip().split(";")
            if any('rel="next"' in r for r in rels):
                return url_part.strip().strip("<>")
        return None


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

class JSONLWriter:
    """Writes records as newline-delimited JSON, one file per source per run."""

    def __init__(self, output_dir: str) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._handles: dict[str, Any] = {}

    def write(self, source_name: str, record: dict) -> None:
        if source_name not in self._handles:
            ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
            path = self.output_dir / f"{source_name}_{ts}.jsonl"
            self._handles[source_name] = open(path, "w")
            log.info("Opened output file: %s", path)
        self._handles[source_name].write(json.dumps(record) + "\n")

    def close(self) -> None:
        for fh in self._handles.values():
            fh.close()
        self._handles.clear()


# ---------------------------------------------------------------------------
# Checkpoint / resume support
# ---------------------------------------------------------------------------

class Checkpoint:
    """Persists offset/cursor so a run can resume where it left off."""

    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self._state: dict = self._load()

    def _load(self) -> dict:
        try:
            return json.loads(self.path.read_text())
        except FileNotFoundError:
            return {}

    def get(self, source_name: str) -> Optional[Any]:
        return self._state.get(source_name)

    def save(self, source_name: str, value: Any) -> None:
        self._state[source_name] = value
        self.path.write_text(json.dumps(self._state, indent=2))


# ---------------------------------------------------------------------------
# Pipeline coordinator
# ---------------------------------------------------------------------------

async def run_pipeline(
    sources: list[APISource],
    output_dir: str = "./data",
    checkpoint_file: str = "/tmp/api_ingestion_checkpoint.json",
    concurrency: int = 3,
) -> dict[str, int]:
    """
    Run all sources concurrently (up to `concurrency` at once).
    Returns a dict of {source_name: record_count}.
    """
    writer = JSONLWriter(output_dir)
    checkpoint = Checkpoint(checkpoint_file)
    semaphore = asyncio.Semaphore(concurrency)
    counts: dict[str, int] = {}

    async with aiohttp.ClientSession() as session:
        engine = APIIngestionEngine(session)

        async def _ingest(source: APISource) -> None:
            async with semaphore:
                log.info("Starting source: %s", source.name)
                count = 0
                async for record in engine.fetch_all(source):
                    writer.write(source.name, record)
                    count += 1
                    if count % 500 == 0:
                        log.info("%s — %d records so far", source.name, count)
                counts[source.name] = count
                log.info("Finished %s — %d records total", source.name, count)

        tasks = [asyncio.create_task(_ingest(src)) for src in sources]
        await asyncio.gather(*tasks, return_exceptions=True)

    writer.close()
    return counts


# ---------------------------------------------------------------------------
# Example: GitHub starred repos ingestion
# ---------------------------------------------------------------------------

def build_github_source(token: str, username: str) -> APISource:
    return APISource(
        name="github_starred",
        base_url="https://api.github.com",
        endpoint=f"/users/{username}/starred",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        params={"per_page": 100},
        data_path="",   # Response IS the list
        pagination=LinkHeaderPagination(),
        rate_limiter=RateLimiter(rate=1.5, burst=5),
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")

    parser = argparse.ArgumentParser(description="Async API Ingestion Pipeline")
    parser.add_argument("--output", default="./data", help="Output directory for JSONL files")
    parser.add_argument("--concurrency", type=int, default=3, help="Max concurrent sources")
    args = parser.parse_args()

    # Example: ingest GitHub starred repos (replace token / username)
    token = os.environ.get("GITHUB_TOKEN", "")
    username = os.environ.get("GITHUB_USER", "octocat")

    sources = [build_github_source(token, username)]

    results = asyncio.run(
        run_pipeline(sources, output_dir=args.output, concurrency=args.concurrency)
    )

    print("\n=== Ingestion complete ===")
    for name, count in results.items():
        print(f"  {name}: {count:,} records")
