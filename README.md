# Async API Ingestion Pipeline

A production-ready async pipeline for ingesting data from REST APIs with:
automatic rate limiting, multiple pagination strategies, back-off on errors,
and checkpoint/resume support for large runs.

## Features

- **Async-first** — `asyncio` + `aiohttp`, multiple sources run in parallel
- **Rate limiting** — token-bucket per source, respects `Retry-After` headers
- **Pagination** — offset, cursor, and HTTP `Link` header (GitHub-style)
- **Retry logic** — exponential back-off on 429/5xx, configurable max retries
- **Checkpointing** — saves offset/cursor to disk; resumes after crashes
- **Pluggable output** — JSONL files out of the box; swap for S3/BigQuery

## Quick start

```bash
pip install aiohttp
GITHUB_TOKEN=ghp_... GITHUB_USER=yourname python api_ingestion.py --output ./out
```

## Adding a new source

```python
from api_ingestion import APISource, CursorPagination, RateLimiter, run_pipeline
import asyncio

stripe_invoices = APISource(
    name="stripe_invoices",
    base_url="https://api.stripe.com",
    endpoint="/v1/invoices",
    headers={"Authorization": "Bearer sk_live_..."},
    params={"limit": 100},
    data_path="data",
    pagination=CursorPagination(cursor_field="next_page", param_name="starting_after"),
    rate_limiter=RateLimiter(rate=25, burst=50),
)

results = asyncio.run(run_pipeline([stripe_invoices], output_dir="./stripe_data"))
print(results)  # {'stripe_invoices': 4312}
```

## Pagination strategies

| Class | Use case |
|---|---|
| `OffsetPagination` | Classic `limit` + `offset` |
| `CursorPagination` | Token in response body (`next_cursor`, `next_page`, etc.) |
| `LinkHeaderPagination` | `Link: <url>; rel="next"` header (GitHub, Jira) |

## Configuration reference

```python
APISource(
    name="my_api",           # Unique identifier for checkpointing + output files
    base_url="https://...",
    endpoint="/v1/items",
    params={"limit": 100},   # Base query params (pagination adds to these)
    headers={"Authorization": "Bearer ..."},
    data_path="results",     # Dot-notation path into response JSON to find the list
    pagination=OffsetPagination(limit=100),
    rate_limiter=RateLimiter(rate=10, burst=20),  # 10 req/s, burst up to 20
    max_retries=3,
    timeout_s=30.0,
)
```

## Output format

Each source writes to `{output_dir}/{source_name}_{timestamp}.jsonl`:

```json
{"id": 1, "title": "...", "__source": "my_api", "__ingested_at": "2026-05-17T10:00:00Z"}
{"id": 2, "title": "...", "__source": "my_api", "__ingested_at": "2026-05-17T10:00:01Z"}
```

Every record gets `__source` and `__ingested_at` fields appended automatically.
