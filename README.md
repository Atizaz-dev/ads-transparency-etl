# Ads Intelligence ETL

Production-style ingestion pipeline that pulls advertiser creative records from a simulated Ads Transparency API, applies validation and version-aware upserts, and loads them into PostgreSQL.

## Stack

| Piece | Choice |
| --- | --- |
| Source | Simulated paginated HTTP API (`source`) |
| Worker + status API | Python 3.12, FastAPI, httpx, SQLAlchemy |
| Store | PostgreSQL 16 |

## Quick start

```bash
docker compose up --build
```

First boot builds images, starts Postgres + source API, then runs the ingest worker. The status API listens on port `8000` as soon as the container is up; ingest continues in the background until the feed is exhausted.

Typical first run takes a few minutes because the source intentionally injects rate limits and transient failures.

## Query results

Loaded vs isolated counts:

```bash
curl http://localhost:8000/stats
```

Isolated (dead-letter) records:

```bash
curl "http://localhost:8000/errors?limit=20"
```

Sample of successfully loaded ads:

```bash
curl "http://localhost:8000/ads?limit=10"
```

Health:

```bash
curl http://localhost:8000/health
```

### Example `/stats` shape

```json
{
  "loaded_successfully": 760,
  "isolated_errors": 42,
  "pipeline": {
    "name": "ads_transparency_ingest",
    "status": "completed",
    "cursor": null,
    "records_seen": 980,
    "records_loaded": 820,
    "records_skipped": 118,
    "records_failed": 42,
    "started_at": "...",
    "updated_at": "...",
    "completed_at": "..."
  }
}
```

`loaded_successfully` is the distinct row count in `ads`. `records_skipped` counts duplicate/older versions that were intentionally not applied.

## Resume behavior

Checkpoints are stored in `pipeline_checkpoints`. After each source page is fully processed the cursor is committed. Restarting the stack mid-run (`docker compose restart pipeline`) continues from the last committed cursor without duplicating canonical rows or dead-letter entries.

## Project layout

```
db/init.sql          schema
source/              simulated Ads Transparency API
pipeline/            ETL worker + HTTP status API
tests/               pytest unit + integration suite
docker-compose.yml
DECISIONS.md
```

## Tests

```bash
pip install -r pipeline/requirements.txt -r requirements-dev.txt
pytest
```

The suite covers validation, source-feed realism, retry/backoff, version-aware upserts, dead-letter isolation, checkpoint resume, and the `/stats` `/errors` `/ads` API.

DB-backed tests need PostgreSQL. By default they boot a local throwaway cluster when Postgres 15+ binaries are available. Alternatively set:

```bash
# PowerShell
$env:TEST_DATABASE_URL="postgresql+psycopg://etl:etl@127.0.0.1:5432/ads_intel_test"
pytest
```

## Configuration

Tunable via environment variables in `docker-compose.yml`:

- Source realism: `SOURCE_RECORD_COUNT`, `SOURCE_DUPLICATE_RATE`, `SOURCE_MALFORMED_RATE`, `SOURCE_TRANSIENT_ERROR_RATE`, `SOURCE_RATE_LIMIT_RPS`
- Worker retries: `HTTP_MAX_RETRIES`, `HTTP_BASE_DELAY_MS`, `HTTP_MAX_DELAY_MS`
