# Decisions

## Architecture

Three containers: a simulated source API, PostgreSQL, and a single pipeline service that runs ingest then serves status endpoints. Keeping worker and query API together avoids an extra process while still exposing the required inspection surface over HTTP.

Domain shape mirrors Ads Transparency / advertiser creative metadata (ad id, advertiser, platform, impressions, version timestamps) because that matches the target role without requiring external credentials.

## Source simulation

The source is intentionally hostile:

- cursor pagination with deterministic fixtures
- at-least-once delivery (duplicate ids, older snapshots after newer ones)
- intermittent 503/504 responses
- token-bucket style rate limiting (HTTP 429 + `Retry-After`)
- a small malformed fraction (missing fields, wrong types, invalid enums)

This exercises the same failure modes a live integration would, without depending on a third-party API during evaluation.

## Idempotency and ordering

Canonical table key is `ad_id`. Loads use PostgreSQL `INSERT ... ON CONFLICT DO UPDATE` gated by `(updated_at, version)` so reprocessing never creates duplicates and never downgrades a newer row to an older snapshot.

Malformed payloads are written to `pipeline_errors` keyed by a SHA-256 of the canonical JSON body (`ON CONFLICT DO NOTHING`), so retries/resumes do not multiply dead-letter rows.

## Resilience

HTTP reads use exponential backoff with jitter, and honor `Retry-After` on 429. Retries are scoped to a single page request; a poisoned record cannot stop the run because validation failures are isolated per record.

## Resume after interruption

`pipeline_checkpoints.cursor_value` advances only after a full page has been validated, upserted, and committed. A crash mid-page re-reads that page; upserts and error hashes keep the destination correct. Counter fields on the checkpoint can over-count slightly in that narrow window; destination tables remain the source of truth for `/stats`.

## Throughput

Loading is batched per page instead of one round trip per record: `upsert_ads_batch`/`isolate_errors_batch` issue a single multi-row `INSERT ... ON CONFLICT` per page (chunked by `PIPELINE_BATCH_SIZE` so a very large page can't produce one oversized statement). Same-page duplicate `ad_id`s are collapsed to the newest `(updated_at, version)` before the statement runs, since Postgres rejects an `ON CONFLICT DO UPDATE` that would touch one row twice in a single statement; the collapsed rows are counted as `skipped`, matching the outcome the old per-record loop already produced. The `/errors` and `/ads` endpoints now run a single targeted `COUNT`, rather than the full `/stats` snapshot (which also queries the other table and the checkpoint) just to read one number.

## Trade-offs / simplifications

- PostgreSQL instead of BigQuery: identical upsert/idempotency semantics locally, no cloud credentials required for `docker compose up`.
- Page-level checkpoints instead of per-record cursors: simpler implementation, acceptable re-read cost given idempotent writes.
- In-process background thread for ingest alongside uvicorn: fewer moving parts for the assessment; a real deployment would separate the worker and API (and likely use a queue/orchestrator).
- No metrics backend or alerting wiring: structured logs + SQL/HTTP inspection cover failure visibility for this scope.
- Completed runs are not automatically replayed on container restart; re-running requires resetting checkpoint state. Idempotent upserts would still protect the `ads` table if replay were forced.
- Automated tests (`pytest`) cover the assessment behaviors with a mix of unit and Postgres-backed integration tests rather than a full `docker compose` e2e harness, to keep the feedback loop fast.
