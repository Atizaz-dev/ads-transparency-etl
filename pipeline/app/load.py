from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from pydantic import ValidationError
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from .config import settings
from .db import Ad, PipelineCheckpoint, PipelineError, PipelineRun
from .models import AdRecord


def _chunked(items: list, size: int) -> list[list]:
    return [items[i : i + size] for i in range(0, len(items), max(size, 1))]


def validate_record(raw: dict) -> tuple[AdRecord | None, str | None]:
    try:
        return AdRecord.model_validate(raw), None
    except ValidationError as exc:
        return None, "; ".join(
            f"{'.'.join(str(p) for p in err['loc'])}: {err['msg']}" for err in exc.errors()
        )
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)


def payload_hash(payload: dict) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def upsert_ad(session: Session, record: AdRecord) -> str:
    """Insert or keep the newest version. Returns loaded|skipped."""
    now = datetime.now(timezone.utc)
    stmt = insert(Ad).values(
        ad_id=record.ad_id,
        advertiser_id=record.advertiser_id,
        advertiser_name=record.advertiser_name,
        platform=record.platform,
        creative_url=record.creative_url,
        impression_count=record.impression_count,
        first_shown_at=record.first_shown_at,
        last_shown_at=record.last_shown_at,
        version=record.version,
        updated_at=record.updated_at,
        ingested_at=now,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[Ad.ad_id],
        set_={
            "advertiser_id": stmt.excluded.advertiser_id,
            "advertiser_name": stmt.excluded.advertiser_name,
            "platform": stmt.excluded.platform,
            "creative_url": stmt.excluded.creative_url,
            "impression_count": stmt.excluded.impression_count,
            "first_shown_at": stmt.excluded.first_shown_at,
            "last_shown_at": stmt.excluded.last_shown_at,
            "version": stmt.excluded.version,
            "updated_at": stmt.excluded.updated_at,
            "ingested_at": now,
        },
        where=(
            (stmt.excluded.updated_at > Ad.updated_at)
            | (
                (stmt.excluded.updated_at == Ad.updated_at)
                & (stmt.excluded.version > Ad.version)
            )
        ),
    ).returning(Ad.ad_id)
    # RETURNING is empty when the conflict WHERE clause skips the update.
    result = session.execute(stmt)
    return "loaded" if result.first() is not None else "skipped"


def _newest_per_ad_id(records: list[AdRecord]) -> tuple[list[AdRecord], int]:
    """Collapse same-batch duplicates to one row per ad_id (same precedence as
    the ON CONFLICT WHERE gate below), since Postgres rejects a single
    ON CONFLICT DO UPDATE statement that touches the same row twice."""
    best: dict[str, AdRecord] = {}
    for record in records:
        current = best.get(record.ad_id)
        if current is None or (record.updated_at, record.version) > (current.updated_at, current.version):
            best[record.ad_id] = record
    return list(best.values()), len(records) - len(best)


def upsert_ads_batch(session: Session, records: list[AdRecord]) -> tuple[int, int]:
    """Bulk version of upsert_ad. Returns (loaded_count, skipped_count)."""
    if not records:
        return 0, 0

    now = datetime.now(timezone.utc)
    loaded = 0
    skipped = 0

    for chunk in _chunked(records, settings.pipeline_batch_size):
        deduped, in_batch_skipped = _newest_per_ad_id(chunk)
        skipped += in_batch_skipped

        stmt = insert(Ad).values(
            [
                {
                    "ad_id": r.ad_id,
                    "advertiser_id": r.advertiser_id,
                    "advertiser_name": r.advertiser_name,
                    "platform": r.platform,
                    "creative_url": r.creative_url,
                    "impression_count": r.impression_count,
                    "first_shown_at": r.first_shown_at,
                    "last_shown_at": r.last_shown_at,
                    "version": r.version,
                    "updated_at": r.updated_at,
                    "ingested_at": now,
                }
                for r in deduped
            ]
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[Ad.ad_id],
            set_={
                "advertiser_id": stmt.excluded.advertiser_id,
                "advertiser_name": stmt.excluded.advertiser_name,
                "platform": stmt.excluded.platform,
                "creative_url": stmt.excluded.creative_url,
                "impression_count": stmt.excluded.impression_count,
                "first_shown_at": stmt.excluded.first_shown_at,
                "last_shown_at": stmt.excluded.last_shown_at,
                "version": stmt.excluded.version,
                "updated_at": stmt.excluded.updated_at,
                "ingested_at": now,
            },
            where=(
                (stmt.excluded.updated_at > Ad.updated_at)
                | (
                    (stmt.excluded.updated_at == Ad.updated_at)
                    & (stmt.excluded.version > Ad.version)
                )
            ),
        ).returning(Ad.ad_id)

        result = session.execute(stmt)
        chunk_loaded = len(result.scalars().all())
        loaded += chunk_loaded
        skipped += len(deduped) - chunk_loaded

    return loaded, skipped


def isolate_error(
    session: Session,
    payload: dict,
    reason: str,
    source_cursor: str | None,
) -> None:
    stmt = (
        insert(PipelineError)
        .values(
            payload_hash=payload_hash(payload),
            source_cursor=source_cursor,
            record_payload=payload,
            error_reason=reason,
        )
        .on_conflict_do_nothing(index_elements=["payload_hash"])
    )
    session.execute(stmt)


def isolate_errors_batch(
    session: Session,
    failures: list[tuple[dict, str, str | None]],
) -> None:
    """Bulk version of isolate_error. Each item is (payload, reason, source_cursor)."""
    if not failures:
        return

    stmt = insert(PipelineError).values(
        [
            {
                "payload_hash": payload_hash(payload),
                "source_cursor": source_cursor,
                "record_payload": payload,
                "error_reason": reason,
            }
            for payload, reason, source_cursor in failures
        ]
    ).on_conflict_do_nothing(index_elements=["payload_hash"])
    session.execute(stmt)


def get_or_create_checkpoint(session: Session, pipeline_name: str) -> PipelineCheckpoint:
    checkpoint = session.get(PipelineCheckpoint, pipeline_name)
    if checkpoint is None:
        checkpoint = PipelineCheckpoint(
            pipeline_name=pipeline_name,
            cursor_value=None,
            status="running",
            records_seen=0,
            records_loaded=0,
            records_skipped=0,
            records_failed=0,
            started_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        session.add(checkpoint)
        session.flush()
        return checkpoint

    if checkpoint.status == "completed":
        return checkpoint

    checkpoint.status = "running"
    checkpoint.completed_at = None
    checkpoint.updated_at = datetime.now(timezone.utc)
    if checkpoint.started_at is None:
        checkpoint.started_at = datetime.now(timezone.utc)
    session.flush()
    return checkpoint


def persist_checkpoint(
    session: Session,
    checkpoint: PipelineCheckpoint,
    *,
    cursor_value: str | None,
    seen_delta: int = 0,
    loaded_delta: int = 0,
    skipped_delta: int = 0,
    failed_delta: int = 0,
    status: str | None = None,
) -> None:
    checkpoint.cursor_value = cursor_value
    checkpoint.records_seen += seen_delta
    checkpoint.records_loaded += loaded_delta
    checkpoint.records_skipped += skipped_delta
    checkpoint.records_failed += failed_delta
    checkpoint.updated_at = datetime.now(timezone.utc)
    if status:
        checkpoint.status = status
        if status in {"completed", "failed"}:
            checkpoint.completed_at = datetime.now(timezone.utc)


def start_run(session: Session, pipeline_name: str) -> PipelineRun:
    run = PipelineRun(pipeline_name=pipeline_name, status="running")
    session.add(run)
    session.flush()
    return run


def finish_run(session: Session, run: PipelineRun, status: str, detail: dict) -> None:
    run.status = status
    run.finished_at = datetime.now(timezone.utc)
    run.detail = detail


def count_ads(session: Session) -> int:
    return session.scalar(select(func.count()).select_from(Ad)) or 0


def count_errors(session: Session) -> int:
    return session.scalar(select(func.count()).select_from(PipelineError)) or 0


def stats_snapshot(session: Session, pipeline_name: str | None = None) -> dict:
    name = pipeline_name or settings.pipeline_name
    loaded = count_ads(session)
    errors = count_errors(session)
    checkpoint = session.get(PipelineCheckpoint, name)
    return {
        "loaded_successfully": loaded,
        "isolated_errors": errors,
        "pipeline": None
        if checkpoint is None
        else {
            "name": checkpoint.pipeline_name,
            "status": checkpoint.status,
            "cursor": checkpoint.cursor_value,
            "records_seen": checkpoint.records_seen,
            "records_loaded": checkpoint.records_loaded,
            "records_skipped": checkpoint.records_skipped,
            "records_failed": checkpoint.records_failed,
            "started_at": checkpoint.started_at.isoformat() if checkpoint.started_at else None,
            "updated_at": checkpoint.updated_at.isoformat() if checkpoint.updated_at else None,
            "completed_at": checkpoint.completed_at.isoformat() if checkpoint.completed_at else None,
        },
    }


def wait_for_db(engine, attempts: int = 60) -> None:
    import time

    last_error: Exception | None = None
    for _ in range(attempts):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(1)
    raise RuntimeError(f"database not ready: {last_error}")


def wait_for_source(base_url: str, attempts: int = 60) -> None:
    import time

    import httpx

    last_error: Exception | None = None
    for _ in range(attempts):
        try:
            response = httpx.get(f"{base_url.rstrip('/')}/health", timeout=2.0)
            if response.status_code == 200:
                return
            last_error = RuntimeError(f"status {response.status_code}")
        except Exception as exc:  # noqa: BLE001
            last_error = exc
        time.sleep(1)
    raise RuntimeError(f"source not ready: {last_error}")
