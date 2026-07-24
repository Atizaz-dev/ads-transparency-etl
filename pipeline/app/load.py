from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from pydantic import ValidationError
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from .db import Ad, PipelineCheckpoint, PipelineError, PipelineRun
from .models import AdRecord


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


def stats_snapshot(session: Session, pipeline_name: str | None = None) -> dict:
    from .config import settings

    name = pipeline_name or settings.pipeline_name
    loaded = session.scalar(select(func.count()).select_from(Ad)) or 0
    errors = session.scalar(select(func.count()).select_from(PipelineError)) or 0
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
