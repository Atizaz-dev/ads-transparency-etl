from __future__ import annotations

from app.load import (
    get_or_create_checkpoint,
    isolate_error,
    persist_checkpoint,
    upsert_ad,
    validate_record,
)
from app.models import AdRecord
from conftest import make_ad_payload


def _record(**overrides) -> AdRecord:
    parsed, error = validate_record(make_ad_payload(**overrides))
    assert error is None and parsed is not None
    return parsed


def test_upsert_inserts_then_skips_older_and_applies_newer(db_session):
    first = _record(version=1, updated_at="2024-03-02T00:00:00Z", impression_count=1000)
    assert upsert_ad(db_session, first) == "loaded"
    db_session.commit()

    older = _record(version=1, updated_at="2024-03-01T00:00:00Z", impression_count=50)
    assert upsert_ad(db_session, older) == "skipped"
    db_session.commit()

    newer = _record(version=2, updated_at="2024-03-05T00:00:00Z", impression_count=5000)
    assert upsert_ad(db_session, newer) == "loaded"
    db_session.commit()

    from app.db import Ad

    stored = db_session.get(Ad, "ad_test_001")
    assert stored is not None
    assert stored.version == 2
    assert stored.impression_count == 5000


def test_isolate_error_is_idempotent_by_payload_hash(db_session):
    payload = {"bad": True, "impression_count": "x"}
    isolate_error(db_session, payload=payload, reason="bad type", source_cursor="c0")
    isolate_error(db_session, payload=payload, reason="bad type again", source_cursor="c1")
    db_session.commit()

    from app.db import PipelineError
    from sqlalchemy import func, select

    count = db_session.scalar(select(func.count()).select_from(PipelineError))
    assert count == 1


def test_checkpoint_advances_and_completes(db_session):
    checkpoint = get_or_create_checkpoint(db_session, "ads_transparency_ingest")
    persist_checkpoint(
        db_session,
        checkpoint,
        cursor_value="cursor-1",
        seen_delta=50,
        loaded_delta=40,
        skipped_delta=5,
        failed_delta=5,
        status="running",
    )
    db_session.commit()

    again = get_or_create_checkpoint(db_session, "ads_transparency_ingest")
    assert again.cursor_value == "cursor-1"
    assert again.records_seen == 50

    persist_checkpoint(db_session, again, cursor_value=None, status="completed")
    db_session.commit()
    assert again.status == "completed"
    assert again.completed_at is not None


def test_same_updated_at_higher_version_wins(db_session):
    ts = "2024-03-02T00:00:00Z"
    assert upsert_ad(db_session, _record(version=1, updated_at=ts, impression_count=10)) == "loaded"
    db_session.commit()
    assert upsert_ad(db_session, _record(version=2, updated_at=ts, impression_count=99)) == "loaded"
    db_session.commit()

    from app.db import Ad

    row = db_session.get(Ad, "ad_test_001")
    assert row.version == 2
    assert row.impression_count == 99
