from __future__ import annotations

from sqlalchemy import func, select
from fastapi.testclient import TestClient

from app.api import create_app
from app.db import Ad, PipelineCheckpoint, PipelineError
from app.load import isolate_error, upsert_ad, validate_record
from app.pipeline import AdsIngestPipeline
from conftest import make_ad_payload


def test_stats_and_errors_and_ads_endpoints(session_factory, db_session):
    record, error = validate_record(make_ad_payload())
    assert error is None
    upsert_ad(db_session, record)
    isolate_error(db_session, {"bad": 1}, "invalid", "c0")
    db_session.add(
        PipelineCheckpoint(
            pipeline_name="ads_transparency_ingest",
            cursor_value=None,
            status="completed",
            records_seen=2,
            records_loaded=1,
            records_skipped=0,
            records_failed=1,
        )
    )
    db_session.commit()

    client = TestClient(create_app(session_factory))

    assert client.get("/health").json()["status"] == "ok"

    stats = client.get("/stats").json()
    assert stats["loaded_successfully"] == 1
    assert stats["isolated_errors"] == 1
    assert stats["pipeline"]["status"] == "completed"

    errors = client.get("/errors?limit=10").json()
    assert errors["total"] == 1
    assert errors["items"][0]["error_reason"] == "invalid"

    ads = client.get("/ads?limit=10").json()
    assert ads["total"] == 1
    assert ads["items"][0]["ad_id"] == "ad_test_001"


def test_pipeline_isolates_bad_records_and_is_idempotent(session_factory, monkeypatch):
    good = make_ad_payload(ad_id="ad_good_1")
    older = make_ad_payload(
        ad_id="ad_good_1",
        updated_at="2024-01-01T00:00:00Z",
        impression_count=1,
    )
    newer = make_ad_payload(
        ad_id="ad_good_1",
        version=2,
        updated_at="2024-06-01T00:00:00Z",
        impression_count=999,
    )
    bad = make_ad_payload(ad_id="ad_bad_1", impression_count="nope")

    pages = [
        (None, [good, bad, older], "cursor-1"),
        ("cursor-1", [newer], None),
    ]

    class FakeSource:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def iter_pages(self, start_cursor=None):
            for page in pages:
                yield page

    monkeypatch.setattr("app.pipeline.SourceClient", lambda: FakeSource())

    pipeline = AdsIngestPipeline(session_factory)
    first = pipeline.run()
    assert first.status == "completed"
    assert first.records_failed == 1
    assert first.records_seen == 4

    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Ad)) == 1
        assert session.scalar(select(func.count()).select_from(PipelineError)) == 1
        ad = session.get(Ad, "ad_good_1")
        assert ad is not None
        assert ad.version == 2
        assert ad.impression_count == 999

    second = pipeline.run()
    assert second.status == "completed"
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Ad)) == 1
        assert session.scalar(select(func.count()).select_from(PipelineError)) == 1


def test_pipeline_resumes_from_checkpoint_after_crash(session_factory, monkeypatch):
    page1 = [
        make_ad_payload(ad_id="ad_a"),
        make_ad_payload(ad_id="ad_b", impression_count="bad"),
    ]
    page2 = [make_ad_payload(ad_id="ad_c")]
    state = {"crash_after_first_page": True, "start_cursors": []}

    class FakeSource:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def iter_pages(self, start_cursor=None):
            state["start_cursors"].append(start_cursor)
            if start_cursor is None:
                yield None, page1, "cursor-1"
                if state["crash_after_first_page"]:
                    raise RuntimeError("simulated crash after page commit")
                return
            yield start_cursor, page2, None

    monkeypatch.setattr("app.pipeline.SourceClient", lambda: FakeSource())
    pipeline = AdsIngestPipeline(session_factory)

    try:
        pipeline.run()
        assert False, "expected crash"
    except RuntimeError as exc:
        assert "simulated crash" in str(exc)

    with session_factory() as session:
        checkpoint = session.get(PipelineCheckpoint, "ads_transparency_ingest")
        assert checkpoint is not None
        assert checkpoint.cursor_value == "cursor-1"
        assert checkpoint.status == "failed"
        assert session.scalar(select(func.count()).select_from(Ad)) == 1
        assert session.scalar(select(func.count()).select_from(PipelineError)) == 1

    state["crash_after_first_page"] = False
    resumed = pipeline.run()
    assert resumed.status == "completed"
    assert state["start_cursors"][-1] == "cursor-1"

    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Ad)) == 2
        assert session.get(Ad, "ad_a") is not None
        assert session.get(Ad, "ad_c") is not None
        assert session.scalar(select(func.count()).select_from(PipelineError)) == 1
        checkpoint = session.get(PipelineCheckpoint, "ads_transparency_ingest")
        assert checkpoint.status == "completed"
        assert checkpoint.cursor_value is None
