from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture()
def source_data(monkeypatch):
    monkeypatch.setenv("SOURCE_SEED", "42")
    monkeypatch.setenv("SOURCE_RECORD_COUNT", "40")
    monkeypatch.setenv("SOURCE_DUPLICATE_RATE", "0.2")
    monkeypatch.setenv("SOURCE_MALFORMED_RATE", "0.1")
    monkeypatch.setenv("SOURCE_TRANSIENT_ERROR_RATE", "0.0")
    monkeypatch.setenv("SOURCE_RATE_LIMIT_RPS", "100")
    monkeypatch.setenv("SOURCE_PAGE_SIZE", "10")

    # Isolate source package from pipeline's `app`.
    for key in list(sys.modules):
        if key == "app" or key.startswith("app."):
            del sys.modules[key]

    source_path = str(ROOT / "source")
    sys.path.insert(0, source_path)
    try:
        import app.config as source_config

        importlib.reload(source_config)
        import app.data as source_data_mod

        importlib.reload(source_data_mod)
        yield source_data_mod
    finally:
        if sys.path and sys.path[0] == source_path:
            sys.path.pop(0)
        for key in list(sys.modules):
            if key == "app" or key.startswith("app."):
                del sys.modules[key]
        # Restore pipeline app on path for subsequent tests in same worker.
        pipeline_path = str(ROOT / "pipeline")
        if pipeline_path not in sys.path:
            sys.path.insert(0, pipeline_path)


def test_source_feed_is_deterministic(source_data):
    feed_a = source_data.build_feed()
    feed_b = source_data.build_feed()
    assert feed_a == feed_b
    assert len(feed_a) >= 40


def test_source_feed_includes_duplicates_and_malformed(source_data):
    feed = source_data.FEED
    ids = [row.get("ad_id") for row in feed]
    assert len(ids) > len(set(ids)), "expected at-least-once duplicate ad_ids"

    malformed = 0
    for row in feed:
        if "advertiser_id" not in row:
            malformed += 1
        elif not isinstance(row.get("impression_count"), int):
            malformed += 1
        elif isinstance(row.get("version"), int) and row["version"] <= 0:
            malformed += 1
        elif row.get("updated_at") == "yesterday":
            malformed += 1
        elif row.get("platform") is None:
            malformed += 1
    assert malformed > 0
