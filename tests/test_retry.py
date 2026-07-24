from __future__ import annotations

import importlib
import sys
from pathlib import Path

import httpx
import pytest
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _restore_pipeline_modules():
    yield
    # Retry tests reload modules under mutated env; restore defaults afterwards.
    pipeline_path = str(ROOT / "pipeline")
    if pipeline_path not in sys.path:
        sys.path.insert(0, pipeline_path)
    import app.config as config
    import app.extract as extract_mod
    import app.retry as retry_mod

    importlib.reload(config)
    importlib.reload(retry_mod)
    importlib.reload(extract_mod)


def test_fetch_page_retries_transient_errors(monkeypatch):
    monkeypatch.setenv("HTTP_MAX_RETRIES", "5")
    monkeypatch.setenv("HTTP_BASE_DELAY_MS", "1")
    monkeypatch.setenv("HTTP_MAX_DELAY_MS", "5")

    import app.config as config
    import app.extract as extract_mod
    import app.retry as retry_mod

    importlib.reload(config)
    importlib.reload(retry_mod)
    importlib.reload(extract_mod)

    calls = {"n": 0}

    class FakeResponse:
        def __init__(self, status_code: int, payload=None, headers=None):
            self.status_code = status_code
            self._payload = payload or {"data": [], "next_cursor": None}
            self.headers = headers or {}

        def json(self):
            return self._payload

        def raise_for_status(self):
            if self.status_code >= 400:
                raise httpx.HTTPStatusError("err", request=MagicMock(), response=MagicMock())

    def fake_get(path, params=None):
        calls["n"] += 1
        if calls["n"] < 3:
            return FakeResponse(503)
        return FakeResponse(200, {"data": [{"ok": True}], "next_cursor": None})

    client = extract_mod.SourceClient(base_url="http://example.test", page_size=10)
    client._client.get = fake_get  # type: ignore[method-assign]
    payload = client.fetch_page(None)
    assert payload["data"] == [{"ok": True}]
    assert calls["n"] == 3
    client.close()


def test_fetch_page_honors_rate_limit(monkeypatch):
    monkeypatch.setenv("HTTP_MAX_RETRIES", "3")
    monkeypatch.setenv("HTTP_BASE_DELAY_MS", "1")
    monkeypatch.setenv("HTTP_MAX_DELAY_MS", "5")

    import app.config as config
    import app.extract as extract_mod
    import app.retry as retry_mod

    importlib.reload(config)
    importlib.reload(retry_mod)
    importlib.reload(extract_mod)

    calls = {"n": 0}

    class FakeResponse:
        def __init__(self, status_code: int, headers=None, payload=None):
            self.status_code = status_code
            self.headers = headers or {}
            self._payload = payload or {"data": [], "next_cursor": None}

        def json(self):
            return self._payload

        def raise_for_status(self):
            return None

    def fake_get(path, params=None):
        calls["n"] += 1
        if calls["n"] == 1:
            return FakeResponse(429, headers={"Retry-After": "0.01"})
        return FakeResponse(200, payload={"data": [1], "next_cursor": None})

    client = extract_mod.SourceClient(base_url="http://example.test", page_size=10)
    client._client.get = fake_get  # type: ignore[method-assign]
    assert client.fetch_page(None)["data"] == [1]
    assert calls["n"] == 2
    client.close()
