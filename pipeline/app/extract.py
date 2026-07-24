from __future__ import annotations

import logging
from typing import Any, Iterator

import httpx

from .config import settings
from .retry import TransientSourceError, with_source_retries

logger = logging.getLogger(__name__)


class SourceClient:
    def __init__(self, base_url: str | None = None, page_size: int | None = None) -> None:
        self.base_url = (base_url or settings.source_base_url).rstrip("/")
        self.page_size = page_size or settings.source_page_size
        self._client = httpx.Client(
            base_url=self.base_url,
            timeout=httpx.Timeout(10.0, connect=5.0),
            headers={"Accept": "application/json"},
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "SourceClient":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    @with_source_retries
    def fetch_page(self, cursor: str | None) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": self.page_size}
        if cursor:
            params["cursor"] = cursor

        try:
            response = self._client.get("/v1/ads", params=params)
        except httpx.TimeoutException as exc:
            raise TransientSourceError("source request timed out") from exc
        except httpx.TransportError as exc:
            raise TransientSourceError(f"source transport error: {exc}") from exc

        if response.status_code == 429:
            retry_after_raw = response.headers.get("Retry-After")
            retry_after = float(retry_after_raw) if retry_after_raw else None
            raise TransientSourceError("rate limited", retry_after=retry_after)

        if response.status_code >= 500:
            raise TransientSourceError(f"source returned {response.status_code}")

        if response.status_code >= 400:
            response.raise_for_status()

        return response.json()

    def iter_pages(self, start_cursor: str | None = None) -> Iterator[tuple[str | None, list[dict[str, Any]], str | None]]:
        cursor = start_cursor
        while True:
            payload = self.fetch_page(cursor)
            records = payload.get("data") or []
            next_cursor = payload.get("next_cursor")
            yield cursor, records, next_cursor
            if not next_cursor:
                break
            cursor = next_cursor
