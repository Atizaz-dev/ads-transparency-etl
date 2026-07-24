from __future__ import annotations

import base64
import json
from typing import Any

from fastapi import FastAPI, HTTPException, Query

from .config import settings
from .data import FEED
from .middleware import SourceBehaviorMiddleware

app = FastAPI(title="Ads Transparency Source", version="1.0.0")
app.add_middleware(SourceBehaviorMiddleware)


def _encode_cursor(offset: int) -> str:
    payload = json.dumps({"o": offset}).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def _decode_cursor(cursor: str | None) -> int:
    if not cursor:
        return 0
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode(padded.encode())
        data = json.loads(raw.decode())
        offset = int(data["o"])
        if offset < 0:
            raise ValueError("negative offset")
        return offset
    except Exception as exc:
        raise HTTPException(status_code=400, detail="invalid cursor") from exc


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/v1/ads")
def list_ads(
    cursor: str | None = Query(default=None),
    limit: int = Query(default=settings.page_size, ge=1, le=200),
) -> dict[str, Any]:
    offset = _decode_cursor(cursor)
    page = FEED[offset : offset + limit]
    next_offset = offset + len(page)
    next_cursor = _encode_cursor(next_offset) if next_offset < len(FEED) else None
    return {
        "data": page,
        "next_cursor": next_cursor,
        "page_size": limit,
        "meta": {
            "approx_total": len(FEED),
            "offset": offset,
        },
    }
