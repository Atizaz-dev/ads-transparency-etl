from __future__ import annotations

from fastapi import FastAPI, Query
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from .db import Ad, PipelineError
from .load import count_ads, count_errors, stats_snapshot


def create_app(session_factory: sessionmaker[Session]) -> FastAPI:
    app = FastAPI(title="Ads Intelligence ETL", version="1.0.0")

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.get("/stats")
    def stats() -> dict:
        with session_factory() as session:
            return stats_snapshot(session)

    @app.get("/errors")
    def errors(
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> dict:
        with session_factory() as session:
            total = count_errors(session)
            rows = session.scalars(
                select(PipelineError)
                .order_by(PipelineError.id.asc())
                .offset(offset)
                .limit(limit)
            ).all()
            return {
                "total": total,
                "limit": limit,
                "offset": offset,
                "items": [
                    {
                        "id": row.id,
                        "source_cursor": row.source_cursor,
                        "error_reason": row.error_reason,
                        "record_payload": row.record_payload,
                        "isolated_at": row.isolated_at.isoformat(),
                    }
                    for row in rows
                ],
            }

    @app.get("/ads")
    def ads(
        limit: int = Query(default=20, ge=1, le=200),
        offset: int = Query(default=0, ge=0),
    ) -> dict:
        with session_factory() as session:
            total = count_ads(session)
            rows = session.scalars(
                select(Ad).order_by(Ad.updated_at.desc()).offset(offset).limit(limit)
            ).all()
            return {
                "total": total,
                "limit": limit,
                "offset": offset,
                "items": [
                    {
                        "ad_id": row.ad_id,
                        "advertiser_id": row.advertiser_id,
                        "advertiser_name": row.advertiser_name,
                        "platform": row.platform,
                        "creative_url": row.creative_url,
                        "impression_count": row.impression_count,
                        "first_shown_at": row.first_shown_at.isoformat(),
                        "last_shown_at": row.last_shown_at.isoformat(),
                        "version": row.version,
                        "updated_at": row.updated_at.isoformat(),
                        "ingested_at": row.ingested_at.isoformat(),
                    }
                    for row in rows
                ],
            }

    return app
