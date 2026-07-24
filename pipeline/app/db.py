from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Integer,
    MetaData,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    metadata = MetaData()


class Ad(Base):
    __tablename__ = "ads"

    ad_id: Mapped[str] = mapped_column(Text, primary_key=True)
    advertiser_id: Mapped[str] = mapped_column(Text, nullable=False)
    advertiser_name: Mapped[str] = mapped_column(Text, nullable=False)
    platform: Mapped[str] = mapped_column(Text, nullable=False)
    creative_url: Mapped[str] = mapped_column(Text, nullable=False)
    impression_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    first_shown_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_shown_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class PipelineError(Base):
    __tablename__ = "pipeline_errors"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    payload_hash: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    source_cursor: Mapped[str | None] = mapped_column(Text, nullable=True)
    record_payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    error_reason: Mapped[str] = mapped_column(Text, nullable=False)
    isolated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class PipelineCheckpoint(Base):
    __tablename__ = "pipeline_checkpoints"
    __table_args__ = (
        CheckConstraint(
            "status IN ('running', 'completed', 'failed')",
            name="pipeline_checkpoints_status_check",
        ),
    )

    pipeline_name: Mapped[str] = mapped_column(Text, primary_key=True)
    cursor_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    records_seen: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    records_loaded: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    records_skipped: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    records_failed: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    run_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    pipeline_name: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    detail: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
