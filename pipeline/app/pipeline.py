from __future__ import annotations

import logging
from dataclasses import dataclass

from sqlalchemy.orm import Session, sessionmaker

from .config import settings
from .db import PipelineRun
from .extract import SourceClient
from .load import (
    finish_run,
    get_or_create_checkpoint,
    isolate_errors_batch,
    persist_checkpoint,
    start_run,
    upsert_ads_batch,
    validate_record,
)

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    status: str
    records_seen: int
    records_loaded: int
    records_skipped: int
    records_failed: int


class AdsIngestPipeline:
    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self.session_factory = session_factory
        self.pipeline_name = settings.pipeline_name

    def run(self) -> PipelineResult:
        with self.session_factory() as session:
            checkpoint = get_or_create_checkpoint(session, self.pipeline_name)
            if checkpoint.status == "completed":
                logger.info("ingest already completed for %s", self.pipeline_name)
                result = PipelineResult(
                    status="completed",
                    records_seen=checkpoint.records_seen,
                    records_loaded=checkpoint.records_loaded,
                    records_skipped=checkpoint.records_skipped,
                    records_failed=checkpoint.records_failed,
                )
                session.commit()
                return result

            start_cursor = checkpoint.cursor_value
            run = start_run(session, self.pipeline_name)
            run_id = run.run_id
            session.commit()

        try:
            with SourceClient() as source, self.session_factory() as session:
                checkpoint = get_or_create_checkpoint(session, self.pipeline_name)

                for page_cursor, records, next_cursor in source.iter_pages(start_cursor):
                    valid_records = []
                    failures = []

                    for raw in records:
                        payload = raw if isinstance(raw, dict) else {"raw": raw}
                        record, error = validate_record(payload)
                        if error or record is None:
                            failures.append((payload, error or "unknown validation failure", page_cursor))
                        else:
                            valid_records.append(record)

                    isolate_errors_batch(session, failures)
                    page_loaded, page_skipped = upsert_ads_batch(session, valid_records)
                    page_failed = len(failures)

                    persist_checkpoint(
                        session,
                        checkpoint,
                        cursor_value=next_cursor,
                        seen_delta=len(records),
                        loaded_delta=page_loaded,
                        skipped_delta=page_skipped,
                        failed_delta=page_failed,
                        status="running",
                    )
                    session.commit()
                    logger.info(
                        "committed page next_cursor=%s seen=%s loaded=%s skipped=%s failed=%s",
                        next_cursor,
                        len(records),
                        page_loaded,
                        page_skipped,
                        page_failed,
                    )

                persist_checkpoint(
                    session,
                    checkpoint,
                    cursor_value=None,
                    status="completed",
                )
                run = session.get(PipelineRun, run_id)
                if run is not None:
                    finish_run(
                        session,
                        run,
                        "completed",
                        {
                            "records_seen": checkpoint.records_seen,
                            "records_loaded": checkpoint.records_loaded,
                            "records_skipped": checkpoint.records_skipped,
                            "records_failed": checkpoint.records_failed,
                        },
                    )
                session.commit()

                return PipelineResult(
                    status="completed",
                    records_seen=checkpoint.records_seen,
                    records_loaded=checkpoint.records_loaded,
                    records_skipped=checkpoint.records_skipped,
                    records_failed=checkpoint.records_failed,
                )
        except Exception as exc:
            logger.exception("pipeline failed: %s", exc)
            with self.session_factory() as session:
                checkpoint = get_or_create_checkpoint(session, self.pipeline_name)
                persist_checkpoint(
                    session,
                    checkpoint,
                    cursor_value=checkpoint.cursor_value,
                    status="failed",
                )
                run = session.get(PipelineRun, run_id)
                if run is not None:
                    finish_run(session, run, "failed", {"error": str(exc)})
                session.commit()
            raise
