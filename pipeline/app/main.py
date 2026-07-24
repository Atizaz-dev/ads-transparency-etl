from __future__ import annotations

import logging
import threading

import uvicorn
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .api import create_app
from .config import settings
from .load import wait_for_db, wait_for_source
from .pipeline import AdsIngestPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    engine = create_engine(
        settings.database_url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=5,
    )
    wait_for_db(engine)
    wait_for_source(settings.source_base_url)
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False)

    app = create_app(session_factory)

    def run_ingest() -> None:
        try:
            result = AdsIngestPipeline(session_factory).run()
            logger.info(
                "ingest finished status=%s seen=%s loaded=%s skipped=%s failed=%s",
                result.status,
                result.records_seen,
                result.records_loaded,
                result.records_skipped,
                result.records_failed,
            )
        except Exception:
            logger.exception("ingest terminated with failure; API remains available")

    worker = threading.Thread(target=run_ingest, name="etl-worker", daemon=True)
    worker.start()

    uvicorn.run(app, host=settings.api_host, port=settings.api_port, log_level="info")


if __name__ == "__main__":
    main()
