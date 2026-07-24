from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "db" / "init.sql"
PG_BIN = Path(r"C:\Program Files\PostgreSQL\15\bin")
PGVERIFY = ROOT.parent / ".pgverify-tests"


def _pg_cmd(name: str) -> str:
    candidate = PG_BIN / f"{name}.exe"
    if candidate.exists():
        return str(candidate)
    return name


@pytest.fixture(scope="session")
def database_url() -> str:
    """Prefer TEST_DATABASE_URL; otherwise boot a throwaway local Postgres cluster."""
    configured = os.getenv("TEST_DATABASE_URL")
    if configured:
        yield configured
        return

    if sys.platform.startswith("win") and not (PG_BIN / "initdb.exe").exists():
        pytest.skip("PostgreSQL binaries not found; set TEST_DATABASE_URL to run DB tests")

    port = "55433"
    data_dir = PGVERIFY / "data"
    log_file = PGVERIFY / "pg.log"
    pg_ctl = _pg_cmd("pg_ctl")
    initdb = _pg_cmd("initdb")
    createdb = _pg_cmd("createdb")
    psql = _pg_cmd("psql")

    if PGVERIFY.exists():
        subprocess.run([pg_ctl, "-D", str(data_dir), "stop", "-m", "fast"], check=False, capture_output=True)
        time.sleep(0.5)

    PGVERIFY.mkdir(parents=True, exist_ok=True)
    if data_dir.exists():
        import shutil

        shutil.rmtree(data_dir, ignore_errors=True)

    subprocess.run(
        [initdb, "-D", str(data_dir), "-U", "etl", "--auth=trust", "--encoding=UTF8", "--locale=C"],
        check=True,
        capture_output=True,
        text=True,
    )
    with (data_dir / "postgresql.conf").open("a", encoding="utf-8") as conf:
        conf.write(f"\nport = {port}\nlisten_addresses = '127.0.0.1'\nmax_connections = 40\n")
    (data_dir / "pg_hba.conf").write_text(
        "local all all trust\nhost all all 127.0.0.1/32 trust\nhost all all ::1/128 trust\n",
        encoding="ascii",
    )
    subprocess.run([pg_ctl, "-D", str(data_dir), "-l", str(log_file), "start", "-w"], check=True)
    subprocess.run([createdb, "-h", "127.0.0.1", "-p", port, "-U", "etl", "ads_intel_test"], check=True)
    subprocess.run(
        [psql, "-h", "127.0.0.1", "-p", port, "-U", "etl", "-d", "ads_intel_test", "-v", "ON_ERROR_STOP=1", "-f", str(SCHEMA_PATH)],
        check=True,
    )

    url = f"postgresql+psycopg://etl@127.0.0.1:{port}/ads_intel_test"
    yield url

    subprocess.run([pg_ctl, "-D", str(data_dir), "stop", "-m", "fast"], check=False, capture_output=True)


@pytest.fixture()
def db_session(database_url: str):
    engine = create_engine(database_url, pool_pre_ping=True)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    with engine.begin() as conn:
        conn.execute(
            text(
                "TRUNCATE ads, pipeline_errors, pipeline_runs, pipeline_checkpoints RESTART IDENTITY CASCADE"
            )
        )
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        engine.dispose()


@pytest.fixture()
def session_factory(database_url: str):
    engine = create_engine(database_url, pool_pre_ping=True)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    with engine.begin() as conn:
        conn.execute(
            text(
                "TRUNCATE ads, pipeline_errors, pipeline_runs, pipeline_checkpoints RESTART IDENTITY CASCADE"
            )
        )

    yield SessionLocal
    engine.dispose()


def make_ad_payload(**overrides):
    base = {
        "ad_id": "ad_test_001",
        "advertiser_id": "adv_1001",
        "advertiser_name": "PeakForm Apparel",
        "platform": "YOUTUBE",
        "creative_url": "https://cdn.example.com/creatives/ad_test_001.mp4",
        "impression_count": 1000,
        "first_shown_at": "2024-02-01T00:00:00Z",
        "last_shown_at": "2024-03-01T00:00:00Z",
        "version": 1,
        "updated_at": "2024-03-02T00:00:00Z",
    }
    base.update(overrides)
    return base
