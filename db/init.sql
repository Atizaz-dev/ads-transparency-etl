CREATE TABLE IF NOT EXISTS ads (
    ad_id            TEXT PRIMARY KEY,
    advertiser_id    TEXT        NOT NULL,
    advertiser_name  TEXT        NOT NULL,
    platform         TEXT        NOT NULL,
    creative_url     TEXT        NOT NULL,
    impression_count BIGINT      NOT NULL CHECK (impression_count >= 0),
    first_shown_at   TIMESTAMPTZ NOT NULL,
    last_shown_at    TIMESTAMPTZ NOT NULL,
    version          INTEGER     NOT NULL CHECK (version > 0),
    updated_at       TIMESTAMPTZ NOT NULL,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ads_advertiser_id ON ads (advertiser_id);
CREATE INDEX IF NOT EXISTS idx_ads_updated_at ON ads (updated_at DESC);

CREATE TABLE IF NOT EXISTS pipeline_errors (
    id              BIGSERIAL PRIMARY KEY,
    payload_hash    TEXT         NOT NULL UNIQUE,
    source_cursor   TEXT,
    record_payload  JSONB        NOT NULL,
    error_reason    TEXT         NOT NULL,
    isolated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_errors_isolated_at
    ON pipeline_errors (isolated_at DESC);

CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
    pipeline_name   TEXT PRIMARY KEY,
    cursor_value    TEXT,
    status          TEXT         NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    records_seen    BIGINT       NOT NULL DEFAULT 0,
    records_loaded  BIGINT       NOT NULL DEFAULT 0,
    records_skipped BIGINT       NOT NULL DEFAULT 0,
    records_failed  BIGINT       NOT NULL DEFAULT 0,
    started_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          BIGSERIAL PRIMARY KEY,
    pipeline_name   TEXT         NOT NULL,
    status          TEXT         NOT NULL,
    started_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    detail          JSONB
);
