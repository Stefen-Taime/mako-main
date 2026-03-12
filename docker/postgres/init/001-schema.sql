-- ═══════════════════════════════════════════════════════════
-- Mako — PostgreSQL Init Schema
-- ═══════════════════════════════════════════════════════════
-- This runs automatically on first `docker compose up`.
-- Creates the tables that Mako pipelines write to.

-- ── Extensions ───────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── Pipeline metadata (mako internal) ────────────────────
CREATE SCHEMA IF NOT EXISTS mako;

CREATE TABLE mako.pipelines (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name            VARCHAR(63) UNIQUE NOT NULL,
    spec_yaml       TEXT NOT NULL,
    state           VARCHAR(20) NOT NULL DEFAULT 'stopped',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deployed_at     TIMESTAMPTZ,
    owner           VARCHAR(255),
    description     TEXT
);

CREATE TABLE mako.pipeline_metrics (
    id              BIGSERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(63) NOT NULL REFERENCES mako.pipelines(name),
    events_in       BIGINT NOT NULL DEFAULT 0,
    events_out      BIGINT NOT NULL DEFAULT 0,
    errors          BIGINT NOT NULL DEFAULT 0,
    dlq_count       BIGINT NOT NULL DEFAULT 0,
    lag             BIGINT NOT NULL DEFAULT 0,
    throughput_eps  DOUBLE PRECISION NOT NULL DEFAULT 0,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_pipeline_metrics_name_time
    ON mako.pipeline_metrics(pipeline_name, recorded_at DESC);

-- Dead Letter Queue storage
CREATE TABLE mako.dead_letter_queue (
    id              BIGSERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(63) NOT NULL,
    event_key       BYTEA,
    event_value     JSONB NOT NULL,
    error_message   TEXT,
    source_topic    VARCHAR(255),
    source_partition INTEGER,
    source_offset   BIGINT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dlq_pipeline_time
    ON mako.dead_letter_queue(pipeline_name, created_at DESC);

-- ── Example sink tables (analytics) ─────────────────────
CREATE SCHEMA IF NOT EXISTS analytics;

-- Generic events table (used by simple pipeline example)
CREATE TABLE analytics.events (
    event_id        VARCHAR(255),
    event_type      VARCHAR(255),
    event_data      JSONB NOT NULL,
    pipeline_name   VARCHAR(63),
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_events_type_time
    ON analytics.events(event_type, loaded_at DESC);

-- Order events (matches examples/simple/pipeline.yaml)
CREATE TABLE analytics.order_events (
    event_id        VARCHAR(255) PRIMARY KEY,
    event_type      VARCHAR(255),
    customer_id     VARCHAR(255),
    amount          DOUBLE PRECISION,
    status          VARCHAR(50),
    environment     VARCHAR(50),
    email_hash      VARCHAR(64),
    phone_hash      VARCHAR(64),
    pii_processed   BOOLEAN DEFAULT FALSE,
    raw_data        JSONB,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Payment events (matches examples/advanced/pipeline.yaml)
CREATE TABLE analytics.payment_events (
    event_id        VARCHAR(255) PRIMARY KEY,
    event_type      VARCHAR(255),
    customer_id     VARCHAR(255),
    amount          DOUBLE PRECISION,
    amount_with_tax DOUBLE PRECISION,
    risk_score      DOUBLE PRECISION,
    risk_category   VARCHAR(20),
    status          VARCHAR(50),
    email_hash      VARCHAR(64),
    phone_hash      VARCHAR(64),
    pii_processed   BOOLEAN DEFAULT FALSE,
    raw_data        JSONB,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Grants ───────────────────────────────────────────────
GRANT ALL PRIVILEGES ON SCHEMA mako TO mako;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO mako;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA mako TO mako;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics TO mako;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA mako TO mako;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA analytics TO mako;
