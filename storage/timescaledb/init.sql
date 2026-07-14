-- TimescaleDB schema for the windowed metrics Flink writes.
-- Runs automatically on first container start (docker-entrypoint-initdb.d).

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- One row per (service, window_type, window_start). We store BOTH the tumbling
-- 1-minute rollups and the sliding 1m/10s moving windows in the same table,
-- distinguished by window_type, so Grafana can pick whichever it needs.
CREATE TABLE IF NOT EXISTS service_metrics (
    window_start    TIMESTAMPTZ      NOT NULL,
    window_end      TIMESTAMPTZ      NOT NULL,
    service_name    TEXT             NOT NULL,
    window_type     TEXT             NOT NULL,
    count           BIGINT,
    error_count     BIGINT,
    warn_count      BIGINT,
    error_rate      DOUBLE PRECISION,
    avg_latency_ms  DOUBLE PRECISION,
    max_latency_ms  DOUBLE PRECISION,
    -- The PK MUST include the time column so it can also be the hypertable
    -- partition key; it doubles as the ON CONFLICT target for idempotent upserts
    -- when Flink replays a window after recovery.
    PRIMARY KEY (service_name, window_type, window_start)
);

-- Turn the plain table into a hypertable: Timescale transparently partitions it
-- into time-based "chunks" (here by window_start), which makes time-range
-- queries and retention/compaction fast at scale.
SELECT create_hypertable('service_metrics', 'window_start', if_not_exists => TRUE);

-- Helpful secondary index for "latest metrics for a service" lookups.
CREATE INDEX IF NOT EXISTS idx_service_time
    ON service_metrics (service_name, window_type, window_start DESC);
