\connect scanning

CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER DATABASE scanning OWNER TO scanning;
ALTER SCHEMA public OWNER TO scanning;
GRANT ALL PRIVILEGES ON SCHEMA public TO scanning;

CREATE TABLE IF NOT EXISTS recurring_scans (
    id UUID PRIMARY KEY,

    asset_id UUID NOT NULL,
    asset_type TEXT NOT NULL,

    frequency TEXT NOT NULL CHECK (frequency IN ('daily', 'weekly', 'monthly', 'manual')),
    priority INT NOT NULL DEFAULT 10,

    status TEXT NOT NULL CHECK (status IN ('scheduled', 'claiming', 'enqueued', 'disabled')),

    last_run_at TIMESTAMPTZ NULL,
    next_run_at TIMESTAMPTZ NOT NULL,

    spread_offset_seconds INT NOT NULL DEFAULT 0,

    claim_owner TEXT NULL,
    claimed_at TIMESTAMPTZ NULL,
    claim_expires_at TIMESTAMPTZ NULL,

    enabled BOOLEAN NOT NULL DEFAULT true,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_recurring_scans_due
ON recurring_scans (status, next_run_at, priority DESC)
WHERE enabled = true;

CREATE TABLE IF NOT EXISTS recurring_scan_runs (
    id UUID PRIMARY KEY,

    recurring_scan_id UUID NOT NULL REFERENCES recurring_scans(id),
    asset_id UUID NOT NULL,
    asset_type TEXT NOT NULL,

    scheduled_for TIMESTAMPTZ NOT NULL,

    status TEXT NOT NULL CHECK (status IN ('created', 'enqueued', 'running', 'succeeded', 'failed', 'max_attempts_exceeded')),

    priority INT NOT NULL DEFAULT 10,

    redis_stream TEXT NULL,
    redis_message_id TEXT NULL,
    redis_job_key TEXT NOT NULL,

    airflow_dag_id TEXT NULL,
    airflow_dag_run_id TEXT NULL,

    attempt_count INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,

    started_at TIMESTAMPTZ NULL,
    finished_at TIMESTAMPTZ NULL,

    result JSONB NULL,
    failure_reason TEXT NULL,
    failure_details JSONB NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (redis_job_key)
);

CREATE INDEX IF NOT EXISTS idx_recurring_scan_runs_status
ON recurring_scan_runs (status, created_at DESC);

CREATE TABLE IF NOT EXISTS worker_runtime_config (
    worker_group TEXT PRIMARY KEY,
    workers_per_controller INT NOT NULL,
    max_queue_depth INT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO worker_runtime_config (worker_group, workers_per_controller, max_queue_depth, enabled)
VALUES ('airflow-scan-workers', 100, 5000, true)
ON CONFLICT (worker_group) DO NOTHING;

INSERT INTO recurring_scans (
    id,
    asset_id,
    asset_type,
    frequency,
    priority,
    status,
    last_run_at,
    next_run_at,
    spread_offset_seconds,
    enabled,
    created_at,
    updated_at
)
VALUES (
    '11111111-1111-1111-1111-111111111111',
    '01234567-89ab-cdef-0123-456789abcdef',
    'url',
    'weekly',
    10,
    'scheduled',
    NULL,
    now() - interval '30 seconds',
    0,
    true,
    now(),
    now()
)
ON CONFLICT (id) DO NOTHING;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO scanning;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO scanning;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO scanning;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO scanning;
