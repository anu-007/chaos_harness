-- FarLabs distributed job dispatcher — persistent schema (Postgres = source of truth).
-- Applied idempotently on coordinator startup. Everything here exists to satisfy the
-- invariants checked by chaos_harness.py::_verify (dedup, no-lost, no-double-commit,
-- per-job + global fence monotonicity, no stale-commit).

CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- gen_random_uuid()

-- Global fencing token source. A single sequence is monotonic across every coordinator,
-- restart and replica, so nextval() is strictly increasing system-wide.
CREATE SEQUENCE IF NOT EXISTS fence_seq;

-- Millisecond wall time taken from the DATABASE clock, never a coordinator clock.
-- Using DB time for issued_at_ms keeps global fence ordering (sorted by issued_at_ms)
-- consistent even when the clock_skew fault skews a coordinator's logical clock.
CREATE OR REPLACE FUNCTION db_now_ms() RETURNS bigint
    LANGUAGE sql VOLATILE AS $$
    SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint;
$$;

-- Jobs. idempotency_key UNIQUE enforces dedup even for concurrent same-key submissions
-- landing on different coordinators in the same millisecond.
-- state in: pending | leased | succeeded | failed | cancelled
CREATE TABLE IF NOT EXISTS jobs (
    job_id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key text        NOT NULL UNIQUE,
    payload         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    state           text        NOT NULL DEFAULT 'pending',
    result          jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- Partial index for the dispatch claim: SELECT ... WHERE state='pending' ORDER BY created_at
-- FOR UPDATE SKIP LOCKED. Keeps claiming cheap as the queue grows.
CREATE INDEX IF NOT EXISTS jobs_pending_idx ON jobs (created_at) WHERE state = 'pending';

-- Append-only state transition log. Powers /audit "transitions" and the no-lost check
-- (a job with a terminal transition is accounted for even without an accepted commit).
CREATE TABLE IF NOT EXISTS job_transitions (
    id          bigserial PRIMARY KEY,
    job_id      uuid      NOT NULL,
    from_state  text,
    to_state    text      NOT NULL,
    at_ms       bigint    NOT NULL,
    coordinator text      NOT NULL
);
CREATE INDEX IF NOT EXISTS job_transitions_job_idx ON job_transitions (job_id);

-- Append-only lease history. fence + issued_at_ms are written together from DB sources
-- (nextval + db_now_ms) so both per-job and global monotonicity hold. expired_at_ms is
-- NULL while the lease is live and stamped by the reaper (or clean disconnect) on expiry.
CREATE TABLE IF NOT EXISTS leases (
    id            bigserial PRIMARY KEY,
    job_id        uuid      NOT NULL,
    fence         bigint    NOT NULL,
    worker        text      NOT NULL,
    issued_at_ms  bigint    NOT NULL,
    expires_at_ms bigint    NOT NULL,
    expired_at_ms bigint
);
CREATE INDEX IF NOT EXISTS leases_job_idx ON leases (job_id);
-- Reaper scan: find live leases that have passed their deadline.
CREATE INDEX IF NOT EXISTS leases_live_idx ON leases (expires_at_ms) WHERE expired_at_ms IS NULL;

-- Append-only commit attempt log. Powers /audit "commits" and the double-commit check.
-- The partial unique index guarantees at most ONE accepted commit per job, so a worker
-- retrying after a dropped ack (drop_acks fault) cannot double-record a result.
CREATE TABLE IF NOT EXISTS commits (
    id       bigserial PRIMARY KEY,
    job_id   uuid      NOT NULL,
    fence    bigint    NOT NULL,
    worker   text      NOT NULL,
    accepted boolean   NOT NULL,
    at_ms    bigint    NOT NULL
);
CREATE INDEX IF NOT EXISTS commits_job_idx ON commits (job_id);
CREATE UNIQUE INDEX IF NOT EXISTS one_accept ON commits (job_id) WHERE accepted;
