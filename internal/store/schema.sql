-- PostgreSQL 16 schema for the acceptance-sampling service.
--
-- Consistency model for inspection streams:
--   * stream_events is the append-mostly source of truth (batches, flag
--     changes, manual resumes). Each stream has its own advisory lock
--     (pg_advisory_xact_lock keyed by stream id) so concurrent writers
--     to the SAME stream serialize; different streams never block each
--     other.
--   * batch_results / streams current-state columns are DERIVED data,
--     rebuilt inside the same transaction by replaying every event from
--     the beginning. They are cache only; truth comes from the events.

CREATE TABLE IF NOT EXISTS plans (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL UNIQUE,
    kind         TEXT NOT NULL CHECK (kind IN ('single','double')),
    distribution TEXT NOT NULL CHECK (distribution IN ('hypergeometric','binomial','poisson')),
    n_lot        INTEGER,
    n            INTEGER NOT NULL,
    c            INTEGER NOT NULL,
    n1           INTEGER NOT NULL DEFAULT 0,
    c1           INTEGER NOT NULL DEFAULT 0,
    r1           INTEGER NOT NULL DEFAULT 0,
    n2           INTEGER NOT NULL DEFAULT 0,
    c2           INTEGER NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS streams (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL UNIQUE,
    normal_id    TEXT NOT NULL REFERENCES plans(id) ON DELETE RESTRICT,
    tightened_id TEXT NOT NULL REFERENCES plans(id) ON DELETE RESTRICT,
    reduced_id   TEXT NOT NULL REFERENCES plans(id) ON DELETE RESTRICT,
    -- Derived current state (cache of the full replay).
    current_severity TEXT NOT NULL DEFAULT 'normal'
        CHECK (current_severity IN ('normal','tightened','reduced','suspended')),
    current_score    INTEGER NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- seq is a per-stream monotonic allocation order; order by (at, seq).
CREATE TABLE IF NOT EXISTS stream_events (
    stream_id  TEXT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    seq        BIGINT NOT NULL,
    kind       TEXT NOT NULL CHECK (kind IN ('batch','flag','resume')),
    at         TIMESTAMPTZ NOT NULL,
    -- batch payload (NULL for non-batch events)
    batch_id   TEXT,
    lot_no     TEXT,
    d1         INTEGER,
    has_d2     BOOLEAN NOT NULL DEFAULT FALSE,
    d2         INTEGER,
    -- flag payload
    flag       TEXT CHECK (flag IS NULL OR flag IN ('stable','approved')),
    flag_value BOOLEAN,
    PRIMARY KEY (stream_id, seq),
    UNIQUE (stream_id, batch_id)
);

CREATE INDEX IF NOT EXISTS idx_stream_events_order
    ON stream_events (stream_id, at, seq);

-- Derived per-batch outcomes, rebuilt wholesale after every mutation.
CREATE TABLE IF NOT EXISTS batch_results (
    stream_id  TEXT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    seq        BIGINT NOT NULL,
    batch_id   TEXT NOT NULL,
    lot_no     TEXT NOT NULL,
    at         TIMESTAMPTZ NOT NULL,
    severity   TEXT NOT NULL,
    plan_id    TEXT NOT NULL,
    plan_name  TEXT NOT NULL,
    decision   TEXT NOT NULL CHECK (decision IN ('accepted','rejected','not_inspected')),
    accepted   BOOLEAN NOT NULL,
    d1         INTEGER NOT NULL,
    d2         INTEGER,
    score      INTEGER NOT NULL,
    note       TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (stream_id, seq)
);
