-- PostgreSQL 16 schema for the acceptance-sampling service.
--
-- Consistency model:
--   * plans are append-only versioned. plan_revisions holds every
--     revision; the numeric columns on "plans" are a convenience copy of
--     the LATEST revision (for plan CRUD / analysis endpoints). Revisions
--     are immutable: corrections are appended, never overwritten, and a
--     revision may take effect in the future or be backdated into the
--     past. At inspection time t the effective revision is the one with
--     the greatest effective_at <= t; ties resolve to the greater
--     revision number.
--   * stream_events is the append-mostly source of truth (batches, flag
--     changes, manual resumes). Each stream has its own advisory lock
--     (pg_advisory_xact_lock keyed by stream id) so concurrent writers to
--     the SAME stream serialize; different streams never block.
--   * batch_results / streams current-state columns are DERIVED data,
--     rebuilt inside the same transaction that appends a revision or
--     mutates events, by replaying every event from the beginning with
--     the plan revisions effective at each lot's inspection time. They
--     are cache only; truth comes from events + revisions.

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

-- Immutable revision history of every plan.
CREATE TABLE IF NOT EXISTS plan_revisions (
    plan_id      TEXT NOT NULL REFERENCES plans(id) ON DELETE CASCADE,
    revision_no  INTEGER NOT NULL CHECK (revision_no >= 1),
    effective_at TIMESTAMPTZ NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    name         TEXT NOT NULL,
    distribution TEXT NOT NULL CHECK (distribution IN ('hypergeometric','binomial','poisson')),
    n_lot        INTEGER,
    n            INTEGER NOT NULL,
    c            INTEGER NOT NULL,
    n1           INTEGER NOT NULL DEFAULT 0,
    c1           INTEGER NOT NULL DEFAULT 0,
    r1           INTEGER NOT NULL DEFAULT 0,
    n2           INTEGER NOT NULL DEFAULT 0,
    c2           INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (plan_id, revision_no),
    UNIQUE (plan_id, effective_at)
);

CREATE INDEX IF NOT EXISTS idx_plan_revisions_effective
    ON plan_revisions (plan_id, effective_at, revision_no);

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
-- plan_revision records which revision of the judged plan slot produced
-- the row (1 for data migrated from the pre-revision schema).
CREATE TABLE IF NOT EXISTS batch_results (
    stream_id     TEXT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    seq           BIGINT NOT NULL,
    batch_id      TEXT NOT NULL,
    lot_no        TEXT NOT NULL,
    at            TIMESTAMPTZ NOT NULL,
    severity      TEXT NOT NULL,
    plan_id       TEXT NOT NULL,
    plan_revision INTEGER NOT NULL DEFAULT 1,
    plan_name     TEXT NOT NULL,
    decision      TEXT NOT NULL CHECK (decision IN ('accepted','rejected','not_inspected')),
    accepted      BOOLEAN NOT NULL,
    d1            INTEGER NOT NULL,
    d2            INTEGER,
    score         INTEGER NOT NULL,
    note          TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (stream_id, seq)
);

-- ---------------------------------------------------------------------------
-- In-place upgrade from the pre-revision schema. Every statement is
-- idempotent so restarting the service repeatedly neither duplicates
-- revisions nor rewrites derived data.
-- ---------------------------------------------------------------------------

-- 1. Add the plan_revision column when upgrading an old database. A fresh
--    database already has it, so guard with an information_schema check.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'batch_results' AND column_name = 'plan_revision'
    ) THEN
        ALTER TABLE batch_results ADD COLUMN plan_revision INTEGER NOT NULL DEFAULT 1;
    END IF;
END $$;

-- 2. Every pre-existing plan gets exactly ONE initial revision covering
--    all of history (effective at year 1). The WHERE NOT EXISTS guard
--    makes this run once even across repeated restarts; plans created by
--    the new code already have their initial revision.
INSERT INTO plan_revisions
    (plan_id, revision_no, effective_at, created_at,
     name, distribution, n_lot, n, c, n1, c1, r1, n2, c2)
SELECT p.id, 1, '0001-01-01 00:00:00+00', p.created_at,
       p.name, p.distribution, p.n_lot, p.n, p.c,
       p.n1, p.c1, p.r1, p.n2, p.c2
FROM plans p
WHERE NOT EXISTS (
    SELECT 1 FROM plan_revisions r WHERE r.plan_id = p.id
);

