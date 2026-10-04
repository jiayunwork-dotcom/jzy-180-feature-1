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
--
-- Revision history for plans:
--   * plans holds only the immutable-ish header (id, name, kind; kind can
--     never change). plan_revisions is the append-only history of
--     parameter sets, one row per revision, with an inclusive effective
--     time. A lot inspected at t is judged under the latest revision
--     whose effective_at <= t (highest revision number breaks ties).
--   * The numeric columns on plans are a denormalized mirror of the
--     revision currently in force, kept in the same transaction that
--     appends a revision so plan analysis endpoints stay a single-row
--     read. Revision history is the source of truth; the mirror exists
--     only for read convenience.
--   * Appending a revision locks the plan (a dedicated advisory lock)
--     and then every affected stream (in stream-id order) inside ONE
--     transaction, rebuilds them synchronously and commits the history
--     and the recomputed streams atomically. Readers therefore never
--     observe a half-rebuilt stream.

CREATE TABLE IF NOT EXISTS plans (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL UNIQUE,
    kind         TEXT NOT NULL CHECK (kind IN ('single','double')),
    -- Mirror of the currently effective revision (see file header).
    distribution TEXT NOT NULL DEFAULT 'binomial'
        CHECK (distribution IN ('hypergeometric','binomial','poisson')),
    n_lot        INTEGER,
    n            INTEGER NOT NULL DEFAULT 0,
    c            INTEGER NOT NULL DEFAULT 0,
    n1           INTEGER NOT NULL DEFAULT 0,
    c1           INTEGER NOT NULL DEFAULT 0,
    r1           INTEGER NOT NULL DEFAULT 0,
    n2           INTEGER NOT NULL DEFAULT 0,
    c2           INTEGER NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS plan_revisions (
    plan_id      TEXT NOT NULL REFERENCES plans(id) ON DELETE CASCADE,
    revision_no  INTEGER NOT NULL CHECK (revision_no >= 1),
    effective_at TIMESTAMPTZ NOT NULL,
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
    PRIMARY KEY (plan_id, revision_no)
);

-- Revision lookup for a stream replay: newest effective_at <= t.
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
-- revision_no names the revision of the bound severity plan that judged
-- the lot; it is NULL for suspended not-inspected lots and for pre-
-- revision rows back-filled by the in-place migration of those lots.
CREATE TABLE IF NOT EXISTS batch_results (
    stream_id    TEXT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    seq          BIGINT NOT NULL,
    batch_id     TEXT NOT NULL,
    lot_no       TEXT NOT NULL,
    at           TIMESTAMPTZ NOT NULL,
    severity     TEXT NOT NULL,
    plan_id      TEXT NOT NULL,
    plan_name    TEXT NOT NULL,
    revision_no  INTEGER,
    decision     TEXT NOT NULL CHECK (decision IN ('accepted','rejected','not_inspected')),
    accepted     BOOLEAN NOT NULL,
    d1           INTEGER NOT NULL,
    d2           INTEGER,
    score        INTEGER NOT NULL,
    note         TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (stream_id, seq)
);

-- ---------------------------------------------------------------------------
-- In-place migration from the revision-less schema.
--
-- A volume written by the previous version has plans with NOT NULL
-- distribution and no plan_revisions table. Each such plan becomes one
-- initial revision (revision_no = 1) covering all history; batch_results
-- gains revision_no, set to 1 exactly where a lot was actually judged.
-- The block is idempotent: on a fresh database plan_revisions is empty
-- and there is nothing to seed; on repeated restarts every plan already
-- has its revision 1 so the anti-join inserts nothing, and batch_results
-- backfill only touches still-NULL rows that were judged.
-- ---------------------------------------------------------------------------

-- Add the revision_no column when upgrading an older batch_results table.
ALTER TABLE batch_results ADD COLUMN IF NOT EXISTS revision_no INTEGER;

INSERT INTO plan_revisions
    (plan_id, revision_no, effective_at, distribution, n_lot,
     n, c, n1, c1, r1, n2, c2, created_at)
SELECT p.id, 1, '0001-01-01 00:00:00+00', p.distribution, p.n_lot,
       p.n, p.c, p.n1, p.c1, p.r1, p.n2, p.c2, COALESCE(p.created_at, now())
FROM plans p
WHERE NOT EXISTS (
    SELECT 1 FROM plan_revisions r
    WHERE r.plan_id = p.id AND r.revision_no = 1
);

UPDATE batch_results
SET revision_no = 1
WHERE revision_no IS NULL
  AND decision <> 'not_inspected';
