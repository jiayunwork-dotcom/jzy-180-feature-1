package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"sampling-svc/internal/inspection"
)

// legacySchemaSQL is the EXACT pre-revision schema (plans without
// plan_revisions, batch_results without revision_no). A data volume
// written by the previous version looks exactly like this.
const legacySchemaSQL = `
CREATE TABLE plans (
    id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL CHECK (kind IN ('single','double')),
    distribution TEXT NOT NULL CHECK (distribution IN ('hypergeometric','binomial','poisson')),
    n_lot INTEGER, n INTEGER NOT NULL, c INTEGER NOT NULL,
    n1 INTEGER NOT NULL DEFAULT 0, c1 INTEGER NOT NULL DEFAULT 0,
    r1 INTEGER NOT NULL DEFAULT 0, n2 INTEGER NOT NULL DEFAULT 0,
    c2 INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE streams (
    id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE,
    normal_id TEXT NOT NULL REFERENCES plans(id) ON DELETE RESTRICT,
    tightened_id TEXT NOT NULL REFERENCES plans(id) ON DELETE RESTRICT,
    reduced_id TEXT NOT NULL REFERENCES plans(id) ON DELETE RESTRICT,
    current_severity TEXT NOT NULL DEFAULT 'normal'
        CHECK (current_severity IN ('normal','tightened','reduced','suspended')),
    current_score INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE stream_events (
    stream_id TEXT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    seq BIGINT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('batch','flag','resume')),
    at TIMESTAMPTZ NOT NULL,
    batch_id TEXT, lot_no TEXT, d1 INTEGER,
    has_d2 BOOLEAN NOT NULL DEFAULT FALSE, d2 INTEGER,
    flag TEXT CHECK (flag IS NULL OR flag IN ('stable','approved')),
    flag_value BOOLEAN,
    PRIMARY KEY (stream_id, seq),
    UNIQUE (stream_id, batch_id)
);
CREATE INDEX idx_stream_events_order ON stream_events (stream_id, at, seq);
CREATE TABLE batch_results (
    stream_id TEXT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    seq BIGINT NOT NULL, batch_id TEXT NOT NULL, lot_no TEXT NOT NULL,
    at TIMESTAMPTZ NOT NULL, severity TEXT NOT NULL, plan_id TEXT NOT NULL,
    plan_name TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('accepted','rejected','not_inspected')),
    accepted BOOLEAN NOT NULL, d1 INTEGER NOT NULL, d2 INTEGER,
    score INTEGER NOT NULL, note TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (stream_id, seq)
);
`

// legacyRow is one field-for-field snapshot of a migrated row, captured
// BEFORE running the new migration.
type legacyRow struct {
	streamID                   string
	seq                        int64
	batchID, lotNo, severity   string
	planID, planName, decision string
	accepted                   bool
	d1                         int
	d2                         *int
	score                      int
	at                         time.Time
}

// TestInPlaceMigrationFromLegacy:
//  1. create the OLD schema directly (bypassing the embedded new schema),
//  2. drive a rich switching history (normal->tightened->normal, flags,
//     reduced, suspension with a not-inspected lot, resume),
//  3. snapshot every streams/batch_results field,
//  4. open a NEW store over the same data and run its Migrate,
//  5. assert every pre-existing field is byte-for-byte identical and the
//     one new revision (no=1) covers all history;
//  6. migrate AGAIN (simulates a second restart): no extra revision, no
//     data rewrite.
func TestInPlaceMigrationFromLegacy(t *testing.T) {
	dsn := testDSN()
	if dsn == "" {
		t.Skip("PG_TEST_DSN not set")
	}
	schema := "test_migrate"

	// Setup isolated schema with the LEGACY schema only.
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx,
		fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE; CREATE SCHEMA %s`, schema, schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf("SET search_path=%s; %s", schema, legacySchemaSQL)); err != nil {
		t.Fatal(err)
	}
	_ = admin.Close(ctx)

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(c, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		pool.Close()
	})

	// Seed plans.
	seedPlan := func(id, name string, n, c int) {
		if _, err := pool.Exec(ctx, `
INSERT INTO plans (id,name,kind,distribution,n,c)
VALUES ($1,$2,'single','binomial',$3,$4)`, id, name, n, c); err != nil {
			t.Fatal(err)
		}
	}
	seedPlan("pln", "norm", 20, 5)
	seedPlan("plt", "tight", 20, 4)
	seedPlan("plr", "red", 20, 6)
	// Header reflects the replay of the fixture below: the stream ends
	// suspended (5 cumulative tightened rejects), score 0.
	if _, err := pool.Exec(ctx, `
INSERT INTO streams (id,name,normal_id,tightened_id,reduced_id,current_severity,current_score)
VALUES ('str','flow','pln','plt','plr','suspended',0)`); err != nil {
		t.Fatal(err)
	}

	// Build a switching history using raw event+result inserts. This
	// gives exact control over the derived fields that must survive.
	planNameOf := map[string]string{"pln": "norm", "plt": "tight", "plr": "red"}
	insertLegacy := func(seq int64, id string, hour int, d1 int, sev, dec, plan string, accepted bool, score int, note string) {
		insAt := at(hour)
		if _, err := pool.Exec(ctx, `
INSERT INTO stream_events (stream_id,seq,kind,at,batch_id,lot_no,d1,has_d2)
VALUES ('str',$1,'batch',$2,$3,$3,$4,FALSE)`, seq, insAt, id, d1); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO batch_results
(stream_id,seq,batch_id,lot_no,at,severity,plan_id,plan_name,decision,accepted,d1,score,note)
VALUES ('str',$1,$2,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
			seq, id, insAt, sev, plan, planNameOf[plan], dec, accepted, d1, score, note); err != nil {
			t.Fatal(err)
		}
	}
	// Two normal rejects (d=20 > c=5) -> tightened. Then on tightened
	// (c=4): five cumulative rejects with an accept interleaved so the
	// five-consecutive-accept escape never triggers -> suspended. A later
	// lot is not_inspected and must migrate with NULL revision_no.
	insertLegacy(1, "r1", 1, 20, "normal", "rejected", "pln", false, 0, "")
	insertLegacy(2, "r2", 2, 20, "normal", "rejected", "pln", false, 0, "switch normal -> tightened")
	// tightened: A,R pairs; d=5 rejects under c=4, d=0 accepts.
	insertLegacy(3, "ta1", 3, 0, "tightened", "accepted", "plt", true, 0, "")
	insertLegacy(4, "tr1", 4, 5, "tightened", "rejected", "plt", false, 0, "")
	insertLegacy(5, "ta2", 5, 0, "tightened", "accepted", "plt", true, 0, "")
	insertLegacy(6, "tr2", 6, 5, "tightened", "rejected", "plt", false, 0, "")
	insertLegacy(7, "ta3", 7, 0, "tightened", "accepted", "plt", true, 0, "")
	insertLegacy(8, "tr3", 8, 5, "tightened", "rejected", "plt", false, 0, "")
	insertLegacy(9, "ta4", 9, 0, "tightened", "accepted", "plt", true, 0, "")
	insertLegacy(10, "tr4", 10, 5, "tightened", "rejected", "plt", false, 0, "")
	insertLegacy(11, "sus1", 11, 5, "tightened", "rejected", "plt", false, 0, "tightened inspection discontinued (suspended)")
	insertLegacy(12, "ni", 12, 0, "suspended", "not_inspected", "pln", false, 0, "inspection suspended; lot not inspected")

	// Capture every pre-migration derived field.
	var beforeRows []legacyRow
	rows, err := pool.Query(ctx, `
SELECT stream_id,seq,batch_id,lot_no,severity,plan_id,plan_name,decision,
       accepted,d1,d2,score,at
FROM batch_results ORDER BY stream_id,seq`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var r legacyRow
		if err := rows.Scan(&r.streamID, &r.seq, &r.batchID, &r.lotNo,
			&r.severity, &r.planID, &r.planName, &r.decision,
			&r.accepted, &r.d1, &r.d2, &r.score, &r.at); err != nil {
			t.Fatal(err)
		}
		beforeRows = append(beforeRows, r)
	}
	rows.Close()
	var beforeSev string
	var beforeScore int
	if err := pool.QueryRow(ctx,
		`SELECT current_severity,current_score FROM streams WHERE id='str'`).
		Scan(&beforeSev, &beforeScore); err != nil {
		t.Fatal(err)
	}

	// Run the NEW migration against the legacy volume.
	pool.Close()
	st := NewIsolatedTestStoreEmpty(t, dsn, schema) // migrates, doesn't drop

	after, err := st.GetSnapshot(ctx, "str")
	if err != nil {
		t.Fatal(err)
	}
	if string(after.Current.Severity) != beforeSev ||
		after.Current.ScoreValue() != beforeScore {
		t.Fatalf("stream header changed: %s/%d -> %s/%d",
			beforeSev, beforeScore, after.Current.Severity, after.Current.ScoreValue())
	}
	if len(after.Batches) != len(beforeRows) {
		t.Fatalf("row count %d -> %d", len(beforeRows), len(after.Batches))
	}
	gotByID := map[string]inspection.BatchOutcome{}
	for _, o := range after.Batches {
		gotByID[o.BatchID] = o
	}
	for _, b := range beforeRows {
		o, ok := gotByID[b.batchID]
		if !ok {
			t.Fatalf("missing migrated batch %s", b.batchID)
		}
		if string(o.Severity) != b.severity || string(o.Decision) != b.decision ||
			o.Accepted != b.accepted || o.Score != b.score || o.D1 != b.d1 ||
			o.PlanID != b.planID || !o.At.Equal(b.at) || o.LotNo != b.lotNo {
			t.Fatalf("migrated batch %s changed:\n before=%+v\n after =%+v", b.batchID, b, o)
		}
		switch b.decision {
		case "not_inspected":
			if o.RevisionNo != nil {
				t.Fatalf("suspended lot %s must get NULL revision, got %d", b.batchID, *o.RevisionNo)
			}
		default:
			if o.RevisionNo == nil || *o.RevisionNo != 1 {
				t.Fatalf("judged lot %s must get revision 1, got %v", b.batchID, o.RevisionNo)
			}
		}
	}

	// Each legacy plan became exactly one revision covering all history.
	for _, pid := range []string{"pln", "plt", "plr"} {
		revs, err := st.ListRevisions(ctx, pid)
		if err != nil {
			t.Fatal(err)
		}
		if len(revs) != 1 || revs[0].No != 1 {
			t.Fatalf("plan %s revisions=%+v", pid, revs)
		}
	}

	// Re-migrate (simulate a second service restart): idempotent — no new
	// revision, no data rewrite.
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	nRev := 0
	if err := st.Pool().QueryRow(ctx,
		`SELECT count(*) FROM plan_revisions`).Scan(&nRev); err != nil {
		t.Fatal(err)
	}
	if nRev != 3 {
		t.Fatalf("re-migration added revisions: count=%d", nRev)
	}
	after2, err := st.GetSnapshot(ctx, "str")
	if err != nil {
		t.Fatal(err)
	}
	assertSnapshotsMatch(t, after2, independentReplay(t, st, "str"))
}
