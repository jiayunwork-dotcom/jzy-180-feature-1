package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sampling-svc/internal/inspection"
	"sampling-svc/internal/plan"
)

// oldSchemaDDL is the EXACT pre-revision schema (plans/streams/
// stream_events/batch_results without plan_revisions and without
// batch_results.plan_revision). The migration test first creates an
// isolated schema with these tables, fills representative data INCLUDING
// switching history, then runs the new idempotent migration against it
// and compares every derived field.
const oldSchemaDDL = `
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
    at TIMESTAMPTZ NOT NULL, severity TEXT NOT NULL,
    plan_id TEXT NOT NULL, plan_name TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('accepted','rejected','not_inspected')),
    accepted BOOLEAN NOT NULL, d1 INTEGER NOT NULL, d2 INTEGER,
    score INTEGER NOT NULL, note TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (stream_id, seq)
);
`

// TestInPlaceMigrationFromOldSchema fills the pre-revision schema with a
// stream that has already switched normal -> tightened -> suspended, then
// migrates and checks every field is preserved.
func TestInPlaceMigrationFromOldSchema(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	pool := st.pool

	// 1. Replace the fresh schema with the OLD one.
	if _, err := pool.Exec(ctx, `
DROP TABLE IF EXISTS batch_results, stream_events, streams,
                      plan_revisions, plans CASCADE`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, oldSchemaDDL); err != nil {
		t.Fatal(err)
	}

	// 2. Fill data using direct SQL (old table shape).
	insertPlan := func(id string, n, c int) {
		if _, err := pool.Exec(ctx, `
INSERT INTO plans (id,name,kind,distribution,n,c) VALUES ($1,$2,'single','binomial',$3,$4)`,
			"p-"+id, id, n, c); err != nil {
			t.Fatal(err)
		}
	}
	insertPlan("n", 20, 5)
	insertPlan("t", 20, 4)
	insertPlan("r", 20, 6)
	if _, err := pool.Exec(ctx, `
INSERT INTO streams (id,name,normal_id,tightened_id,reduced_id,
                     current_severity,current_score)
VALUES ('s1','mig','p-n','p-t','p-r','suspended',0)`); err != nil {
		t.Fatal(err)
	}
	// Two rejects (switch -> tightened), then five tightened rejects
	// interleaved with accepts -> suspended.
	base := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	addEvent := func(seq int64, hour int, batchID, lot string, d1 int, sev, decision string, accepted bool, score int, note string) {
		at := base.Add(time.Duration(hour) * time.Hour)
		if _, err := pool.Exec(ctx, `
INSERT INTO stream_events (stream_id,seq,kind,at,batch_id,lot_no,d1)
VALUES ('s1',$1,'batch',$2,$3,$4,$5)`, seq, at, batchID, lot, d1); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO batch_results
(stream_id,seq,batch_id,lot_no,at,severity,plan_id,plan_name,
 decision,accepted,d1,score,note)
VALUES ('s1',$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
			seq, batchID, lot, at, sev, "p-"+slotOf(sev), slotName(sev),
			decision, accepted, d1, score, note); err != nil {
			t.Fatal(err)
		}
	}
	// hours: reject, reject -> tightened; then A,R pairs on tightened.
	type row struct {
		seq                      int64
		hour                     int
		bid, lot, sev, dec, note string
		d                        int
		acc                      bool
		score                    int
	}
	rows := []row{
		{1, 0, "b1", "B1", "normal", "rejected", "", 20, false, 0},
		{2, 1, "b2", "B2", "normal", "rejected", "switch normal -> tightened", 20, false, 0},
	}
	h := 2
	for k := 0; k < 5; k++ {
		note := ""
		sev := "tightened"
		// accepted lot
		if k == 4 {
			// no 5th accept: ensures suspension
		}
		rows = append(rows, row{int64(2*k + 3), h, fmt.Sprintf("a%d", k), "A", sev, "accepted", note, 0, true, 0})
		h++
		rnote := ""
		if k == 4 {
			rnote = "tightened inspection discontinued (suspended)"
		}
		rows = append(rows, row{int64(2*k + 4), h, fmt.Sprintf("r%d", k), "R", sev, "rejected", rnote, 5, false, 0})
		h++
	}
	for _, x := range rows {
		addEvent(x.seq, x.hour, x.bid, x.lot, x.d, x.sev, x.dec, x.acc, x.score, x.note)
	}
	// One not-inspected lot after suspension.
	lastHour := h
	if _, err := pool.Exec(ctx, `
INSERT INTO stream_events (stream_id,seq,kind,at,batch_id,lot_no,d1)
VALUES ('s1',100,'batch',$1,'z','Z',0)`, base.Add(time.Duration(lastHour)*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO batch_results
(stream_id,seq,batch_id,lot_no,at,severity,plan_id,plan_name,
 decision,accepted,d1,score,note)
VALUES ('s1',100,'z','Z',$1,'suspended','','','not_inspected',false,0,0,
        'inspection suspended; lot not inspected')`,
		base.Add(time.Duration(lastHour)*time.Hour)); err != nil {
		t.Fatal(err)
	}

	// 3. Capture expected outcomes (pre-migration, verbatim fields).
	type expRow struct {
		bid, sev, pid, pname, dec string
		acc                       bool
		d1, score                 int
		note                      string
		at                        time.Time
	}
	var expected []expRow
	erows, err := pool.Query(ctx, `
SELECT batch_id,severity,plan_id,plan_name,decision,accepted,d1,score,note,at
FROM batch_results ORDER BY at,seq`)
	if err != nil {
		t.Fatal(err)
	}
	for erows.Next() {
		var e expRow
		if err := erows.Scan(&e.bid, &e.sev, &e.pid, &e.pname, &e.dec,
			&e.acc, &e.d1, &e.score, &e.note, &e.at); err != nil {
			t.Fatal(err)
		}
		expected = append(expected, e)
	}
	erows.Close()

	// 4. Run the migration.
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// 5. Exactly one initial revision per plan, effective at year 1.
	var nRev int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM plan_revisions`).Scan(&nRev); err != nil {
		t.Fatal(err)
	}
	if nRev != 3 {
		t.Fatalf("want 3 initial revisions, got %d", nRev)
	}
	var eff time.Time
	if err := pool.QueryRow(ctx, `
SELECT min(effective_at) FROM plan_revisions`).Scan(&eff); err != nil {
		t.Fatal(err)
	}
	if !eff.Equal(plan.Epoch) {
		t.Fatalf("initial revision effective=%v want %v", eff, plan.Epoch)
	}
	// Initial revision content equals the plan row content.
	var n1, n2, c1, c2 int
	if err := pool.QueryRow(ctx, `
SELECT p.n, r.n, p.c, r.c FROM plans p JOIN plan_revisions r
ON p.id=r.plan_id WHERE p.id='p-n' AND r.revision_no=1`).
		Scan(&n1, &n2, &c1, &c2); err != nil {
		t.Fatal(err)
	}
	if n1 != 20 || n2 != 20 || c1 != 5 || c2 != 5 {
		t.Fatalf("initial revision content mismatch n=%d/%d c=%d/%d", n1, n2, c1, c2)
	}

	// 6. Re-running the migration adds no revision, changes no row count.
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM plan_revisions`).Scan(&nRev); err != nil {
		t.Fatal(err)
	}
	if nRev != 3 {
		t.Fatalf("repeated migration duplicated revisions: %d", nRev)
	}

	// 7. Every derived field survives unchanged; plan_revision defaults 1.
	snap, err := st.GetSnapshot(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Batches) != len(expected) {
		t.Fatalf("batch count got=%d want=%d", len(snap.Batches), len(expected))
	}
	for i, e := range expected {
		got := snap.Batches[i]
		if got.BatchID != e.bid || string(got.Severity) != e.sev ||
			got.PlanID != e.pid || got.PlanName != e.pname ||
			string(got.Decision) != e.dec || got.Accepted != e.acc ||
			got.D1 != e.d1 || got.Score != e.score || got.Note != e.note ||
			!got.At.Equal(e.at) || got.PlanRevision != 1 {
			t.Fatalf("migrated row %d mismatch:\n got=%+v\nwant=%+v", i, got, e)
		}
	}
	if snap.Current.Severity != inspection.SeveritySuspended {
		t.Fatalf("current severity got=%s want suspended", snap.Current.Severity)
	}

	// 8. Domain replay over the migrated histories agrees field-for-field.
	assertSnapshotsMatch(t, snap, independentReplay(t, st, "s1"))
}

func slotOf(sev string) string {
	switch sev {
	case "normal":
		return "n"
	case "tightened":
		return "t"
	default:
		return "r"
	}
}
func slotName(sev string) string { return slotOf(sev) }
