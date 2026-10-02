package store

import (
	"context"
	"testing"
	"time"

	"sampling-svc/internal/inspection"
	"sampling-svc/internal/plan"
)

func createPlanRow(t *testing.T, st *Store, name string,
	n, c int) *plan.Plan {
	t.Helper()
	p := &plan.Plan{
		ID: "plan-" + name, Name: name, Kind: plan.KindSingle,
		Distribution: plan.DistBinomial, SampleSize: n, AcceptNumber: c,
	}
	if err := st.CreatePlan(context.Background(), p); err != nil {
		t.Fatalf("create plan %s: %v", name, err)
	}
	return p
}

func createStreamRow(t *testing.T, st *Store, name string, n, c int) string {
	t.Helper()
	// normal / tightened / reduced plans: c, c-1, c+1
	tc := c - 1
	if tc < 0 {
		tc = 0
	}
	rc := c + 1
	if rc >= n {
		rc = n - 1
	}
	createPlanRow(t, st, name+"-normal", n, c)
	createPlanRow(t, st, name+"-tight", n, tc)
	createPlanRow(t, st, name+"-reduced", n, rc)
	id := "stream-" + name
	err := st.CreateStream(context.Background(), StreamRecord{
		ID: id, Name: name,
		NormalID:    "plan-" + name + "-normal",
		TightenedID: "plan-" + name + "-tight",
		ReducedID:   "plan-" + name + "-reduced",
	})
	if err != nil {
		t.Fatalf("create stream %s: %v", name, err)
	}
	return id
}

// independentReplay loads events straight from the table and runs the
// pure domain Replay, for comparison with the derived state the store
// wrote during the mutating transaction.
func independentReplay(t *testing.T, st *Store, streamID string) inspection.Snapshot {
	t.Helper()
	ctx := context.Background()
	dm, _, err := loadDomainStream(ctx, st.pool, streamID)
	if err != nil {
		t.Fatal(err)
	}
	events, err := loadEvents(ctx, st.pool, streamID)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := inspection.Replay(dm, events)
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

func assertSnapshotsMatch(t *testing.T, got, want inspection.Snapshot) {
	t.Helper()
	if got.Current.Severity != want.Current.Severity {
		t.Fatalf("severity got=%s want=%s", got.Current.Severity, want.Current.Severity)
	}
	if got.Current.ScoreValue() != want.Current.ScoreValue() {
		t.Fatalf("score got=%d want=%d", got.Current.ScoreValue(), want.Current.ScoreValue())
	}
	if len(got.Batches) != len(want.Batches) {
		t.Fatalf("batch count got=%d want=%d", len(got.Batches), len(want.Batches))
	}
	for i := range got.Batches {
		a, b := got.Batches[i], want.Batches[i]
		if a.BatchID != b.BatchID || a.Severity != b.Severity ||
			a.PlanID != b.PlanID || a.Decision != b.Decision ||
			a.Accepted != b.Accepted || a.Score != b.Score ||
			!a.At.Equal(b.At) || a.D1 != b.D1 {
			t.Fatalf("batch %d mismatch:\n got=%+v\nwant=%+v", i, a, b)
		}
	}
}

func TestPlanCRUD(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	N := 500
	p, err := plan.Build(plan.Request{
		Name: "p1", Kind: "single",
		Single: &plan.SingleParam{N: &N, SampleSize: 80, AcceptNumber: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	p.ID = "p1"
	if err := st.CreatePlan(ctx, p); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetPlanByName(ctx, "p1")
	if err != nil {
		t.Fatal(err)
	}
	if got.SampleSize != 80 || got.AcceptNumber != 2 ||
		got.Distribution != plan.DistHypergeometric {
		t.Fatalf("stored plan wrong: %+v", got)
	}
	if err := st.CreatePlan(ctx, p); err == nil {
		t.Fatal("duplicate name must conflict")
	}
	if err := st.UpdatePlan(ctx, p); err != nil {
		t.Fatal(err)
	}
	if err := st.DeletePlan(ctx, p.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetPlan(ctx, p.ID); err == nil {
		t.Fatal("plan should be deleted")
	}
}

func TestStreamTransitionsPersisted(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	id := createStreamRow(t, st, "tr", 20, 5)

	// Two normal rejects -> tightened.
	if _, err := st.AddBatch(ctx, id, BatchRecord{ID: "b0", LotNo: "B0", At: at(0), D1: 20}); err != nil {
		t.Fatal(err)
	}
	snap, err := st.AddBatch(ctx, id, BatchRecord{ID: "b1", LotNo: "B1", At: at(1), D1: 20})
	if err != nil {
		t.Fatal(err)
	}
	if snap.Current.Severity != inspection.SeverityTightened {
		t.Fatalf("severity=%s", snap.Current.Severity)
	}
	// Five consecutive tightened accepts -> normal.
	for i := 2; i < 7; i++ {
		snap, err = st.AddBatch(ctx, id, BatchRecord{
			ID: "acc" + itoaTest(i), LotNo: "B", At: at(i), D1: 0})
		if err != nil {
			t.Fatal(err)
		}
	}
	if snap.Current.Severity != inspection.SeverityNormal {
		t.Fatalf("severity=%s want normal", snap.Current.Severity)
	}
	// Derived snapshot matches a fresh domain replay of raw events.
	assertSnapshotsMatch(t, snap, independentReplay(t, st, id))
}

func TestBackfillAndDeleteRecompute(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	id := createStreamRow(t, st, "bf", 10, 2)

	// Four accepts first.
	for i := 0; i < 4; i++ {
		if _, err := st.AddBatch(ctx, id, BatchRecord{
			ID: "orig" + string(rune('a'+i)), LotNo: "O",
			At: at(10 + i), D1: 0}); err != nil {
			t.Fatal(err)
		}
	}
	snap, _ := st.GetSnapshot(ctx, id)
	if snap.Current.Severity != inspection.SeverityNormal {
		t.Fatal("precondition normal")
	}
	// Backfill two rejects earlier than the accepts: state must flip.
	if _, err := st.AddBatch(ctx, id, BatchRecord{ID: "old1", LotNo: "X", At: at(0), D1: 10}); err != nil {
		t.Fatal(err)
	}
	snap, err := st.AddBatch(ctx, id, BatchRecord{ID: "old2", LotNo: "Y", At: at(1), D1: 10})
	if err != nil {
		t.Fatal(err)
	}
	if snap.Current.Severity != inspection.SeverityTightened {
		t.Fatalf("backfill must flip to tightened, got %s", snap.Current.Severity)
	}
	assertSnapshotsMatch(t, snap, independentReplay(t, st, id))

	// Correct old2 into an accept -> the trigger disappears.
	snap, err = st.UpdateBatch(ctx, id, "old2", BatchRecord{
		ID: "old2", LotNo: "Y", At: at(1), D1: 0})
	if err != nil {
		t.Fatal(err)
	}
	if snap.Current.Severity != inspection.SeverityNormal {
		t.Fatalf("edit must restore normal, got %s", snap.Current.Severity)
	}
	assertSnapshotsMatch(t, snap, independentReplay(t, st, id))

	// Delete the remaining backfilled reject -> still normal, one fewer batch.
	snap, err = st.DeleteBatch(ctx, id, "old1")
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Batches) != 5 {
		t.Fatalf("expected 5 batches, got %d", len(snap.Batches))
	}
	assertSnapshotsMatch(t, snap, independentReplay(t, st, id))
}

func TestFlagsAndResume(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	id := createStreamRow(t, st, "fl", 50, 0)

	if _, err := st.SetFlag(ctx, id, inspection.FlagStable, true, at(-2)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SetFlag(ctx, id, inspection.FlagApproved, true, at(-1)); err != nil {
		t.Fatal(err)
	}
	// 15 clean lots: c=0 awards 2 each -> 30 -> reduced.
	var snap inspection.Snapshot
	for i := 0; i < 15; i++ {
		snap, _ = st.AddBatch(ctx, id, BatchRecord{
			ID: "red" + itoaTest(i), LotNo: "R", At: at(i), D1: 0})
	}
	if snap.Current.Severity != inspection.SeverityReduced {
		t.Fatalf("severity=%s want reduced", snap.Current.Severity)
	}
	// Revoke stable -> back to normal immediately.
	snap, err := st.SetFlag(ctx, id, inspection.FlagStable, false, at(20))
	if err != nil {
		t.Fatal(err)
	}
	if snap.Current.Severity != inspection.SeverityNormal {
		t.Fatalf("revoke stable must return to normal, got %s", snap.Current.Severity)
	}

	// Drive into suspension, then resume.
	_, _ = st.AddBatch(ctx, id, BatchRecord{ID: "j0", LotNo: "J", At: at(21), D1: 50})
	_, _ = st.AddBatch(ctx, id, BatchRecord{ID: "j1", LotNo: "J", At: at(22), D1: 50})
	for k := 0; k < 5; k++ {
		_, _ = st.AddBatch(ctx, id, BatchRecord{
			ID: "ta" + itoaTest(k), LotNo: "TA", At: at(23 + 2*k), D1: 0})
		_, _ = st.AddBatch(ctx, id, BatchRecord{
			ID: "tr" + itoaTest(k), LotNo: "TR", At: at(24 + 2*k), D1: 50})
	}
	snap, _ = st.GetSnapshot(ctx, id)
	if snap.Current.Severity != inspection.SeveritySuspended {
		t.Fatalf("want suspended, got %s", snap.Current.Severity)
	}
	snap, err = st.Resume(ctx, id, at(60))
	if err != nil {
		t.Fatal(err)
	}
	if snap.Current.Severity != inspection.SeverityTightened {
		t.Fatalf("resume must restart tightened, got %s", snap.Current.Severity)
	}
	assertSnapshotsMatch(t, snap, independentReplay(t, st, id))
}

func TestDefectValidationPersisted(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	id := createStreamRow(t, st, "v", 10, 2)
	if _, err := st.AddBatch(ctx, id, BatchRecord{ID: "x", LotNo: "X", At: at(0), D1: 11}); err == nil {
		t.Fatal("d1>n must be rejected at storage layer")
	}
	if _, err := st.AddBatch(ctx, id, BatchRecord{ID: "y", LotNo: "Y", At: at(0), D1: -1}); err == nil {
		t.Fatal("negative d1 must be rejected")
	}
}

func TestDoubleSamplingStream(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	mk := func(name string, c1, r1, c2 int) {
		p := &plan.Plan{ID: "plan-" + name, Name: name, Kind: plan.KindDouble,
			Distribution: plan.DistBinomial,
			N1:           10, C1: c1, R1: r1, N2: 10, C2: c2}
		if err := st.CreatePlan(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	mk("dbl-n", 2, 5, 6)
	mk("dbl-t", 1, 4, 4)
	mk("dbl-r", 3, 6, 8)
	id := "stream-dbl"
	if err := st.CreateStream(ctx, StreamRecord{
		ID: id, Name: "dbl",
		NormalID: "plan-dbl-n", TightenedID: "plan-dbl-t",
		ReducedID: "plan-dbl-r",
	}); err != nil {
		t.Fatal(err)
	}

	// First-stage accept: d1<=c1.
	snap, err := st.AddBatch(ctx, id, BatchRecord{ID: "d-a", LotNo: "DA", At: at(0), D1: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !snap.Batches[0].Accepted {
		t.Fatal("d1=1 <= c1=2 must accept on first sample")
	}
	// Grey zone d1=3 without d2 must be rejected by validation.
	if _, err := st.AddBatch(ctx, id, BatchRecord{ID: "d-g", LotNo: "DG", At: at(1), D1: 3}); err == nil {
		t.Fatal("grey-zone lot without d2 must be rejected")
	}
	// Grey zone with d2 bringing total to 6 -> accept.
	d2 := 3
	snap, err = st.AddBatch(ctx, id, BatchRecord{
		ID: "d-g2", LotNo: "DG2", At: at(1), D1: 3, HasD2: true, D2: d2})
	if err != nil {
		t.Fatal(err)
	}
	if !snap.Batches[len(snap.Batches)-1].Accepted {
		t.Fatal("d1+d2=6 <= c2=6 must accept")
	}
	if snap.Batches[len(snap.Batches)-1].D2 == nil {
		t.Fatal("d2 must be echoed in the result")
	}
	// Grey zone with total 7 -> reject.
	d2 = 4
	snap, err = st.AddBatch(ctx, id, BatchRecord{
		ID: "d-g3", LotNo: "DG3", At: at(2), D1: 3, HasD2: true, D2: d2})
	if err != nil {
		t.Fatal(err)
	}
	if snap.Batches[len(snap.Batches)-1].Accepted {
		t.Fatal("d1+d2=7 > c2=6 must reject")
	}
	assertSnapshotsMatch(t, snap, independentReplay(t, st, id))
}

func TestTimeOrderingWithSameTimestamp(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	id := createStreamRow(t, st, "ts", 10, 2)
	// Same timestamp inserted out of order: seq must break the tie
	// deterministically.
	t0 := at(5)
	_, err := st.AddBatch(ctx, id, BatchRecord{ID: "late", LotNo: "L", At: t0, D1: 10})
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.AddBatch(ctx, id, BatchRecord{ID: "early", LotNo: "E", At: t0, D1: 10})
	if err != nil {
		t.Fatal(err)
	}
	snap := independentReplay(t, st, id)
	if snap.Batches[0].BatchID != "late" || snap.Batches[1].BatchID != "early" {
		t.Fatalf("same-timestamp tie-break wrong: %s,%s",
			snap.Batches[0].BatchID, snap.Batches[1].BatchID)
	}
}

var _ = time.Now
