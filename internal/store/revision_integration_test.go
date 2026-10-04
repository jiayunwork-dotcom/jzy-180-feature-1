package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"sampling-svc/internal/inspection"
	"sampling-svc/internal/plan"
)

// mkSingleRevisionPlan builds the domain *plan.Plan payload for a single
// revision (binomial).
func mkSingleRevision(name string, n, c int) *plan.Plan {
	return &plan.Plan{Name: name, Kind: plan.KindSingle,
		Distribution: plan.DistBinomial, SampleSize: n, AcceptNumber: c}
}

func appendRev(t *testing.T, st *Store, planID string, n, c int, at time.Time) RevisionResult {
	t.Helper()
	p := mkSingleRevision("", n, c)
	res, err := st.AppendRevision(context.Background(), planID, p, nil, &at)
	if err != nil {
		t.Fatalf("append revision: %v", err)
	}
	return res
}

// createStreamWithPlans sets up a stream with three explicitly-created
// plans and returns (streamID, normalID, tightID, redID).
func createStreamWithPlans(t *testing.T, st *Store, name string, n, nc, tc, rc int) (string, string, string, string) {
	t.Helper()
	ctx := context.Background()
	ids := [3]string{"plan-" + name + "-normal", "plan-" + name + "-normal-t", "plan-" + name + "-normal-r"}
	mk := func(id string, c int) {
		p := mkSingleRevision(name+"-"+id, n, c)
		p.ID = id
		if err := st.CreatePlan(ctx, p); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	mk(ids[0], nc)
	// Distinct names to avoid unique-name collisions across streams.
	pt := mkSingleRevision(name+"-tight", n, tc)
	pt.ID = "plan-" + name + "-tight"
	pr := mkSingleRevision(name+"-red", n, rc)
	pr.ID = "plan-" + name + "-red"
	if err := st.CreatePlan(ctx, pt); err != nil {
		t.Fatal(err)
	}
	if err := st.CreatePlan(ctx, pr); err != nil {
		t.Fatal(err)
	}
	sid := "stream-" + name
	err := st.CreateStream(ctx, StreamRecord{
		ID: sid, Name: name,
		NormalID:    ids[0],
		TightenedID: "plan-" + name + "-tight",
		ReducedID:   "plan-" + name + "-red",
	})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	return sid, ids[0], "plan-" + name + "-tight", "plan-" + name + "-red"
}

// TestRevisionHandComputed drives the required n=80 c=2 -> c=1 scenario
// through the STORE (not just the domain) and pins every batch's
// decision/score/revision before and after the cut.
func TestRevisionHandComputed(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	sid, nid, _, _ := createStreamWithPlans(t, st, "hc", 80, 2, 1, 3)

	cut := at(100)
	type row struct {
		id    string
		hour  int
		d     int
		pre   string // accepted/rejected under c=2
		post  string // accepted/rejected under c=1
		score int    // score AFTER the revision (final timeline)
		rev   int
	}
	rows := []row{
		{"h0", 10, 0, "accepted", "accepted", 3, 1},
		{"h1", 20, 1, "accepted", "accepted", 5, 1},
		{"h2", 30, 2, "accepted", "accepted", 6, 1},
		{"h3", 110, 0, "accepted", "accepted", 8, 2},
		{"h4", 120, 1, "accepted", "accepted", 9, 2},
		{"h5", 130, 2, "rejected", "rejected", 0, 2}, // flips accept->reject
	}
	for _, q := range rows {
		if _, err := st.AddBatch(ctx, sid, BatchRecord{
			ID: q.id, LotNo: q.id, At: at(q.hour), D1: q.d}); err != nil {
			t.Fatal(err)
		}
	}

	// Pre-revision sanity: h5 (d=2) accepted under c=2.
	pre, err := st.GetSnapshot(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]inspection.BatchOutcome{}
	for _, o := range pre.Batches {
		byID[o.BatchID] = o
	}
	if !byID["h5"].Accepted {
		t.Fatal("pre-revision h5 must be accepted under c=2")
	}

	// Append revision c=1 at the cut.
	res := appendRev(t, st, nid, 80, 1, cut)

	// Diff must name exactly the changed batch(es): h5 flips, and every
	// lot's revision (h0..h2 stay 1, h3..h5 become 2) — h3/h4 also move
	// to revision 2. Current severity/score: the reject h5 under normal
	// is a lone reject, state stays normal; score resets to 0.
	var sd *StreamDiff
	for i := range res.Streams {
		if res.Streams[i].StreamID == sid {
			sd = &res.Streams[i]
		}
	}
	if sd == nil {
		t.Fatalf("diff missing stream: %+v", res.Streams)
	}
	changedIDs := map[string]bool{}
	for _, b := range sd.Batches {
		changedIDs[b.BatchID] = true
	}
	if !changedIDs["h5"] {
		t.Fatalf("h5 must appear in diff: %+v", sd.Batches)
	}

	post, err := st.GetSnapshot(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	if len(post.Batches) != len(rows) {
		t.Fatalf("batch count %d", len(post.Batches))
	}
	for _, q := range rows {
		o, ok := findBatch(post.Batches, q.id)
		if !ok {
			t.Fatalf("missing %s", q.id)
		}
		if o.RevisionNo == nil || *o.RevisionNo != q.rev {
			t.Fatalf("%s revision=%v want %d", q.id, o.RevisionNo, q.rev)
		}
		wantDec := inspection.DecisionAccepted
		if q.post == "rejected" {
			wantDec = inspection.DecisionRejected
		}
		if o.Decision != wantDec {
			t.Fatalf("%s decision=%s want %s", q.id, o.Decision, q.post)
		}
		if o.Score != q.score {
			t.Fatalf("%s score=%d want %d", q.id, o.Score, q.score)
		}
	}
	// Independent replay of events+revisions matches.
	assertSnapshotsMatch(t, post, independentReplay(t, st, sid))
}

func findBatch(bs []inspection.BatchOutcome, id string) (inspection.BatchOutcome, bool) {
	for _, b := range bs {
		if b.BatchID == id {
			return b, true
		}
	}
	return inspection.BatchOutcome{}, false
}

// TestRevisionBackdatedInsertion: insert a revision in the middle of an
// existing history and confirm the affected lots re-resolve, including a
// severity trajectory change (two rejects become rejections).
func TestRevisionBackdatedInsertion(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	sid, nid, _, _ := createStreamWithPlans(t, st, "bd", 80, 5, 4, 6)
	// Lots under c=5: all accepted (d<=5).
	for i := 0; i < 4; i++ {
		if _, err := st.AddBatch(ctx, sid, BatchRecord{
			ID: "l" + itoaTest(i), LotNo: "L", At: at(10 + i), D1: 5}); err != nil {
			t.Fatal(err)
		}
	}
	// Insert a PAST revision c=1 at hour 10, covering all four lots:
	// d=5 rejects every one -> two rejects within first 5 -> tightened.
	res := appendRev(t, st, nid, 80, 1, at(10))
	sd := res.Streams[0]
	if !sd.Changed {
		t.Fatal("backdated revision must change the stream")
	}
	if sd.SeverityChanged == nil ||
		sd.SeverityChanged.To != string(inspection.SeverityTightened) {
		t.Fatalf("severity must become tightened: %+v", sd.SeverityChanged)
	}
	snap, err := st.GetSnapshot(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Current.Severity != inspection.SeverityTightened {
		t.Fatalf("got %s", snap.Current.Severity)
	}
	assertSnapshotsMatch(t, snap, independentReplay(t, st, sid))

	// List history: two revisions ordered.
	revs, err := st.ListRevisions(ctx, nid)
	if err != nil {
		t.Fatal(err)
	}
	if len(revs) != 2 {
		t.Fatalf("revisions=%d", len(revs))
	}
	if revs[0].No != 1 || revs[1].No != 2 {
		t.Fatalf("order %d,%d", revs[0].No, revs[1].No)
	}
}

// TestRevisionFutureDoesNotTouchPast: at the moment a future-dated
// revision is appended, NOTHING derived changes (its effective time has
// not arrived). Lots recorded afterwards resolve per their own inspection
// time — the same rule as every replay.
func TestRevisionFutureDoesNotTouchPast(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	sid, nid, _, _ := createStreamWithPlans(t, st, "fu", 80, 2, 1, 3)
	if _, err := st.AddBatch(ctx, sid, BatchRecord{ID: "old", LotNo: "O", At: at(1), D1: 2}); err != nil {
		t.Fatal(err)
	}
	future := at(100)
	res := appendRev(t, st, nid, 80, 1, future)
	if res.Streams[0].Changed {
		t.Fatalf("future revision must not change existing results: %+v", res.Streams[0])
	}
	// Still revision 1 / accepted immediately after the append.
	snap0, _ := st.GetSnapshot(ctx, sid)
	if o, _ := findBatch(snap0.Batches, "old"); !o.Accepted ||
		o.RevisionNo == nil || *o.RevisionNo != 1 {
		t.Fatalf("old lot must be untouched right after a future append: %+v", o)
	}
	// A lot after the future time is judged (per its inspection time)
	// under c=1; the full replay re-resolves every lot at its own time.
	snap, err := st.AddBatch(ctx, sid, BatchRecord{ID: "later", LotNo: "L", At: at(101), D1: 0})
	if err != nil {
		t.Fatal(err)
	}
	o, _ := findBatch(snap.Batches, "later")
	if o.RevisionNo == nil || *o.RevisionNo != 2 {
		t.Fatalf("later lot must use revision 2: %+v", o)
	}
	o, _ = findBatch(snap.Batches, "old")
	// old at hour 1 still resolves to revision 1 (revision 2 starts at
	// hour 100): its decision is unchanged.
	if o.RevisionNo == nil || *o.RevisionNo != 1 || !o.Accepted {
		t.Fatalf("old lot must keep revision 1 / accepted: %+v", o)
	}
}

// TestRevisionRejectedOnUnjudgeable: shrinking n below recorded d rejects
// the revision and names each offending batch; history is untouched.
func TestRevisionRejectedOnUnjudgeable(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	sid, nid, _, _ := createStreamWithPlans(t, st, "cf", 80, 2, 1, 3)
	// One accepted normal lot with d=2 (c=2 accepts it; n=80 fits).
	if _, err := st.AddBatch(ctx, sid, BatchRecord{ID: "b1", LotNo: "B", At: at(1), D1: 2}); err != nil {
		t.Fatal(err)
	}
	// Shrink the normal sample size BELOW the recorded defect count:
	// n=1 cannot judge a lot with d=2 (c=0 is the only valid c<n).
	bad := mkSingleRevision("", 1, 0)
	_, err := st.AppendRevision(ctx, nid, bad, nil, ptrTime(at(0)))
	var rc ErrRevisionConflicts
	if !errors.As(err, &rc) {
		t.Fatalf("want ErrRevisionConflicts, got %v", err)
	}
	if len(rc.Conflicts) != 1 || rc.Conflicts[0].BatchID != "b1" {
		t.Fatalf("conflicts=%+v", rc.Conflicts)
	}
	if rc.Conflicts[0].RevisionNo != 2 {
		t.Fatalf("conflict must cite the proposed revision 2: %+v", rc.Conflicts[0])
	}
	// No revision was committed.
	revs, _ := st.ListRevisions(ctx, nid)
	if len(revs) != 1 {
		t.Fatalf("revision must not exist after rejection, got %d", len(revs))
	}
	// Stored results untouched: b1 stays the originally accepted lot.
	snap, _ := st.GetSnapshot(ctx, sid)
	if o, _ := findBatch(snap.Batches, "b1"); !o.Accepted {
		t.Fatal("b1 must remain the originally stored accepted result")
	}
}

// TestLegacyUpdateAppendsImmediateRevision: the old UpdatePlan entry
// point appends a revision effective now and never rewrites earlier
// history.
func TestLegacyUpdateAppendsImmediateRevision(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	sid, nid, _, _ := createStreamWithPlans(t, st, "leg", 80, 2, 1, 3)
	if _, err := st.AddBatch(ctx, sid, BatchRecord{ID: "old", LotNo: "O", At: at(0), D1: 2}); err != nil {
		t.Fatal(err)
	}
	p, err := st.GetPlan(ctx, nid)
	if err != nil {
		t.Fatal(err)
	}
	p.AcceptNumber = 1
	if err := st.UpdatePlan(ctx, p); err != nil {
		t.Fatal(err)
	}
	revs, _ := st.ListRevisions(ctx, nid)
	if len(revs) != 2 {
		t.Fatalf("revisions=%d", len(revs))
	}
	// Immediate revision means the old lot (well in the past) still uses
	// revision 1 and stays accepted.
	snap, _ := st.GetSnapshot(ctx, sid)
	o, _ := findBatch(snap.Batches, "old")
	if o.RevisionNo == nil || *o.RevisionNo != 1 || !o.Accepted {
		t.Fatalf("old lot must be unchanged: rev=%v accepted=%v", o.RevisionNo, o.Accepted)
	}
}

func ptrTime(t time.Time) *time.Time { return &t }

// TestBackdatedBatchUsesItsEraRevision: once a revision shrank the sample
// size for later times, back-filling a lot whose inspection time falls in
// the EARLIER period must be judged under that period's (larger) revision
// and must be accepted — validation is per inspection time, not against
// the current revision.
func TestBackdatedBatchUsesItsEraRevision(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	sid, nid, _, _ := createStreamWithPlans(t, st, "era", 80, 2, 1, 3)
	// New revision n=40,c=1 effective at hour 50.
	appendRev(t, st, nid, 40, 1, at(50))

	// Back-dated lot at hour 10 with d=40: fits the OLD revision (n=80,
	// where d=40 > c=2 is a rejection, but it IS judgeable). It must be
	// recorded using revision 1.
	snap, err := st.AddBatch(ctx, sid, BatchRecord{ID: "old", LotNo: "O", At: at(10), D1: 40})
	if err != nil {
		t.Fatalf("back-dated lot judged under its era must validate: %v", err)
	}
	o, _ := findBatch(snap.Batches, "old")
	if o.RevisionNo == nil || *o.RevisionNo != 1 || o.Accepted {
		t.Fatalf("old lot: rev=%v accepted=%v (want rev1, rejected under c=2)", o.RevisionNo, o.Accepted)
	}

	// A count above the new sample size at hour 60 cannot be judged
	// under the new n=40 and must be rejected at entry with a field error.
	if _, err := st.AddBatch(ctx, sid, BatchRecord{ID: "new", LotNo: "N", At: at(60), D1: 41}); err == nil {
		t.Fatal("lot exceeding the in-force revision's sample size must be rejected")
	}
	assertSnapshotsMatch(t, mustSnap(t, st, sid), independentReplay(t, st, sid))
}

func mustSnap(t *testing.T, st *Store, sid string) inspection.Snapshot {
	t.Helper()
	snap, err := st.GetSnapshot(context.Background(), sid)
	if err != nil {
		t.Fatal(err)
	}
	return snap
}
