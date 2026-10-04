package store

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"sampling-svc/internal/inspection"
	"sampling-svc/internal/plan"
)

// T is the hand-computed change instant: normal plan switches from
// n=80,c=2 to n=80,c=1 at 2026-03-15 00:00:00 UTC.
var handChange = time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)

// createRevStream creates one stream with a normal n=80 plan plus
// tightened/reduced companions, returning the stream and normal plan ids.
func createRevStream(t *testing.T, st *Store, name string) (streamID, normalID string) {
	t.Helper()
	ctx := context.Background()
	mk := func(suffix string, n, c int) string {
		p := &plan.Plan{
			ID: "p-" + name + "-" + suffix, Name: name + "-" + suffix,
			Kind: plan.KindSingle, Distribution: plan.DistBinomial,
			SampleSize: n, AcceptNumber: c,
		}
		if err := st.CreatePlan(ctx, p); err != nil {
			t.Fatalf("create %s: %v", suffix, err)
		}
		return p.ID
	}
	nid := mk("n", 80, 2)
	mk("t", 80, 1) // tightened c=1
	mk("r", 80, 3) // reduced c=3
	sid := "s-" + name
	if err := st.CreateStream(ctx, StreamRecord{
		ID: sid, Name: name,
		NormalID:    nid,
		TightenedID: "p-" + name + "-t",
		ReducedID:   "p-" + name + "-r",
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	return sid, nid
}

func appendRevAt(t *testing.T, st *Store, planID string, n, c int, at time.Time) RevisionResult {
	t.Helper()
	cur, err := st.GetPlan(context.Background(), planID)
	if err != nil {
		t.Fatal(err)
	}
	N := (*int)(nil)
	res, err := st.AppendRevision(context.Background(), planID, plan.RevisionRequest{
		Request: plan.Request{
			Name: cur.Name, Kind: "single",
			Single: &plan.SingleParam{N: N, SampleSize: n, AcceptNumber: c},
		},
	}, at)
	if err != nil {
		t.Fatalf("append revision c=%d at %s: %v", c, at, err)
	}
	return res
}

// TestHandComputedC2ToC1 is the explicit hand-checkable scenario:
// normal n=80,c=2 -> c=1 at a fixed instant, with lots on both sides.
//
// Pre-change  (c=2): d=0 +3, d=1 +2, d=2 +1 (accept).
// Post-change (c=1): d=0 +2, d=1 +1, d=2 reject (score reset).
//
// Expected per-batch decision / score:
//
//	hour -4 d=0 accept score 3  (rev 1)
//	hour -3 d=1 accept score 5  (rev 1)
//	hour -2 d=2 accept score 6  (rev 1)  <- accepted under c=2
//	hour -1 d=0 accept score 9  (rev 1)
//	hour +1 d=0 accept score 11 (rev 2)
//	hour +2 d=1 accept score 12 (rev 2)
//	hour +3 d=2 reject score 0  (rev 2)  <- rejected under c=1
//	hour +4 d=0 accept score 2  (rev 2)
func TestHandComputedC2ToC1(t *testing.T) {
	st := newTestStore(t)
	sid, nid := createRevStream(t, st, "hand")
	ctx := context.Background()

	type q struct {
		id string
		at time.Time
		d  int
	}
	pre := []q{
		{"a", handChange.Add(-4 * time.Hour), 0},
		{"b", handChange.Add(-3 * time.Hour), 1},
		{"c", handChange.Add(-2 * time.Hour), 2},
		{"d", handChange.Add(-1 * time.Hour), 0},
	}
	post := []q{
		{"e", handChange.Add(1 * time.Hour), 0},
		{"f", handChange.Add(2 * time.Hour), 1},
		{"g", handChange.Add(3 * time.Hour), 2},
		{"h", handChange.Add(4 * time.Hour), 0},
	}
	for _, x := range pre {
		if _, err := st.AddBatch(ctx, sid, BatchRecord{
			ID: x.id, LotNo: x.id, At: x.at, D1: x.d}); err != nil {
			t.Fatal(err)
		}
	}

	// Record the post-change lots too (they exist but are still judged by
	// rev 1 until the revision lands; this mirrors the real-world
	// "revised tomorrow" situation).
	for _, x := range post {
		if _, err := st.AddBatch(ctx, sid, BatchRecord{
			ID: x.id, LotNo: x.id, At: x.at, D1: x.d}); err != nil {
			t.Fatal(err)
		}
	}

	// Before the revision all eight are under c=2: g (d=2) accepted.
	snap, err := st.GetSnapshot(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	if g := findBatch(t, snap, "g"); !g.Accepted || g.PlanRevision != 1 {
		t.Fatalf("pre-revision g must be accepted under rev1, got %+v", g)
	}

	// Append c=1 effective exactly at T.
	res := appendRevAt(t, st, nid, 80, 1, handChange)

	want := map[string]struct {
		accepted bool
		score    int
		rev      int
	}{
		"a": {true, 3, 1},
		"b": {true, 5, 1},
		"c": {true, 6, 1},
		"d": {true, 9, 1},
		"e": {true, 11, 2},
		"f": {true, 12, 2},
		"g": {false, 0, 2},
		"h": {true, 2, 2},
	}
	snap, err = st.GetSnapshot(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range snap.Batches {
		w, ok := want[b.BatchID]
		if !ok {
			t.Fatalf("unexpected batch %s", b.BatchID)
		}
		if b.Accepted != w.accepted || b.Score != w.score || b.PlanRevision != w.rev {
			t.Fatalf("batch %s: accepted=%v score=%d rev=%d want accepted=%v score=%d rev=%d",
				b.BatchID, b.Accepted, b.Score, b.PlanRevision,
				w.accepted, w.score, w.rev)
		}
	}

	// The returned diff must describe exactly the changed batches.
	if len(res.Streams) != 1 {
		t.Fatalf("want one affected stream, got %d", len(res.Streams))
	}
	changed := map[string]BatchDiff{}
	for _, d := range res.Streams[0].Batches {
		changed[d.BatchID] = d
	}
	// c (d=2) stays accepted under both, but its c-based score changes
	// downstream; g flips acceptance.
	if _, ok := changed["g"]; !ok {
		t.Fatal("g must appear in the diff (accepted -> rejected)")
	}
	if changed["g"].Decision == nil ||
		changed["g"].Decision.From != "accepted" ||
		changed["g"].Decision.To != "rejected" {
		t.Fatalf("g decision diff wrong: %+v", changed["g"])
	}
	if changed["g"].Plan == nil || changed["g"].Plan.FromRevision != 1 ||
		changed["g"].Plan.ToRevision != 2 {
		t.Fatalf("g plan diff wrong: %+v", changed["g"].Plan)
	}

	// A subsequent full independent replay must be field-identical.
	assertSnapshotsMatch(t, snap, independentReplay(t, st, sid))
}

// TestRevisionDiffNoChanges: a revision identical to the current content
// reports every bound stream with an empty batch list.
func TestRevisionDiffNoChanges(t *testing.T) {
	st := newTestStore(t)
	sid, nid := createRevStream(t, st, "nochange")
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := st.AddBatch(ctx, sid, BatchRecord{
			ID: fmt.Sprintf("b%d", i), LotNo: "L", At: at(i), D1: 0}); err != nil {
			t.Fatal(err)
		}
	}
	res := appendRevAt(t, st, nid, 80, 2, handChange)
	if len(res.Streams) != 1 {
		t.Fatalf("bound stream must be reported, got %d", len(res.Streams))
	}
	if len(res.Streams[0].Batches) != 0 {
		t.Fatalf("identical revision must report no changed batches, got %+v",
			res.Streams[0].Batches)
	}
}

// TestRevisionConflictSampleShrunk: shrinking n so recorded defect
// counts exceed the new sample size rejects the revision and lists each
// offending batch.
func TestRevisionConflictSampleShrunk(t *testing.T) {
	st := newTestStore(t)
	sid, nid := createRevStream(t, st, "conflict")
	ctx := context.Background()
	if _, err := st.AddBatch(ctx, sid, BatchRecord{
		ID: "ok", LotNo: "OK", At: at(1), D1: 5}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AddBatch(ctx, sid, BatchRecord{
		ID: "big", LotNo: "BIG", At: at(2), D1: 50}); err != nil {
		t.Fatal(err)
	}
	// New revision shrinks the normal sample to n=20, c=0.
	_, err := st.AppendRevision(ctx, nid, plan.RevisionRequest{
		Request: plan.Request{
			Name: "conflict-n", Kind: "single",
			Single: &plan.SingleParam{SampleSize: 20, AcceptNumber: 0},
		},
	}, at(0))
	var ce *RevisionConflictError
	if !asConflict(err, &ce) {
		t.Fatalf("want RevisionConflictError, got %v", err)
	}
	if len(ce.Conflicts) != 1 || ce.Conflicts[0].BatchID != "big" {
		t.Fatalf("conflict must name only 'big', got %+v", ce.Conflicts)
	}
	if ce.Conflicts[0].Field != "d1" {
		t.Fatalf("conflict field=%s want d1", ce.Conflicts[0].Field)
	}
	// Nothing was written: still rev 1 and "big" is accepted.
	revs, err := st.ListRevisions(ctx, nid)
	if err != nil || len(revs) != 1 {
		t.Fatalf("revision must be rolled back: %d revs err=%v", len(revs), err)
	}
	snap, _ := st.GetSnapshot(ctx, sid)
	big := findBatch(t, snap, "big")
	if big.Accepted {
		t.Fatal("unexpected: d=50 accepted under c=2")
	}
	if big.PlanRevision != 1 {
		t.Fatalf("rejected revision must leave prior outcomes untouched, rev=%d", big.PlanRevision)
	}
}

// TestBackdatedRevisionInsertsBetween: append rev2 effective in the
// future, then rev3 backdated before it; resolution and current view stay
// consistent.
func TestBackdatedRevisionInsertsBetween(t *testing.T) {
	st := newTestStore(t)
	sid, nid := createRevStream(t, st, "between")
	ctx := context.Background()

	if _, err := st.AddBatch(ctx, sid, BatchRecord{
		ID: "old", LotNo: "O", At: at(10), D1: 2}); err != nil { // accept c=2
		t.Fatal(err)
	}
	if _, err := st.AddBatch(ctx, sid, BatchRecord{
		ID: "new", LotNo: "N", At: at(30), D1: 2}); err != nil {
		t.Fatal(err)
	}

	// rev2 at hour 20: c=1 (affects "new" at hour 30).
	appendRevAt(t, st, nid, 80, 1, at(20))
	snap, _ := st.GetSnapshot(ctx, sid)
	if b := findBatch(t, snap, "new"); b.Accepted {
		t.Fatal("new (d=2) must reject under c=1")
	}
	if b := findBatch(t, snap, "old"); !b.Accepted {
		t.Fatal("old (d=2) must still accept under rev1 c=2")
	}
	// rev3 backdated to hour 5 (between epoch and hour 20): c=1, so
	// "old" flips too; d=2 still fits n=80 (no hard conflict).
	appendRevAt(t, st, nid, 80, 1, at(5))
	snap, _ = st.GetSnapshot(ctx, sid)
	old := findBatch(t, snap, "old")
	new := findBatch(t, snap, "new")
	if old.PlanRevision != 3 || old.Accepted {
		t.Fatalf("old must use rev3 and reject: %+v", old)
	}
	if new.PlanRevision != 2 || new.Accepted {
		t.Fatalf("new must still use rev2 and reject: %+v", new)
	}
	// Current plan view = latest effective = rev2 (hour 40), c=1.
	cur, err := st.GetPlan(ctx, nid)
	if err != nil || cur.AcceptNumber != 1 {
		t.Fatalf("current plan must be rev2 c=1, got %+v err=%v", cur, err)
	}
	// Revision numbers assigned in append order.
	revs, _ := st.ListRevisions(ctx, nid)
	if len(revs) != 3 || revs[0].Number != 1 || revs[1].Number != 3 || revs[2].Number != 2 {
		t.Fatalf("revision ordering wrong: %+v", revs)
	}
}

// TestFutureRevisionNotCurrent: a revision effective in the future must
// not change the "current plan" view nor any recorded lot until its
// effective time.
func TestFutureRevisionNotCurrent(t *testing.T) {
	st := newTestStore(t)
	sid, nid := createRevStream(t, st, "future")
	ctx := context.Background()
	appendRevAt(t, st, nid, 80, 1, time.Now().UTC().Add(48*time.Hour))

	// Current plan view still reports c=2.
	cur, err := st.GetPlan(ctx, nid)
	if err != nil || cur.AcceptNumber != 2 {
		t.Fatalf("future revision must not change current plan: c=%d err=%v",
			cur.AcceptNumber, err)
	}
	// A lot recorded now is still judged by rev 1.
	snap, err := st.AddBatch(ctx, sid, BatchRecord{
		ID: "now", LotNo: "NOW", At: time.Now().UTC(), D1: 2})
	if err != nil {
		t.Fatal(err)
	}
	if b := snap.Batches[len(snap.Batches)-1]; !b.Accepted || b.PlanRevision != 1 {
		t.Fatalf("current lot must be accepted under rev1: %+v", b)
	}
	// A lot whose inspection time is beyond the future revision uses rev2.
	future := time.Now().UTC().Add(72 * time.Hour)
	snap, err = st.AddBatch(ctx, sid, BatchRecord{
		ID: "later", LotNo: "LATER", At: future, D1: 2})
	if err != nil {
		t.Fatal(err)
	}
	if b := snap.Batches[len(snap.Batches)-1]; b.Accepted || b.PlanRevision != 2 {
		t.Fatalf("future lot must be rejected under rev2: %+v", b)
	}
}

// TestLegacyUpdateAppendsImmediateRevision: the old PUT-style modify with
// no effective time appends a revision effective now and never rewrites
// earlier history.
func TestLegacyUpdateAppendsImmediateRevision(t *testing.T) {
	st := newTestStore(t)
	sid, nid := createRevStream(t, st, "legacy")
	ctx := context.Background()
	if _, err := st.AddBatch(ctx, sid, BatchRecord{
		ID: "hist", LotNo: "H", At: at(-1000), D1: 2}); err != nil {
		t.Fatal(err)
	}
	p := &plan.Plan{
		ID: nid, Name: "legacy-n", Kind: plan.KindSingle,
		Distribution: plan.DistBinomial, SampleSize: 80, AcceptNumber: 1,
	}
	if err := st.UpdatePlan(ctx, p); err != nil {
		t.Fatal(err)
	}
	revs, _ := st.ListRevisions(ctx, nid)
	if len(revs) != 2 {
		t.Fatalf("legacy update must append a revision, got %d", len(revs))
	}
	// The historical lot at hour -1000 stays judged by rev1 c=2.
	snap, _ := st.GetSnapshot(ctx, sid)
	if b := findBatch(t, snap, "hist"); !b.Accepted || b.PlanRevision != 1 {
		t.Fatalf("legacy update rewrote history: %+v", b)
	}
}

func findBatch(t *testing.T, snap inspection.Snapshot, id string) inspection.BatchOutcome {
	t.Helper()
	for _, b := range snap.Batches {
		if b.BatchID == id {
			return b
		}
	}
	t.Fatalf("batch %s not found", id)
	return inspection.BatchOutcome{}
}

func asConflict(err error, target **RevisionConflictError) bool {
	for err != nil {
		if e, ok := err.(*RevisionConflictError); ok {
			*target = e
			return true
		}
		if x, ok := err.(interface{ Unwrap() error }); ok {
			err = x.Unwrap()
			continue
		}
		break
	}
	return false
}

// ---------------------------------------------------------------------------
// Random interleaving: append revisions (future, backdated, between),
// backfill, delete and edit batches in shuffled order; after every step
// the stored derived state must equal a from-scratch replay of all events
// with all revisions, and the revision diff must match the actual
// before/after comparison.
// ---------------------------------------------------------------------------

func TestRandomRevisionInterleaving(t *testing.T) {
	st := newTestStore(t)
	sid, nid := createRevStream(t, st, "rand")
	ctx := context.Background()
	rng := rand.New(rand.NewSource(20261004))

	type batch struct {
		id   string
		at   time.Time
		d    int
		dead bool
	}
	var batches []*batch
	batchByID := map[string]*batch{}
	newBatchID := func() string {
		return fmt.Sprintf("B%04d", len(batchByID)+1)
	}
	usedHour := map[int]bool{}
	uniqueHour := func() int {
		for {
			h := rng.Intn(2000) - 1000
			if !usedHour[h] {
				usedHour[h] = true
				return h
			}
		}
	}

	// revision content: the accept number currently in force per hour
	// interval is derived from the full revision list; use c in {2,1,0}
	// (all share n=80 so no count conflicts; counts kept <=2).
	type rev struct {
		no int
		at time.Time
		c  int
	}
	var revs []rev
	nextRev := 2 // rev1 already exists (c=2 at epoch)
	revs = append(revs, rev{1, plan.Epoch, 2})

	snapshotBefore := func() map[string]inspection.BatchOutcome {
		snap, err := st.GetSnapshot(ctx, sid)
		if err != nil {
			t.Fatal(err)
		}
		m := map[string]inspection.BatchOutcome{}
		for _, b := range snap.Batches {
			m[b.BatchID] = b
		}
		return m
	}
	outcomesEqual := func(a, b inspection.BatchOutcome) bool {
		return a.BatchID == b.BatchID && a.Severity == b.Severity &&
			a.PlanID == b.PlanID && a.PlanRevision == b.PlanRevision &&
			a.Decision == b.Decision && a.Accepted == b.Accepted &&
			a.Score == b.Score && a.D1 == b.D1 && a.At.Equal(b.At)
	}
	assertMatchesReplay := func() {
		got, err := st.GetSnapshot(ctx, sid)
		if err != nil {
			t.Fatal(err)
		}
		assertSnapshotsMatch(t, got, independentReplay(t, st, sid))
	}

	for step := 0; step < 120; step++ {
		switch rng.Intn(4) {
		case 0, 1:
			// Add/backfill a batch with a small defect count (0..2) that
			// is judgeable under every c in {0,1,2} with n=80.
			id := newBatchID()
			h := uniqueHour()
			d := rng.Intn(3)
			b := &batch{id: id, at: at(h), d: d}
			batches = append(batches, b)
			batchByID[id] = b
			before := snapshotBefore()
			if _, err := st.AddBatch(ctx, sid, BatchRecord{
				ID: id, LotNo: id, At: b.at, D1: d}); err != nil {
				t.Fatalf("step %d add %s: %v", step, id, err)
			}
			assertMatchesReplay()
			_ = before

		case 2:
			// Delete a random live batch.
			var live []*batch
			for _, b := range batches {
				if !b.dead {
					live = append(live, b)
				}
			}
			if len(live) == 0 {
				continue
			}
			b := live[rng.Intn(len(live))]
			if _, err := st.DeleteBatch(ctx, sid, b.id); err != nil {
				t.Fatalf("step %d delete: %v", step, err)
			}
			b.dead = true
			delete(usedHour, int(b.at.Sub(at(0)).Hours()))
			assertMatchesReplay()

		default:
			// Append a revision at a random (possibly backdated) hour not
			// already used by a revision; c in {2,1}.
			var h int
			for tries := 0; tries < 50; tries++ {
				h = rng.Intn(2000) - 1000
				collision := false
				for _, r := range revs {
					if int(r.at.Sub(at(0)).Hours()) == h {
						collision = true
					}
				}
				if !collision {
					break
				}
			}
			c := []int{2, 1}[rng.Intn(2)]
			before := snapshotBefore()
			res, err := st.AppendRevision(ctx, nid, plan.RevisionRequest{
				Request: plan.Request{
					Name: "rand-n", Kind: "single",
					Single: &plan.SingleParam{SampleSize: 80, AcceptNumber: c},
				},
			}, at(h))
			if err != nil {
				t.Fatalf("step %d revision: %v", step, err)
			}
			revs = append(revs, rev{nextRev, at(h), c})
			nextRev++

			// The reported diff must equal the actual before/after map.
			afterSnap, err := st.GetSnapshot(ctx, sid)
			if err != nil {
				t.Fatal(err)
			}
			after := map[string]inspection.BatchOutcome{}
			for _, b := range afterSnap.Batches {
				after[b.BatchID] = b
			}
			gotChanges := map[string]bool{}
			for _, sd := range res.Streams {
				if sd.StreamID != sid {
					t.Fatalf("unexpected stream in diff: %s", sd.StreamID)
				}
				for _, bd := range sd.Batches {
					gotChanges[bd.BatchID] = true
					old, ok := before[bd.BatchID]
					if !ok {
						t.Fatalf("diff mentions new batch %s", bd.BatchID)
					}
					nw := after[bd.BatchID]
					if bd.Severity != nil &&
						(bd.Severity.From != string(old.Severity) ||
							bd.Severity.To != string(nw.Severity)) {
						t.Fatalf("severity diff mismatch for %s", bd.BatchID)
					}
					if bd.Decision != nil &&
						(bd.Decision.From != string(old.Decision) ||
							bd.Decision.To != string(nw.Decision)) {
						t.Fatalf("decision diff mismatch for %s", bd.BatchID)
					}
					if bd.Score != nil &&
						(bd.Score.From != old.Score || bd.Score.To != nw.Score) {
						t.Fatalf("score diff mismatch for %s", bd.BatchID)
					}
					if bd.Plan != nil {
						if bd.Plan.FromRevision != old.PlanRevision ||
							bd.Plan.ToRevision != nw.PlanRevision {
							t.Fatalf("plan-revision diff mismatch for %s", bd.BatchID)
						}
					}
				}
			}
			// Every actual change must be reported.
			for id, nw := range after {
				old, ok := before[id]
				if !ok {
					continue
				}
				if !outcomesEqual(old, nw) && !gotChanges[id] {
					t.Fatalf("batch %s changed but missing from diff: %+v -> %+v",
						id, old, nw)
				}
			}
			assertMatchesReplay()
		}
	}

	// Final ordering of revisions by (effective_at, no) is consistent.
	got, err := st.ListRevisions(ctx, nid)
	if err != nil {
		t.Fatal(err)
	}
	keys := make([][2]int64, 0, len(got))
	for _, r := range got {
		keys = append(keys, [2]int64{r.EffectiveAt.Unix(), int64(r.Number)})
	}
	if !sort.SliceIsSorted(keys, func(i, j int) bool {
		return keys[i][0] < keys[j][0] ||
			(keys[i][0] == keys[j][0] && keys[i][1] < keys[j][1])
	}) {
		t.Fatal("revisions not returned in effective order")
	}
}
