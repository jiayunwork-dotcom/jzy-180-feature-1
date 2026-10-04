package store

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"sampling-svc/internal/plan"
)

// Concurrent revision appends and concurrent batch recording against the
// same plan/stream must lose neither revisions nor batches, must never
// double-count the switching score, and every batch must end up stamped
// with the revision effective at its own inspection time.
func TestConcurrentRevisionsAndBatches(t *testing.T) {
	st := newTestStore(t)
	sid, nid := createRevStream(t, st, "concrev")
	ctx := context.Background()

	// All revisions use c in {2,1}; batches use d in {0,1} so they stay
	// judgeable under every revision (n=80 always).
	const batchWriters = 8
	const perWriter = 40
	const revWriters = 6

	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, batchWriters*perWriter+revWriters)

	// Revision effective hours are chosen so concurrent appenders cannot
	// collide on (plan, effective_at).
	revHours := make(chan int, revWriters)
	for i := 0; i < revWriters; i++ {
		revHours <- (i + 1) * 7 // 7,14,21,28,35,42
	}

	for w := 0; w < revWriters; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			h := <-revHours
			c := 1
			if h%2 == 0 {
				c = 2
			}
			_, err := st.AppendRevision(ctx, nid, plan.RevisionRequest{
				Request: plan.Request{
					Name: "concrev-n", Kind: "single",
					Single: &plan.SingleParam{SampleSize: 80, AcceptNumber: c},
				},
			}, at(h))
			if err != nil {
				errs <- fmt.Errorf("revision h=%d: %w", h, err)
			}
		}()
	}

	for w := 0; w < batchWriters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < perWriter; i++ {
				id := fmt.Sprintf("w%02d-%03d", w, i)
				hour := 50 + (w*perWriter+i*7)%2000
				_, err := st.AddBatch(ctx, sid, BatchRecord{
					ID: id, LotNo: "L", At: at(hour),
					D1: (w + i) % 2, // 0 or 1
				})
				if err != nil {
					errs <- fmt.Errorf("batch %s: %w", id, err)
					return
				}
			}
		}(w)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// Revisions: exactly the initial + revWriters, numbers gap-free.
	revs, err := st.ListRevisions(ctx, nid)
	if err != nil {
		t.Fatal(err)
	}
	if len(revs) != 1+revWriters {
		t.Fatalf("lost revisions: %d want %d", len(revs), 1+revWriters)
	}
	nos := make([]int, 0, len(revs))
	for _, r := range revs {
		nos = append(nos, r.Number)
	}
	sort.Ints(nos)
	for i, n := range nos {
		if n != i+1 {
			t.Fatalf("revision numbers not gap-free: %v", nos)
		}
	}

	// Batches: none lost, none duplicated; results count matches.
	wantBatches := batchWriters * perWriter
	var nEvents, nResults int
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM stream_events WHERE stream_id=$1 AND kind='batch'`, sid).
		Scan(&nEvents); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM batch_results WHERE stream_id=$1`, sid).
		Scan(&nResults); err != nil {
		t.Fatal(err)
	}
	if nEvents != wantBatches || nResults != wantBatches {
		t.Fatalf("lost/dup batches events=%d results=%d want=%d",
			nEvents, nResults, wantBatches)
	}

	// Every stamped revision must be the one effective at that batch's
	// inspection time (tie -> higher number).
	snap, err := st.GetSnapshot(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range snap.Batches {
		wantRev := effectiveRevision(revs, b.At)
		if b.PlanRevision != wantRev {
			t.Fatalf("batch %s at %s stamped rev %d, want %d",
				b.BatchID, b.At, b.PlanRevision, wantRev)
		}
	}

	// Whole stream equals an independent from-scratch replay.
	assertSnapshotsMatch(t, snap, independentReplay(t, st, sid))
}

// effectiveRevision re-implements the resolution rule independently.
func effectiveRevision(revs []plan.Revision, t time.Time) int {
	eff := map[int]time.Time{}
	for _, r := range revs {
		eff[r.Number] = r.EffectiveAt
	}
	best := 0
	for _, r := range revs {
		if r.EffectiveAt.After(t) {
			continue
		}
		if best == 0 || r.EffectiveAt.After(eff[best]) ||
			(r.EffectiveAt.Equal(eff[best]) && r.Number > best) {
			best = r.Number
		}
	}
	return best
}

// Concurrent appends of two revisions at the SAME effective time: exactly
// one wins, the other gets a conflict; both still yield gap-free numbers.
func TestConcurrentRevisionsSameEffectiveAt(t *testing.T) {
	st := newTestStore(t)
	_, nid := createRevStream(t, st, "sameeff")
	ctx := context.Background()

	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := st.AppendRevision(ctx, nid, plan.RevisionRequest{
				Request: plan.Request{
					Name: "sameeff-n", Kind: "single",
					Single: &plan.SingleParam{SampleSize: 80, AcceptNumber: 1},
				},
			}, at(99))
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	var ok, conflict int
	for err := range results {
		switch {
		case err == nil:
			ok++
		case isConflictErr(err):
			conflict++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if ok != 1 || conflict != 1 {
		t.Fatalf("want 1 ok + 1 conflict, got ok=%d conflict=%d", ok, conflict)
	}
	revs, _ := st.ListRevisions(ctx, nid)
	if len(revs) != 2 {
		t.Fatalf("want 2 revisions, got %d", len(revs))
	}
}

func isConflictErr(err error) bool {
	var ec ErrConflict
	if asErrConflict(err, &ec) {
		return ec.Msg != ""
	}
	return false
}

func asErrConflict(err error, target *ErrConflict) bool {
	for err != nil {
		if e, ok := err.(ErrConflict); ok {
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
