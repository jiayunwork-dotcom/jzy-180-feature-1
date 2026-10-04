package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// TestConcurrentRevisionsAndBatches hammers one plan with concurrent
// revision appends AND its bound stream with concurrent batch inserts.
// Guarantees checked:
//
//   - no revision is lost: revision numbers are gap-free 1..N;
//   - no batch is lost and no score is double-counted (result rows match
//     event rows; final state equals an independent replay);
//   - every judged batch names the revision that was effective at its
//     inspection time;
//   - no deadlock (the test completing is itself the assertion: plan lock
//     is always taken before stream locks).
func TestConcurrentRevisionsAndBatches(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	sid, nid, _, _ := createStreamWithPlans(t, st, "cc", 80, 2, 1, 3)

	const batchWriters = 12
	const batchesPer = 40
	const revWriters = 4
	const revsPer = 8

	var wg sync.WaitGroup
	errs := make(chan error, batchWriters*batchesPer+revWriters*revsPer)
	start := make(chan struct{})

	// Batch writers spread inspection times over a fixed window so each
	// batch's effective revision depends on how many revisions had an
	// effective time before it. d=0 is accepted under every c we append.
	for w := 0; w < batchWriters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < batchesPer; i++ {
				id := fmt.Sprintf("bw%02d-%03d", w, i)
				hour := 100 + (w*batchesPer+i*7)%5000
				_, err := st.AddBatch(ctx, sid, BatchRecord{
					ID: id, LotNo: id, At: at(hour), D1: 0})
				if err != nil {
					errs <- fmt.Errorf("add %s: %w", id, err)
					return
				}
			}
		}(w)
	}
	// Revision writers append revisions at back-dated effective times so
	// they land inside the batch window. All keep n=80 and vary c in the
	// d=0-always-accept range, so appends are never conflict-rejected.
	for w := 0; w < revWriters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < revsPer; i++ {
				hour := 100 + (w*revsPer+i*131)%5000
				p := mkSingleRevision("", 80, 2)
				_, err := st.AppendRevision(ctx, nid, p, nil, ptrTime(at(hour)))
				if err != nil {
					errs <- fmt.Errorf("rev w%d i%d: %w", w, i, err)
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

	// Gap-free revisions 1..N, unique effective/number pairs.
	var maxNo, cntNo int
	if err := st.pool.QueryRow(ctx,
		`SELECT max(revision_no), count(*) FROM plan_revisions WHERE plan_id=$1`, nid).
		Scan(&maxNo, &cntNo); err != nil {
		t.Fatal(err)
	}
	wantRevs := 1 + revWriters*revsPer
	if maxNo != wantRevs || cntNo != wantRevs {
		t.Fatalf("revisions max=%d count=%d want %d", maxNo, cntNo, wantRevs)
	}
	var gaps int
	if err := st.pool.QueryRow(ctx, `
SELECT count(*) FROM generate_series(1,$2) g
WHERE NOT EXISTS (
    SELECT 1 FROM plan_revisions WHERE plan_id=$1 AND revision_no=g)`,
		nid, wantRevs).Scan(&gaps); err != nil {
		t.Fatal(err)
	}
	if gaps != 0 {
		t.Fatalf("revision number gaps=%d", gaps)
	}

	// No lost batches.
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
	if nEvents != batchWriters*batchesPer || nResults != nEvents {
		t.Fatalf("events=%d results=%d want=%d", nEvents, nResults, batchWriters*batchesPer)
	}

	// Final state equals independent replay, which resolves each batch to
	// the revision effective at its inspection time.
	snap, err := st.GetSnapshot(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	assertSnapshotsMatch(t, snap, independentReplay(t, st, sid))

	// Direct assertion: each judged batch's stored revision number is
	// exactly the latest revision effective at its inspection time
	// (highest revision number on ties).
	rows, err := st.pool.Query(ctx, `
SELECT br.batch_id, br.revision_no,
       (SELECT r.revision_no FROM plan_revisions r
         WHERE r.plan_id = br.plan_id AND r.effective_at <= br.at
         ORDER BY r.effective_at DESC, r.revision_no DESC
         LIMIT 1) AS want_no
FROM batch_results br
WHERE br.stream_id=$1 AND br.decision<>'not_inspected'`, sid)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var bid string
		var got, want *int
		if err := rows.Scan(&bid, &got, &want); err != nil {
			t.Fatal(err)
		}
		if !revEq(got, want) {
			t.Fatalf("batch %s judged under revision %v, want %v", bid, got, want)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

// TestConcurrentRevisionsOnePlan serializes appends to the same plan:
// numbers are gap-free and the final current mirror matches the latest
// effective revision.
func TestConcurrentRevisionsOnePlan(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	_, nid, _, _ := createStreamWithPlans(t, st, "sr", 80, 2, 1, 3)
	const n = 20
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			p := mkSingleRevision("", 80, (i%3)+1)
			if _, err := st.AppendRevision(ctx, nid, p, nil, ptrTime(at(100+i))); err != nil {
				errs <- err
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var maxNo, cnt int
	if err := st.pool.QueryRow(ctx,
		`SELECT max(revision_no), count(*) FROM plan_revisions WHERE plan_id=$1`, nid).
		Scan(&maxNo, &cnt); err != nil {
		t.Fatal(err)
	}
	if maxNo != n+1 || cnt != n+1 {
		t.Fatalf("max=%d count=%d want %d", maxNo, cnt, n+1)
	}
}
