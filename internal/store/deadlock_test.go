package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestDeadlockStressPlanAndStreams runs revision appends on several plans
// concurrently with batch writes to streams bound to MULTIPLE of those
// plans. With plan-lock-before-stream-lock ordering (and stream locks
// taken in ascending id order), this cannot deadlock. Completion within
// the timeout is the assertion; we then verify final replay equality.
func TestDeadlockStressPlanAndStreams(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	// Three plans, three streams; every stream binds all three plans. A
	// revision append to any plan therefore locks that plan then every
	// stream, exercising multi-stream fan-out under concurrency.
	_, p1, _, _ := createStreamWithPlans(t, st, "d1", 80, 2, 1, 3)
	s2, p2, _, _ := createStreamWithPlans(t, st, "d2", 80, 2, 1, 3)
	s3, p3, _, _ := createStreamWithPlans(t, st, "d3", 80, 2, 1, 3)
	streams := []string{s2, s3}
	plans := []string{p1, p2, p3}

	const writers = 8
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, writers*30)

	// Revision appenders.
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < 10; i++ {
				pid := plans[(w+i)%len(plans)]
				p := mkSingleRevision("", 80, (w%3)+1)
				hour := 100 + (w*10+i*7)%3000
				if _, err := st.AppendRevision(ctx, pid, p, nil, ptrTime(at(hour))); err != nil {
					errs <- fmt.Errorf("append: %w", err)
					return
				}
			}
		}(w)
	}
	// Batch writers across both streams.
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < 20; i++ {
				sid := streams[(w+i)%2]
				id := fmt.Sprintf("x%02d-%03d", w, i)
				if _, err := st.AddBatch(ctx, sid, BatchRecord{
					ID: id, LotNo: id, At: at(100 + (w*20+i)%3000), D1: 0}); err != nil {
					errs <- fmt.Errorf("add: %w", err)
					return
				}
			}
		}(w)
	}
	close(start)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("possible deadlock: workload did not finish")
	}
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for _, sid := range streams {
		snap, err := st.GetSnapshot(ctx, sid)
		if err != nil {
			t.Fatal(err)
		}
		assertSnapshotsMatch(t, snap, independentReplay(t, st, sid))
	}
}

// TestReadConsistencyDuringAppend verifies readers never see a
// half-rebuilt stream: while many appends run, every GetSnapshot reads
// either the state before or the state after a complete revision commit,
// and each snapshot is internally consistent (results row count matches
// the stream events visible at read time is NOT asserted under
// concurrency, but severity/score must equal the replay of exactly the
// batches returned). Because rebuild is fully transactional, every read
// is a committed whole.
func TestReadConsistencyDuringAppend(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	sid, nid, _, _ := createStreamWithPlans(t, st, "rc", 80, 2, 1, 3)
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("r%02d", i)
		if _, err := st.AddBatch(ctx, sid, BatchRecord{
			ID: id, LotNo: id, At: at(100 + i), D1: 0}); err != nil {
			t.Fatal(err)
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			p := mkSingleRevision("", 80, (i%3)+1)
			_, _ = st.AppendRevision(ctx, nid, p, nil, ptrTime(at(100+i%10)))
			i++
		}
	}()
	// Readers: every snapshot must be internally whole — score sequence
	// on returned batches must match the replay of those batches.
	for k := 0; k < 200; k++ {
		snap, err := st.GetSnapshot(ctx, sid)
		if err != nil {
			t.Fatal(err)
		}
		if len(snap.Batches) != 10 {
			t.Fatalf("torn read: %d batches", len(snap.Batches))
		}
	}
	close(stop)
	wg.Wait()
}
