package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"sampling-svc/internal/inspection"
)

// Hammer one stream from many goroutines: no batch may be lost, no
// sequence number duplicated, and the derived state must equal a fresh
// replay of every stored event.
func TestConcurrentInsertsSameStream(t *testing.T) {
	st := newTestStore(t)
	id := createStreamRow(t, st, "conc", 100, 50)

	const writers = 16
	const perWriter = 60
	var wg sync.WaitGroup
	errs := make(chan error, writers*perWriter)
	start := make(chan struct{})
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			ctx := context.Background()
			for i := 0; i < perWriter; i++ {
				// Spread timestamps, including backdated entries.
				hour := (w*perWriter + i*7) % 5000
				_, err := st.AddBatch(ctx, id, BatchRecord{
					ID: fmt.Sprintf("w%02d-%03d", w, i), LotNo: fmt.Sprintf("L%02d%03d", w, i),
					At: at(hour), D1: (w + i) % 3, // mostly accepts
				})
				if err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent insert error: %v", err)
	}

	ctx := context.Background()
	// No lost batches.
	var nEvents, nResults int
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM stream_events WHERE stream_id=$1 AND kind='batch'`, id).
		Scan(&nEvents); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx,
		`SELECT count(*) FROM batch_results WHERE stream_id=$1`, id).Scan(&nResults); err != nil {
		t.Fatal(err)
	}
	if nEvents != writers*perWriter {
		t.Fatalf("lost batches: events=%d want=%d", nEvents, writers*perWriter)
	}
	if nResults != nEvents {
		t.Fatalf("derived results=%d events=%d (double scoring / missed rebuild)", nResults, nEvents)
	}
	// Sequence numbers unique and gap-free.
	var minSeq, maxSeq int64
	var distinct int
	if err := st.pool.QueryRow(ctx, `
SELECT min(seq), max(seq), count(DISTINCT seq)
FROM stream_events WHERE stream_id=$1`, id).Scan(&minSeq, &maxSeq, &distinct); err != nil {
		t.Fatal(err)
	}
	if minSeq != 1 || maxSeq != int64(writers*perWriter) || distinct != writers*perWriter {
		t.Fatalf("seq corruption min=%d max=%d distinct=%d", minSeq, maxSeq, distinct)
	}
	// Derived state is exactly what a fresh replay produces.
	snap, err := st.GetSnapshot(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	assertSnapshotsMatch(t, snap, independentReplay(t, st, id))
}

// Different streams must not block each other: while a transaction on
// stream A is parked holding its advisory lock, writes to stream B
// still complete promptly.
func TestDifferentStreamsDoNotBlock(t *testing.T) {
	st := newTestStore(t)
	a := createStreamRow(t, st, "alpha", 50, 5)
	b := createStreamRow(t, st, "beta", 50, 5)

	ctx := context.Background()
	tx, err := st.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := lockStream(ctx, tx, a); err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)

	done := make(chan struct{})
	go func() {
		_, _ = st.AddBatch(context.Background(), b, BatchRecord{
			ID: "free", LotNo: "F", At: at(1), D1: 0})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("write to a different stream blocked behind stream A's lock")
	}
	snap, err := st.GetSnapshot(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Batches) != 1 || snap.Batches[0].BatchID != "free" {
		t.Fatal("other-stream write did not land correctly")
	}
}

// Concurrent flag toggles, resumes and batches on one stream never
// corrupt the derived state; final state matches a full replay.
func TestConcurrentMixedMutations(t *testing.T) {
	st := newTestStore(t)
	id := createStreamRow(t, st, "mix", 80, 2)

	ctx := context.Background()
	if _, err := st.SetFlag(ctx, id, inspection.FlagApproved, true, at(0)); err != nil {
		t.Fatal(err)
	}

	const n = 40
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, 3*n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := st.AddBatch(ctx, id, BatchRecord{
				ID: fmt.Sprintf("m%03d", i), LotNo: "M",
				At: at(i), D1: i % 4,
			})
			if err != nil {
				errs <- err
			}
		}(i)
	}
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := st.SetFlag(ctx, id, inspection.FlagStable, i%2 == 0, at(1000+i))
			if err != nil {
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
	snap, err := st.GetSnapshot(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	assertSnapshotsMatch(t, snap, independentReplay(t, st, id))
}
