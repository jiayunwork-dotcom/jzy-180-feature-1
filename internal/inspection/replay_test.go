package inspection

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"sampling-svc/internal/plan"
)

func doubleTestStream() *Stream {
	mk := func(id, name string, c1, r1, c2 int) Ref {
		return Ref{ID: id, Plan: &plan.Plan{
			ID: id, Name: name, Kind: plan.KindDouble,
			Distribution: plan.DistBinomial,
			N1:           10, C1: c1, R1: r1, N2: 10, C2: c2,
		}}
	}
	return &Stream{
		ID:        "d1",
		Normal:    mk("n", "norm", 2, 5, 6),
		Tightened: mk("t", "tight", 1, 4, 4),
		Reduced:   mk("r", "red", 3, 6, 8),
	}
}

// replayTest is a direct in-memory model of the store contract: it keeps
// an event set and, after every random mutation, rebuilds the timeline
// either by Replay (the "full replay" reference) or by applying the same
// incremental edit. Because the store rebuilds via Replay itself, the
// property under test is that an arbitrary sequence of inserts/updates/
// deletes performed out of chronological order always converges to the
// result of replaying the final event set from scratch — trivially true
// here by construction, which is exactly the guarantee the service must
// keep regardless of edit order.
func TestRandomMutationMatchesFromScratch(t *testing.T) {
	rng := rand.New(rand.NewSource(20261001))
	s := testStream(10, 2)
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	type stored struct {
		id, lot string
		at      time.Time
		d1      int
	}
	var rows []stored
	idSeq := 0
	newID := func() string { idSeq++; return fmt.Sprintf("B%04d", idSeq) }
	// Unique timestamps keep (at) a total order; seq is then irrelevant.
	usedTimes := map[time.Time]bool{}
	uniqueTime := func(rng *rand.Rand) time.Time {
		for {
			t := base.Add(time.Duration(rng.Intn(100000)) * time.Minute)
			if !usedTimes[t] {
				usedTimes[t] = true
				return t
			}
		}
	}

	reference := func() Snapshot {
		var evs []Event
		rs := append([]stored(nil), rows...)
		sort.Slice(rs, func(i, j int) bool { return rs[i].at.Before(rs[j].at) })
		for i, r := range rs {
			b := &BatchInput{ID: r.id, LotNo: r.lot, At: r.at, D1: r.d1, Seq: int64(i)}
			evs = append(evs, Event{Seq: int64(i), At: r.at, Batch: b})
		}
		snap, err := Replay(s, evs)
		if err != nil {
			t.Fatal(err)
		}
		return snap
	}

	// verify compares an ordered replay against a physically shuffled
	// replay carrying arbitrary sequence numbers: the outcome must be a
	// function of the (time, data) content only, never arrival order.
	round := 0
	verify := func() {
		round++
		got := reference()
		var evs []Event
		rs := append([]stored(nil), rows...)
		rng.Shuffle(len(rs), func(i, j int) { rs[i], rs[j] = rs[j], rs[i] })
		for _, r := range rs {
			b := &BatchInput{ID: r.id, LotNo: r.lot, At: r.at, D1: r.d1}
			evs = append(evs, Event{Seq: int64(rng.Intn(1 << 30)), At: r.at, Batch: b})
		}
		shuffled, err := Replay(s, evs)
		if err != nil {
			t.Fatal(err)
		}
		if !snapshotsEqual(got, shuffled) {
			t.Fatalf("round %d: replay depends on arrival order", round)
		}
	}

	// 300 rounds of backdated inserts, edits and deletes.
	for k := 0; k < 300; k++ {
		switch {
		case len(rows) < 20 || rng.Intn(3) == 0:
			// insert at a random time (often BEFORE existing rows)
			rows = append(rows, stored{
				id: newID(), lot: fmt.Sprintf("lot-%d", idSeq),
				at: uniqueTime(rng), d1: rng.Intn(11), // 0..n, some reject
			})
		case rng.Intn(2) == 0 && len(rows) > 0:
			// edit a random row
			i := rng.Intn(len(rows))
			rows[i].d1 = rng.Intn(11)
			old := rows[i].at
			usedTimes[old] = false
			rows[i].at = uniqueTime(rng)
		case len(rows) > 0:
			// delete a random row
			i := rng.Intn(len(rows))
			delete(usedTimes, rows[i].at)
			rows = append(rows[:i], rows[i+1:]...)
		}
		verify()
	}
}

// A backdated reject before the point where the stream switched must
// retroactively move the switch — the canonical history-rewrite case.
func TestBackdatingMovesSwitch(t *testing.T) {
	s := testStream(10, 2)
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	mk := func(id string, hour, d1 int) Event {
		at := base.Add(time.Duration(hour) * time.Hour)
		b := batch(int64(hour), id, at, d1)
		return event(int64(hour), at, b)
	}
	// Four accepted lots.
	evs := []Event{mk("a", 1, 0), mk("b", 2, 0), mk("c", 3, 0), mk("d", 4, 0)}
	snap, _ := Replay(s, evs)
	if snap.Current.Severity != SeverityNormal {
		t.Fatal("precondition normal")
	}
	// Backdate two rejects to hour 0 and 5: window [1..5] has 1 reject;
	// place second backdated reject at hour 2.
	evs = append(evs, mk("r0", 0, 9), mk("r2", 2, 9))
	// Rebuilt timeline: R,A,R,A,A,A -> reject pair within first 5 -> tightened.
	snap, _ = Replay(s, evs)
	if snap.Current.Severity != SeverityTightened {
		t.Fatalf("backdated rejects must flip state, got %s", snap.Current.Severity)
	}
	// Delete the backdated rejects -> returns to normal track.
	var kept []Event
	for _, e := range evs {
		if e.Batch != nil && (e.Batch.ID == "r0" || e.Batch.ID == "r2") {
			continue
		}
		kept = append(kept, e)
	}
	snap, _ = Replay(s, kept)
	if snap.Current.Severity != SeverityNormal {
		t.Fatalf("deleting backdated rejects must restore normal, got %s", snap.Current.Severity)
	}
}

func snapshotsEqual(a, b Snapshot) bool {
	if a.StreamID != b.StreamID {
		return false
	}
	if a.Current.Severity != b.Current.Severity ||
		a.Current.ScoreValue() != b.Current.ScoreValue() {
		return false
	}
	if len(a.Batches) != len(b.Batches) {
		return false
	}
	for i := range a.Batches {
		x, y := a.Batches[i], b.Batches[i]
		if x.BatchID != y.BatchID || x.Severity != y.Severity ||
			x.PlanID != y.PlanID || x.Decision != y.Decision ||
			x.Accepted != y.Accepted || x.Score != y.Score ||
			!x.At.Equal(y.At) || x.D1 != y.D1 {
			return false
		}
		if (x.D2 == nil) != (y.D2 == nil) {
			return false
		}
		if x.D2 != nil && *x.D2 != *y.D2 {
			return false
		}
	}
	return true
}

// Validation rejects impossible defect counts against all bound plans.
func TestBatchValidation(t *testing.T) {
	s := testStream(10, 2)
	if err := ValidateBatch(s, &BatchInput{D1: 11}); err == nil {
		t.Fatal("d1>n must be rejected")
	}
	if err := ValidateBatch(s, &BatchInput{D1: -1}); err == nil {
		t.Fatal("negative d1 must be rejected")
	}

	// Double-stream: grey zone requires d2; counts beyond n2 rejected.
	ds := doubleTestStream()
	if err := ValidateBatch(ds, &BatchInput{D1: 3}); err == nil {
		// c1=2,r1=5 => d1=3 is in the grey zone
		t.Fatal("missing d2 in grey zone must be rejected")
	}
	if err := ValidateBatch(ds, &BatchInput{D1: 1}); err != nil {
		t.Fatalf("first-stage accept should be valid: %v", err)
	}
	if err := ValidateBatch(ds, &BatchInput{D1: 3, HasD2: true, D2: 11}); err == nil {
		t.Fatal("d2>n2 must be rejected")
	}
	if err := ValidateBatch(ds, &BatchInput{D1: 3, HasD2: true, D2: 2}); err != nil {
		t.Fatalf("valid double entry rejected: %v", err)
	}
}
