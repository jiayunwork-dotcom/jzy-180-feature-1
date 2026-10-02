package inspection

import (
	"fmt"
	"testing"
	"time"

	"sampling-svc/internal/plan"
)

func mkSingle(n, c int) *plan.Plan {
	return &plan.Plan{ID: "p", Name: "pl", Kind: plan.KindSingle,
		Distribution: plan.DistBinomial, SampleSize: n, AcceptNumber: c}
}

func testStream(n, c int) *Stream {
	pn := mkSingle(n, c)
	pt := mkSingle(n, max0(c-1))
	pr := mkSingle(n, c+1)
	return &Stream{
		ID:        "s1",
		Name:      "s",
		Normal:    Ref{ID: "n", Plan: pn},
		Tightened: Ref{ID: "t", Plan: pt},
		Reduced:   Ref{ID: "r", Plan: pr},
	}
}

func max0(x int) int {
	if x < 0 {
		return 0
	}
	return x
}

func batch(seq int64, lot string, t time.Time, d1 int) *BatchInput {
	return &BatchInput{ID: lot, LotNo: lot, At: t, D1: d1, Seq: seq}
}

func event(seq int64, t time.Time, b *BatchInput) Event {
	return Event{Seq: seq, At: t, Batch: b}
}

func flagEvent(seq int64, t time.Time, name FlagName, v bool) Event {
	return Event{Seq: seq, At: t, Flag: name, FlagValue: v}
}

// Normal: two rejects within the last five lots -> tightened.
func TestNormalToTightened(t *testing.T) {
	s := testStream(20, 5)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var evs []Event
	// lots: accept, reject, accept x3, reject -> the 2nd reject lands
	// within a 5-lot window (lots 2..6: two rejects).
	data := []int{1, 6, 2, 2, 2, 6}
	for i, d := range data {
		b := batch(int64(i), lotID(i), base.Add(time.Duration(i)*time.Hour), d)
		evs = append(evs, event(int64(i), b.At, b))
	}
	snap, err := Replay(s, evs)
	if err != nil {
		t.Fatal(err)
	}
	last := snap.Batches[len(snap.Batches)-1]
	if last.Severity != SeverityNormal {
		t.Fatalf("triggering lot judged under %s, want normal", last.Severity)
	}
	if snap.Current.Severity != SeverityTightened {
		t.Fatalf("severity=%s want tightened", snap.Current.Severity)
	}
	if last.Note == "" {
		t.Fatal("expected switch note on batch")
	}
}

// Tightened: five consecutive accepts -> normal.
func TestTightenedToNormal(t *testing.T) {
	s := testStream(20, 5)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Enter tightened first.
	evs := []Event{
		rej(0, base), rej(1, base.Add(time.Hour)),
	}
	// Five consecutive accepts on tightened (tightened c=4 here).
	for i := 2; i < 7; i++ {
		b := batch(int64(i), lotID(i), base.Add(time.Duration(i)*time.Hour), 0)
		evs = append(evs, event(int64(i), b.At, b))
	}
	snap, _ := Replay(s, evs)
	if snap.Current.Severity != SeverityNormal {
		t.Fatalf("severity=%s want normal", snap.Current.Severity)
	}
	if snap.Batches[len(snap.Batches)-1].Severity != SeverityTightened {
		t.Fatal("the fifth accept must have been judged under tightened")
	}
}

// Normal score reaches 30 with stable+approved -> reduced.
func TestNormalToReduced(t *testing.T) {
	s := testStream(50, 0) // c=0: each accepted lot awards 2
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var evs []Event
	evs = append(evs,
		flagEvent(100, base.Add(-2*time.Hour), FlagStable, true),
		flagEvent(101, base.Add(-time.Hour), FlagApproved, true))
	// 15 defect-free lots -> score 30 on the 15th.
	for i := 0; i < 15; i++ {
		b := batch(int64(i), lotID(i), base.Add(time.Duration(i)*time.Minute), 0)
		evs = append(evs, event(int64(i), b.At, b))
	}
	snap, _ := Replay(s, evs)
	if snap.Current.Severity != SeverityReduced {
		t.Fatalf("severity=%s want reduced (score path: last=%d)",
			snap.Current.Severity, snap.Batches[len(snap.Batches)-1].Score)
	}
	// Before flags exist the score still accumulates but no switch:
	evs2 := append(evs[:0:0], evs...)
	snap2, _ := Replay(s, evs2[2:]) // drop flags
	if snap2.Current.Severity != SeverityNormal {
		t.Fatal("without both flags must stay normal")
	}
}

// Reduced: one rejected lot -> normal.
func TestReducedToNormalOnReject(t *testing.T) {
	s := testStream(50, 0)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var evs []Event
	evs = append(evs,
		flagEvent(100, base.Add(-2*time.Hour), FlagStable, true),
		flagEvent(101, base.Add(-time.Hour), FlagApproved, true))
	for i := 0; i < 16; i++ {
		b := batch(int64(i), lotID(i), base.Add(time.Duration(i)*time.Minute), 0)
		evs = append(evs, event(int64(i), b.At, b))
	}
	// 16th lot (i=15) pushed to reduced; add a reject judged under reduced
	// (reduced c=c+1=1, so d1=2 rejects).
	b := batch(200, "L-rej", base.Add(2*time.Hour), 2)
	evs = append(evs, event(200, b.At, b))
	snap, _ := Replay(s, evs)
	last := snap.Batches[len(snap.Batches)-1]
	if last.Severity != SeverityReduced || last.Accepted {
		t.Fatalf("trigger lot severity=%s accepted=%v", last.Severity, last.Accepted)
	}
	if snap.Current.Severity != SeverityNormal {
		t.Fatalf("severity=%s want normal", snap.Current.Severity)
	}
}

// Reduced: revoking the stable flag -> normal immediately.
func TestReducedToNormalOnFlagRevoke(t *testing.T) {
	s := testStream(50, 0)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	evs := []Event{
		flagEvent(100, base, FlagStable, true),
		flagEvent(101, base.Add(time.Minute), FlagApproved, true),
	}
	for i := 0; i < 15; i++ {
		b := batch(int64(i), lotID(i), base.Add(2*time.Minute+time.Duration(i)*time.Minute), 0)
		evs = append(evs, event(int64(i), b.At, b))
	}
	if snap, _ := Replay(s, evs); snap.Current.Severity != SeverityReduced {
		t.Fatalf("precondition severity=%s", snap.Current.Severity)
	}
	evs = append(evs, flagEvent(200, base.Add(20*time.Hour), FlagStable, false))
	snap, _ := Replay(s, evs)
	if snap.Current.Severity != SeverityNormal {
		t.Fatalf("severity=%s want normal after revoking stable", snap.Current.Severity)
	}
}

// Tightened: five cumulative rejects -> suspended; resume restarts tightened.
func TestSuspensionAndResume(t *testing.T) {
	s := testStream(20, 5) // tightened c=4: d1=5 rejects
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	evs := []Event{rej(0, base), rej(1, base.Add(time.Hour))}
	hour := 2
	// After the switch, accumulate 5 tightened rejects with accepts
	// interleaved (never 5 consecutive accepts): A R x5.
	for k := 0; k < 5; k++ {
		bAcc := batch(int64(100+k*2), lotID(100+k*2), base.Add(time.Duration(hour)*time.Hour), 0)
		evs = append(evs, event(bAcc.Seq, bAcc.At, bAcc))
		hour++
		bRej := batch(int64(101+k*2), lotID(101+k*2), base.Add(time.Duration(hour)*time.Hour), 5)
		evs = append(evs, event(bRej.Seq, bRej.At, bRej))
		hour++
	}
	snap, _ := Replay(s, evs)
	if snap.Current.Severity != SeveritySuspended {
		t.Fatalf("severity=%s want suspended", snap.Current.Severity)
	}
	// A lot arriving while suspended is recorded but not inspected.
	later := batch(300, "L-later", base.Add(30*time.Hour), 0)
	evs = append(evs, event(300, later.At, later))
	snap, _ = Replay(s, evs)
	last := snap.Batches[len(snap.Batches)-1]
	if last.Decision != DecisionNotInspected || last.Severity != SeveritySuspended {
		t.Fatalf("suspended lot decision=%s severity=%s", last.Decision, last.Severity)
	}
	// Manual resume -> tightened, counters cleared.
	evs = append(evs, Event{Seq: 400, At: base.Add(31 * time.Hour), Resume: true})
	snap, _ = Replay(s, evs)
	if snap.Current.Severity != SeverityTightened {
		t.Fatalf("after resume severity=%s want tightened", snap.Current.Severity)
	}
	// The prior not-inspected lot must remain not-inspected (it precedes resume).
	for _, o := range snap.Batches {
		if o.BatchID == "L-later" && o.Decision != DecisionNotInspected {
			t.Fatal("historical suspended lot changed after resume")
		}
	}
}

// Score resets on any normal reject; awards follow the standard table.
func TestScoreRules(t *testing.T) {
	s := testStream(20, 2) // normal c=2: 3 / 2 / 1 awards
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seq := []struct {
		d     int
		score int
	}{
		{0, 3}, // d<=c-2 -> +3
		{1, 5}, // d=c-1 -> +2
		{2, 6}, // d=c -> +1
		{3, 0}, // reject -> reset
		{0, 3},
	}
	var evs []Event
	for i, q := range seq {
		b := batch(int64(i), lotID(i), base.Add(time.Duration(i)*time.Hour), q.d)
		evs = append(evs, event(int64(i), b.At, b))
	}
	snap, _ := Replay(s, evs)
	for i, q := range seq {
		if snap.Batches[i].Score != q.score {
			t.Fatalf("lot %d score=%d want %d", i, snap.Batches[i].Score, q.score)
		}
	}
}

func rej(seq int64, t time.Time) Event {
	b := batch(seq, lotID(int(seq)), t, 20) // d1=n: rejected under any c
	return Event{Seq: seq, At: t, Batch: b}
}

func lotID(i int) string {
	return "L" + fmt.Sprintf("%03d", i)
}
