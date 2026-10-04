package inspection

import (
	"testing"
	"time"

	"sampling-svc/internal/plan"
)

// revStream builds a single-kind stream whose normal slot has two
// revisions: c=2 before "change", c=1 from "change" on. Tightened and
// reduced slots are single-revision companions.
func revStream(change time.Time) *Stream {
	normal := Slot{ID: "n", Revisions: []RevisionRef{
		{Number: 1, EffectiveAt: plan.Epoch, Plan: mkSingle(80, 2)},
		{Number: 2, EffectiveAt: change, Plan: mkSingle80(1)},
	}}
	return &Stream{
		ID:        "s",
		Name:      "s",
		Normal:    normal,
		Tightened: SingleSlot("t", mkSingle80(1)),
		Reduced:   SingleSlot("r", mkSingle80(3)),
	}
}

func mkSingle80(c int) *plan.Plan {
	return &plan.Plan{ID: "p", Name: "pl", Kind: plan.KindSingle,
		Distribution: plan.DistBinomial, SampleSize: 80, AcceptNumber: c}
}

// Slot.At resolution: effective_at exactly equal to the inspection time
// uses the NEW revision; strictly earlier uses the old one.
func TestSlotAtBoundary(t *testing.T) {
	T := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)
	slot := Slot{Revisions: []RevisionRef{
		{Number: 1, EffectiveAt: plan.Epoch, Plan: mkSingle80(2)},
		{Number: 2, EffectiveAt: T, Plan: mkSingle80(1)},
	}}
	if r := slot.At(T.Add(-time.Nanosecond)); r.Number != 1 {
		t.Fatalf("just before T must be rev1, got %d", r.Number)
	}
	if r := slot.At(T); r.Number != 2 {
		t.Fatalf("exactly T must be rev2 (new wins tie), got %d", r.Number)
	}
	if r := slot.At(T.Add(time.Second)); r.Number != 2 {
		t.Fatalf("after T must be rev2, got %d", r.Number)
	}
}

// A revision inserted between two revisions resolves correctly.
func TestSlotAtBackdatedBetween(t *testing.T) {
	t0 := plan.Epoch
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	slot := Slot{Revisions: []RevisionRef{
		{Number: 1, EffectiveAt: t0, Plan: mkSingle80(2)},
		{Number: 2, EffectiveAt: t2, Plan: mkSingle80(0)},
		{Number: 3, EffectiveAt: t1, Plan: mkSingle80(1)},
	}}
	cases := []struct {
		at time.Time
		c  int
	}{
		{t1.Add(-time.Hour), 2},
		{t1, 1},
		{t2.Add(-time.Hour), 1},
		{t2, 0},
	}
	for _, tc := range cases {
		if r := slot.At(tc.at); r.Plan.AcceptNumber != tc.c {
			t.Fatalf("at %s got c=%d want %d", tc.at, r.Plan.AcceptNumber, tc.c)
		}
	}
}

// Score continuity: the switching score carries across a mid-normal-spell
// revision change purely by the existing award rules — no special case.
// Under c=2: d=0 +3 (score 3); after switching to c=1 the SAME score is
// kept and d=0 now awards +2 (score 5).
func TestScoreContinuesAcrossRevision(t *testing.T) {
	T := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)
	s := revStream(T)
	evs := []Event{
		event(1, T.Add(-2*time.Hour), batch(1, "a", T.Add(-2*time.Hour), 0)), // c=2: +3
		event(2, T.Add(-time.Hour), batch(2, "b", T.Add(-time.Hour), 0)),     // c=2: +3 => 6
		event(3, T.Add(time.Hour), batch(3, "c", T.Add(time.Hour), 0)),       // c=1: +2 => 8
		event(4, T.Add(2*time.Hour), batch(4, "d", T.Add(2*time.Hour), 1)),   // c=1: d=1 +1 => 9
	}
	snap, err := Replay(s, evs)
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		rev, score int
		acc        bool
	}{
		{1, 3, true},
		{1, 6, true},
		{2, 8, true},
		{2, 9, true},
	}
	for i, w := range want {
		b := snap.Batches[i]
		if b.PlanRevision != w.rev || b.Score != w.score || b.Accepted != w.acc {
			t.Fatalf("batch %d rev=%d score=%d acc=%v want rev=%d score=%d acc=%v",
				i, b.PlanRevision, b.Score, b.Accepted, w.rev, w.score, w.acc)
		}
	}
}

// A lot that the revision effective at its time cannot judge is reported
// as a conflict by the diagnostic replay, while the walk continues.
func TestRevisionConflictReported(t *testing.T) {
	T := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)
	normal := Slot{ID: "n", Revisions: []RevisionRef{
		{Number: 1, EffectiveAt: plan.Epoch, Plan: mkSingle80(2)},
		{Number: 2, EffectiveAt: T, Plan: mkSingle80(1)}, // n still 80 here
	}}
	s := &Stream{
		ID: "s", Normal: normal,
		Tightened: SingleSlot("t", mkSingle80(1)),
		Reduced:   SingleSlot("r", mkSingle80(3)),
	}
	// Record d=50 after T: c/n=80 allows the count, but to force a real
	// "cannot judge" conflict shrink the sample via a fresh rev at T+1h.
	shrink := &plan.Plan{ID: "p", Name: "pl", Kind: plan.KindSingle,
		Distribution: plan.DistBinomial, SampleSize: 20, AcceptNumber: 0}
	s.Normal.Revisions = append(s.Normal.Revisions,
		RevisionRef{Number: 3, EffectiveAt: T.Add(time.Hour), Plan: shrink})
	evs := []Event{
		event(1, T.Add(2*time.Hour), batch(1, "big", T.Add(2*time.Hour), 50)),
		event(2, T.Add(3*time.Hour), batch(2, "later", T.Add(3*time.Hour), 0)),
	}
	_, conflicts, err := ReplayDiagnose(s, evs)
	if err != nil {
		t.Fatal(err)
	}
	if len(conflicts) != 1 {
		t.Fatalf("want exactly 1 conflict, got %+v", conflicts)
	}
	if conflicts[0].BatchID != "big" || conflicts[0].Revision != 3 ||
		conflicts[0].Field != "d1" {
		t.Fatalf("conflict wrong: %+v", conflicts[0])
	}
}
