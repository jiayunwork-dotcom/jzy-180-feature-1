package inspection

import (
	"testing"
	"time"

	"sampling-svc/internal/plan"
)

// revStream builds a single-kind stream whose NORMAL plan carries a
// revision history; tightened/reduced stay single-revision refs.
func revStream(normal *Ref, tightC, redC int, n int) *Stream {
	mk := func(id string, c int) Ref {
		return Ref{ID: id, Plan: &plan.Plan{
			ID: id, Name: id, Kind: plan.KindSingle,
			Distribution: plan.DistBinomial, SampleSize: n, AcceptNumber: c,
		}}
	}
	s := &Stream{
		ID: "s", Name: "s", Normal: *normal,
		Tightened: mk("t", tightC), Reduced: mk("r", redC),
	}
	return s
}

// The hand-computable acceptance scenario:
//
//	n=80, normal c=2 until T, then c=1 (revision 2).
//
// Score awards under c=2: d=0 -> 3, d=1 -> 2, d=2 -> 1, d>=3 resets.
// Under c=1: d=0 -> 2, d=1 -> 1, d>=2 resets.
//
// Lots (all accepted unless noted):
//
//	before T:  d=0, d=1, d=2         -> scores 3, 5, 6, all revision 1
//	after  T:  d=0, d=1, d=2(rej)    -> +2=8, +1=9, reset 0
//
// Decisions: the d=2 lot after T is a REJECTION under c=1 whereas the
// d=2 lot before T was an ACCEPT under c=2. The score carries straight
// across the revision boundary: no special reset, the existing table
// rule applied to each lot's in-force revision naturally continues it.
func TestRevisionHistoryHandComputed(t *testing.T) {
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	cut := base.Add(10 * time.Hour)

	rv1Plan := &plan.Plan{ID: "n", Name: "normal", Kind: plan.KindSingle,
		Distribution: plan.DistBinomial, SampleSize: 80, AcceptNumber: 2}
	rv2Plan := &plan.Plan{ID: "n", Name: "normal", Kind: plan.KindSingle,
		Distribution: plan.DistBinomial, SampleSize: 80, AcceptNumber: 1}
	normal := Ref{ID: "n", Plan: rv2Plan, Revisions: []plan.Revision{
		{PlanID: "n", No: 1, EffectiveAt: time.Time{}, Plan: rv1Plan},
		{PlanID: "n", No: 2, EffectiveAt: cut, Plan: rv2Plan},
	}}
	s := revStream(&normal, 1, 2, 80)

	type want struct {
		id     string
		hour   int
		d      int
		accept bool
		score  int
		rev    int
		decOK  bool
	}
	lots := []want{
		{"a", 1, 0, true, 3, 1, true},
		{"b", 2, 1, true, 5, 1, true},
		{"c", 3, 2, true, 6, 1, true},
		{"d", 11, 0, true, 8, 2, true},
		{"e", 12, 1, true, 9, 2, true},
		{"f", 13, 2, false, 0, 2, true}, // rejected under c=1, score resets
	}
	var evs []Event
	for _, q := range lots {
		at := base.Add(time.Duration(q.hour) * time.Hour)
		b := &BatchInput{ID: q.id, LotNo: q.id, At: at, D1: q.d, Seq: int64(q.hour)}
		evs = append(evs, Event{Seq: int64(q.hour), At: at, Batch: b})
	}
	snap, err := Replay(s, evs)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Batches) != len(lots) {
		t.Fatalf("batches=%d", len(snap.Batches))
	}
	for i, q := range lots {
		o := snap.Batches[i]
		if o.BatchID != q.id {
			t.Fatalf("order %d", i)
		}
		if o.Accepted != q.accept {
			t.Fatalf("lot %s accepted=%v want %v", q.id, o.Accepted, q.accept)
		}
		if o.Score != q.score {
			t.Fatalf("lot %s score=%d want %d", q.id, o.Score, q.score)
		}
		if o.RevisionNo == nil || *o.RevisionNo != q.rev {
			t.Fatalf("lot %s rev=%v want %d", q.id, o.RevisionNo, q.rev)
		}
	}
}

// A revision whose sample size shrinks below a recorded lot's defect
// count must be named as a conflict and never silently flip the lot to
// "rejected".
func TestRevisionConflictSmallerSample(t *testing.T) {
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	rv1Plan := &plan.Plan{ID: "n", Name: "normal", Kind: plan.KindSingle,
		Distribution: plan.DistBinomial, SampleSize: 80, AcceptNumber: 2}
	rv2Plan := &plan.Plan{ID: "n", Name: "normal", Kind: plan.KindSingle,
		Distribution: plan.DistBinomial, SampleSize: 50, AcceptNumber: 1}
	normal := Ref{ID: "n", Plan: rv1Plan, Revisions: []plan.Revision{
		{PlanID: "n", No: 1, EffectiveAt: time.Time{}, Plan: rv1Plan},
		{PlanID: "n", No: 2, EffectiveAt: base, Plan: rv2Plan},
	}}
	s := revStream(&normal, 49, 51, 80)

	at0 := base.Add(-2 * time.Hour)
	at1 := base.Add(2 * time.Hour)
	evs := []Event{
		{Seq: 1, At: at0, Batch: &BatchInput{ID: "old", LotNo: "old", At: at0, D1: 10, Seq: 1}}, // fits n=80
		{Seq: 2, At: at1, Batch: &BatchInput{ID: "new", LotNo: "new", At: at1, D1: 55, Seq: 2}}, // > 50, conflict
	}
	_, conflicts, err := ReplayChecked(s, evs)
	if err != nil {
		t.Fatal(err)
	}
	if len(conflicts) != 1 || conflicts[0].BatchID != "new" ||
		conflicts[0].RevisionNo != 2 {
		t.Fatalf("conflicts=%+v", conflicts)
	}
}

// An inserted-in-the-middle revision picks up lots at exactly its
// effective time (inclusive boundary): lots at T use the new revision,
// the lot just before T keeps the old one.
func TestRevisionBoundaryInclusive(t *testing.T) {
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	rv1 := &plan.Plan{ID: "n", Name: "n", Kind: plan.KindSingle,
		Distribution: plan.DistBinomial, SampleSize: 80, AcceptNumber: 2}
	rv2 := &plan.Plan{ID: "n", Name: "n", Kind: plan.KindSingle,
		Distribution: plan.DistBinomial, SampleSize: 80, AcceptNumber: 0}
	normal := Ref{ID: "n", Plan: rv2, Revisions: []plan.Revision{
		{PlanID: "n", No: 1, EffectiveAt: time.Time{}, Plan: rv1},
		{PlanID: "n", No: 2, EffectiveAt: base, Plan: rv2},
	}}
	s := revStream(&normal, 0, 3, 80)

	atBefore := base.Add(-time.Nanosecond)
	atExact := base
	evs := []Event{
		{Seq: 1, At: atBefore, Batch: &BatchInput{ID: "before", LotNo: "b", At: atBefore, D1: 1, Seq: 1}},
		{Seq: 2, At: atExact, Batch: &BatchInput{ID: "exact", LotNo: "e", At: atExact, D1: 1, Seq: 2}},
	}
	snap, err := Replay(s, evs)
	if err != nil {
		t.Fatal(err)
	}
	if !snap.Batches[0].Accepted || snap.Batches[0].RevisionNo == nil ||
		*snap.Batches[0].RevisionNo != 1 {
		t.Fatalf("before-boundary lot: %+v", snap.Batches[0])
	}
	if snap.Batches[1].Accepted || snap.Batches[1].RevisionNo == nil ||
		*snap.Batches[1].RevisionNo != 2 {
		t.Fatalf("exact-boundary lot must use revision 2 and reject: %+v", snap.Batches[1])
	}
}
