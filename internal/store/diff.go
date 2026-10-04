package store

import (
	"sampling-svc/internal/inspection"
)

// diffSnapshots compares the stored (pre-append) derived snapshot with the
// freshly replayed one and renders only the changed scalar fields per
// batch. A stream with zero changes is reported as Changed=false — the
// caller always emits one entry per affected stream.
func diffSnapshots(name string, before, after inspection.Snapshot) StreamDiff {
	d := StreamDiff{StreamID: after.StreamID, Name: name}

	if before.Current.Severity != after.Current.Severity {
		d.SeverityChanged = &FieldChange{
			From: string(before.Current.Severity), To: string(after.Current.Severity),
		}
	}
	if bs, as := before.Current.ScoreValue(), after.Current.ScoreValue(); bs != as {
		d.ScoreChanged = &FieldChange{From: bs, To: as}
	}

	byID := make(map[string]inspection.BatchOutcome, len(before.Batches))
	for _, b := range before.Batches {
		byID[b.BatchID] = b
	}
	for _, a := range after.Batches {
		b, ok := byID[a.BatchID]
		if !ok {
			// A revision append never changes the event set, so every
			// after-batch has a before-row.
			continue
		}
		bd := BatchDiff{
			BatchID: a.BatchID, LotNo: a.LotNo, InspectedAt: a.At,
		}
		any := false
		if b.Severity != a.Severity {
			bd.Severity = &FieldChange{From: string(b.Severity), To: string(a.Severity)}
			any = true
		}
		if b.Decision != a.Decision {
			bd.Decision = &FieldChange{From: string(b.Decision), To: string(a.Decision)}
			any = true
		}
		if b.Accepted != a.Accepted {
			bd.Accepted = &FieldChange{From: b.Accepted, To: a.Accepted}
			any = true
		}
		if b.Score != a.Score {
			bd.Score = &FieldChange{From: b.Score, To: a.Score}
			any = true
		}
		if b.PlanID != a.PlanID {
			bd.PlanID = &FieldChange{From: b.PlanID, To: a.PlanID}
			any = true
		}
		if !revEq(b.RevisionNo, a.RevisionNo) {
			bd.RevisionNo = &FieldChange{From: revOrNil(b.RevisionNo), To: revOrNil(a.RevisionNo)}
			any = true
		}
		if any {
			d.Batches = append(d.Batches, bd)
		}
	}
	d.Changed = d.SeverityChanged != nil || d.ScoreChanged != nil || len(d.Batches) > 0
	return d
}

func revEq(a, b *int) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}

func revOrNil(a *int) any {
	if a == nil {
		return nil
	}
	return *a
}
