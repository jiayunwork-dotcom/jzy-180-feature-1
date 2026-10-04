package inspection

import (
	"errors"
	"sort"
)

// ErrEmptyTimeline is returned when a replay is attempted without events.
var ErrEmptyTimeline = errors.New("no events")

// Replay rebuilds the full stream state from the beginning.
//
// Events are ordered by (inspected_at, sequence number). The sequence
// number is a per-stream monotonic counter allocated at insert time, so
// the order is a total order and independent of physical arrival order:
// back-dated entries, edits and deletes all reproduce exactly the same
// per-batch severity, decision and score as a fresh chronological walk.
//
// Each lot is judged with the revision of its slot that is effective at
// the lot's own inspection time.
func Replay(s *Stream, events []Event) (Snapshot, error) {
	snap, _, err := ReplayDiagnose(s, events)
	return snap, err
}

// ReplayDiagnose behaves like Replay but additionally reports every lot
// that could not be judged under the revision effective at its inspection
// time (e.g. a recorded defect count beyond a revised, smaller sample
// size). The walk continues past such lots with a deterministic fallback
// disposition so one conflict never hides later ones; callers must treat
// a non-empty conflict list as a hard error (the store rejects the
// offending revision/mutation).
func ReplayDiagnose(s *Stream, events []Event) (Snapshot, []BatchConflict, error) {
	if s == nil {
		return Snapshot{}, nil, errors.New("nil stream")
	}
	ordered := append([]Event(nil), events...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if !ordered[i].At.Equal(ordered[j].At) {
			return ordered[i].At.Before(ordered[j].At)
		}
		return ordered[i].Seq < ordered[j].Seq
	})

	st := InitialState()
	snap := Snapshot{StreamID: s.ID}
	var conflicts []BatchConflict
	for _, ev := range ordered {
		switch {
		case ev.Batch != nil:
			next, res := applyBatch(st, s, ev.Batch)
			st = next
			o := res.outcome
			if res.note != "" {
				o.Note = res.note
			}
			if res.conflict != nil {
				cf := *res.conflict
				cf.StreamID = s.ID
				conflicts = append(conflicts, cf)
			}
			snap.Batches = append(snap.Batches, o)
		case ev.Flag != "":
			st = applyFlag(st, ev.Flag, ev.FlagValue)
		case ev.Resume:
			st = applyResume(st)
		}
	}
	snap.Current = st
	return snap, conflicts, nil
}

// ValidateBatch checks a lot against all revisions of all bound plans.
// Used by the store layer before a mutation is committed.
func ValidateBatch(s *Stream, b *BatchInput) error { return validateBatch(s, b) }
