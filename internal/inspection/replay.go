package inspection

import (
	"errors"
	"sort"
)

// ErrEmptyTimeline is returned when a replay is attempted without events.
var ErrEmptyTimeline = errors.New("no events")

// orderEvents returns events sorted by (inspected_at, sequence number).
// The sequence number is a per-stream monotonic counter allocated at
// insert time, so the order is a total order and independent of physical
// arrival order: back-dated entries, edits and deletes all reproduce
// exactly the same per-batch severity, decision and score as a fresh
// chronological walk.
func orderEvents(events []Event) []Event {
	ordered := append([]Event(nil), events...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if !ordered[i].At.Equal(ordered[j].At) {
			return ordered[i].At.Before(ordered[j].At)
		}
		return ordered[i].Seq < ordered[j].Seq
	})
	return ordered
}

// Replay rebuilds the full stream state from the beginning.
func Replay(s *Stream, events []Event) (Snapshot, error) {
	snap, _, err := replay(s, events, false)
	if err != nil {
		return Snapshot{}, err
	}
	return snap, nil
}

// ReplayChecked is Replay's strict twin. It walks the same total order,
// but a lot whose defect counts do not fit the revision in force for its
// (severity, inspection time) is reported as a BatchConflict instead of
// being silently treated as a rejection. The walk then continues past
// the lot without moving any transition counter, so EVERY unreachable
// lot in the one coherent continuation is named in a single response.
//
// The store rejects an appended revision whenever this returns a
// non-empty conflict list. After the user fixes one batch, re-validating
// re-walks the history and may surface further conflicts whose severity
// location depended on the earlier resolution.
//
// The snapshot is meaningless when conflicts are non-empty.
func ReplayChecked(s *Stream, events []Event) (Snapshot, []BatchConflict, error) {
	return replay(s, events, true)
}

func replay(s *Stream, events []Event, strict bool) (Snapshot, []BatchConflict, error) {
	if s == nil {
		return Snapshot{}, nil, errors.New("nil stream")
	}
	ordered := orderEvents(events)

	st := InitialState()
	snap := Snapshot{StreamID: s.ID}
	var conflicts []BatchConflict
	seenConflict := map[string]bool{}
	for _, ev := range ordered {
		switch {
		case ev.Batch != nil:
			next, res, conflict := applyBatch(st, s, ev.Batch, strict)
			if conflict != nil {
				if !seenConflict[conflict.BatchID] {
					seenConflict[conflict.BatchID] = true
					conflicts = append(conflicts, *conflict)
				}
				// Leave st untouched: the unjudgeable lot neither counts
				// as accepted nor rejected.
				continue
			}
			st = next
			o := res.outcome
			if res.note != "" {
				o.Note = res.note
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

// ValidateBatch checks a lot against all bound plans. Used by the store
// layer before a mutation is committed.
func ValidateBatch(s *Stream, b *BatchInput) error { return validateBatch(s, b) }
