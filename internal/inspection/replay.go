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
func Replay(s *Stream, events []Event) (Snapshot, error) {
	if s == nil {
		return Snapshot{}, errors.New("nil stream")
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
	for _, ev := range ordered {
		switch {
		case ev.Batch != nil:
			next, res := applyBatch(st, s, ev.Batch)
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
	return snap, nil
}

// ValidateBatch checks a lot against all bound plans. Used by the store
// layer before a mutation is committed.
func ValidateBatch(s *Stream, b *BatchInput) error { return validateBatch(s, b) }
