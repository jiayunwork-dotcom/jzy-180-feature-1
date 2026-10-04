package inspection

import (
	"errors"

	"sampling-svc/internal/plan"
)

// slotFor returns the bound slot for the current severity.
func (s *Stream) slotFor(sev Severity) Slot {
	switch sev {
	case SeverityTightened:
		return s.Tightened
	case SeverityReduced:
		return s.Reduced
	default:
		return s.Normal
	}
}

func cloneState(st State) State {
	out := st
	if st.normalWindow != nil {
		out.normalWindow = append([]bool(nil), st.normalWindow...)
	}
	return out
}

// applyFlag advances the state for a stable/approved flag event.
// Revoking "production stable" while on reduced inspection immediately
// returns the stream to normal inspection.
func applyFlag(st State, name FlagName, value bool) State {
	switch name {
	case FlagStable:
		st.stable = value
		if !value && st.Severity == SeverityReduced {
			st.Severity = SeverityNormal
			st.Score = 0
		}
	case FlagApproved:
		st.approved = value
	}
	return st
}

// applyResume handles a manual resume after suspension: inspection
// restarts on the tightened track with all tightened counters cleared.
func applyResume(st State) State {
	if st.Severity == SeveritySuspended {
		st.Severity = SeverityTightened
		st.tightenedAcceptedRun = 0
		st.tightenedRejectTotal = 0
	}
	return st
}

// stepResult captures everything that happened while processing a batch.
type stepResult struct {
	outcome  BatchOutcome
	note     string
	conflict *BatchConflict
}

// applyBatch advances the state by one lot. The incoming state is not
// mutated; the returned state and outcome describe the world after the
// lot. Determinism across replays comes entirely from this function.
func applyBatch(st State, s *Stream, b *BatchInput) (State, stepResult) {
	st = cloneState(st)
	res := stepResult{outcome: BatchOutcome{
		BatchID: b.ID, LotNo: b.LotNo, At: b.At, D1: b.D1, Score: st.Score,
	}}
	if b.HasD2 {
		d2 := b.D2
		res.outcome.D2 = &d2
	}

	if st.Severity == SeveritySuspended {
		res.outcome.Severity = SeveritySuspended
		res.outcome.Decision = DecisionNotInspected
		res.outcome.Accepted = false
		res.note = "inspection suspended; lot not inspected"
		return st, res
	}

	slot := s.slotFor(st.Severity)
	rev := slot.At(b.At)
	pl := rev.Plan
	res.outcome.Severity = st.Severity
	res.outcome.PlanID = slot.ID
	res.outcome.PlanRevision = rev.Number
	res.outcome.PlanName = pl.Name

	accepted, err := LotDecision(pl, b.D1, b.HasD2, b.D2)
	if err != nil {
		// The lot cannot be judged under the revision effective at its
		// inspection time. Report the conflict; use a deterministic
		// rejected fallback so the walk can continue and surface every
		// subsequent conflict in the same run.
		accepted = false
		field, msg := "d1", err.Error()
		var fe plan.FieldError
		if errors.As(err, &fe) {
			field, msg = fe.Field, fe.Message
		}
		res.conflict = &BatchConflict{
			BatchID: b.ID, LotNo: b.LotNo, Severity: st.Severity,
			Revision: rev.Number, Field: field, Message: msg,
		}
		res.note = err.Error()
	}
	res.outcome.Accepted = accepted
	if accepted {
		res.outcome.Decision = DecisionAccepted
	} else {
		res.outcome.Decision = DecisionRejected
	}

	switch st.Severity {
	case SeverityNormal:
		st.normalWindow = append(st.normalWindow, accepted)
		if len(st.normalWindow) > 5 {
			st.normalWindow = st.normalWindow[len(st.normalWindow)-5:]
		}
		award := switchScoreAward(pl, accepted, b.D1, b.HasD2, b.D2)
		if award == 0 {
			st.Score = 0
		} else {
			st.Score += award
		}
		res.outcome.Score = st.Score

		if rejectsInWindow(st.normalWindow) >= 2 {
			// Two rejected lots within any window of the last five
			// normal-inspected lots -> tightened.
			st.Severity = SeverityTightened
			st.Score = 0
			st.normalWindow = nil
			st.tightenedAcceptedRun = 0
			st.tightenedRejectTotal = 0
			res.note = "switch normal -> tightened"
			return st, res
		}
		if st.Score >= 30 && st.stable && st.approved {
			// Score threshold plus both prerequisites -> reduced.
			st.Severity = SeverityReduced
			st.Score = 0
			res.note = "switch normal -> reduced"
			return st, res
		}

	case SeverityTightened:
		if accepted {
			st.tightenedAcceptedRun++
			if st.tightenedAcceptedRun >= 5 {
				// Five consecutive accepted lots on tightened -> normal.
				st.Severity = SeverityNormal
				st.tightenedAcceptedRun = 0
				st.tightenedRejectTotal = 0
				st.normalWindow = nil
				st.Score = 0
				res.note = "switch tightened -> normal"
			}
		} else {
			st.tightenedAcceptedRun = 0
			st.tightenedRejectTotal++
			if st.tightenedRejectTotal >= 5 {
				// Five cumulative rejects on tightened -> discontinue.
				st.Severity = SeveritySuspended
				res.note = "tightened inspection discontinued (suspended)"
			}
		}
		res.outcome.Score = 0

	case SeverityReduced:
		res.outcome.Score = 0
		if !accepted {
			// Any rejected lot on reduced inspection -> normal.
			st.Severity = SeverityNormal
			st.Score = 0
			st.normalWindow = nil
			res.note = "switch reduced -> normal"
		}
	}
	return st, res
}

func rejectsInWindow(w []bool) int {
	r := 0
	for _, a := range w {
		if !a {
			r++
		}
	}
	return r
}

// validateBatch checks a recorded lot against every revision of every
// plan bound to the stream, since backdated data may be judged under any
// severity slot and any historical revision. A lot that is impossible to
// judge under even one revision is rejected so the derived state can
// never silently diverge from replay.
func validateBatch(s *Stream, b *BatchInput) error {
	for _, slot := range []Slot{s.Normal, s.Tightened, s.Reduced} {
		seen := map[*plan.Plan]bool{}
		for _, rev := range slot.Revisions {
			if rev.Plan == nil || seen[rev.Plan] {
				continue
			}
			seen[rev.Plan] = true
			if err := validateBatchAgainst(rev.Plan, b); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateBatchAgainst checks one lot against one concrete plan.
func validateBatchAgainst(pl *plan.Plan, b *BatchInput) error {
	if pl.Kind == plan.KindSingle {
		if b.HasD2 {
			return fieldErr("d2", "not allowed for a single-sampling plan")
		}
		if b.D1 < 0 || b.D1 > pl.SampleSize {
			return fieldErr("d1", "must be within [0,n]")
		}
		return nil
	}
	if b.D1 < 0 || b.D1 > pl.N1 {
		return fieldErr("d1", "must be within [0,n1]")
	}
	if b.HasD2 {
		if b.D2 < 0 || b.D2 > pl.N2 {
			return fieldErr("d2", "must be within [0,n2]")
		}
		if b.D1+b.D2 > pl.N1+pl.N2 {
			return fieldErr("d2", "d1+d2 must be within [0,n1+n2]")
		}
	} else if b.D1 > pl.C1 && b.D1 < pl.R1 {
		return fieldErr("d2", "required when c1 < d1 < r1")
	}
	return nil
}

// BatchConflict identifies one recorded lot that a candidate revision
// history could no longer judge.
type BatchConflict struct {
	StreamID string
	BatchID  string
	LotNo    string
	Severity Severity
	Revision int
	Field    string
	Message  string
}

type vfe struct{ f, m string }

func (e vfe) Error() string            { return e.f + ": " + e.m }
func (e vfe) Fields() (string, string) { return e.f, e.m }
func fieldErr(f, m string) error       { return vfe{f, m} }
