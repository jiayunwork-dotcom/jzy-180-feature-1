package inspection

import "sampling-svc/internal/plan"

// planFor returns the plan bound to the current severity slot.
func (s *Stream) planFor(sev Severity) Ref {
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
	outcome BatchOutcome
	note    string
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

	ref := s.planFor(st.Severity)
	pl := ref.Plan
	res.outcome.Severity = st.Severity
	res.outcome.PlanID = ref.ID
	res.outcome.PlanName = pl.Name

	accepted, err := LotDecision(pl, b.D1, b.HasD2, b.D2)
	if err != nil {
		// Caller validates before replay; keep the stream defensible.
		accepted = false
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

// validateBatch checks a recorded lot against every plan bound to the
// stream, since backdated data may be judged under any severity slot.
func validateBatch(s *Stream, b *BatchInput) error {
	for _, ref := range []Ref{s.Normal, s.Tightened, s.Reduced} {
		if ref.Plan == nil {
			continue
		}
		pl := ref.Plan
		if pl.Kind == plan.KindSingle {
			if b.HasD2 {
				return fieldErr("d2", "not allowed for a single-sampling plan")
			}
			if b.D1 < 0 || b.D1 > pl.SampleSize {
				return fieldErr("d1", "must be within [0,n]")
			}
			continue
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
	}
	return nil
}

type vfe struct{ f, m string }

func (e vfe) Error() string            { return e.f + ": " + e.m }
func (e vfe) Fields() (string, string) { return e.f, e.m }
func fieldErr(f, m string) error       { return vfe{f, m} }
