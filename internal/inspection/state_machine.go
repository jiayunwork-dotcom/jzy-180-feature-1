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
//
// When strict is true, a lot that cannot be judged under the revision in
// force for its (severity, inspection time) (e.g. d1 exceeds that
// revision's sample size) returns a BatchConflict instead of being
// silently accepted/rejected.
func applyBatch(st State, s *Stream, b *BatchInput, strict bool) (State, stepResult, *BatchConflict) {
	st = cloneState(st)
	res := stepResult{outcome: BatchOutcome{
		BatchID: b.ID, LotNo: b.LotNo, At: b.At, D1: b.D1, Score: st.Score,
	}}
	if b.HasD2 {
		d2 := b.D2
		res.outcome.D2 = &d2
	}

	if st.Severity == SeveritySuspended {
		// The lot is not judged, so no revision is consulted
		// (RevisionNo stays nil), but the outcome still names the plan
		// bound to the current severity slot for traceability.
		ref := s.planFor(st.Severity)
		res.outcome.Severity = SeveritySuspended
		res.outcome.PlanID = ref.ID
		if ref.Plan != nil {
			res.outcome.PlanName = ref.Plan.Name
		}
		res.outcome.Decision = DecisionNotInspected
		res.outcome.Accepted = false
		res.note = "inspection suspended; lot not inspected"
		return st, res, nil
	}

	ref := s.planFor(st.Severity)
	pl, revNo := ref.At(b.At)
	if revNo > 0 {
		n := revNo
		res.outcome.RevisionNo = &n
	}
	res.outcome.Severity = st.Severity
	res.outcome.PlanID = ref.ID
	res.outcome.PlanName = pl.Name

	if _, err := LotDecision(pl, b.D1, b.HasD2, b.D2); err != nil {
		if strict {
			return st, res, &BatchConflict{
				BatchID: b.ID, LotNo: b.LotNo, At: b.At,
				Severity:   st.Severity,
				PlanID:     ref.ID,
				RevisionNo: revNo,
				Reason:     err.Error(),
			}
		}
	}
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
			return st, res, nil
		}
		if st.Score >= 30 && st.stable && st.approved {
			// Score threshold plus both prerequisites -> reduced.
			st.Severity = SeverityReduced
			st.Score = 0
			res.note = "switch normal -> reduced"
			return st, res, nil
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
	return st, res, nil
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

// validateBatch checks a recorded lot against exactly the parameter set
// that could judge it: for each severity slot, the revision EFFECTIVE AT
// THE LOT'S INSPECTION TIME. (A back-dated lot whose own period used a
// larger sample size must be accepted even if a later revision shrank
// it.) Whether a lot ultimately lands on the normal, tightened or
// reduced slot is decided by the replay, so all three in-force revisions
// are candidates; a suspended lot is not judged at all.
func validateBatch(s *Stream, b *BatchInput) error {
	for _, ref := range []Ref{s.Normal, s.Tightened, s.Reduced} {
		var pl *plan.Plan
		if len(ref.Revisions) == 0 {
			pl = ref.Plan
		} else {
			pl, _ = ref.At(b.At)
		}
		if pl == nil {
			continue
		}
		if err := validateBatchAgainst(pl, b); err != nil {
			return err
		}
	}
	return nil
}

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

type vfe struct{ f, m string }

func (e vfe) Error() string            { return e.f + ": " + e.m }
func (e vfe) Fields() (string, string) { return e.f, e.m }
func fieldErr(f, m string) error       { return vfe{f, m} }
