package inspection

import (
	"sampling-svc/internal/plan"
)

// LotDecision applies the acceptance rules of one plan to a recorded lot.
func LotDecision(pl *plan.Plan, d1 int, hasD2 bool, d2 int) (bool, error) {
	if d1 < 0 || d1 > sampleSize(pl, 1) {
		return false, plan.FieldError{Field: "d1",
			Message: "defect count must be within [0, first sample size]"}
	}
	if pl.Kind == plan.KindSingle {
		if hasD2 {
			return false, plan.FieldError{Field: "d2",
				Message: "second sample defect count not allowed for a single plan"}
		}
		return d1 <= pl.AcceptNumber, nil
	}
	// Double
	if d1 <= pl.C1 {
		return true, nil
	}
	if d1 >= pl.R1 {
		return false, nil
	}
	// Grey zone c1 < d1 < r1: second sample mandatory.
	if !hasD2 {
		return false, plan.FieldError{Field: "d2",
			Message: "second sample defect count required when c1 < d1 < r1"}
	}
	if d2 < 0 || d2 > pl.N2 {
		return false, plan.FieldError{Field: "d2",
			Message: "second-sample defect count must be within [0, n2]"}
	}
	return d1+d2 <= pl.C2, nil
}

func sampleSize(pl *plan.Plan, stage int) int {
	if pl.Kind == plan.KindSingle {
		return pl.SampleSize
	}
	if stage == 1 {
		return pl.N1
	}
	return pl.N2
}

// needsSecond reports whether a first-sample count lands in the grey zone.
func needsSecond(pl *plan.Plan, d1 int) bool {
	return pl.Kind == plan.KindDouble && d1 > pl.C1 && d1 < pl.R1
}
