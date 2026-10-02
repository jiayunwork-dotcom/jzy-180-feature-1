// Package design implements two-point plan search: given AQL/alpha and
// LTPD/beta constraints it finds the minimum-sample single plan or the
// minimum-ASN(at AQL) double plan satisfying both risks.
package design

import (
	"errors"
	"math"

	"sampling-svc/internal/plan"
)

// Default search caps. Both can be overridden per request (up to hard
// bounds) so a caller can pay for a wider search explicitly.
const (
	DefaultSingleNMax = 2000
	HardSingleNMax    = 20000

	DefaultDoubleN1Max = 200
	HardDoubleN1Max    = 1000
)

// ErrInfeasible signals that no plan within the search cap satisfies
// both risk constraints.
var ErrInfeasible = errors.New("no feasible plan within the search upper bound")

// Params are the common two-point design inputs.
type Params struct {
	AQL   float64
	Alpha float64 // producer risk in (0,1)
	LTPD  float64
	Beta  float64 // consumer risk in (0,1)
	N     *int    // optional finite lot size
	// Distribution: ""/auto picks hypergeometric with N, else binomial.
	Distribution string
}

// Result reports the designed plan and the risks it actually attains.
type Result struct {
	Plan        *plan.Plan
	PaAQL       float64
	PaLTPD      float64
	Alpha       float64
	Beta        float64
	ASNAQL      float64 // double plans only
	Approximate bool
}

func validateParams(in Params) (plan.Distribution, plan.ValidationErrors) {
	var errs plan.ValidationErrors
	check := func(v float64, field string, allowZeroOne bool) {
		if math.IsNaN(v) || v < 0 || v > 1 {
			errs = append(errs, plan.FieldError{Field: field, Message: "must be within [0,1]"})
		}
		_ = allowZeroOne
	}
	check(in.AQL, "aql", true)
	check(in.LTPD, "ltpd", true)
	if !(in.AQL < in.LTPD) {
		errs = append(errs, plan.FieldError{Field: "aql", Message: "must satisfy AQL < LTPD"})
	}
	if !(in.Alpha > 0 && in.Alpha < 1) {
		errs = append(errs, plan.FieldError{Field: "alpha", Message: "must be within (0,1)"})
	}
	if !(in.Beta > 0 && in.Beta < 1) {
		errs = append(errs, plan.FieldError{Field: "beta", Message: "must be within (0,1)"})
	}
	var d plan.Distribution
	switch plan.Distribution(in.Distribution) {
	case "", "auto":
		if in.N != nil {
			d = plan.DistHypergeometric
		} else {
			d = plan.DistBinomial
		}
	case plan.DistHypergeometric, plan.DistBinomial, plan.DistPoisson:
		d = plan.Distribution(in.Distribution)
	default:
		errs = append(errs, plan.FieldError{Field: "distribution",
			Message: "must be one of hypergeometric, binomial, poisson"})
	}
	if d == plan.DistHypergeometric && in.N == nil {
		errs = append(errs, plan.FieldError{Field: "distribution",
			Message: "hypergeometric requires lot size N"})
	}
	if in.N != nil && *in.N < 1 {
		errs = append(errs, plan.FieldError{Field: "n_lot", Message: "must be >= 1"})
	}
	return d, errs
}
