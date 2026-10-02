package oc

import (
	"errors"
	"math"

	"sampling-svc/internal/plan"
)

// AOQ is the average outgoing quality under rectifying inspection:
// rejected lots are 100% inspected and defectives removed, so
// AOQ(p) = p * Pa * (N-n_inspected) / N (remaining-fraction form).
// Requires a finite lot size.
func AOQ(pl *plan.Plan, p float64, pr Probabilities) (float64, error) {
	if pl.N == nil {
		return 0, errors.New("AOQ requires a finite lot size N")
	}
	nInspected := inspectedSize(pl, pr)
	return p * pr.Pa * (float64(*pl.N) - nInspected) / float64(*pl.N), nil
}

// ATI is the average total inspection count under rectifying inspection,
// bounded by the minimum inspection effort and the lot size.
func ATI(pl *plan.Plan, p float64, pr Probabilities) (float64, error) {
	if pl.N == nil {
		return 0, errors.New("ATI requires a finite lot size N")
	}
	N := float64(*pl.N)
	nInspected := inspectedSize(pl, pr)
	return nInspected + (N-nInspected)*(1-pr.Pa), nil
}

func inspectedSize(pl *plan.Plan, pr Probabilities) float64 {
	if pl.Kind == plan.KindSingle {
		return float64(pl.SampleSize)
	}
	// n1 + n2 * P(second sample is actually drawn)
	return float64(pl.N1) + float64(pl.N2)*pr.P2
}

// AOQLResult is the maximum of the AOQ curve over p in [0,1].
type AOQLResult struct {
	AOQL float64 `json:"aoql"`
	P    float64 `json:"p"`
}

// AOQL locates max_p AOQ(p). A dense scan brackets the maximizer, then
// golden-section refinement drives it to high precision; AOQ is zero at
// both ends and unimodal for sampling plans.
func AOQL(pl *plan.Plan) (AOQLResult, error) {
	if pl.N == nil {
		return AOQLResult{}, errors.New("AOQL requires a finite lot size N")
	}
	const grid = 2049
	f := func(p float64) float64 {
		pr := Evaluate(pl, p)
		v, _ := AOQ(pl, p, pr)
		return v
	}
	bestP := 0.0
	bestV := 0.0
	step := 1.0 / (grid - 1)
	for i := 1; i < grid-1; i++ {
		p := float64(i) * step
		if v := f(p); v > bestV {
			bestV, bestP = v, p
		}
	}
	lo := math.Max(0, bestP-step)
	hi := math.Min(1, bestP+step)
	g := goldenMax(f, lo, hi, 1e-12)
	gv := f(g)
	if gv >= bestV {
		bestV, bestP = gv, g
	}
	return AOQLResult{AOQL: bestV, P: bestP}, nil
}

// goldenMax maximizes f on [lo,hi] (f assumed unimodal on the interval).
func goldenMax(f func(float64) float64, lo, hi, tol float64) float64 {
	gr := (math.Sqrt(5) - 1) / 2 // 0.618...
	c := hi - gr*(hi-lo)
	d := lo + gr*(hi-lo)
	fc, fd := f(c), f(d)
	for hi-lo > tol {
		if fc > fd {
			hi, d, fd = d, c, fc
			c = hi - gr*(hi-lo)
			fc = f(c)
		} else {
			lo, c, fc = c, d, fd
			d = lo + gr*(hi-lo)
			fd = f(d)
		}
	}
	return (lo + hi) / 2
}
