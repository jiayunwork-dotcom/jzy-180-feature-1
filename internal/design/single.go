package design

import (
	"sampling-svc/internal/dist"
	"sampling-svc/internal/plan"
)

// Single finds the minimum-n single plan (smallest c on ties) such that
// Pa(AQL) >= 1-alpha and Pa(LTPD) <= beta.
//
// For a fixed n:
//
//	cMin(n) = smallest c with Pa(n,c,AQL)   >= 1-alpha
//	cMax(n) = largest  c with Pa(n,c,LTPD) <= beta
//
// n is feasible exactly when cMin(n) <= cMax(n). Pa(n,c,p) is the
// binomial/hypergeometric CDF, monotone in c, and this feasibility
// predicate is monotone in n (as n grows the two OC points separate), so
// the minimum n is found by binary search; both boundary c's come from
// binary searches over [0,n-1].
func Single(in Params, nMax int) (*Result, error) {
	d, errs := validateParams(in)
	if len(errs) > 0 {
		return nil, errs
	}
	if nMax <= 0 {
		nMax = DefaultSingleNMax
	}
	if nMax > HardSingleNMax {
		nMax = HardSingleNMax
	}

	cdf := func(n, c int, p float64) float64 {
		return dist.NewModel(d, n, in.N, p).Cdf(c)
	}
	producerOK := 1 - in.Alpha

	// cMin: smallest c in [0,n-1] meeting the producer point.
	cMin := func(n int) int {
		lo, hi := 0, n-1
		for lo < hi {
			mid := (lo + hi) / 2
			if cdf(n, mid, in.AQL) >= producerOK {
				hi = mid
			} else {
				lo = mid + 1
			}
		}
		return lo
	}
	// cMax: largest c in [0,n-1] meeting the consumer point; -1 if none.
	cMax := func(n int) int {
		lo, hi := -1, n-1
		for lo < hi {
			mid := (lo + hi + 1) / 2
			if cdf(n, mid, in.LTPD) <= in.Beta {
				lo = mid
			} else {
				hi = mid - 1
			}
		}
		return lo
	}
	feasible := func(n int) bool {
		return cdf(n, cMin(n), in.AQL) >= producerOK && cMin(n) <= cMax(n)
	}

	if !feasible(nMax) {
		return nil, ErrInfeasible
	}
	lo, hi := 1, nMax
	for lo < hi {
		mid := (lo + hi) / 2
		if feasible(mid) {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	n := lo
	c := cMin(n)
	if cdf(n, c, in.LTPD) > in.Beta { // defensive: predicate must guarantee this
		return nil, ErrInfeasible
	}
	pl := &plan.Plan{
		Kind: plan.KindSingle, Distribution: d, Approximate: d == plan.DistPoisson,
		N: in.N, SampleSize: n, AcceptNumber: c,
	}
	paA := cdf(n, c, in.AQL)
	paL := cdf(n, c, in.LTPD)
	return &Result{Plan: pl, PaAQL: paA, PaLTPD: paL,
		Alpha: 1 - paA, Beta: paL, Approximate: pl.Approximate}, nil
}
