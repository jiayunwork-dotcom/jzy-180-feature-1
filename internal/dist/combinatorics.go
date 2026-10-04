// Package dist provides log-domain combinatorics and the three
// defect-count probability models used by the OC engine:
//
//   - binomial: unbounded lot, each sampled unit independent with P(defect)=p;
//   - hypergeometric: finite lot of size N holding K defectives, sampling
//     without replacement; the double-sampling second stage uses the
//     CONDITIONAL hypergeometric (after n1 units and d1 defectives are
//     removed, the remaining lot is (N-n1, K-d1));
//   - poisson: the classical rare-event approximation, flagged approximate.
//
// Every probability is computed and summed in the log domain. Out-of-range
// combinatorial inputs return -Inf instead of panicking or silently
// becoming zero, so impossible events are never mistaken for rare ones.
package dist

import (
	"math"
)

// LogComb returns the natural log of C(n,k). Out-of-range inputs (n<0,
// k<0 or k>n) yield -Inf, the log of an impossible event.
func LogComb(n, k int) float64 {
	if n < 0 || k < 0 || k > n {
		return math.Inf(-1)
	}
	if k == 0 || k == n {
		return 0
	}
	// Symmetry cuts the number of lgamma/lgamma-style terms and the
	// accumulated rounding for lopsided k.
	if k > n-k {
		k = n - k
	}
	// log C(n,k) = sum_{i=1..k} log(n-k+i) - log(i), accumulated in a
	// single pass; exact for all representable terms and safe for n in
	// the thousands (no integer factorial is ever formed).
	l := 0.0
	for i := 1; i <= k; i++ {
		l += math.Log(float64(n-k+i)) - math.Log(float64(i))
	}
	return l
}

// logSumExp sums log-probabilities without leaving the log domain.
// An empty slice is -Inf (log of an empty union of events).
func logSumExp(ls []float64) float64 {
	m := math.Inf(-1)
	for _, v := range ls {
		if v > m {
			m = v
		}
	}
	if math.IsInf(m, -1) {
		return m
	}
	var s float64
	for _, v := range ls {
		if math.IsInf(v, -1) {
			continue
		}
		s += math.Exp(v - m)
	}
	return m + math.Log(s)
}
