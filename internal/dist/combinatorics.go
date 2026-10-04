// Package dist holds the log-domain combinatorial primitives and the
// binomial / poisson / hypergeometric probability models used by the OC
// engine.
//
// Every combinatorial quantity and probability is computed in the log
// domain: LogComb uses lgamma so n up to several thousand never overflows,
// and impossible events are represented by -Inf rather than a silently
// underflowed zero. Probabilities are exponentized at most once via
// log-sum-exp.
package dist

import "math"

// LogComb returns log( C(n,k) ) = log of "n choose k".
// Out-of-range inputs (n<0, k<0, k>n) yield -Inf: an impossible event
// must never quietly become a zero that gets folded into a probability mass.
func LogComb(n, k int) float64 {
	if n < 0 || k < 0 || k > n {
		return math.Inf(-1)
	}
	lg, _ := math.Lgamma(float64(n) + 1)
	lk, _ := math.Lgamma(float64(k) + 1)
	lnk, _ := math.Lgamma(float64(n-k) + 1)
	return lg - lk - lnk
}

// logSumExp returns log(sum_i exp(x_i)) in a numerically stable form.
// Empty and all--Inf slices yield -Inf.
func logSumExp(xs []float64) float64 {
	m := math.Inf(-1)
	for _, x := range xs {
		if x > m {
			m = x
		}
	}
	if math.IsInf(m, -1) {
		return math.Inf(-1)
	}
	var sum float64
	for _, x := range xs {
		sum += math.Exp(x - m)
	}
	return m + math.Log(sum)
}

// logPMFer is implemented by every single-stage distribution (Model and
// Model2); it exposes the support bounds and log-domain PMF.
type logPMFer interface {
	Support() (lo, hi int)
	LogPmf(k int) float64
}

// RangeLog returns log( sum_{k=lo}^{hi} exp(m.LogPmf(k)) ), clipping the
// requested range to the model's support. It is -Inf when the range
// contains no mass.
func RangeLog(m logPMFer, lo, hi int) float64 {
	slo, shi := m.Support()
	if lo < slo {
		lo = slo
	}
	if hi > shi {
		hi = shi
	}
	if lo > hi {
		return math.Inf(-1)
	}
	terms := make([]float64, 0, hi-lo+1)
	for k := lo; k <= hi; k++ {
		terms = append(terms, m.LogPmf(k))
	}
	return logSumExp(terms)
}
