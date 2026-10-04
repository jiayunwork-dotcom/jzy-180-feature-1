package dist

import (
	"math"

	"sampling-svc/internal/plan"
)

// Model is the defect-count distribution of one sampling stage.
//
// Probabilities are read in the log domain (LogPmf); Cdf and RangeLog
// sum without leaving it. Support reports the integer range over which
// the model assigns non-zero mass.
type Model interface {
	// LogPmf returns log P(X=k); out-of-support k give -Inf.
	LogPmf(k int) float64
	// Cdf returns P(X <= k).
	Cdf(k int) float64
	// Support returns the inclusive [lo,hi] range of possible counts;
	// lo > hi means an empty support.
	Support() (lo, hi int)
}

// Stage2 is the second-stage model of a double-sampling plan. It is
// parameterized by the observed first-stage defect count d1.
type Stage2 interface {
	Model
	// GivenFirst fixes the conditional second-stage distribution after
	// n1 units containing d1 defectives were drawn.
	GivenFirst(d1 int)
}

// NewModel builds the single-sample (or first-stage) model.
//
// lot must be non-nil for the hypergeometric distribution; callers
// validate this when resolving the distribution.
func NewModel(d Distribution, n int, lot *int, p float64) Model {
	switch d {
	case DistHypergeometric:
		N := 0
		if lot != nil {
			N = *lot
		}
		return &hyperModel{n: n, N: N, K: lotDefectives(N, p)}
	case DistPoisson:
		return &poissonModel{n: n, p: p}
	default:
		return &binomModel{n: n, p: p}
	}
}

// NewModel2 builds the conditional second-stage model. For binomial and
// poisson models the stages are independent, so GivenFirst is a no-op;
// for hypergeometric sampling it shrinks the remaining lot to
// (N-n1, K-d1).
func NewModel2(d Distribution, n1, n2 int, lot *int, p float64) Stage2 {
	switch d {
	case DistHypergeometric:
		N := 0
		if lot != nil {
			N = *lot
		}
		return &condHyperModel{n1: n1, n2: n2, N: N, K: lotDefectives(N, p)}
	case DistPoisson:
		return &poissonModel{n: n2, p: p}
	default:
		return &binomModel{n: n2, p: p}
	}
}

// Distribution re-exports the plan package's model selector so callers
// need not import both packages just to name a distribution.
type Distribution = plan.Distribution

const (
	DistHypergeometric = plan.DistHypergeometric
	DistBinomial       = plan.DistBinomial
	DistPoisson        = plan.DistPoisson
)

// lotDefectives resolves the integer number K of defectives in a lot of
// size N under fraction p. The exact endpoints p=0 and p=1 map to K=0
// and K=N so endpoint probabilities are exact rather than rounded.
func lotDefectives(N int, p float64) int {
	switch {
	case p <= 0:
		return 0
	case p >= 1:
		return N
	default:
		return int(math.Round(float64(N) * p))
	}
}

// RangeLog returns log P(lo <= X <= hi). An inverted range or one that
// misses the support entirely is -Inf.
func RangeLog(m Model, lo, hi int) float64 {
	if lo > hi {
		return math.Inf(-1)
	}
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

// cdf is the shared log-domain CDF implementation.
func cdf(m Model, k int) float64 {
	if k < 0 {
		return 0
	}
	lo, hi := m.Support()
	if k >= hi {
		// Full mass (LogPmf over the whole support is normalized) but
		// return it through the same summation so endpoint cases where
		// the support itself degenerates stay exact (p=0 -> 1, p=1 -> 0).
		if lo > hi {
			return 0
		}
		return math.Exp(RangeLog(m, lo, hi))
	}
	return math.Exp(RangeLog(m, lo, k))
}

// --- binomial ---

type binomModel struct {
	n int
	p float64
}

func (m *binomModel) Support() (int, int) {
	if m.n < 0 {
		return 0, -1
	}
	// The combinatorial support is [0,n]; the p=0/p=1 endpoint models
	// collapse it to a single point via -Inf LogPmf values.
	return 0, m.n
}

func (m *binomModel) LogPmf(k int) float64 {
	if k < 0 || k > m.n {
		return math.Inf(-1)
	}
	switch {
	case m.p == 0:
		if k == 0 {
			return 0
		}
		return math.Inf(-1)
	case m.p == 1:
		if k == m.n {
			return 0
		}
		return math.Inf(-1)
	default:
		return LogComb(m.n, k) +
			float64(k)*math.Log(m.p) +
			float64(m.n-k)*math.Log1p(-m.p)
	}
}

func (m *binomModel) Cdf(k int) float64 { return cdf(m, k) }

// GivenFirst is a no-op: second-stage draws are independent Bernoulli.
func (m *binomModel) GivenFirst(d1 int) {}

// --- poisson approximation ---

type poissonModel struct {
	n int
	p float64
}

func (m *poissonModel) Support() (int, int) {
	// The theoretical support is unbounded; defect counts above the
	// sample size never arise in acceptance sampling.
	return 0, m.n
}

func (m *poissonModel) lambda() float64 { return float64(m.n) * m.p }

func (m *poissonModel) LogPmf(k int) float64 {
	if k < 0 || k > m.n {
		return math.Inf(-1)
	}
	switch {
	case m.p == 1:
		// Infinite mean: no finite count carries mass, so every bounded
		// CDF is exactly 0, matching the other models at p=1.
		return math.Inf(-1)
	case m.p == 0:
		if k == 0 {
			return 0
		}
		return math.Inf(-1)
	default:
		lambda := m.lambda()
		lg, _ := math.Lgamma(float64(k + 1))
		return float64(k)*math.Log(lambda) - lambda - lg
	}
}

func (m *poissonModel) Cdf(k int) float64 { return cdf(m, k) }

// GivenFirst is a no-op under the independent-arrival approximation.
func (m *poissonModel) GivenFirst(d1 int) {}

// --- hypergeometric (single / first stage) ---

type hyperModel struct {
	n int // sample size
	N int // lot size
	K int // defectives in the lot
}

func (m *hyperModel) Support() (int, int) {
	lo := m.n - (m.N - m.K)
	if lo < 0 {
		lo = 0
	}
	hi := m.n
	if m.K < hi {
		hi = m.K
	}
	if lo > hi || m.N <= 0 || m.K < 0 || m.K > m.N {
		return 0, -1
	}
	return lo, hi
}

func (m *hyperModel) LogPmf(k int) float64 {
	// log C(K,k) + log C(N-K, n-k) - log C(N, n)
	return LogComb(m.K, k) + LogComb(m.N-m.K, m.n-k) - LogComb(m.N, m.n)
}

func (m *hyperModel) Cdf(k int) float64 { return cdf(m, k) }

// --- conditional hypergeometric (second stage of double sampling) ---

// After the first stage removes n1 units, d1 of them defective, the
// remaining lot holds N-n1 units of which K-d1 are defective; the
// second sample of n2 is drawn from that remainder.
type condHyperModel struct {
	n1, n2 int
	N, K   int

	remN, remK int
	ready      bool
}

func (m *condHyperModel) GivenFirst(d1 int) {
	m.remN = m.N - m.n1
	m.remK = m.K - d1
	m.ready = true
}

func (m *condHyperModel) Support() (int, int) {
	if !m.ready || m.remN <= 0 || m.remK < 0 || m.remK > m.remN {
		return 0, -1
	}
	lo := m.n2 - (m.remN - m.remK)
	if lo < 0 {
		lo = 0
	}
	hi := m.n2
	if m.remK < hi {
		hi = m.remK
	}
	if lo > hi {
		return 0, -1
	}
	return lo, hi
}

func (m *condHyperModel) LogPmf(k int) float64 {
	if !m.ready {
		return math.Inf(-1)
	}
	// log C(K-d1, k) + log C((N-n1)-(K-d1), n2-k) - log C(N-n1, n2)
	return LogComb(m.remK, k) +
		LogComb(m.remN-m.remK, m.n2-k) -
		LogComb(m.remN, m.n2)
}

func (m *condHyperModel) Cdf(k int) float64 { return cdf(m, k) }
