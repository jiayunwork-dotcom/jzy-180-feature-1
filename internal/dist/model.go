package dist

import (
	"math"

	"sampling-svc/internal/plan"
)

// kind is the probability model backing a Model.
type kind int

const (
	kBinomial kind = iota
	kPoisson
	kHypergeometric
)

// Model is the distribution of the number of defectives found in one
// sample. Lot parameters are stored explicitly so the degenerate cases
// p==0 (mass only at zero) and p==1 (mass only at n) are exact for every
// distribution rather than the limit of a rounded computation.
type Model struct {
	k kind
	n int
	p float64
	// hypergeometric parameters (finite lot)
	N, K, drawn int
	// poisson rate; n2size is the declared sample size cap
	lambda float64
}

// NewModel builds the model for a single sample of size n from a lot of
// size lot (nil = unbounded) at defective fraction p.
func NewModel(d plan.Distribution, n int, lot *int, p float64) *Model {
	m := &Model{n: n, p: p}
	switch d {
	case plan.DistPoisson:
		m.k = kPoisson
		m.lambda = float64(n) * p
	default:
		if lot != nil {
			m.k = kHypergeometric
			m.N = *lot
			m.K = roundDefectives(p, *lot)
			m.drawn = n
		} else {
			m.k = kBinomial
		}
	}
	return m
}

// roundDefectives maps a defective fraction to an integer number of
// defective items in the lot, clipped to [0,N].
func roundDefectives(p float64, N int) int {
	K := int(math.Round(p * float64(N)))
	if K < 0 {
		return 0
	}
	if K > N {
		return N
	}
	return K
}

// Support returns the inclusive [lo,hi] range of counts carrying nonzero
// mass. lo > hi means an empty support.
func (m *Model) Support() (lo, hi int) {
	switch m.k {
	case kBinomial:
		switch {
		case m.p == 0:
			return 0, 0
		case m.p == 1:
			return m.n, m.n
		default:
			return 0, m.n
		}
	case kPoisson:
		if m.p == 0 {
			return 0, 0
		}
		// The poisson model stands in for the count in a sample of n, so
		// counts beyond n cannot occur from the caller's point of view.
		return 0, m.n
	default:
		lo = max(0, m.drawn-(m.N-m.K))
		hi = min(m.drawn, m.K)
		return lo, hi
	}
}

// LogPmf returns log P(X == k); out-of-support counts are -Inf.
func (m *Model) LogPmf(k int) float64 {
	lo, hi := m.Support()
	if k < lo || k > hi {
		return math.Inf(-1)
	}
	switch m.k {
	case kBinomial:
		// Degenerate p: the support already pinned the mass to one point.
		if m.p == 0 || m.p == 1 {
			return 0
		}
		return LogComb(m.n, k) +
			float64(k)*math.Log(m.p) +
			float64(m.n-k)*math.Log1p(-m.p)
	case kPoisson:
		// p==0 is degenerate at zero (lambda==0).
		if m.p == 0 {
			return 0
		}
		return float64(k)*math.Log(m.lambda) - m.lambda - lfact(k)
	default:
		// C(K,k) C(N-K, n-k) / C(N, n)
		return LogComb(m.K, k) +
			LogComb(m.N-m.K, m.drawn-k) -
			LogComb(m.N, m.drawn)
	}
}

// Cdf returns P(X <= c). c below the support yields exactly 0, c at or
// above the support yields exactly 1.
func (m *Model) Cdf(c int) float64 {
	lo, hi := m.Support()
	if lo > hi || c < lo {
		return 0
	}
	if c >= hi {
		return 1
	}
	return math.Exp(RangeLog(m, lo, c))
}

// --- second-stage models ---

// Model2 is the distribution of the defective count in the SECOND sample
// of a double-sampling scheme. For binomial/poisson it is just the
// independent-model law on n2; GivenFirst is a no-op. For hypergeometric
// it is the conditional hypergeometric on the lot remaining after drawing
// the first sample: lot size N-n1, defectives K-d1.
type Model2 struct {
	k    kind
	n1   int
	n2   int
	p    float64
	N    int // remaining lot size (hypergeom)
	K    int // remaining defectives (hypergeom)
	lot0 int // original lot size, kept for reconditioning
	lotK int
}

// NewModel2 builds the unconditional second-stage model; call GivenFirst
// before each evaluation to condition the hypergeometric parameters.
func NewModel2(d plan.Distribution, n1, n2 int, lot *int, p float64) *Model2 {
	m := &Model2{n1: n1, n2: n2, p: p}
	switch d {
	case plan.DistPoisson:
		m.k = kPoisson
	default:
		if lot != nil {
			m.k = kHypergeometric
			m.lot0 = *lot
			m.lotK = roundDefectives(p, *lot)
			m.N = *lot - n1
			m.K = m.lotK
		} else {
			m.k = kBinomial
		}
	}
	return m
}

// GivenFirst conditions the second sample on having drawn d1 defectives
// in the first sample. No-op for the independent (unbounded-lot) models.
func (m *Model2) GivenFirst(d1 int) {
	if m.k != kHypergeometric {
		return
	}
	m.N = m.lot0 - m.n1
	m.K = m.lotK - d1
}

// Support returns the inclusive second-sample support.
func (m *Model2) Support() (lo, hi int) {
	switch m.k {
	case kBinomial:
		switch {
		case m.p == 0:
			return 0, 0
		case m.p == 1:
			return m.n2, m.n2
		default:
			return 0, m.n2
		}
	case kPoisson:
		if m.p == 0 {
			return 0, 0
		}
		return 0, m.n2
	default:
		lo = max(0, m.n2-(m.N-m.K))
		hi = min(m.n2, m.K)
		return lo, hi
	}
}

// LogPmf returns log P(D2 == k | D1=d1); out-of-support counts are -Inf.
func (m *Model2) LogPmf(k int) float64 {
	lo, hi := m.Support()
	if k < lo || k > hi {
		return math.Inf(-1)
	}
	switch m.k {
	case kBinomial:
		if m.p == 0 || m.p == 1 {
			return 0
		}
		return LogComb(m.n2, k) +
			float64(k)*math.Log(m.p) +
			float64(m.n2-k)*math.Log1p(-m.p)
	case kPoisson:
		if m.p == 0 {
			return 0
		}
		lambda := float64(m.n2) * m.p
		return float64(k)*math.Log(lambda) - lambda - lfact(k)
	default:
		return LogComb(m.K, k) +
			LogComb(m.N-m.K, m.n2-k) -
			LogComb(m.N, m.n2)
	}
}

// lfact returns log(k!).
func lfact(k int) float64 {
	l, _ := math.Lgamma(float64(k) + 1)
	return l
}
