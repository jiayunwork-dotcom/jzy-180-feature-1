package design

import (
	"math"
	"sort"

	"sampling-svc/internal/dist"
	"sampling-svc/internal/oc"
	"sampling-svc/internal/plan"
)

// DoubleN2Mode restricts the second sample size.
type DoubleN2Mode string

const (
	N2EqualN1 DoubleN2Mode = "equal" // n2 = n1
	N2TwiceN1 DoubleN2Mode = "twice" // n2 = 2*n1
)

type stagePMF struct {
	logpmf []float64 // indexed by defect count, log-domain
	lo, hi int       // support bounds
}

func (s stagePMF) prob(k int) float64 {
	if k < s.lo || k > s.hi {
		return 0
	}
	return math.Exp(s.logpmf[k])
}

// buildPMFs tabulates the first-stage pmf and the per-d1 second-stage
// prefix pref[d1][t+1] = P(D2 <= t | D1=d1). Stage pmfs sum to 1, so
// entries are exponentiated once and never underflow as a body.
func buildPMFs(d plan.Distribution, n1, n2 int, lot *int, p float64) (stagePMF, [][]float64) {
	m1 := dist.NewModel(d, n1, lot, p)
	lo1, hi1 := m1.Support()
	if lo1 > hi1 {
		return stagePMF{lo: 0, hi: -1, logpmf: make([]float64, n1+1)}, nil
	}
	first := stagePMF{lo: lo1, hi: hi1, logpmf: make([]float64, n1+1)}
	for d1 := 0; d1 <= n1; d1++ {
		first.logpmf[d1] = m1.LogPmf(d1)
	}
	m2 := dist.NewModel2(d, n1, n2, lot, p)
	pref := make([][]float64, n1+1)
	for d1 := lo1; d1 <= hi1; d1++ {
		m2.GivenFirst(d1)
		slo, shi := m2.Support()
		arr := make([]float64, n2+2)
		var run float64
		for d2 := 0; d2 <= n2; d2++ {
			if d2 >= slo && d2 <= shi {
				run += math.Exp(m2.LogPmf(d2))
			}
			arr[d2+1] = run
		}
		pref[d1] = arr
	}
	return first, pref
}

// firstCDF returns F1[k] = P(D1 <= k) for k in [0,n1]; F1[n1] == 1.
func firstCDF(first stagePMF, n1 int) []float64 {
	f := make([]float64, n1+1)
	var run float64
	for k := 0; k <= n1; k++ {
		run += first.prob(k)
		f[k] = run
	}
	return f
}

// gPrefixes returns, for a fixed c2, arrays over k in [0,n1]:
//
//	G[k] = sum_{d1<=k} p1[d1]*P(D1+D2<=c2|d1)
//	H[k] = sum_{d1<=k} p1[d1]              (= F1, used for P2)
//
// Pa(c1,r1) = F1[c1] - G[c1] + G[r1-1]
// P2(c1,r1) = H[r1-1] - H[c1]
func gPrefixes(first stagePMF, pref [][]float64, n1, n2, c2 int) (g, h []float64) {
	g = make([]float64, n1+1)
	h = make([]float64, n1+1)
	var cg, ch float64
	for d1 := 0; d1 <= n1; d1++ {
		p1 := first.prob(d1)
		ch += p1
		t := c2 - d1
		if t >= 0 && pref[d1] != nil {
			idx := t + 1
			if idx > n2+1 {
				idx = n2 + 1
			}
			cg += p1 * pref[d1][idx]
		}
		g[d1], h[d1] = cg, ch
	}
	return g, h
}

func getAt(arr []float64, k int) float64 {
	if k < 0 {
		return 0
	}
	if k >= len(arr) {
		return arr[len(arr)-1]
	}
	return arr[k]
}

type doubleCandidate struct {
	n1, c1, r1, n2, c2 int
	paA, paL, asnA     float64
}

// Double enumerates double plans with n2 = n1 or n2 = 2*n1 and returns
// the feasible one minimizing ASN at AQL.
//
// Enumeration is reduced to O(n1^2 log n1) per n1 using monotonicity:
// for fixed c2 and c1, Pa and P2 are both nondecreasing in r1, so the
// feasible r1 range (if any) is contiguous and its smallest member both
// satisfies the consumer constraint and minimizes ASN. It is located by
// a lower-bound binary search on the G prefix.
func Double(in Params, mode DoubleN2Mode, n1Max int) (*Result, error) {
	d, errs := validateParams(in)
	if len(errs) > 0 {
		return nil, errs
	}
	if mode == "" {
		mode = N2EqualN1
	}
	if mode != N2EqualN1 && mode != N2TwiceN1 {
		return nil, plan.ValidationErrors{{Field: "n2_mode",
			Message: "must be 'equal' or 'twice'"}}
	}
	if n1Max <= 0 {
		n1Max = DefaultDoubleN1Max
	}
	if n1Max > HardDoubleN1Max {
		n1Max = HardDoubleN1Max
	}
	const eps = 1e-12
	target := 1 - in.Alpha

	var best *doubleCandidate
	for n1 := 1; n1 <= n1Max; n1++ {
		n2 := n1
		if mode == N2TwiceN1 {
			n2 = 2 * n1
		}
		if in.N != nil && n1+n2 > *in.N {
			break
		}
		firstA, prefA := buildPMFs(d, n1, n2, in.N, in.AQL)
		firstL, prefL := buildPMFs(d, n1, n2, in.N, in.LTPD)
		if firstA.lo > firstA.hi || firstL.lo > firstL.hi {
			continue
		}
		fA := firstCDF(firstA, n1) // first-stage CDF at AQL
		fL := firstCDF(firstL, n1) // first-stage CDF at LTPD

		// Every valid plan accepts first-sample d1<=c1 outright, and
		// c1>=0, so Pa(LTPD) >= P(D1=0). If even that exceeds beta, no
		// rule at this n1 can meet the consumer constraint.
		if fL[0] > in.Beta+eps {
			continue
		}
		// c1 cannot exceed the largest first-stage accept count whose
		// LTPD CDF is still within beta (c1 is always <= n1-1).
		c1max := sort.Search(n1, func(k int) bool { return fL[k] > in.Beta+eps }) - 1
		if c1max < 0 {
			continue
		}
		maxTotal := n1 + n2 - 1

		// c2Lo: smallest c2 for which the loosest possible rule
		// (c1=r1-1=min(c2,n1-1), Pa = F1[that]) can meet the producer
		// point. Necessary condition on c2, monotone in c2.
		c2Lo := sort.Search(maxTotal+1, func(c2 int) bool {
			k := c2
			if k > n1-1 {
				k = n1 - 1
			}
			return fA[k] >= target-eps
		})
		if c2Lo > maxTotal {
			continue
		}

		for c2 := c2Lo; c2 <= maxTotal; c2++ {
			gA, hA := gPrefixes(firstA, prefA, n1, n2, c2)
			gL, _ := gPrefixes(firstL, prefL, n1, n2, c2)
			c1hi := c1max
			if c1hi > c2 {
				c1hi = c2
			}
			// idx = r1-1 ranges over [c1, min(c2,n1)]; idx=n1 means
			// r1=n1+1, i.e. the first stage never rejects outright.
			capIdx := c2
			if capIdx > n1 {
				capIdx = n1
			}
			for c1 := 0; c1 <= c1hi; c1++ {
				// Smallest idx with F1A[c1]-GA[c1]+GA[idx] >= target.
				base := fA[c1] - getAt(gA, c1)
				need := target - base
				idx := c1 + sort.Search(capIdx-c1+1, func(j int) bool {
					return getAt(gA, c1+j) >= need-eps
				})
				if idx > capIdx || getAt(gA, idx) < need-eps {
					continue // producer constraint unreachable at this c2
				}
				// Consumer check at the smallest (ASN-minimal) r1.
				paL := fL[c1] - getAt(gL, c1) + getAt(gL, idx)
				if paL > in.Beta+eps {
					continue
				}
				r1 := idx + 1
				paA := base + getAt(gA, idx)
				p2A := getAt(hA, idx) - getAt(hA, c1)
				asnA := float64(n1) + float64(n2)*p2A
				cand := doubleCandidate{
					n1: n1, c1: c1, r1: r1, n2: n2, c2: c2,
					paA: paA, paL: paL, asnA: asnA,
				}
				if best == nil || better(cand, *best) {
					c := cand
					best = &c
				}
			}
		}
	}
	if best == nil {
		return nil, ErrInfeasible
	}
	pl := &plan.Plan{
		Kind: plan.KindDouble, Distribution: d, Approximate: d == plan.DistPoisson,
		N:  in.N,
		N1: best.n1, C1: best.c1, R1: best.r1, N2: best.n2, C2: best.c2,
	}
	// Report risks from the single public OC code path.
	r := oc.ComputeRisks(pl, in.AQL, in.LTPD)
	return &Result{Plan: pl, PaAQL: r.PaAQL, PaLTPD: r.PaLTPD,
		Alpha: r.Alpha, Beta: r.Beta, ASNAQL: r.ASNAQL,
		Approximate: pl.Approximate}, nil
}

func better(a, b doubleCandidate) bool {
	const eps = 1e-15
	if math.Abs(a.asnA-b.asnA) > eps {
		return a.asnA < b.asnA
	}
	if a.n1+a.n2 != b.n1+b.n2 {
		return a.n1+a.n2 < b.n1+b.n2
	}
	if a.c1 != b.c1 {
		return a.c1 < b.c1
	}
	return a.r1 < b.r1
}
