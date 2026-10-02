package oc

import (
	"math"

	"sampling-svc/internal/dist"
	"sampling-svc/internal/plan"
)

// Probabilities holds the full decomposition of one double-sampling
// evaluation. For single plans only Pa is meaningful.
type Probabilities struct {
	Pa    float64 `json:"pa"`
	P1Acc float64 `json:"p1_acc,omitempty"` // accept on first sample
	P1Rej float64 `json:"p1_rej,omitempty"` // reject on first sample
	P2    float64 `json:"p2,omitempty"`     // second sample required
	P2Rej float64 `json:"p2_rej,omitempty"` // final reject via second sample
}

func expLog(l float64) float64 {
	if math.IsInf(l, -1) {
		return 0
	}
	return math.Exp(l)
}

// Evaluate returns acceptance probability (and the double-plan
// decomposition) at defective fraction p.
func Evaluate(pl *plan.Plan, p float64) Probabilities {
	if pl.Kind == plan.KindSingle {
		m := dist.NewModel(pl.Distribution, pl.SampleSize, pl.N, p)
		return Probabilities{Pa: m.Cdf(pl.AcceptNumber)}
	}
	return evaluateDouble(pl, p)
}

func evaluateDouble(pl *plan.Plan, p float64) Probabilities {
	m1 := dist.NewModel(pl.Distribution, pl.N1, pl.N, p)
	m2 := dist.NewModel2(pl.Distribution, pl.N1, pl.N2, pl.N, p)

	var lAcc, l1Acc, l1Rej, l2, l2Rej []float64
	for d1 := 0; d1 <= pl.N1; d1++ {
		lp1 := m1.LogPmf(d1)
		if math.IsInf(lp1, -1) {
			continue
		}
		switch {
		case d1 <= pl.C1:
			l1Acc = append(l1Acc, lp1)
			lAcc = append(lAcc, lp1)
		case d1 >= pl.R1:
			l1Rej = append(l1Rej, lp1)
		default: // c1 < d1 < r1: second sample
			l2 = append(l2, lp1)
			m2.GivenFirst(d1)
			lp2Acc := dist.RangeLog(m2, 0, pl.C2-d1)
			if !math.IsInf(lp2Acc, -1) {
				lAcc = append(lAcc, lp1+lp2Acc)
			}
			switch {
			case lp2Acc == 0:
				// second stage always accepts -> never rejected here
			case math.IsInf(lp2Acc, -1):
				l2Rej = append(l2Rej, lp1)
			default:
				l2Rej = append(l2Rej, lp1+math.Log1p(-math.Exp(lp2Acc)))
			}
		}
	}
	sum := func(ls []float64) float64 { return expLog(logSum(ls)) }
	return Probabilities{
		Pa:    sum(lAcc),
		P1Acc: sum(l1Acc),
		P1Rej: sum(l1Rej),
		P2:    sum(l2),
		P2Rej: sum(l2Rej),
	}
}

// ASN is the average sample number of a double plan:
// n1 + n2 * P(second sample needed). It lies in [n1, n1+n2].
// A single plan returns n.
func ASN(pl *plan.Plan, p float64) float64 {
	if pl.Kind == plan.KindSingle {
		return float64(pl.SampleSize)
	}
	pr := evaluateDouble(pl, p)
	return float64(pl.N1) + float64(pl.N2)*pr.P2
}
