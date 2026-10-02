package oc

import (
	"math"

	"sampling-svc/internal/plan"
)

// logSum is the OC-layer alias so probability sums stay in log domain.
func logSum(ls []float64) float64 {
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
		s += math.Exp(v - m)
	}
	return m + math.Log(s)
}

// Risks reports producer risk alpha = 1-Pa(AQL) and consumer risk
// beta = Pa(LTPD).
type Risks struct {
	AQL    float64 `json:"aql"`
	LTPD   float64 `json:"ltpd"`
	PaAQL  float64 `json:"pa_aql"`
	PaLTPD float64 `json:"pa_ltpd"`
	Alpha  float64 `json:"alpha"`
	Beta   float64 `json:"beta"`
	// ASNAQL is the average sample number at AQL (double plans only).
	ASNAQL float64 `json:"asn_aql,omitempty"`
}

// ComputeRisks evaluates both risk points of a plan.
func ComputeRisks(pl *plan.Plan, aql, ltpd float64) Risks {
	paA := Evaluate(pl, aql).Pa
	paL := Evaluate(pl, ltpd).Pa
	r := Risks{
		AQL: aql, LTPD: ltpd,
		PaAQL: paA, PaLTPD: paL,
		Alpha: 1 - paA, Beta: paL,
	}
	if pl.Kind == plan.KindDouble {
		r.ASNAQL = ASN(pl, aql)
	}
	return r
}

// CurvePoint is one point of an OC / ASN / rectification curve.
type CurvePoint struct {
	P   float64 `json:"p"`
	Pa  float64 `json:"pa"`
	ASN float64 `json:"asn,omitempty"`
	AOQ float64 `json:"aoq,omitempty"`
	ATI float64 `json:"ati,omitempty"`
}

// CurveRequest parameterizes an OC curve.
type CurveRequest struct {
	PMin   float64
	PMax   float64
	Points int // 2..500
}

// Curve samples the plan's metrics over an evenly spaced grid of p
// values. AOQ/ATI columns are included only when the plan carries N.
func Curve(pl *plan.Plan, req CurveRequest) ([]CurvePoint, error) {
	if req.Points < 2 {
		req.Points = 2
	}
	if req.Points > 500 {
		return nil, plan.FieldError{Field: "points",
			Message: "must be <= 500"}
	}
	if err := plan.ValidateProbability(req.PMin, "p_min"); err != nil {
		return nil, err
	}
	if err := plan.ValidateProbability(req.PMax, "p_max"); err != nil {
		return nil, err
	}
	if req.PMin > req.PMax {
		return nil, plan.FieldError{Field: "p_min",
			Message: "p_min must be <= p_max"}
	}
	out := make([]CurvePoint, req.Points)
	for i := 0; i < req.Points; i++ {
		var p float64
		if req.Points == 1 {
			p = req.PMin
		} else {
			p = req.PMin + (req.PMax-req.PMin)*float64(i)/float64(req.Points-1)
		}
		// keep endpoints exact
		if i == 0 {
			p = req.PMin
		}
		if i == req.Points-1 {
			p = req.PMax
		}
		pr := Evaluate(pl, p)
		pt := CurvePoint{P: p, Pa: pr.Pa}
		if pl.Kind == plan.KindDouble {
			pt.ASN = float64(pl.N1) + float64(pl.N2)*pr.P2
		}
		if pl.N != nil {
			if v, err := AOQ(pl, p, pr); err == nil {
				pt.AOQ = v
			}
			if v, err := ATI(pl, p, pr); err == nil {
				pt.ATI = v
			}
		}
		out[i] = pt
	}
	return out, nil
}

// PoissonWarning applies when the caller explicitly asked for the
// poisson approximation and the mean exceeds 5 at a given p.
func PoissonWarning(pl *plan.Plan, p float64) string {
	if pl.Distribution != plan.DistPoisson {
		return ""
	}
	var n float64
	if pl.Kind == plan.KindSingle {
		n = float64(pl.SampleSize)
	} else {
		n = float64(pl.N1)
	}
	if n*p > 5 {
		return "poisson approximation with n*p > 5; binomial/hypergeometric recommended"
	}
	return ""
}
