package dist

import (
	"math"
	"testing"

	"sampling-svc/internal/plan"
)

// LogComb exact values and impossible-event handling.
func TestLogComb(t *testing.T) {
	if LogComb(5, 0) != 0 || LogComb(5, 5) != 0 {
		t.Fatal("C(n,0)=C(n,n)=1 -> log 0")
	}
	if got := math.Exp(LogComb(5, 2)); math.Abs(got-10) > 1e-9 {
		t.Fatalf("C(5,2)=10 got %v", got)
	}
	for _, c := range [][2]int{{-1, 0}, {5, -1}, {5, 6}} {
		if !math.IsInf(LogComb(c[0], c[1]), -1) {
			t.Fatalf("LogComb(%d,%d) must be -Inf", c[0], c[1])
		}
	}
}

// Binomial endpoint precision and n=80 c=2 hand value.
func TestBinomialCDF(t *testing.T) {
	m := NewModel(plan.DistBinomial, 80, nil, 0.01)
	if got := m.Cdf(2); math.Abs(got-0.9534467) > 1e-5 {
		t.Fatalf("Pa(0.01)=%.7f", got)
	}
	if NewModel(plan.DistBinomial, 80, nil, 0).Cdf(0) != 1 {
		t.Fatal("p=0 must be exactly 1")
	}
	if NewModel(plan.DistBinomial, 80, nil, 1).Cdf(2) != 0 {
		t.Fatal("p=1, c<n must be exactly 0")
	}
}

// Hypergeometric second stage is conditional on the first sample.
func TestConditionalHypergeometric(t *testing.T) {
	N := 200
	m2 := NewModel2(plan.DistHypergeometric, 40, 40, &N, 0.1)
	// d1=10 defectives drawn: remainder 160 of which K-d1 = 20-10 = 10.
	m2.GivenFirst(10)
	lo, hi := m2.Support()
	if lo != 0 || hi != 10 {
		t.Fatalf("conditional support [%d,%d], want [0,10]", lo, hi)
	}
	if got := math.Exp(m2.LogPmf(10)); got <= 0 || got > 1 {
		t.Fatalf("conditional P(d2=10)=%v", got)
	}
}
