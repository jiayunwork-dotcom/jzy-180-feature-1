package oc

import (
	"fmt"
	"math"
	"testing"

	"sampling-svc/internal/plan"
)

func singlePlan(n, c int, d plan.Distribution, N *int) *plan.Plan {
	return &plan.Plan{Name: "x", Kind: plan.KindSingle, Distribution: d,
		Approximate: d == plan.DistPoisson, N: N, SampleSize: n, AcceptNumber: c}
}

func doublePlan(n1, c1, r1, n2, c2 int, d plan.Distribution, N *int) *plan.Plan {
	return &plan.Plan{Name: "d", Kind: plan.KindDouble, Distribution: d,
		Approximate: d == plan.DistPoisson, N: N,
		N1: n1, C1: c1, R1: r1, N2: n2, C2: c2}
}

// p=0 -> Pa exactly 1; p=1 & c<n -> Pa exactly 0 (all distributions).
func TestAcceptanceEndpoints(t *testing.T) {
	N := 500
	plans := []*plan.Plan{
		singlePlan(80, 2, plan.DistBinomial, nil),
		singlePlan(80, 2, plan.DistPoisson, nil),
		singlePlan(80, 2, plan.DistHypergeometric, &N),
		doublePlan(50, 2, 5, 50, 6, plan.DistBinomial, nil),
	}
	for _, pl := range plans {
		if got := Evaluate(pl, 0).Pa; got != 1 {
			t.Fatalf("%s p=0 Pa=%v want exactly 1", pl.Describe(), got)
		}
		if got := Evaluate(pl, 1).Pa; got != 0 {
			t.Fatalf("%s p=1 Pa=%v want exactly 0", pl.Describe(), got)
		}
	}
}

// n fixed, c increasing -> Pa curve nowhere decreases.
func TestMonotoneInC(t *testing.T) {
	ps := grid(0, 1, 201)
	prev := make([]float64, len(ps))
	for i := range prev {
		prev[i] = -1
	}
	for c := 0; c < 20; c++ {
		pl := singlePlan(20, c, plan.DistBinomial, nil)
		for i, p := range ps {
			pa := Evaluate(pl, p).Pa
			if pa < prev[i]-1e-12 {
				t.Fatalf("c=%d p=%.3f Pa decreased vs c-1: %.8f < %.8f", c, p, pa, prev[i])
			}
			prev[i] = pa
		}
	}
}

// c fixed, n increasing -> Pa strictly decreases for 0<p<1.
func TestDecreasingInN(t *testing.T) {
	ps := grid(0.02, 0.98, 50)
	prev := make(map[float64]float64)
	for _, n := range []int{10, 12, 16, 25, 40, 80} {
		pl := singlePlan(n, 2, plan.DistBinomial, nil)
		for _, p := range ps {
			pa := Evaluate(pl, p).Pa
			if old, ok := prev[p]; ok && pa > old+1e-9 {
				t.Fatalf("n=%d p=%.3f Pa=%.8f > previous %.8f", n, p, pa, old)
			}
			prev[p] = pa
		}
	}
}

// Degenerate double (n2=0,c1=c2,r1=c1+1) is bit-identical to single.
func TestDoubleDegenerateExact(t *testing.T) {
	s := singlePlan(50, 3, plan.DistBinomial, nil)
	d := doublePlan(50, 3, 4, 0, 3, plan.DistBinomial, nil)
	for _, p := range append(grid(0, 1, 101), 0.037, 0.123) {
		a := Evaluate(s, p).Pa
		b := Evaluate(d, p).Pa
		if a != b {
			t.Fatalf("p=%v single=%.17g double=%.17g", p, a, b)
		}
		if ASN(d, p) != 50 {
			t.Fatalf("degenerate ASN must be n1, got %v", ASN(d, p))
		}
	}
	// Poisson and hypergeometric too.
	N := 400
	sh := singlePlan(50, 3, plan.DistHypergeometric, &N)
	dh := doublePlan(50, 3, 4, 0, 3, plan.DistHypergeometric, &N)
	sp := singlePlan(50, 3, plan.DistPoisson, nil)
	dp := doublePlan(50, 3, 4, 0, 3, plan.DistPoisson, nil)
	for _, p := range grid(0, 0.5, 51) {
		if Evaluate(sh, p).Pa != Evaluate(dh, p).Pa {
			t.Fatalf("hyper degenerate mismatch p=%v", p)
		}
		if Evaluate(sp, p).Pa != Evaluate(dp, p).Pa {
			t.Fatalf("poisson degenerate mismatch p=%v", p)
		}
	}
}

// ASN is always within [n1, n1+n2].
func TestASNRange(t *testing.T) {
	pl := doublePlan(40, 2, 6, 40, 7, plan.DistBinomial, nil)
	for _, p := range grid(0, 1, 201) {
		a := ASN(pl, p)
		if a < 40-1e-9 || a > 80+1e-9 {
			t.Fatalf("p=%.3f ASN=%v out of [40,80]", p, a)
		}
	}
	// n2=n1: grey zone possible, ASN strictly between at interior p.
	mid := ASN(pl, 0.05)
	if !(mid > 40 && mid < 80) {
		t.Fatalf("interior ASN=%v should be strictly within bounds", mid)
	}
}

// ATI within [n, N]; AOQL >= every curve AOQ.
func TestRectification(t *testing.T) {
	N := 1000
	pl := singlePlan(80, 2, plan.DistHypergeometric, &N)
	top, err := AOQL(pl)
	if err != nil {
		t.Fatal(err)
	}
	pts, err := Curve(pl, CurveRequest{PMin: 0, PMax: 1, Points: 500})
	if err != nil {
		t.Fatal(err)
	}
	for _, pt := range pts {
		if pt.ATI < 80-1e-9 || pt.ATI > 1000+1e-9 {
			t.Fatalf("ATI=%v out of [80,1000] at p=%.3f", pt.ATI, pt.P)
		}
		if pt.AOQ > top.AOQL+1e-10 {
			t.Fatalf("AOQ=%.8f exceeds AOQL=%.8f at p=%.4f", pt.AOQ, top.AOQL, pt.P)
		}
	}
	// AOQ is zero at endpoints.
	if math.Abs(pts[0].AOQ) > 0 || math.Abs(pts[len(pts)-1].AOQ) > 0 {
		t.Fatal("AOQ must be 0 at p=0 and p=1")
	}
}

// n=80, c=2 hand-checkable example against direct binomial summation.
func TestHandCheck80_2(t *testing.T) {
	pl := singlePlan(80, 2, plan.DistBinomial, nil)
	checks := map[float64]float64{
		0.01: 0.9534467, // sum_{k=0}^2 C(80,k) .01^k .99^(80-k)
		0.05: 0.2306189,
	}
	for p, want := range checks {
		got := Evaluate(pl, p).Pa
		if math.Abs(got-want) > 1e-5 {
			t.Fatalf("n=80 c=2 p=%.2f Pa=%.7f hand=%.7f", p, got, want)
		}
	}
	risks := ComputeRisks(pl, 0.01, 0.05)
	if math.Abs(risks.PaAQL-checks[0.01]) > 1e-5 {
		t.Fatal("risk Pa(AQL) mismatch")
	}
	if math.Abs(risks.Alpha-(1-checks[0.01])) > 1e-5 {
		t.Fatal("alpha mismatch")
	}
	if math.Abs(risks.Beta-checks[0.05]) > 1e-5 {
		t.Fatal("beta mismatch")
	}
}

// Curve point cap and validation.
func TestCurveValidation(t *testing.T) {
	pl := singlePlan(80, 2, plan.DistBinomial, nil)
	if _, err := Curve(pl, CurveRequest{Points: 501}); err == nil {
		t.Fatal("points>500 must be rejected")
	}
	if _, err := Curve(pl, CurveRequest{PMin: -0.1, Points: 10}); err == nil {
		t.Fatal("negative p must be rejected")
	}
	pts, err := Curve(pl, CurveRequest{PMin: 0, PMax: 0.1, Points: 11})
	if err != nil {
		t.Fatal(err)
	}
	if len(pts) != 11 {
		t.Fatalf("got %d points", len(pts))
	}
}

// Designed-plan tests live in the design package (it already imports oc).

// Double-plan probability decomposition must be consistent:
// P1Acc + P1Rej + P2 == 1 and Pa + (P1Rej+P2Rej) == 1.
func TestDoubleDecomposition(t *testing.T) {
	pl := doublePlan(40, 2, 6, 40, 7, plan.DistBinomial, nil)
	for _, p := range grid(0, 1, 101) {
		pr := Evaluate(pl, p)
		if math.Abs(pr.P1Acc+pr.P1Rej+pr.P2-1) > 1e-12 {
			t.Fatalf("p=%.2f stage probs sum %.12f", p, pr.P1Acc+pr.P1Rej+pr.P2)
		}
		if math.Abs(pr.Pa+pr.P1Rej+pr.P2Rej-1) > 1e-12 {
			t.Fatalf("p=%.2f accept/reject probs sum %.12f", p, pr.Pa+pr.P1Rej+pr.P2Rej)
		}
		if pr.Pa < pr.P1Acc-1e-12 {
			t.Fatal("Pa must be at least the first-stage accept mass")
		}
	}
}

// Hypergeometric double sampling: conditional second-stage model must
// make the stage probabilities sum to 1.
func TestDoubleHyperDecomposition(t *testing.T) {
	N := 2000
	pl := doublePlan(40, 2, 6, 40, 7, plan.DistHypergeometric, &N)
	for _, p := range []float64{0.01, 0.05, 0.1, 0.3} {
		pr := Evaluate(pl, p)
		if math.Abs(pr.P1Acc+pr.P1Rej+pr.P2-1) > 1e-9 {
			t.Fatalf("p=%v hyper stage sum %.9f", p, pr.P1Acc+pr.P1Rej+pr.P2)
		}
		if math.Abs(pr.Pa+pr.P1Rej+pr.P2Rej-1) > 1e-9 {
			t.Fatalf("p=%v hyper decision sum %.9f", p, pr.Pa+pr.P1Rej+pr.P2Rej)
		}
	}
}

func grid(lo, hi float64, n int) []float64 {
	out := make([]float64, n)
	for i := 0; i < n; i++ {
		out[i] = lo + (hi-lo)*float64(i)/float64(n-1)
	}
	return out
}

func ExampleEvaluate_n80c2() {
	pl := singlePlan(80, 2, plan.DistBinomial, nil)
	fmt.Printf("Pa(0.01)=%.4f Pa(0.05)=%.4f", Evaluate(pl, 0.01).Pa, Evaluate(pl, 0.05).Pa)
	// Output: Pa(0.01)=0.9534 Pa(0.05)=0.2306
}
