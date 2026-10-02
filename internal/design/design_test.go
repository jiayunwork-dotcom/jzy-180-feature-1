package design

import (
	"errors"
	"testing"

	"sampling-svc/internal/oc"
	"sampling-svc/internal/plan"
)

func single(n, c int) *plan.Plan {
	return &plan.Plan{Kind: plan.KindSingle, Distribution: plan.DistBinomial,
		SampleSize: n, AcceptNumber: c}
}

func double(n1, c1, r1, n2, c2 int) *plan.Plan {
	return &plan.Plan{Kind: plan.KindDouble, Distribution: plan.DistBinomial,
		N1: n1, C1: c1, R1: r1, N2: n2, C2: c2}
}

// Designed single plan satisfies constraints; n-1 with any c cannot.
func TestSingleDesignMinimal(t *testing.T) {
	in := Params{AQL: 0.02, Alpha: 0.05, LTPD: 0.08, Beta: 0.10}
	res, err := Single(in, 0)
	if err != nil {
		t.Fatal(err)
	}
	p := res.Plan
	if p.Kind != plan.KindSingle {
		t.Fatal("wrong kind")
	}
	if res.Alpha > in.Alpha+1e-9 {
		t.Fatalf("alpha violated: %v", res.Alpha)
	}
	if res.Beta > in.Beta+1e-9 {
		t.Fatalf("beta violated: %v", res.Beta)
	}
	n := p.SampleSize
	for c := 0; c <= n-2; c++ {
		r := oc.ComputeRisks(single(n-1, c), in.AQL, in.LTPD)
		if r.Alpha <= in.Alpha+1e-9 && r.Beta <= in.Beta+1e-9 {
			t.Fatalf("n-1 with c=%d unexpectedly feasible", c)
		}
	}
	if p.AcceptNumber > 0 {
		r := oc.ComputeRisks(single(n, p.AcceptNumber-1), in.AQL, in.LTPD)
		if r.Alpha <= in.Alpha+1e-9 && r.Beta <= in.Beta+1e-9 {
			t.Fatal("a smaller c at the same n is feasible; tie-break wrong")
		}
	}
}

func TestSingleDesignInfeasible(t *testing.T) {
	in := Params{AQL: 0.001, Alpha: 0.001, LTPD: 0.002, Beta: 0.001}
	_, err := Single(in, 50)
	if !errors.Is(err, ErrInfeasible) {
		t.Fatalf("expected ErrInfeasible, got %v", err)
	}
}

func TestDesignValidation(t *testing.T) {
	cases := []Params{
		{AQL: 0.1, Alpha: 0.05, LTPD: 0.05, Beta: 0.1}, // AQL>=LTPD
		{AQL: 0.02, Alpha: 0, LTPD: 0.08, Beta: 0.1},   // alpha boundary
		{AQL: 0.02, Alpha: 1.5, LTPD: 0.08, Beta: 0.1}, // alpha>1
		{AQL: 0.02, Alpha: 0.05, LTPD: 0.08, Beta: 1},  // beta boundary
	}
	for i, in := range cases {
		if _, err := Single(in, 1000); err == nil {
			t.Fatalf("case %d expected validation error", i)
		}
	}
}

// Brute-force confirm the double search returns the minimum-ASN feasible
// plan. The optimum lands near n1=34 with ASN ~50.7; any plan with
// n1>=51 has ASN>=n1>50.7 and cannot beat it, so an exhaustive scan
// through n1=50 plus the n1 lower bound proves global optimality. The
// scan uses the same O(1) prefix evaluator as the search itself.
func TestDoubleDesignOptimalEqual(t *testing.T) {
	in := Params{AQL: 0.02, Alpha: 0.05, LTPD: 0.10, Beta: 0.10}
	const bruteCap = 50
	res, err := Double(in, N2EqualN1, 150)
	if err != nil {
		t.Fatal(err)
	}
	p := res.Plan
	if p.N2 != p.N1 || !(p.C1 < p.R1 && p.R1 <= p.C2+1) {
		t.Fatalf("ill-formed result %s", p.Describe())
	}
	if res.Alpha > in.Alpha+1e-9 || res.Beta > in.Beta+1e-9 {
		t.Fatalf("risk violated alpha=%v beta=%v", res.Alpha, res.Beta)
	}
	best := res.ASNAQL
	if p.N1 > bruteCap {
		t.Fatalf("optimum n1=%d outside brute-force range %d", p.N1, bruteCap)
	}
	brute := func(mode DoubleN2Mode, capN int) {
		for n1 := 1; n1 <= capN; n1++ {
			n2 := n1
			if mode == N2TwiceN1 {
				n2 = 2 * n1
			}
			firstA, prefA := buildPMFs(plan.DistBinomial, n1, n2, nil, in.AQL)
			firstL, prefL := buildPMFs(plan.DistBinomial, n1, n2, nil, in.LTPD)
			fA := firstCDF(firstA, n1)
			fL := firstCDF(firstL, n1)
			for c2 := 0; c2 < n1+n2; c2++ {
				gA, hA := gPrefixes(firstA, prefA, n1, n2, c2)
				gL, _ := gPrefixes(firstL, prefL, n1, n2, c2)
				for c1 := 0; c1 <= c2 && c1 < n1; c1++ {
					for r1 := c1 + 1; r1 <= c2+1 && r1 <= n1+1; r1++ {
						paA := fA[c1] - getAt(gA, c1) + getAt(gA, r1-1)
						paL := fL[c1] - getAt(gL, c1) + getAt(gL, r1-1)
						if paA >= 1-in.Alpha-1e-9 && paL <= in.Beta+1e-9 {
							asn := float64(n1) + float64(n2)*
								(getAt(hA, r1-1)-getAt(hA, c1))
							if asn < best-1e-9 {
								t.Fatalf("n1=%d c1=%d r1=%d c2=%d ASN %.6f beats chosen %.6f",
									n1, c1, r1, c2, asn, best)
							}
						}
					}
				}
			}
		}
	}
	brute(N2EqualN1, bruteCap)
}

func TestDoubleDesignTwice(t *testing.T) {
	in := Params{AQL: 0.015, Alpha: 0.05, LTPD: 0.07, Beta: 0.10}
	res, err := Double(in, N2TwiceN1, 150)
	if err != nil {
		t.Fatal(err)
	}
	p := res.Plan
	if p.N2 != 2*p.N1 {
		t.Fatal("n2 must be 2*n1")
	}
	if res.Alpha > in.Alpha+1e-9 || res.Beta > in.Beta+1e-9 {
		t.Fatalf("risk violated alpha=%v beta=%v", res.Alpha, res.Beta)
	}
	if res.ASNAQL < float64(p.N1)-1e-9 || res.ASNAQL > float64(3*p.N1)+1e-9 {
		t.Fatal("ASN bounds violated")
	}
	// Brute-force over the n1 range that could possibly beat the result
	// (ASN >= n1 rules out larger n1).
	if p.N1 > 100 {
		t.Fatalf("optimum n1=%d too large for brute check", p.N1)
	}
	first := res.ASNAQL
	for n1 := 1; n1 <= int(first)+1 && n1 <= 100; n1++ {
		n2 := 2 * n1
		firstA, prefA := buildPMFs(plan.DistBinomial, n1, n2, nil, in.AQL)
		firstL, prefL := buildPMFs(plan.DistBinomial, n1, n2, nil, in.LTPD)
		fA := firstCDF(firstA, n1)
		fL := firstCDF(firstL, n1)
		for c2 := 0; c2 < n1+n2; c2++ {
			gA, hA := gPrefixes(firstA, prefA, n1, n2, c2)
			gL, _ := gPrefixes(firstL, prefL, n1, n2, c2)
			for c1 := 0; c1 <= c2 && c1 < n1; c1++ {
				for r1 := c1 + 1; r1 <= c2+1 && r1 <= n1+1; r1++ {
					paA := fA[c1] - getAt(gA, c1) + getAt(gA, r1-1)
					paL := fL[c1] - getAt(gL, c1) + getAt(gL, r1-1)
					if paA >= 1-in.Alpha-1e-9 && paL <= in.Beta+1e-9 {
						asn := float64(n1) + float64(n2)*
							(getAt(hA, r1-1)-getAt(hA, c1))
						if asn < first-1e-9 {
							t.Fatalf("n1=%d c1=%d r1=%d c2=%d ASN %.6f beats %.6f",
								n1, c1, r1, c2, asn, first)
						}
					}
				}
			}
		}
	}
}

// A too-small cap yields a clear "no feasible plan" result.
func TestDoubleInfeasible(t *testing.T) {
	in := Params{AQL: 0.001, Alpha: 0.001, LTPD: 0.005, Beta: 0.001}
	if _, err := Double(in, N2EqualN1, 3); !errors.Is(err, ErrInfeasible) {
		t.Fatalf("got %v", err)
	}
}
