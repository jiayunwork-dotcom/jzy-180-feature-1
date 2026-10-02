package plan

import (
	"strings"
	"testing"
)

func TestSingleValidation(t *testing.T) {
	N := 10
	cases := []struct {
		name   string
		req    Request
		fields []string
	}{
		{"missing name", Request{Kind: "single", Single: &SingleParam{SampleSize: 10, AcceptNumber: 2}}, []string{"name"}},
		{"n<1", Request{Name: "x", Kind: "single", Single: &SingleParam{SampleSize: 0, AcceptNumber: 0}}, []string{"single.n"}},
		{"c<0", Request{Name: "x", Kind: "single", Single: &SingleParam{SampleSize: 10, AcceptNumber: -1}}, []string{"single.c"}},
		{"c>=n", Request{Name: "x", Kind: "single", Single: &SingleParam{SampleSize: 5, AcceptNumber: 5}}, []string{"single.c"}},
		{"N<n", Request{Name: "x", Kind: "single", Single: &SingleParam{N: &N, SampleSize: 50, AcceptNumber: 2}}, []string{"single.n_lot"}},
		{"bad kind", Request{Name: "x", Kind: "triple", Single: &SingleParam{SampleSize: 5}}, []string{"kind"}},
		{"hyper without N", Request{Name: "x", Kind: "single",
			Single: &SingleParam{SampleSize: 5, AcceptNumber: 0, Distribution: "hypergeometric"}},
			[]string{"single.distribution"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Build(tc.req)
			if err == nil {
				t.Fatal("expected validation error")
			}
			msg := err.Error()
			for _, f := range tc.fields {
				if !strings.Contains(msg, f) {
					t.Fatalf("error %q must name field %q", msg, f)
				}
			}
		})
	}
}

func TestDoubleValidation(t *testing.T) {
	cases := []struct {
		name   string
		d      DoubleParam
		fields []string
	}{
		{"c1>=r1", DoubleParam{N1: 10, C1: 5, R1: 5, N2: 10, C2: 6}, []string{"double.r1"}},
		{"r1>c2+1", DoubleParam{N1: 10, C1: 1, R1: 6, N2: 10, C2: 3}, []string{"double.c2"}},
		{"n2<0", DoubleParam{N1: 10, C1: 1, R1: 3, N2: -1, C2: 3}, []string{"double.n2"}},
		{"d1 over", DoubleParam{N1: 2, C1: 5, R1: 6, N2: 10, C2: 6}, []string{"double.c1"}},
		{"bad degenerate", DoubleParam{N1: 10, C1: 2, R1: 4, N2: 0, C2: 2}, []string{"double"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Build(Request{Name: "d", Kind: "double", Double: &tc.d})
			if err == nil {
				t.Fatal("expected validation error")
			}
			for _, f := range tc.fields {
				if !strings.Contains(err.Error(), f) {
					t.Fatalf("error %q must name %q", err.Error(), f)
				}
			}
		})
	}
}

func TestValidBuildsAndDistAuto(t *testing.T) {
	p, err := Build(Request{Name: "a", Kind: "single",
		Single: &SingleParam{SampleSize: 80, AcceptNumber: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if p.Distribution != DistBinomial || p.Approximate {
		t.Fatal("no N must default to binomial")
	}
	N := 1000
	p, err = Build(Request{Name: "b", Kind: "single",
		Single: &SingleParam{N: &N, SampleSize: 80, AcceptNumber: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if p.Distribution != DistHypergeometric {
		t.Fatal("with N must default to hypergeometric")
	}
	p, err = Build(Request{Name: "c", Kind: "single",
		Single: &SingleParam{SampleSize: 200, AcceptNumber: 3, Distribution: "poisson"}})
	if err != nil || !p.Approximate || p.Distribution != DistPoisson {
		t.Fatal("poisson must be honored and flagged")
	}
	// r1=n1+1 is legal: first stage never rejects outright.
	_, err = Build(Request{Name: "d", Kind: "double", Double: &DoubleParam{
		N1: 10, C1: 1, R1: 11, N2: 10, C2: 10}})
	if err != nil {
		t.Fatalf("r1=n1+1 must be allowed: %v", err)
	}
	// But r1=n1+1 still obeys r1 <= c2+1.
	_, err = Build(Request{Name: "e", Kind: "double", Double: &DoubleParam{
		N1: 10, C1: 1, R1: 11, N2: 10, C2: 5}})
	if err == nil {
		t.Fatal("r1=n1+1 with c2 < r1-1 must still violate r1<=c2+1")
	}
}

func TestProbabilityValidation(t *testing.T) {
	for _, p := range []float64{-0.1, 1.1} {
		if err := ValidateProbability(p, "p"); err == nil {
			t.Fatalf("p=%v must be rejected", p)
		}
	}
	if err := ValidateProbability(0.5, "p"); err != nil {
		t.Fatal(err)
	}
}
