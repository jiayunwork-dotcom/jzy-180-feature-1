// Package plan defines sampling-plan (方案档) domain types, persistence
// records and field-level validation rules shared by every other package.
package plan

import (
	"fmt"
	"math"
)

// Kind selects between single and double sampling.
type Kind string

// Distribution selects the probability model used by the OC engine.
type Distribution string

const (
	KindSingle Kind = "single"
	KindDouble Kind = "double"

	// DistHypergeometric requires a finite lot size N.
	DistHypergeometric Distribution = "hypergeometric"
	// DistBinomial is the unbounded-lot model.
	DistBinomial Distribution = "binomial"
	// DistPoisson is the poisson approximation; responses are flagged
	// approximate and a warning is produced when n*p > 5.
	DistPoisson Distribution = "poisson"
)

// Plan is an acceptance-sampling scheme.
//
// Single sampling is fully described by N, C.
// Double sampling adds first/second stage parameters; the constraints
// C1 < R1 <= C2+1 must hold. N2 may be 0, in which case the scheme must
// be the degenerate form C1 == C2, R1 == C1+1 that is numerically
// identical to a single plan (N, C1).
type Plan struct {
	ID           string
	Name         string
	Kind         Kind
	Distribution Distribution // resolved distribution; never "auto" here
	// Approximate is true when Distribution == poisson.
	Approximate bool

	// Lot size; nil means an unbounded lot.
	N *int

	// Single sampling
	SampleSize   int // n
	AcceptNumber int // c

	// Double sampling, first stage
	N1 int
	C1 int
	R1 int
	// Double sampling, second stage
	N2 int
	// C2 applies to the combined two-stage defect count.
	C2 int
}

// SingleParam is the create/update payload for a single plan.
type SingleParam struct {
	N            *int   `json:"n_lot,omitempty"`
	SampleSize   int    `json:"n"`
	AcceptNumber int    `json:"c"`
	Distribution string `json:"distribution,omitempty"` // binomial|poisson|hypergeometric|"" (auto)
}

// DoubleParam is the create/update payload for a double plan.
type DoubleParam struct {
	N            *int   `json:"n_lot,omitempty"`
	N1           int    `json:"n1"`
	C1           int    `json:"c1"`
	R1           int    `json:"r1"`
	N2           int    `json:"n2"`
	C2           int    `json:"c2"`
	Distribution string `json:"distribution,omitempty"`
}

// Request is the wire payload for creating or updating a plan.
type Request struct {
	Name   string       `json:"name"`
	Kind   string       `json:"kind"` // single|double
	Single *SingleParam `json:"single,omitempty"`
	Double *DoubleParam `json:"double,omitempty"`
}

// FieldError points at the request field that failed validation.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e FieldError) Error() string { return e.Field + ": " + e.Message }

// ValidationErrors is an ordered list of field-level problems.
type ValidationErrors []FieldError

func (vs ValidationErrors) Error() string {
	if len(vs) == 0 {
		return "validation failed"
	}
	s := vs[0].Error()
	for _, e := range vs[1:] {
		s += "; " + e.Error()
	}
	return s
}

// Build validates a request and resolves it into a Plan (without ID).
func Build(req Request) (*Plan, error) {
	var errs ValidationErrors
	if req.Name == "" {
		errs = append(errs, FieldError{"name", "must not be empty"})
	}
	switch Kind(req.Kind) {
	case KindSingle:
		if req.Single == nil {
			errs = append(errs, FieldError{"single", "single parameters required"})
			break
		}
		p, perr := buildSingle(req.Single)
		if len(perr) > 0 {
			errs = append(errs, perr...)
			break
		}
		if len(errs) > 0 {
			break
		}
		p.Name = req.Name
		return p, nil
	case KindDouble:
		if req.Double == nil {
			errs = append(errs, FieldError{"double", "double parameters required"})
			break
		}
		p, perr := buildDouble(req.Double)
		if len(perr) > 0 {
			errs = append(errs, perr...)
			break
		}
		if len(errs) > 0 {
			break
		}
		p.Name = req.Name
		return p, nil
	default:
		errs = append(errs, FieldError{"kind", "must be 'single' or 'double'"})
	}
	return nil, errs
}

func resolveDist(raw string, n, n2 int, lot *int, prefix string, errs *ValidationErrors) Distribution {
	var d Distribution
	switch Distribution(raw) {
	case "", "auto":
		if lot != nil {
			d = DistHypergeometric
		} else {
			d = DistBinomial
		}
	case DistHypergeometric, DistBinomial, DistPoisson:
		d = Distribution(raw)
	default:
		*errs = append(*errs, FieldError{prefix + ".distribution",
			"must be one of hypergeometric, binomial, poisson"})
		return ""
	}
	if d == DistHypergeometric && lot == nil {
		*errs = append(*errs, FieldError{prefix + ".distribution",
			"hypergeometric requires lot size n_lot"})
	}
	return d
}

func buildSingle(s *SingleParam) (*Plan, ValidationErrors) {
	var errs ValidationErrors
	if s.SampleSize < 1 {
		errs = append(errs, FieldError{"single.n", "must be >= 1"})
	}
	if s.AcceptNumber < 0 {
		errs = append(errs, FieldError{"single.c", "must be >= 0"})
	}
	if s.SampleSize >= 1 && s.AcceptNumber >= s.SampleSize {
		errs = append(errs, FieldError{"single.c", "must satisfy c < n"})
	}
	if s.N != nil {
		if *s.N < 1 {
			errs = append(errs, FieldError{"single.n_lot", "must be >= 1"})
		} else if s.SampleSize >= 1 && *s.N < s.SampleSize {
			errs = append(errs, FieldError{"single.n_lot", "lot size must be >= sample size n"})
		}
	}
	d := resolveDist(s.Distribution, s.SampleSize, 0, s.N, "single", &errs)
	if len(errs) > 0 {
		return nil, errs
	}
	return &Plan{
		Kind:         KindSingle,
		Distribution: d,
		Approximate:  d == DistPoisson,
		N:            s.N,
		SampleSize:   s.SampleSize,
		AcceptNumber: s.AcceptNumber,
	}, nil
}

func buildDouble(s *DoubleParam) (*Plan, ValidationErrors) {
	var errs ValidationErrors
	if s.N1 < 1 {
		errs = append(errs, FieldError{"double.n1", "must be >= 1"})
	}
	if s.N2 < 0 {
		errs = append(errs, FieldError{"double.n2", "must be >= 0"})
	}
	if s.C1 < 0 {
		errs = append(errs, FieldError{"double.c1", "must be >= 0"})
	}
	if s.R1 < 0 {
		errs = append(errs, FieldError{"double.r1", "must be >= 0"})
	}
	if s.C2 < 0 {
		errs = append(errs, FieldError{"double.c2", "must be >= 0"})
	}
	if s.N1 >= 1 {
		if s.C1 >= s.N1 {
			errs = append(errs, FieldError{"double.c1", "must satisfy c1 < n1"})
		}
		// r1 may equal n1+1: then the first stage never rejects
		// outright and every non-accepted first count goes to stage 2.
		if s.R1 > s.N1+1 {
			errs = append(errs, FieldError{"double.r1", "must satisfy r1 <= n1+1"})
		}
	}
	if s.N2 > 0 && s.C2 >= s.N1+s.N2 {
		errs = append(errs, FieldError{"double.c2", "must satisfy c2 < n1+n2"})
	}
	if !(s.C1 < s.R1) {
		errs = append(errs, FieldError{"double.r1", "must satisfy c1 < r1"})
	}
	if !(s.R1 <= s.C2+1) {
		errs = append(errs, FieldError{"double.c2", "must satisfy r1 <= c2+1"})
	}
	if s.N2 == 0 && (s.C1 != s.C2 || s.R1 != s.C1+1) {
		errs = append(errs, FieldError{"double",
			"n2=0 only allows the degenerate form c1=c2, r1=c1+1"})
	}
	if s.N != nil {
		if *s.N < 1 {
			errs = append(errs, FieldError{"double.n_lot", "must be >= 1"})
		} else {
			total := s.N1 + s.N2
			if total >= 1 && *s.N < total {
				errs = append(errs, FieldError{"double.n_lot",
					"lot size must be >= n1+n2"})
			}
		}
	}
	d := resolveDist(s.Distribution, s.N1, s.N2, s.N, "double", &errs)
	if len(errs) > 0 {
		return nil, errs
	}
	return &Plan{
		Kind:         KindDouble,
		Distribution: d,
		Approximate:  d == DistPoisson,
		N:            s.N,
		N1:           s.N1,
		C1:           s.C1,
		R1:           s.R1,
		N2:           s.N2,
		C2:           s.C2,
	}, nil
}

// ValidateProbability rejects p outside [0,1], reporting the given field.
func ValidateProbability(p float64, field string) error {
	if math.IsNaN(p) || p < 0 || p > 1 {
		return FieldError{field, "must be within [0,1]"}
	}
	return nil
}

// Describe renders a short human-readable signature, e.g. "single(n=80,c=2)".
func (p *Plan) Describe() string {
	if p.Kind == KindSingle {
		return fmt.Sprintf("single(n=%d,c=%d)", p.SampleSize, p.AcceptNumber)
	}
	return fmt.Sprintf("double(n1=%d,c1=%d,r1=%d,n2=%d,c2=%d)",
		p.N1, p.C1, p.R1, p.N2, p.C2)
}
