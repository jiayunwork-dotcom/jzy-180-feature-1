package httpapi

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"sampling-svc/internal/oc"
	"sampling-svc/internal/plan"
)

type evalRequest struct {
	P []float64 `json:"p"`
}

type evalPoint struct {
	P float64 `json:"p"`
	oc.Probabilities
	ASN float64 `json:"asn,omitempty"`
	AOQ float64 `json:"aoq,omitempty"`
	ATI float64 `json:"ati,omitempty"`
}

type evalResponse struct {
	Plan        planJSON    `json:"plan"`
	Approximate bool        `json:"approximate"`
	Warnings    []string    `json:"warnings,omitempty"`
	Points      []evalPoint `json:"points"`
}

func (s *Server) withPlan(c echo.Context) (*plan.Plan, error) {
	return s.store.GetPlan(c.Request().Context(), c.Param("id"))
}

func (s *Server) evaluate(c echo.Context) error {
	pl, err := s.withPlan(c)
	if err != nil {
		return domainError(c, err)
	}
	var req evalRequest
	if err := c.Bind(&req); err != nil {
		return fail(c, http.StatusBadRequest, "invalid JSON: "+err.Error())
	}
	if len(req.P) == 0 {
		return fail(c, http.StatusBadRequest, "p required",
			plan.FieldError{Field: "p", Message: "at least one defective fraction required"})
	}
	resp := evalResponse{Plan: planToJSON(pl), Approximate: pl.Approximate}
	warned := map[string]bool{}
	for _, p := range req.P {
		if err := plan.ValidateProbability(p, "p"); err != nil {
			return domainError(c, err)
		}
		pr := oc.Evaluate(pl, p)
		pt := evalPoint{P: p, Probabilities: pr}
		if pl.Kind == plan.KindDouble {
			pt.ASN = oc.ASN(pl, p)
		}
		if pl.N != nil {
			if v, err := oc.AOQ(pl, p, pr); err == nil {
				pt.AOQ = v
			}
			if v, err := oc.ATI(pl, p, pr); err == nil {
				pt.ATI = v
			}
		}
		if w := oc.PoissonWarning(pl, p); w != "" && !warned[w] {
			resp.Warnings = append(resp.Warnings, w)
			warned[w] = true
		}
		resp.Points = append(resp.Points, pt)
	}
	return c.JSON(http.StatusOK, resp)
}

type curveReq struct {
	PMin   float64 `json:"p_min"`
	PMax   float64 `json:"p_max"`
	Points int     `json:"points"`
}

func (s *Server) curve(c echo.Context) error {
	pl, err := s.withPlan(c)
	if err != nil {
		return domainError(c, err)
	}
	var req curveReq
	req.PMin, req.PMax = 0, 0.2
	req.Points = 101
	if c.Request().ContentLength != 0 {
		if err := c.Bind(&req); err != nil {
			return fail(c, http.StatusBadRequest, "invalid JSON: "+err.Error())
		}
	}
	pts, err := oc.Curve(pl, oc.CurveRequest{
		PMin: req.PMin, PMax: req.PMax, Points: req.Points})
	if err != nil {
		return domainError(c, err)
	}
	return c.JSON(http.StatusOK, map[string]any{
		"plan":        planToJSON(pl),
		"approximate": pl.Approximate,
		"points":      pts,
	})
}

type riskReq struct {
	AQL  float64 `json:"aql"`
	LTPD float64 `json:"ltpd"`
}

func (s *Server) risks(c echo.Context) error {
	pl, err := s.withPlan(c)
	if err != nil {
		return domainError(c, err)
	}
	var req riskReq
	if err := c.Bind(&req); err != nil {
		return fail(c, http.StatusBadRequest, "invalid JSON: "+err.Error())
	}
	var fes []plan.FieldError
	if err := plan.ValidateProbability(req.AQL, "aql"); err != nil {
		fes = append(fes, plan.FieldError{Field: "aql", Message: "must be within [0,1]"})
	}
	if err := plan.ValidateProbability(req.LTPD, "ltpd"); err != nil {
		fes = append(fes, plan.FieldError{Field: "ltpd", Message: "must be within [0,1]"})
	}
	if len(fes) == 0 && !(req.AQL < req.LTPD) {
		fes = append(fes, plan.FieldError{Field: "aql", Message: "must satisfy AQL < LTPD"})
	}
	if len(fes) > 0 {
		return fail(c, http.StatusBadRequest, "validation error", fes...)
	}
	r := oc.ComputeRisks(pl, req.AQL, req.LTPD)
	return c.JSON(http.StatusOK, map[string]any{
		"plan":        planToJSON(pl),
		"approximate": pl.Approximate,
		"risks":       r,
	})
}

func (s *Server) aoql(c echo.Context) error {
	pl, err := s.withPlan(c)
	if err != nil {
		return domainError(c, err)
	}
	if pl.N == nil {
		return fail(c, http.StatusBadRequest, "AOQL requires a finite lot size N",
			plan.FieldError{Field: "n_lot", Message: "required for AOQL/ATI"})
	}
	r, err := oc.AOQL(pl)
	if err != nil {
		return domainError(c, err)
	}
	return c.JSON(http.StatusOK, map[string]any{
		"plan": planToJSON(pl), "aoql": r,
	})
}
