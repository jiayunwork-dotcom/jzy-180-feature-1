package httpapi

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v4"

	"sampling-svc/internal/design"
)

type designReq struct {
	AQL          float64 `json:"aql"`
	Alpha        float64 `json:"alpha"`
	LTPD         float64 `json:"ltpd"`
	Beta         float64 `json:"beta"`
	N            *int    `json:"n_lot,omitempty"`
	Distribution string  `json:"distribution,omitempty"`
	N1Max        int     `json:"n1_max,omitempty"`
	NMax         int     `json:"n_max,omitempty"`
	N2Mode       string  `json:"n2_mode,omitempty"`
}

func designParams(r designReq) design.Params {
	return design.Params{
		AQL: r.AQL, Alpha: r.Alpha, LTPD: r.LTPD, Beta: r.Beta,
		N: r.N, Distribution: r.Distribution,
	}
}

func designResult(c echo.Context, res *design.Result) error {
	return c.JSON(http.StatusOK, map[string]any{
		"plan":        planToJSON(res.Plan),
		"approximate": res.Approximate,
		"pa_aql":      res.PaAQL,
		"pa_ltpd":     res.PaLTPD,
		"alpha":       res.Alpha,
		"beta":        res.Beta,
		"asn_aql":     res.ASNAQL,
	})
}

func (s *Server) designSingle(c echo.Context) error {
	var req designReq
	if err := c.Bind(&req); err != nil {
		return fail(c, http.StatusBadRequest, "invalid JSON: "+err.Error())
	}
	res, err := design.Single(designParams(req), req.NMax)
	if err != nil {
		if errors.Is(err, design.ErrInfeasible) {
			return fail(c, http.StatusNotFound,
				"no feasible plan within the search upper bound; raise n_max or relax risks")
		}
		return domainError(c, err)
	}
	return designResult(c, res)
}

func (s *Server) designDouble(c echo.Context) error {
	var req designReq
	if err := c.Bind(&req); err != nil {
		return fail(c, http.StatusBadRequest, "invalid JSON: "+err.Error())
	}
	mode := design.DoubleN2Mode(req.N2Mode)
	if mode == "" {
		mode = design.N2EqualN1
	}
	res, err := design.Double(designParams(req), mode, req.N1Max)
	if err != nil {
		if errors.Is(err, design.ErrInfeasible) {
			return fail(c, http.StatusNotFound,
				"no feasible double plan within the search upper bound; raise n1_max or relax risks")
		}
		return domainError(c, err)
	}
	return designResult(c, res)
}
