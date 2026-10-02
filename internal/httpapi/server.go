// Package httpapi exposes the service over HTTP using Echo. It contains
// no sampling math and no SQL: it validates wire input, calls the domain
// packages and renders JSON. Handlers are split across plan_handlers.go,
// analyze_handlers.go, design_handlers.go and stream_handlers.go.
package httpapi

import (
	"errors"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	"sampling-svc/internal/inspection"
	"sampling-svc/internal/plan"
	"sampling-svc/internal/store"
)

// Server wires the store to HTTP handlers.
type Server struct {
	store *store.Store
	echo  *echo.Echo
}

// NewServer builds the router.
func NewServer(st *store.Store) *Server {
	e := echo.New()
	e.HideBanner = true
	e.Validator = nopValidator{}
	e.Use(middleware.Recover())
	e.Use(middleware.Logger())

	s := &Server{store: st, echo: e}
	e.GET("/health", s.health)

	api := e.Group("/api/v1")

	pl := api.Group("/plans")
	pl.POST("", s.createPlan)
	pl.GET("", s.listPlans)
	pl.GET("/by-name/:name", s.getPlanByName)
	pl.GET("/:id", s.getPlan)
	pl.PUT("/:id", s.updatePlan)
	pl.DELETE("/:id", s.deletePlan)

	pl.POST("/:id/evaluate", s.evaluate)
	pl.POST("/:id/curve", s.curve)
	pl.POST("/:id/risks", s.risks)
	pl.POST("/:id/aoql", s.aoql)

	api.POST("/design/single", s.designSingle)
	api.POST("/design/double", s.designDouble)

	stg := api.Group("/streams")
	stg.POST("", s.createStream)
	stg.GET("", s.listStreams)
	stg.GET("/:id", s.getStream)
	stg.DELETE("/:id", s.deleteStream)
	stg.POST("/:id/batches", s.addBatch)
	stg.PUT("/:id/batches/:batch_id", s.updateBatch)
	stg.DELETE("/:id/batches/:batch_id", s.deleteBatch)
	stg.POST("/:id/flags", s.setFlag)
	stg.POST("/:id/resume", s.resume)

	return s
}

// Start serves HTTP.
func (s *Server) Start(addr string) error {
	return s.echo.Start(addr)
}

// Echo exposes the engine for tests on ephemeral ports.
func (s *Server) Echo() *echo.Echo { return s.echo }

func (s *Server) health(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
}

// nopValidator performs no struct-tag validation; validation is explicit
// in handlers so field-level errors can be reported precisely.
type nopValidator struct{}

func (nopValidator) Validate(i any) error { return nil }

// --- shared response helpers ---

type errBody struct {
	Error  string            `json:"error"`
	Fields []plan.FieldError `json:"fields,omitempty"`
}

func fail(c echo.Context, status int, msg string, fields ...plan.FieldError) error {
	return c.JSON(status, errBody{Error: msg, Fields: fields})
}

func domainError(c echo.Context, err error) error {
	var fe plan.FieldError
	if errors.As(err, &fe) {
		return fail(c, http.StatusBadRequest, "validation error", fe)
	}
	var ve plan.ValidationErrors
	if errors.As(err, &ve) {
		return fail(c, http.StatusBadRequest, "validation error", ve...)
	}
	var ec store.ErrConflict
	if errors.As(err, &ec) {
		return fail(c, http.StatusConflict, ec.Error())
	}
	if errors.Is(err, store.ErrNotFound) {
		return fail(c, http.StatusNotFound, "not found")
	}
	return fail(c, http.StatusBadRequest, err.Error())
}

// toJSON renders a plan with its full parameter set.
type planJSON struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Kind         string `json:"kind"`
	Distribution string `json:"distribution"`
	Approximate  bool   `json:"approximate"`
	N            *int   `json:"n_lot,omitempty"`
	SampleN      int    `json:"n,omitempty"`
	C            int    `json:"c,omitempty"`
	N1           int    `json:"n1,omitempty"`
	C1           int    `json:"c1,omitempty"`
	R1           int    `json:"r1,omitempty"`
	N2           int    `json:"n2,omitempty"`
	C2           int    `json:"c2,omitempty"`
}

func planToJSON(p *plan.Plan) planJSON {
	j := planJSON{
		ID: p.ID, Name: p.Name, Kind: string(p.Kind),
		Distribution: string(p.Distribution), Approximate: p.Approximate,
		N: p.N,
	}
	if p.Kind == plan.KindSingle {
		j.SampleN, j.C = p.SampleSize, p.AcceptNumber
	} else {
		j.N1, j.C1, j.R1, j.N2, j.C2 = p.N1, p.C1, p.R1, p.N2, p.C2
	}
	return j
}

func parseTime(c echo.Context, raw string) (time.Time, error) {
	if raw == "" {
		return time.Now().UTC(), nil
	}
	return time.Parse(time.RFC3339, raw)
}

// severityJSON is a small alias used by stream responses.
type severityJSON = inspection.Severity
