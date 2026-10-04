package httpapi

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v4"

	"sampling-svc/internal/plan"
	"sampling-svc/internal/store"
)

// appendRevision handles POST /plans/{id}/revisions. The body is the full
// plan parameter set (same as the legacy PUT body) plus an optional
// RFC3339 "effective_at"; omission means immediate effect.
func (s *Server) appendRevision(c echo.Context) error {
	id := c.Param("id")
	existing, err := s.store.GetPlan(c.Request().Context(), id)
	if err != nil {
		return domainError(c, err)
	}
	var req plan.RevisionRequest
	if err := c.Bind(&req); err != nil {
		return fail(c, http.StatusBadRequest, "invalid JSON: "+err.Error())
	}
	var at *time.Time
	if req.EffectiveAt != "" {
		t, err := time.Parse(time.RFC3339, req.EffectiveAt)
		if err != nil {
			return fail(c, http.StatusBadRequest, "validation error",
				fieldErrHTTP("effective_at", "must be an RFC3339 timestamp"))
		}
		at = &t
	}
	// Kind is optional on the wire for old clients; fall back to the
	// header kind when omitted.
	if req.Kind == "" {
		req.Kind = string(existing.Kind)
	}
	p, err := plan.Build(req.AsRequest(existing.Name))
	if err != nil {
		return domainError(c, err)
	}
	if p.Kind != existing.Kind {
		return fail(c, http.StatusBadRequest, "validation error",
			fieldErrHTTP("kind", "revisions cannot change the sampling kind"))
	}
	var rename *string
	if req.Name != "" && req.Name != existing.Name {
		n := req.Name
		rename = &n
	}
	res, err := s.store.AppendRevision(c.Request().Context(), id, p, rename, at)
	if err != nil {
		return revisionError(c, err)
	}
	// Reflect the updated header in the returned plan.
	fresh, err := s.store.GetPlan(c.Request().Context(), id)
	if err != nil {
		return domainError(c, err)
	}
	return c.JSON(http.StatusCreated, map[string]any{
		"plan":      planToJSON(fresh),
		"revision":  res.RevisionNo,
		"effective": res.EffectiveAt,
		"diff":      res.Streams,
	})
}

// listRevisions returns the immutable revision history of a plan.
func (s *Server) listRevisions(c echo.Context) error {
	revs, err := s.store.ListRevisions(c.Request().Context(), c.Param("id"))
	if err != nil {
		return domainError(c, err)
	}
	out := make([]map[string]any, 0, len(revs))
	for _, rv := range revs {
		out = append(out, revisionJSON(rv))
	}
	return c.JSON(http.StatusOK, map[string]any{"revisions": out})
}

// getRevision fetches one revision by number; an unknown number is 404.
func (s *Server) getRevision(c echo.Context) error {
	noRaw := c.Param("no")
	no, err := strconv.Atoi(noRaw)
	if err != nil || no < 1 {
		return fail(c, http.StatusBadRequest, "validation error",
			fieldErrHTTP("revision_no", "must be a positive integer"))
	}
	p, err := s.store.GetRevision(c.Request().Context(), c.Param("id"), no)
	if err != nil {
		return domainError(c, err)
	}
	return c.JSON(http.StatusOK, revisionJSON(plan.Revision{
		PlanID: p.ID, No: no,
		EffectiveAt: p.RevisionEffectiveAt, Plan: p,
	}))
}

func revisionJSON(rv plan.Revision) map[string]any {
	return map[string]any{
		"revision_no":  rv.No,
		"effective_at": rv.EffectiveAt,
		"plan":         planToJSON(rv.Plan),
	}
}

// revisionError maps the revision-specific failures: conflicting
// recorded lots -> 422 with the lot list; everything else to the
// standard domain mapping.
func revisionError(c echo.Context, err error) error {
	var rc store.ErrRevisionConflicts
	if errors.As(err, &rc) {
		return c.JSON(http.StatusUnprocessableEntity, map[string]any{
			"error":     "revision rejected: recorded batch(es) cannot be judged under it",
			"conflicts": rc.Conflicts,
		})
	}
	return domainError(c, err)
}
