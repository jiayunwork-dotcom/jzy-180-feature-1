package httpapi

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"sampling-svc/internal/plan"
)

func (s *Server) createPlan(c echo.Context) error {
	var req plan.Request
	if err := c.Bind(&req); err != nil {
		return fail(c, http.StatusBadRequest, "invalid JSON: "+err.Error())
	}
	p, err := plan.Build(req)
	if err != nil {
		return domainError(c, err)
	}
	p.ID = uuid.NewString()
	if err := s.store.CreatePlan(c.Request().Context(), p); err != nil {
		return domainError(c, err)
	}
	return c.JSON(http.StatusCreated, planToJSON(p))
}

func (s *Server) updatePlan(c echo.Context) error {
	id := c.Param("id")
	existing, err := s.store.GetPlan(c.Request().Context(), id)
	if err != nil {
		return domainError(c, err)
	}
	var req plan.Request
	if err := c.Bind(&req); err != nil {
		return fail(c, http.StatusBadRequest, "invalid JSON: "+err.Error())
	}
	p, err := plan.Build(req)
	if err != nil {
		return domainError(c, err)
	}
	p.ID = existing.ID
	if err := s.store.UpdatePlan(c.Request().Context(), p); err != nil {
		return domainError(c, err)
	}
	return c.JSON(http.StatusOK, planToJSON(p))
}

func (s *Server) getPlan(c echo.Context) error {
	p, err := s.store.GetPlan(c.Request().Context(), c.Param("id"))
	if err != nil {
		return domainError(c, err)
	}
	return c.JSON(http.StatusOK, planToJSON(p))
}

func (s *Server) getPlanByName(c echo.Context) error {
	p, err := s.store.GetPlanByName(c.Request().Context(), c.Param("name"))
	if err != nil {
		return domainError(c, err)
	}
	return c.JSON(http.StatusOK, planToJSON(p))
}

func (s *Server) listPlans(c echo.Context) error {
	ps, err := s.store.ListPlans(c.Request().Context())
	if err != nil {
		return domainError(c, err)
	}
	out := make([]planJSON, 0, len(ps))
	for _, p := range ps {
		out = append(out, planToJSON(p))
	}
	return c.JSON(http.StatusOK, map[string]any{"plans": out})
}

func (s *Server) deletePlan(c echo.Context) error {
	if err := s.store.DeletePlan(c.Request().Context(), c.Param("id")); err != nil {
		return domainError(c, err)
	}
	return c.NoContent(http.StatusNoContent)
}
