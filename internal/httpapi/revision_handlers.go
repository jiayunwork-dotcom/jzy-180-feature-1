package httpapi

import (
	"net/http"
	"time"

	"github.com/labstack/echo/v4"

	"sampling-svc/internal/plan"
	"sampling-svc/internal/store"
)

// revisionReq embeds the ordinary plan parameter shape and adds the
// optional effective time. Both name and kind may be omitted on a
// revision: they then inherit the current plan's values (the kind can
// never change anyway).
type revisionReq struct {
	plan.Request
	EffectiveAt string `json:"effective_at,omitempty"`
}

func (s *Server) appendRevision(c echo.Context) error {
	id := c.Param("id")
	cur, err := s.store.GetPlan(c.Request().Context(), id)
	if err != nil {
		return domainError(c, err)
	}
	var req revisionReq
	if err := c.Bind(&req); err != nil {
		return fail(c, http.StatusBadRequest, "invalid JSON: "+err.Error())
	}
	var eff *time.Time
	if req.EffectiveAt != "" {
		t, err := time.Parse(time.RFC3339, req.EffectiveAt)
		if err != nil {
			return fail(c, http.StatusBadRequest, "validation error",
				fieldErrHTTP("effective_at", "must be an RFC3339 timestamp"))
		}
		t = t.UTC()
		eff = &t
	}
	if req.Name == "" {
		req.Name = cur.Name
	}
	if req.Kind == "" {
		req.Kind = string(cur.Kind)
	}

	res, err := s.store.AppendRevision(c.Request().Context(), id,
		plan.RevisionRequest{EffectiveAt: eff, Request: req.Request},
		time.Now().UTC())
	if err != nil {
		return revisionError(c, err)
	}
	return c.JSON(http.StatusCreated, revisionResultJSON(res))
}

func (s *Server) listRevisions(c echo.Context) error {
	revs, err := s.store.ListRevisions(c.Request().Context(), c.Param("id"))
	if err != nil {
		return domainError(c, err)
	}
	out := make([]revisionJSON, 0, len(revs))
	for _, rv := range revs {
		out = append(out, revisionToJSON(rv))
	}
	return c.JSON(http.StatusOK, map[string]any{"revisions": out})
}

func (s *Server) getRevision(c echo.Context) error {
	no, ok := parseRevisionNumber(c)
	if !ok {
		return fail(c, http.StatusBadRequest, "validation error",
			fieldErrHTTP("revision_no", "must be a positive integer"))
	}
	rv, err := s.store.GetRevision(c.Request().Context(), c.Param("id"), no)
	if err != nil {
		return domainError(c, err)
	}
	return c.JSON(http.StatusOK, revisionToJSON(rv))
}

// parseRevisionNumber extracts and validates the numeric path parameter.
func parseRevisionNumber(c echo.Context) (int, bool) {
	raw := c.Param("number")
	n := 0
	for _, ch := range raw {
		if ch < '0' || ch > '9' {
			return 0, false
		}
		n = n*10 + int(ch-'0')
		if n > 1_000_000_000 {
			return 0, false
		}
	}
	if raw == "" || n < 1 {
		return 0, false
	}
	return n, true
}

type revisionJSON struct {
	PlanID      string    `json:"plan_id"`
	Revision    int       `json:"revision_no"`
	EffectiveAt time.Time `json:"effective_at"`
	CreatedAt   time.Time `json:"created_at"`
	planJSON
}

func revisionToJSON(rv plan.Revision) revisionJSON {
	return revisionJSON{
		PlanID: rv.PlanID, Revision: rv.Number,
		EffectiveAt: rv.EffectiveAt, CreatedAt: rv.CreatedAt,
		planJSON: planToJSON(rv.Plan),
	}
}

func revisionResultJSON(res store.RevisionResult) map[string]any {
	return map[string]any{
		"revision": revisionToJSON(res.Revision),
		"streams":  res.Streams,
	}
}

// revisionError maps revision-specific failures, in particular the
// per-batch conflict list for a revision that makes recorded lots
// impossible to judge.
func revisionError(c echo.Context, err error) error {
	var ce *store.RevisionConflictError
	if asRevisionConflict(err, &ce) {
		type conflictJSON struct {
			StreamID string `json:"stream_id"`
			BatchID  string `json:"batch_id"`
			LotNo    string `json:"lot_no"`
			Severity string `json:"severity"`
			Revision int    `json:"revision_no"`
			Field    string `json:"field"`
			Message  string `json:"message"`
		}
		list := make([]conflictJSON, 0, len(ce.Conflicts))
		for _, cf := range ce.Conflicts {
			list = append(list, conflictJSON{
				StreamID: cf.StreamID, BatchID: cf.BatchID, LotNo: cf.LotNo,
				Severity: string(cf.Severity), Revision: cf.Revision,
				Field: cf.Field, Message: cf.Message,
			})
		}
		return c.JSON(http.StatusUnprocessableEntity, map[string]any{
			"error":       ce.Error(),
			"revision_no": ce.Revision,
			"conflicts":   list,
		})
	}
	return domainError(c, err)
}

// asRevisionConflict is errors.As without an import cycle surprise.
func asRevisionConflict(err error, target **store.RevisionConflictError) bool {
	for err != nil {
		if e, ok := err.(*store.RevisionConflictError); ok {
			*target = e
			return true
		}
		if x, ok := err.(interface{ Unwrap() error }); ok {
			err = x.Unwrap()
			continue
		}
		break
	}
	return false
}
