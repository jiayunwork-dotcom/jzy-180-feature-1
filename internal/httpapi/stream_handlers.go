package httpapi

import (
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"sampling-svc/internal/inspection"
	"sampling-svc/internal/plan"
	"sampling-svc/internal/store"
)

type createStreamReq struct {
	Name        string `json:"name"`
	NormalID    string `json:"normal_plan_id"`
	TightenedID string `json:"tightened_plan_id"`
	ReducedID   string `json:"reduced_plan_id"`
}

type streamJSON struct {
	ID              string              `json:"id"`
	Name            string              `json:"name"`
	NormalID        string              `json:"normal_plan_id"`
	TightenedID     string              `json:"tightened_plan_id"`
	ReducedID       string              `json:"reduced_plan_id"`
	CurrentSeverity inspection.Severity `json:"current_severity"`
	CurrentScore    int                 `json:"current_score"`
}

func recToJSON(r store.StreamRecord) streamJSON {
	return streamJSON{
		ID: r.ID, Name: r.Name,
		NormalID: r.NormalID, TightenedID: r.TightenedID, ReducedID: r.ReducedID,
		CurrentSeverity: r.CurrentSeverity, CurrentScore: r.CurrentScore,
	}
}

func (s *Server) createStream(c echo.Context) error {
	var req createStreamReq
	if err := c.Bind(&req); err != nil {
		return fail(c, http.StatusBadRequest, "invalid JSON: "+err.Error())
	}
	if req.Name == "" {
		return fail(c, http.StatusBadRequest, "validation error",
			fieldErrHTTP("name", "must not be empty"))
	}
	if req.NormalID == "" || req.TightenedID == "" || req.ReducedID == "" {
		return fail(c, http.StatusBadRequest, "validation error",
			fieldErrHTTP("plan ids", "normal_plan_id, tightened_plan_id and reduced_plan_id are all required"))
	}
	rec := store.StreamRecord{
		ID: uuid.NewString(), Name: req.Name,
		NormalID: req.NormalID, TightenedID: req.TightenedID, ReducedID: req.ReducedID,
	}
	if err := s.store.CreateStream(c.Request().Context(), rec); err != nil {
		return domainError(c, err)
	}
	rec.CurrentSeverity = inspection.SeverityNormal
	return c.JSON(http.StatusCreated, recToJSON(rec))
}

func (s *Server) listStreams(c echo.Context) error {
	recs, err := s.store.ListStreams(c.Request().Context())
	if err != nil {
		return domainError(c, err)
	}
	out := make([]streamJSON, 0, len(recs))
	for _, r := range recs {
		out = append(out, recToJSON(r))
	}
	return c.JSON(http.StatusOK, map[string]any{"streams": out})
}

func (s *Server) getStream(c echo.Context) error {
	snap, err := s.store.GetSnapshot(c.Request().Context(), c.Param("id"))
	if err != nil {
		return domainError(c, err)
	}
	return c.JSON(http.StatusOK, map[string]any{
		"stream_id": snap.StreamID,
		"current":   currentJSON(snap),
		"batches":   snap.Batches,
	})
}

func currentJSON(snap inspection.Snapshot) map[string]any {
	return map[string]any{
		"severity": snap.Current.Severity,
		"score":    snap.Current.ScoreValue(),
	}
}

func (s *Server) deleteStream(c echo.Context) error {
	if err := s.store.DeleteStream(c.Request().Context(), c.Param("id")); err != nil {
		return domainError(c, err)
	}
	return c.NoContent(http.StatusNoContent)
}

// --- batches ---

type batchReq struct {
	BatchID string `json:"batch_id,omitempty"`
	LotNo   string `json:"lot_no"`
	At      string `json:"inspected_at,omitempty"`
	D1      int    `json:"d1"`
	D2      *int   `json:"d2,omitempty"`
}

func (r batchReq) toRecord() (store.BatchRecord, error) {
	at := time.Now().UTC()
	if r.At != "" {
		t, err := time.Parse(time.RFC3339, r.At)
		if err != nil {
			return store.BatchRecord{}, badField("inspected_at", "must be RFC3339 timestamp")
		}
		at = t
	}
	id := r.BatchID
	if id == "" {
		id = uuid.NewString()
	}
	rec := store.BatchRecord{
		ID: id, LotNo: r.LotNo, At: at,
		D1: r.D1, HasD2: r.D2 != nil,
	}
	if r.D2 != nil {
		rec.D2 = *r.D2
	}
	return rec, nil
}

func (s *Server) addBatch(c echo.Context) error {
	var req batchReq
	if err := c.Bind(&req); err != nil {
		return fail(c, http.StatusBadRequest, "invalid JSON: "+err.Error())
	}
	rec, err := req.toRecord()
	if err != nil {
		return domainError(c, err)
	}
	snap, err := s.store.AddBatch(c.Request().Context(), c.Param("id"), rec)
	if err != nil {
		return domainError(c, err)
	}
	return c.JSON(http.StatusCreated, snapshotJSON(snap))
}

func (s *Server) updateBatch(c echo.Context) error {
	var req batchReq
	if err := c.Bind(&req); err != nil {
		return fail(c, http.StatusBadRequest, "invalid JSON: "+err.Error())
	}
	rec, err := req.toRecord()
	if err != nil {
		return domainError(c, err)
	}
	snap, err := s.store.UpdateBatch(c.Request().Context(),
		c.Param("id"), c.Param("batch_id"), rec)
	if err != nil {
		return domainError(c, err)
	}
	return c.JSON(http.StatusOK, snapshotJSON(snap))
}

func (s *Server) deleteBatch(c echo.Context) error {
	snap, err := s.store.DeleteBatch(c.Request().Context(),
		c.Param("id"), c.Param("batch_id"))
	if err != nil {
		return domainError(c, err)
	}
	return c.JSON(http.StatusOK, snapshotJSON(snap))
}

type flagReq struct {
	Flag  string `json:"flag"`
	Value bool   `json:"value"`
	At    string `json:"at,omitempty"`
}

func (s *Server) setFlag(c echo.Context) error {
	var req flagReq
	if err := c.Bind(&req); err != nil {
		return fail(c, http.StatusBadRequest, "invalid JSON: "+err.Error())
	}
	name := inspection.FlagName(req.Flag)
	if name != inspection.FlagStable && name != inspection.FlagApproved {
		return fail(c, http.StatusBadRequest, "validation error",
			fieldErrHTTP("flag", "must be 'stable' or 'approved'"))
	}
	at := time.Now().UTC()
	if req.At != "" {
		t, err := time.Parse(time.RFC3339, req.At)
		if err != nil {
			return domainError(c, badField("at", "must be RFC3339 timestamp"))
		}
		at = t
	}
	snap, err := s.store.SetFlag(c.Request().Context(), c.Param("id"), name, req.Value, at)
	if err != nil {
		return domainError(c, err)
	}
	return c.JSON(http.StatusOK, snapshotJSON(snap))
}

type resumeReq struct {
	At string `json:"at,omitempty"`
}

func (s *Server) resume(c echo.Context) error {
	var req resumeReq
	if err := c.Bind(&req); err != nil {
		return fail(c, http.StatusBadRequest, "invalid JSON: "+err.Error())
	}
	at := time.Now().UTC()
	if req.At != "" {
		t, err := time.Parse(time.RFC3339, req.At)
		if err != nil {
			return domainError(c, badField("at", "must be RFC3339 timestamp"))
		}
		at = t
	}
	snap, err := s.store.Resume(c.Request().Context(), c.Param("id"), at)
	if err != nil {
		return domainError(c, err)
	}
	return c.JSON(http.StatusOK, snapshotJSON(snap))
}

func snapshotJSON(snap inspection.Snapshot) map[string]any {
	return map[string]any{
		"stream_id": snap.StreamID,
		"current":   currentJSON(snap),
		"batches":   snap.Batches,
	}
}

func fieldErrHTTP(f, m string) plan.FieldError {
	return plan.FieldError{Field: f, Message: m}
}

func badField(f, m string) error {
	return plan.FieldError{Field: f, Message: m}
}
