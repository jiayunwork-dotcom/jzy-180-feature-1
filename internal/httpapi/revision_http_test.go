package httpapi

import (
	"net/http"
	"strings"
	"testing"
)

// TestHTTPRevisions covers the revision lifecycle: list/get, effective
// time resolution, conflict rejection, invalid revision number, bad
// timestamp and invalid parameters.
func TestHTTPRevisions(t *testing.T) {
	srv, _ := newTestServer(t)
	e := srv.Echo()

	mkPlan := func(name string, n, c int) string {
		_, b := doJSON(t, e, "POST", "/api/v1/plans", map[string]any{
			"name": name, "kind": "single",
			"single": map[string]any{"n": n, "c": c},
		})
		return b["id"].(string)
	}
	pn := mkPlan("rev-n", 80, 2)
	pt := mkPlan("rev-t", 80, 1)
	pr := mkPlan("rev-r", 80, 3)
	code, body := doJSON(t, e, "POST", "/api/v1/streams", map[string]any{
		"name": "rev", "normal_plan_id": pn,
		"tightened_plan_id": pt, "reduced_plan_id": pr,
	})
	if code != 201 {
		t.Fatalf("create stream=%d %v", code, body)
	}
	sid := body["id"].(string)

	// Record a batch with d=2 accepted under c=2.
	code, body = doJSON(t, e, "POST", "/api/v1/streams/"+sid+"/batches", map[string]any{
		"batch_id": "b1", "lot_no": "B1",
		"inspected_at": "2026-03-01T00:00:00Z", "d1": 2,
	})
	if code != 201 {
		t.Fatalf("batch=%d %v", code, body)
	}

	// List revisions: exactly revision 1.
	code, body = doJSON(t, e, "GET", "/api/v1/plans/"+pn+"/revisions", nil)
	if code != 200 || len(body["revisions"].([]any)) != 1 {
		t.Fatalf("list=%d %v", code, body)
	}
	// Unknown revision -> 404.
	if code, _ = doJSON(t, e, "GET", "/api/v1/plans/"+pn+"/revisions/99", nil); code != 404 {
		t.Fatalf("missing revision must be 404, got %d", code)
	}
	// Malformed revision number -> 400 naming revision_no.
	code, body = doJSON(t, e, "GET", "/api/v1/plans/"+pn+"/revisions/abc", nil)
	if code != 400 {
		t.Fatalf("bad revision number must be 400, got %d", code)
	}
	if fs, ok := body["fields"].([]any); !ok ||
		!strings.Contains(fs[0].(map[string]any)["field"].(string), "revision_no") {
		t.Fatalf("field error must name revision_no: %v", body)
	}

	// Bad effective_at -> 400 naming the field.
	code, body = doJSON(t, e, "POST", "/api/v1/plans/"+pn+"/revisions", map[string]any{
		"kind": "single", "single": map[string]any{"n": 80, "c": 1},
		"effective_at": "not-a-time",
	})
	if code != 400 {
		t.Fatalf("bad timestamp must be 400, got %d", code)
	}
	if fs, _ := body["fields"].([]any); !strings.Contains(
		fs[0].(map[string]any)["field"].(string), "effective_at") {
		t.Fatalf("error must name effective_at: %v", body)
	}

	// Invalid parameters (c >= n) -> 400 naming c.
	code, body = doJSON(t, e, "POST", "/api/v1/plans/"+pn+"/revisions", map[string]any{
		"kind": "single", "single": map[string]any{"n": 1, "c": 9},
	})
	if code != 400 {
		t.Fatalf("invalid params must be 400, got %d", code)
	}

	// Attempt to change kind -> 400.
	code, _ = doJSON(t, e, "POST", "/api/v1/plans/"+pn+"/revisions", map[string]any{
		"kind":   "double",
		"double": map[string]any{"n1": 10, "c1": 2, "r1": 5, "n2": 10, "c2": 6},
	})
	if code != 400 {
		t.Fatalf("kind change must be 400, got %d", code)
	}

	// Conflict revision: shrink n below the recorded d -> 422 with the
	// lot named.
	code, body = doJSON(t, e, "POST", "/api/v1/plans/"+pn+"/revisions", map[string]any{
		"kind": "single", "single": map[string]any{"n": 1, "c": 0},
		"effective_at": "2026-01-01T00:00:00Z",
	})
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("unjudgeable revision must be 422, got %d body=%v", code, body)
	}
	conflicts := body["conflicts"].([]any)
	if len(conflicts) != 1 ||
		conflicts[0].(map[string]any)["batch_id"].(string) != "b1" {
		t.Fatalf("conflicts=%v", conflicts)
	}

	// Valid back-dated revision c=1 before the batch: it flips b1 to
	// rejected; response carries revision number and the diff.
	code, body = doJSON(t, e, "POST", "/api/v1/plans/"+pn+"/revisions", map[string]any{
		"kind": "single", "single": map[string]any{"n": 80, "c": 1},
		"effective_at": "2026-02-01T00:00:00Z",
	})
	if code != 201 {
		t.Fatalf("append=%d %v", code, body)
	}
	if body["revision"].(float64) != 2 {
		t.Fatalf("revision=%v", body["revision"])
	}
	diff := body["diff"].([]any)[0].(map[string]any)
	changed := diff["batches"].([]any)
	found := false
	for _, c := range changed {
		if c.(map[string]any)["batch_id"] == "b1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("b1 must appear in diff: %v", diff)
	}
	// Get revision 2 returns c=1.
	code, body = doJSON(t, e, "GET", "/api/v1/plans/"+pn+"/revisions/2", nil)
	if code != 200 {
		t.Fatalf("get rev 2=%d", code)
	}
	if body["plan"].(map[string]any)["c"].(float64) != 1 {
		t.Fatalf("rev2 c=%v", body["plan"])
	}
}

// TestHTTPLegacyPutAppendsRevision: old clients keep using PUT with no
// effective time; it becomes an immediate revision and never errors.
func TestHTTPLegacyPutAppendsRevision(t *testing.T) {
	srv, _ := newTestServer(t)
	e := srv.Echo()
	_, b := doJSON(t, e, "POST", "/api/v1/plans", map[string]any{
		"name": "legacy", "kind": "single",
		"single": map[string]any{"n": 80, "c": 2},
	})
	pid := b["id"].(string)
	code, _ := doJSON(t, e, "PUT", "/api/v1/plans/"+pid, map[string]any{
		"name": "legacy", "kind": "single",
		"single": map[string]any{"n": 80, "c": 1},
	})
	if code != 200 {
		t.Fatalf("legacy PUT=%d", code)
	}
	code, body := doJSON(t, e, "GET", "/api/v1/plans/"+pid+"/revisions", nil)
	if code != 200 || len(body["revisions"].([]any)) != 2 {
		t.Fatalf("PUT must append revision 2: %v", body)
	}
}

// TestHTTPAppendRevisionMissingPlan -> 404.
func TestHTTPAppendRevisionMissingPlan(t *testing.T) {
	srv, _ := newTestServer(t)
	e := srv.Echo()
	code, _ := doJSON(t, e, "POST", "/api/v1/plans/nope/revisions", map[string]any{
		"kind": "single", "single": map[string]any{"n": 80, "c": 1},
	})
	if code != 404 {
		t.Fatalf("missing plan must be 404, got %d", code)
	}
}
