package httpapi

import (
	"net/http"
	"strings"
	"testing"
)

// createPlanForRev creates a single n=80,c=2 plan and returns its id.
func createPlanForRev(t *testing.T, e httpTest, name string) string {
	t.Helper()
	code, body := doJSON(t, e, "POST", "/api/v1/plans", map[string]any{
		"name": name, "kind": "single",
		"single": map[string]any{"n": 80, "c": 2},
	})
	if code != http.StatusCreated {
		t.Fatalf("create plan=%d %v", code, body)
	}
	return body["id"].(string)
}

func createStreamForRev(t *testing.T, e httpTest, name string) (string, string) {
	t.Helper()
	pn := createPlanForRev(t, e, name+"-n")
	pt := createPlanForRev(t, e, name+"-t")
	pr := createPlanForRev(t, e, name+"-r")
	_, body := doJSON(t, e, "POST", "/api/v1/streams", map[string]any{
		"name": name, "normal_plan_id": pn,
		"tightened_plan_id": pt, "reduced_plan_id": pr,
	})
	sid := body["id"].(string)
	return sid, pn
}

// New plans expose exactly one initial revision covering all history.
func TestHTTPRevisionInitial(t *testing.T) {
	srv, _ := newTestServer(t)
	e := srv.Echo()
	pid := createPlanForRev(t, e, "rev0")
	code, body := doJSON(t, e, "GET", "/api/v1/plans/"+pid+"/revisions", nil)
	if code != 200 {
		t.Fatalf("list=%d %v", code, body)
	}
	revs := body["revisions"].([]any)
	if len(revs) != 1 {
		t.Fatalf("want 1 initial revision, got %d", len(revs))
	}
	r0 := revs[0].(map[string]any)
	if r0["revision_no"].(float64) != 1 || r0["c"].(float64) != 2 {
		t.Fatalf("initial revision wrong: %v", r0)
	}
}

// Append with effective_at, list in order, fetch by number.
func TestHTTPRevisionAppendAndGet(t *testing.T) {
	srv, _ := newTestServer(t)
	e := srv.Echo()
	sid, pid := createStreamForRev(t, e, "revflow")

	code, body := doJSON(t, e, "POST", "/api/v1/plans/"+pid+"/revisions", map[string]any{
		"name": "revflow-n", "kind": "single",
		"single":       map[string]any{"n": 80, "c": 1},
		"effective_at": "2026-03-15T00:00:00Z",
	})
	if code != http.StatusCreated {
		t.Fatalf("append=%d %v", code, body)
	}
	rv := body["revision"].(map[string]any)
	if rv["revision_no"].(float64) != 2 || rv["c"].(float64) != 1 {
		t.Fatalf("appended revision wrong: %v", rv)
	}
	streams := body["streams"].([]any)
	if len(streams) != 1 || streams[0].(map[string]any)["stream_id"] != sid {
		t.Fatalf("diff must name the bound stream: %v", streams)
	}

	code, body = doJSON(t, e, "GET", "/api/v1/plans/"+pid+"/revisions/2", nil)
	if code != 200 || body["revision_no"].(float64) != 2 {
		t.Fatalf("get by number=%d %v", code, body)
	}
	// Unknown revision number -> 404.
	code, _ = doJSON(t, e, "GET", "/api/v1/plans/"+pid+"/revisions/99", nil)
	if code != http.StatusNotFound {
		t.Fatalf("missing revision get=%d want 404", code)
	}
	// Non-numeric revision number -> 400 with field.
	code, body = doJSON(t, e, "GET", "/api/v1/plans/"+pid+"/revisions/abc", nil)
	if code != http.StatusBadRequest {
		t.Fatalf("non-numeric revision=%d want 400", code)
	}
}

// Malformed effective time and illegal revision parameters are rejected
// with the offending field named.
func TestHTTPRevisionValidation(t *testing.T) {
	srv, _ := newTestServer(t)
	e := srv.Echo()
	_, pid := createStreamForRev(t, e, "revbad")

	// bad timestamp
	code, body := doJSON(t, e, "POST", "/api/v1/plans/"+pid+"/revisions", map[string]any{
		"name": "revbad-n", "kind": "single",
		"single":       map[string]any{"n": 80, "c": 1},
		"effective_at": "not-a-time",
	})
	if code != http.StatusBadRequest {
		t.Fatalf("bad timestamp=%d want 400", code)
	}
	fields := body["fields"].([]any)
	if !strings.Contains(fields[0].(map[string]any)["field"].(string), "effective_at") {
		t.Fatalf("timestamp error must name effective_at: %v", fields)
	}

	// invalid plan parameters (c>=n)
	code, body = doJSON(t, e, "POST", "/api/v1/plans/"+pid+"/revisions", map[string]any{
		"name": "revbad-n", "kind": "single",
		"single": map[string]any{"n": 80, "c": 99},
	})
	if code != http.StatusBadRequest {
		t.Fatalf("bad params=%d want 400", code)
	}
	fields = body["fields"].([]any)
	if !strings.Contains(fields[0].(map[string]any)["field"].(string), "c") {
		t.Fatalf("param error must name c: %v", fields)
	}

	// kind change rejected
	code, body = doJSON(t, e, "POST", "/api/v1/plans/"+pid+"/revisions", map[string]any{
		"name": "revbad-n", "kind": "double",
		"double": map[string]any{"n1": 10, "c1": 1, "r1": 3, "n2": 10, "c2": 3},
	})
	if code != http.StatusBadRequest {
		t.Fatalf("kind change=%d want 400", code)
	}
	fields = body["fields"].([]any)
	if !strings.Contains(fields[0].(map[string]any)["field"].(string), "kind") {
		t.Fatalf("kind error must name kind: %v", fields)
	}

	// unknown plan id -> 404
	code, _ = doJSON(t, e, "POST", "/api/v1/plans/does-not-exist/revisions", map[string]any{
		"name": "x", "kind": "single",
		"single": map[string]any{"n": 80, "c": 1},
	})
	if code != http.StatusNotFound {
		t.Fatalf("unknown plan=%d want 404", code)
	}
}

// A revision that makes recorded lots unjudgeable returns 422 and lists
// each offending batch.
func TestHTTPRevisionConflict422(t *testing.T) {
	srv, _ := newTestServer(t)
	e := srv.Echo()
	sid, pid := createStreamForRev(t, e, "rev422")
	if code, body := doJSON(t, e, "POST", "/api/v1/streams/"+sid+"/batches", map[string]any{
		"batch_id": "big", "lot_no": "BIG",
		"inspected_at": "2026-09-01T00:00:00Z", "d1": 50,
	}); code != http.StatusCreated {
		t.Fatalf("add batch=%d %v", code, body)
	}
	code, body := doJSON(t, e, "POST", "/api/v1/plans/"+pid+"/revisions", map[string]any{
		"name": "rev422-n", "kind": "single",
		"single":       map[string]any{"n": 20, "c": 0},
		"effective_at": "2026-01-01T00:00:00Z",
	})
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("conflict append=%d want 422 body=%v", code, body)
	}
	conflicts := body["conflicts"].([]any)
	if len(conflicts) != 1 {
		t.Fatalf("want 1 conflict, got %d", len(conflicts))
	}
	c0 := conflicts[0].(map[string]any)
	if c0["batch_id"] != "big" || c0["stream_id"] != sid {
		t.Fatalf("conflict points at wrong batch: %v", c0)
	}
}

// The legacy PUT endpoint keeps working for old clients and app animmediate revision.
func TestHTTPLegacyPUTStillWorks(t *testing.T) {
	srv, _ := newTestServer(t)
	e := srv.Echo()
	_, pid := createStreamForRev(t, e, "leg")
	code, _ := doJSON(t, e, "PUT", "/api/v1/plans/"+pid, map[string]any{
		"name": "leg-n", "kind": "single",
		"single": map[string]any{"n": 80, "c": 1},
	})
	if code != http.StatusOK {
		t.Fatalf("legacy PUT=%d want 200", code)
	}
	code, body := doJSON(t, e, "GET", "/api/v1/plans/"+pid+"/revisions", nil)
	if code != 200 {
		t.Fatalf("list=%d", code)
	}
	if len(body["revisions"].([]any)) != 2 {
		t.Fatalf("legacy PUT must append a revision, got %v", body["revisions"])
	}
}
