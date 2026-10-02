package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"sampling-svc/internal/store"
)

func newTestServer(t *testing.T) (*Server, *store.Store) {
	t.Helper()
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("PG_TEST_DSN not set; skipping HTTP integration test")
	}
	// Isolated schema: runs safely in parallel with the store test binary.
	st := store.NewIsolatedTestStore(t, dsn, "test_httpapi")
	return NewServer(st), st
}

func doJSON(t *testing.T, e httpTest, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rdr = bytes.NewReader(raw)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rdr)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	out := map[string]any{}
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}
	return rec.Code, out
}

type httpTest interface {
	ServeHTTP(w http.ResponseWriter, r *http.Request)
}

func testDSNHTTP() string { return os.Getenv("PG_TEST_DSN") }

// TestHTTPPlanLifecycleAndAnalysis drives plan CRUD plus every analysis
// endpoint and checks the hand-checkable n=80 c=2 numbers.
func TestHTTPPlanLifecycleAndAnalysis(t *testing.T) {
	srv, _ := newTestServer(t)
	e := srv.Echo()

	code, body := doJSON(t, e, "POST", "/api/v1/plans", map[string]any{
		"name": "n80c2", "kind": "single",
		"single": map[string]any{"n": 80, "c": 2},
	})
	if code != http.StatusCreated {
		t.Fatalf("create=%d body=%v", code, body)
	}
	pid := body["id"].(string)

	// by-name lookup
	code, body = doJSON(t, e, "GET", "/api/v1/plans/by-name/n80c2", nil)
	if code != 200 || body["id"] != pid {
		t.Fatalf("by-name=%d %v", code, body)
	}

	// evaluate the hand-checkable points
	code, body = doJSON(t, e, "POST", "/api/v1/plans/"+pid+"/evaluate",
		map[string]any{"p": []float64{0, 0.01, 0.05, 1}})
	if code != 200 {
		t.Fatalf("evaluate=%d %v", code, body)
	}
	pts := body["points"].([]any)
	pa := func(i int) float64 {
		return pts[i].(map[string]any)["pa"].(float64)
	}
	if pa(0) != 1 || pa(3) != 0 {
		t.Fatalf("endpoints wrong: %v %v", pa(0), pa(3))
	}
	if abs(pa(1)-0.9534467) > 1e-5 || abs(pa(2)-0.2306189) > 1e-5 {
		t.Fatalf("n=80 c=2 numbers wrong: %.6f %.6f", pa(1), pa(2))
	}

	// risks
	code, body = doJSON(t, e, "POST", "/api/v1/plans/"+pid+"/risks",
		map[string]any{"aql": 0.01, "ltpd": 0.05})
	if code != 200 {
		t.Fatalf("risks=%d %v", code, body)
	}
	risk := body["risks"].(map[string]any)
	if risk["alpha"].(float64) <= 0 || risk["beta"].(float64) <= 0 {
		t.Fatal("risk values must be positive")
	}

	// curve
	code, body = doJSON(t, e, "POST", "/api/v1/plans/"+pid+"/curve",
		map[string]any{"p_min": 0.0, "p_max": 0.2, "points": 51})
	if code != 200 || len(body["points"].([]any)) != 51 {
		t.Fatalf("curve=%d %v", code, code)
	}

	// validation errors carry the offending field
	code, body = doJSON(t, e, "POST", "/api/v1/plans", map[string]any{
		"name": "bad", "kind": "single",
		"single": map[string]any{"n": 5, "c": 9},
	})
	if code != http.StatusBadRequest {
		t.Fatalf("c>=n should be 400, got %d", code)
	}
	fields := body["fields"].([]any)
	if !strings.Contains(fields[0].(map[string]any)["field"].(string), "c") {
		t.Fatalf("expected field to mention c, got %v", fields)
	}

	// update then delete
	code, _ = doJSON(t, e, "PUT", "/api/v1/plans/"+pid, map[string]any{
		"name": "n80c2", "kind": "single",
		"single": map[string]any{"n": 80, "c": 1},
	})
	if code != 200 {
		t.Fatalf("update=%d", code)
	}
	code, _ = doJSON(t, e, "DELETE", "/api/v1/plans/"+pid, nil)
	if code != http.StatusNoContent {
		t.Fatalf("delete=%d", code)
	}
	code, _ = doJSON(t, e, "GET", "/api/v1/plans/"+pid, nil)
	if code != http.StatusNotFound {
		t.Fatalf("deleted plan get=%d", code)
	}
}

// TestHTTPPoissonWarningAndApproxFlag checks the approximation metadata.
func TestHTTPPoissonWarningAndApproxFlag(t *testing.T) {
	srv, _ := newTestServer(t)
	e := srv.Echo()
	_, body := doJSON(t, e, "POST", "/api/v1/plans", map[string]any{
		"name": "pois", "kind": "single",
		"single": map[string]any{"n": 200, "c": 3, "distribution": "poisson"},
	})
	if body["approximate"] != true {
		t.Fatal("poisson plan must be flagged approximate")
	}
	pid := body["id"].(string)
	_, body = doJSON(t, e, "POST", "/api/v1/plans/"+pid+"/evaluate",
		map[string]any{"p": []float64{0.1}})
	warns, _ := body["warnings"].([]any)
	if len(warns) == 0 || !strings.Contains(warns[0].(string), "n*p > 5") {
		t.Fatalf("expected n*p>5 warning, got %v", warns)
	}
}

// TestHTTPDesignEndpoints exercises both design routes.
func TestHTTPDesignEndpoints(t *testing.T) {
	srv, _ := newTestServer(t)
	e := srv.Echo()
	code, body := doJSON(t, e, "POST", "/api/v1/design/single", map[string]any{
		"aql": 0.02, "alpha": 0.05, "ltpd": 0.08, "beta": 0.10,
	})
	if code != 200 {
		t.Fatalf("design single=%d body=%v", code, body)
	}
	planBody := body["plan"].(map[string]any)
	if planBody["n"].(float64) < 1 {
		t.Fatal("designed n invalid")
	}
	if body["alpha"].(float64) > 0.05+1e-9 || body["beta"].(float64) > 0.10+1e-9 {
		t.Fatal("designed plan violates risks")
	}

	code, body = doJSON(t, e, "POST", "/api/v1/design/double", map[string]any{
		"aql": 0.02, "alpha": 0.05, "ltpd": 0.10, "beta": 0.10,
		"n2_mode": "equal", "n1_max": 150,
	})
	if code != 200 {
		t.Fatalf("design double=%d body=%v", code, body)
	}
	dp := body["plan"].(map[string]any)
	if dp["n1"].(float64) != dp["n2"].(float64) {
		t.Fatal("equal mode violated")
	}

	// infeasible -> explicit 404 message
	code, body = doJSON(t, e, "POST", "/api/v1/design/single", map[string]any{
		"aql": 0.001, "alpha": 0.001, "ltpd": 0.002, "beta": 0.001, "n_max": 10,
	})
	if code != http.StatusNotFound || !strings.Contains(body["error"].(string), "no feasible") {
		t.Fatalf("infeasible response wrong: %d %v", code, body)
	}

	// AQL >= LTPD -> 400 with field
	code, body = doJSON(t, e, "POST", "/api/v1/design/single", map[string]any{
		"aql": 0.1, "alpha": 0.05, "ltpd": 0.05, "beta": 0.1,
	})
	if code != http.StatusBadRequest {
		t.Fatalf("AQL>=LTPD must be 400, got %d", code)
	}
}

// TestHTTPStreamFlow runs a full switching flow through the API.
func TestHTTPStreamFlow(t *testing.T) {
	srv, _ := newTestServer(t)
	e := srv.Echo()

	mkPlan := func(name string, n, c int) string {
		_, b := doJSON(t, e, "POST", "/api/v1/plans", map[string]any{
			"name": name, "kind": "single",
			"single": map[string]any{"n": n, "c": c},
		})
		return b["id"].(string)
	}
	pn := mkPlan("flow-n", 20, 5)
	pt := mkPlan("flow-t", 20, 4)
	pr := mkPlan("flow-r", 20, 6)
	code, body := doJSON(t, e, "POST", "/api/v1/streams", map[string]any{
		"name": "flow", "normal_plan_id": pn,
		"tightened_plan_id": pt, "reduced_plan_id": pr,
	})
	if code != http.StatusCreated {
		t.Fatalf("create stream=%d %v", code, body)
	}
	sid := body["id"].(string)

	// two rejects -> tightened
	for _, d := range []int{20, 20} {
		code, body = doJSON(t, e, "POST", "/api/v1/streams/"+sid+"/batches", map[string]any{
			"lot_no": "R", "d1": d,
		})
		if code != http.StatusCreated {
			t.Fatalf("add batch=%d %v", code, body)
		}
	}
	if body["current"].(map[string]any)["severity"] != "tightened" {
		t.Fatalf("expected tightened, got %v", body["current"])
	}
	// backfill an earlier accept: pair of rejects no longer adjacent in
	// the triggering window? two rejects among 3 still within 5 -> stays tightened.
	code, body = doJSON(t, e, "POST", "/api/v1/streams/"+sid+"/batches", map[string]any{
		"batch_id": "old", "lot_no": "OLD",
		"inspected_at": "2025-12-31T00:00:00Z", "d1": 0,
	})
	if code != http.StatusCreated {
		t.Fatalf("backfill=%d %v", code, body)
	}
	// every recorded batch carries severity/plan/decision/score
	for _, bp := range body["batches"].([]any) {
		o := bp.(map[string]any)
		if o["severity"] == nil || o["plan_id"] == nil ||
			o["decision"] == nil || o["score"] == nil {
			t.Fatalf("batch missing required fields: %v", o)
		}
	}
	// invalid defect count -> 400 naming d1
	code, body = doJSON(t, e, "POST", "/api/v1/streams/"+sid+"/batches", map[string]any{
		"lot_no": "bad", "d1": 21,
	})
	if code != http.StatusBadRequest {
		t.Fatalf("d1>n must be 400, got %d", code)
	}
	if f, ok := body["fields"].([]any); !ok ||
		!strings.Contains(f[0].(map[string]any)["field"].(string), "d1") {
		t.Fatalf("error must name field d1: %v", body)
	}

	// flags + delete a batch via the API
	code, body = doJSON(t, e, "POST", "/api/v1/streams/"+sid+"/flags",
		map[string]any{"flag": "approved", "value": true})
	if code != http.StatusOK {
		t.Fatalf("flag=%d %v", code, body)
	}
	code, body = doJSON(t, e, "DELETE", "/api/v1/streams/"+sid+"/batches/old", nil)
	if code != http.StatusOK {
		t.Fatalf("delete batch=%d %v", code, body)
	}
	nBatches := len(body["batches"].([]any))
	if nBatches != 2 {
		t.Fatalf("expected 2 batches after delete, got %d", nBatches)
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
