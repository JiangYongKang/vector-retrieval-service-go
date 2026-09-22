package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/highcumontoa/vector-retrieval-service-go/internal/engine"
)

func newTestServer(t *testing.T) (*Server, *engine.Engine) {
	t.Helper()
	eng, err := engine.Open(context.Background(), engine.Config{Dim: 3, MaxRecords: 3})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	return New(eng), eng
}

func post(t *testing.T, h http.Handler, path string, body any) (int, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(b))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	out := map[string]any{}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

func TestHTTPEndToEnd(t *testing.T) {
	s, eng := newTestServer(t)
	h := s.Handler()

	// Batch write (one invalid entry must reject the whole batch).
	status, body := post(t, h, "/v1/records", map[string]any{
		"records": []map[string]any{
			{"id": "a", "vector": []float32{1, 0, 0}, "meta": map[string]string{"g": "1"}},
			{"id": "", "vector": []float32{1, 0, 0}},
		},
	})
	if status != http.StatusBadRequest || body["error"] != "invalid_parameter" {
		t.Fatalf("partial-invalid batch: status=%d body=%v", status, body)
	}
	t.Logf("INPUT batch with empty id; RESULT status=%d error=%v; JUDGEMENT whole batch rejected as invalid_parameter", status, body["error"])

	// Valid writes.
	if code, b := post(t, h, "/v1/records", map[string]any{"records": []map[string]any{
		{"id": "a", "vector": []float32{1, 0, 0}, "meta": map[string]string{"g": "1"}},
		{"id": "b", "vector": []float32{0, 1, 0}, "meta": map[string]string{"g": "2"}},
		{"id": "c", "vector": []float32{0.9, 0.1, 0}, "meta": map[string]string{"g": "1"}},
	}}); code != http.StatusOK {
		t.Fatalf("write: %d %v", code, b)
	}

	// Capacity: 4th distinct record over MaxRecords=3 -> 429.
	if code, b := post(t, h, "/v1/records", map[string]any{"records": []map[string]any{
		{"id": "d", "vector": []float32{0, 0, 1}},
	}}); code != http.StatusTooManyRequests || b["error"] != "capacity_exceeded" {
		t.Fatalf("capacity: status=%d body=%v", code, b)
	}
	t.Logf("JUDGEMENT over-capacity write -> 429 capacity_exceeded")

	// Filtered search.
	status, body = post(t, h, "/v1/search", map[string]any{
		"vector": []float32{1, 0, 0}, "k": 5, "filter": map[string]string{"g": "1"},
	})
	if status != http.StatusOK {
		t.Fatalf("search: %d %v", status, body)
	}
	hits := body["hits"].([]any)
	for _, hh := range hits {
		m := hh.(map[string]any)
		if m["id"] == "b" {
			t.Fatalf("filter leaked b: %v", hits)
		}
	}
	t.Logf("INPUT query=[1,0,0] filter g=1; RESULT hits=%v; JUDGEMENT only g=1 records returned", hits)

	// Delete b then confirm it cannot be recalled.
	if code, b := post(t, h, "/v1/delete", map[string]any{"ids": []string{"b"}}); code != http.StatusOK {
		t.Fatalf("delete: %d %v", code, b)
	}
	status, body = post(t, h, "/v1/search", map[string]any{"vector": []float32{0, 1, 0}, "k": 5})
	for _, hh := range body["hits"].([]any) {
		if hh.(map[string]any)["id"] == "b" {
			t.Fatal("deleted b still searchable")
		}
	}
	t.Logf("JUDGEMENT deleted id b absent from post-delete search, live=%d", eng.Stats().LiveRecords)

	// Rebuild then stats.
	if code, b := post(t, h, "/v1/rebuild", map[string]any{}); code != http.StatusOK {
		t.Fatalf("rebuild: %d %v", code, b)
	}
	if st := eng.Stats(); st.IndexSlots != 2 || st.LiveRecords != 2 {
		t.Fatalf("post-rebuild stats unexpected: %+v", st)
	}

	// Bad dimension -> 400 invalid_parameter.
	if code, b := post(t, h, "/v1/search", map[string]any{"vector": []float32{1, 0}, "k": 3}); code != http.StatusBadRequest {
		t.Fatalf("bad dim search: %d %v", code, b)
	}
}
