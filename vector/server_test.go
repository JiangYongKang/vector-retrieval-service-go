package vector

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestServer(t *testing.T) (*Server, *Store) {
	t.Helper()
	store, err := Open(Config{MaxRecords: 5, MaxVectorDim: 8, MaxBatchSize: 3, MaxConcurrent: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return NewServer(store), store
}

func do(t *testing.T, h http.Handler, method, path string, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	out := map[string]any{}
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}
	t.Logf("[HTTP] %s %s body=%s -> %d %s", method, path, body, rec.Code,
		strings.TrimSpace(rec.Body.String()))
	return rec.Code, out
}

func errCode(m map[string]any) string {
	e, _ := m["error"].(map[string]any)
	c, _ := e["code"].(string)
	return c
}

// TestHTTPCodes 验证拒绝原因在 HTTP 层可区分：400/404/507/429。
func TestHTTPCodes(t *testing.T) {
	srv, store := newTestServer(t)
	h := srv.Handler()

	if code, _ := do(t, h, "POST", "/v1/vectors", `{"id":"a","vector":[1,0,0]}`); code != 200 {
		t.Fatalf("写入应 200, got %d", code)
	}
	// 参数非法 -> 400
	if code, m := do(t, h, "POST", "/v1/vectors", `{"id":"b","vector":[1]}`); code != 400 ||
		errCode(m) != string(CodeInvalidArgument) {
		t.Fatalf("维度非法应 400/INVALID_ARGUMENT, got %d %s", code, errCode(m))
	}
	// 容量不足 -> 507（MaxRecords=5）：先填满剩余 4 个名额，再写第 6 条。
	do(t, h, "POST", "/v1/vectors", `{"id":"z1","vector":[1,0,0]}`)
	do(t, h, "POST", "/v1/vectors", `{"id":"z2","vector":[1,0,0]}`)
	do(t, h, "POST", "/v1/vectors", `{"id":"z3","vector":[1,0,0]}`)
	do(t, h, "POST", "/v1/vectors", `{"id":"z4","vector":[1,0,0]}`)
	if code, m := do(t, h, "POST", "/v1/vectors", `{"id":"overflow","vector":[1,0,0]}`); code != 507 ||
		errCode(m) != string(CodeCapacityExceeded) {
		t.Fatalf("记录数超限应 507/CAPACITY_EXCEEDED, got %d %s", code, errCode(m))
	}
	// 超过批量上限（MaxBatchSize=3）也是容量类拒绝 507。
	if code, m := do(t, h, "POST", "/v1/vectors/batch",
		`{"records":[{"id":"a","vector":[1,0,0]},{"id":"a","vector":[1,0,0]},{"id":"a","vector":[1,0,0]},{"id":"a","vector":[1,0,0]}]}`); code != 507 {
		t.Fatalf("批量超限应 507, got %d %s", code, errCode(m))
	}
	// 不存在 -> 404
	if code, m := do(t, h, "DELETE", "/v1/vectors/ghost", ""); code != 404 ||
		errCode(m) != string(CodeNotFound) {
		t.Fatalf("删除不存在应 404/NOT_FOUND, got %d %s", code, errCode(m))
	}
	// 非法 JSON -> 400
	if code, _ := do(t, h, "POST", "/v1/search", `{bad json`); code != 400 {
		t.Fatalf("非法 JSON 应 400, got %d", code)
	}

	// 背压 -> 429：并发槽容量 4，全部占满后请求必须立即被拒绝。
	for i := 0; i < cap(store.sem); i++ {
		store.sem <- struct{}{}
	}
	got429 := false
	for attempt := 0; attempt < 20; attempt++ {
		code, m := do(t, h, "POST", "/v1/vectors", `{"id":"p","vector":[1,0,0]}`)
		if code == 429 && errCode(m) == string(CodeTooBusy) {
			got429 = true
			break
		}
	}
	for i := 0; i < cap(store.sem); i++ {
		<-store.sem
	}
	if !got429 {
		t.Fatal("并发槽占满时未观察到 429/TOO_BUSY 背压拒绝")
	}
}

// TestHTTPSearchFlow 覆盖正常检索链路及 generation 解释字段。
func TestHTTPSearchFlow(t *testing.T) {
	srv, _ := newTestServer(t)
	h := srv.Handler()

	do(t, h, "POST", "/v1/vectors", `{"id":"a","vector":[1,0,0],"metadata":{"cat":"x"}}`)
	do(t, h, "POST", "/v1/vectors", `{"id":"b","vector":[0,1,0],"metadata":{"cat":"y"}}`)

	code, m := do(t, h, "POST", "/v1/search",
		`{"vector":[1,0,0],"top_k":1,"filter":{"equals":{"cat":"x"}}}`)
	if code != 200 {
		t.Fatalf("检索应 200, got %d", code)
	}
	hits, _ := m["hits"].([]any)
	if len(hits) != 1 {
		t.Fatalf("过滤后应命中 1 条, got %v", hits)
	}
	first, _ := hits[0].(map[string]any)
	if first["id"] != "a" {
		t.Fatalf("命中应为 a, got %v", first["id"])
	}
	if gen, _ := m["generation"].(float64); gen < 1 {
		t.Fatalf("响应应携带 generation 解释字段, got %v", m["generation"])
	}
	t.Logf("[判定依据] generation=%v 命中=%v，分数与过滤条件一致", m["generation"], first["id"])
}
