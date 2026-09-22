package vector

import (
	"fmt"
	"testing"
)

func vecs(dim int, vals ...float32) []float32 {
	v := make([]float32, dim)
	copy(v, vals)
	return v
}

func hitIDs(res *SearchResult) []string {
	ids := make([]string, 0, len(res.Hits))
	for _, h := range res.Hits {
		ids = append(ids, h.ID)
	}
	return ids
}

func logSearch(t *testing.T, name string, req SearchRequest, res *SearchResult) {
	t.Helper()
	t.Logf("[%s] 输入: query=%v topK=%d minScore=%.4f filter=%+v",
		name, req.Vector, req.TopK, req.MinScore, req.Filter)
	t.Logf("[%s] 命中 gen=%d took=%dμs:", name, res.Generation, res.TookMicros)
	for i, h := range res.Hits {
		t.Logf("  #%d id=%s score=%.6f metadata=%v", i+1, h.ID, h.Score, h.Metadata)
	}
}

// TestCRUDConsistency 覆盖写入/更新/删除 + 过滤的一致性：
// 删除不召回、更新不返回旧向量、过滤与元数据一致、分数排序正确。
func TestCRUDConsistency(t *testing.T) {
	store, err := Open(Config{MaxRecords: 10, MaxVectorDim: 8, MaxBatchSize: 10, MaxConcurrent: 4})
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer store.Close()

	mustUpsert := func(r Record) {
		t.Helper()
		if err := store.Upsert(r); err != nil {
			t.Fatalf("Upsert %s 失败: %v", r.ID, err)
		}
	}

	mustUpsert(Record{ID: "a", Vector: vecs(3, 1, 0, 0), Metadata: map[string]string{"cat": "x"}})
	mustUpsert(Record{ID: "b", Vector: vecs(3, 0, 1, 0), Metadata: map[string]string{"cat": "y"}})
	mustUpsert(Record{ID: "c", Vector: vecs(3, 0.9, 0.1, 0), Metadata: map[string]string{"cat": "x"}})

	// 判定依据 1：无过滤时，与 a 同向的 a、c 应排在 b 前，且 a 先于 c。
	res, err := store.Search(SearchRequest{Vector: vecs(3, 1, 0, 0), TopK: 3})
	if err != nil {
		t.Fatalf("Search 失败: %v", err)
	}
	logSearch(t, "初始检索", SearchRequest{Vector: vecs(3, 1, 0, 0), TopK: 3}, res)
	got := hitIDs(res)
	want := []string{"a", "c", "b"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("排序不一致: got=%v want=%v（判定依据：与查询向量余弦相似度降序）", got, want)
	}

	// 判定依据 2：过滤必须与元数据一致 —— cat=x 只返回 a、c。
	res, err = store.Search(SearchRequest{
		Vector: vecs(3, 1, 0, 0), TopK: 3,
		Filter: &Filter{Equals: map[string]string{"cat": "x"}},
	})
	if err != nil {
		t.Fatalf("过滤 Search 失败: %v", err)
	}
	logSearch(t, "过滤 cat=x", SearchRequest{
		Vector: vecs(3, 1, 0, 0), TopK: 3,
		Filter: &Filter{Equals: map[string]string{"cat": "x"}}}, res)
	if got := hitIDs(res); fmt.Sprint(got) != "[a c]" {
		t.Fatalf("过滤结果与元数据不一致: got=%v want=[a c]", got)
	}
	for _, h := range res.Hits {
		if h.Metadata["cat"] != "x" {
			t.Fatalf("命中 %s 的元数据不满足过滤条件: %v", h.ID, h.Metadata)
		}
	}

	// 判定依据 3：更新后不得返回旧向量 —— a 改成与 b 同向，b 应排在 a 前。
	mustUpsert(Record{ID: "a", Vector: vecs(3, 0, 1, 0), Metadata: map[string]string{"cat": "x"}})
	if r, ok := store.Get("a"); !ok || r.Vector[0] != 0 || r.Vector[1] != 1 {
		t.Fatalf("更新后 Get 仍返回旧向量: %+v ok=%v", r, ok)
	}
	res, err = store.Search(SearchRequest{Vector: vecs(3, 0, 1, 0), TopK: 2})
	if err != nil {
		t.Fatalf("更新后 Search 失败: %v", err)
	}
	logSearch(t, "更新 a 后", SearchRequest{Vector: vecs(3, 0, 1, 0), TopK: 2}, res)
	if got := hitIDs(res); got[0] != "b" && got[0] != "a" {
		t.Fatalf("更新后召回异常: %v", got)
	}
	if got[0] != "a" && got[0] != "b" {
		t.Fatal("不可达")
	}
	// a 与 b 现在同为 (0,1,0)，分数相同按 ID 升序：a 在前；c 不应进入 top2。
	if got := hitIDs(res); fmt.Sprint(got) != "[a b]" {
		t.Fatalf("更新后结论与预期不符: got=%v want=[a b]（同分时 ID 升序）", got)
	}

	// 判定依据 4：删除后不得继续召回。
	if err := store.Delete("c"); err != nil {
		t.Fatalf("Delete c 失败: %v", err)
	}
	if _, ok := store.Get("c"); ok {
		t.Fatal("删除后 Get 仍可见 c")
	}
	res, err = store.Search(SearchRequest{Vector: vecs(3, 1, 0, 0), TopK: 10})
	if err != nil {
		t.Fatalf("删除后 Search 失败: %v", err)
	}
	logSearch(t, "删除 c 后", SearchRequest{Vector: vecs(3, 1, 0, 0), TopK: 10}, res)
	for _, h := range res.Hits {
		if h.ID == "c" {
			t.Fatalf("已删除记录 c 被召回: %v", hitIDs(res))
		}
	}

	// 判定依据 5：删除不存在的记录返回 NOT_FOUND，与其他拒绝原因可区分。
	if code := errorCode(store.Delete("ghost")); code != CodeNotFound {
		t.Fatalf("删除不存在记录的错误码 got=%s want=%s", code, CodeNotFound)
	}
}
