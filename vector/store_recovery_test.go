package vector

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// openAndSeed 在数据目录写入一组固定数据，模拟“运行一段时间后进程退出”。
func openAndSeed(t *testing.T, dir string, compact bool) {
	t.Helper()
	store, err := Open(Config{DataDir: dir, MaxRecords: 100_000, MaxVectorDim: 8,
		MaxBatchSize: 500, MaxConcurrent: 64})
	if err != nil {
		t.Fatal(err)
	}
	recs := []Record{
		{ID: "keep", Vector: []float32{1, 0, 0}, Metadata: map[string]string{"cat": "a"}},
		{ID: "upd", Vector: []float32{1, 0, 0}, Metadata: map[string]string{"cat": "a"}},
		{ID: "gone", Vector: []float32{1, 0, 0}, Metadata: map[string]string{"cat": "a"}},
	}
	if err := store.BatchUpsert(recs); err != nil {
		t.Fatal(err)
	}
	// 更新：旧向量 (1,0,0) -> 新向量 (0,1,0)，恢复后绝不能召回旧向量。
	if err := store.Upsert(Record{ID: "upd", Vector: []float32{0, 1, 0},
		Metadata: map[string]string{"cat": "b"}}); err != nil {
		t.Fatal(err)
	}
	// 删除：恢复后绝不可见。
	if err := store.Delete("gone"); err != nil {
		t.Fatal(err)
	}
	// 原子批次：整批可见。
	batch := []Record{
		{ID: "b1", Vector: []float32{0, 0, 1}, Metadata: map[string]string{"cat": "c"}},
		{ID: "b2", Vector: []float32{0, 0, 1}, Metadata: map[string]string{"cat": "c"}},
	}
	if err := store.BatchUpsert(batch); err != nil {
		t.Fatal(err)
	}

	if compact {
		if err := store.Rebuild(context.Background()); err != nil {
			t.Fatal(err)
		}
		// 压缩后再追加一条写入，验证“快照 + WAL 尾巴”恢复路径。
		if err := store.Upsert(Record{ID: "tail", Vector: []float32{1, 1, 0},
			Metadata: map[string]string{"cat": "d"}}); err != nil {
			t.Fatal(err)
		}
	}
	// 模拟进程退出：不调用 Close（或调用 Close，二者都应安全）。
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

// verifyRecovered 在重新打开的存储上校验一致状态，返回命中数供日志记录。
func verifyRecovered(t *testing.T, dir string, label string) {
	t.Helper()
	store, err := Open(Config{DataDir: dir, MaxRecords: 100_000, MaxVectorDim: 8,
		MaxBatchSize: 500, MaxConcurrent: 64})
	if err != nil {
		t.Fatalf("[%s] 重新打开失败: %v", label, err)
	}
	defer store.Close()

	st := store.Stats()
	t.Logf("[%s] 恢复后 stats=%+v", label, st)
	if st.Records != 5 { // keep, upd, b1, b2, tail
		t.Fatalf("[%s] 恢复记录数不一致: got=%d want=5", label, st.Records)
	}

	// 删除不复活。
	if _, ok := store.Get("gone"); ok {
		t.Fatalf("[%s] 已删除记录 gone 在恢复后复活", label)
	}
	// 更新不回退到旧向量 / 旧元数据。
	r, ok := store.Get("upd")
	if !ok {
		t.Fatalf("[%s] upd 恢复后不存在", label)
	}
	if r.Vector[1] != 1 || r.Metadata["cat"] != "b" {
		t.Fatalf("[%s] upd 恢复到旧状态: %+v", label, r)
	}

	// 检索判定：查 (0,1,0) 时 upd 必须排第一，而不是返回旧向量的 keep。
	res, err := store.Search(SearchRequest{Vector: []float32{0, 1, 0}, TopK: 5})
	if err != nil {
		t.Fatal(err)
	}
	logSearch(t, label+" 检索 (0,1,0)", SearchRequest{Vector: []float32{0, 1, 0}, TopK: 5}, res)
	if len(res.Hits) == 0 || res.Hits[0].ID != "upd" {
		t.Fatalf("[%s] 恢复后检索结论错误: %v", label, hitIDs(res))
	}

	// 过滤与元数据一致。
	res, err = store.Search(SearchRequest{Vector: []float32{0, 0, 1}, TopK: 10,
		Filter: &Filter{Equals: map[string]string{"cat": "c"}}})
	if err != nil {
		t.Fatal(err)
	}
	logSearch(t, label+" 过滤 cat=c", SearchRequest{Vector: []float32{0, 0, 1}, TopK: 10,
		Filter: &Filter{Equals: map[string]string{"cat": "c"}}}, res)
	if got := hitIDs(res); fmt.Sprint(got) != "[b1 b2]" {
		t.Fatalf("[%s] 原子批次恢复不完整: got=%v want=[b1 b2]", label, got)
	}

	// 恢复后可继续写入、删除并再次检索，状态可演进。
	if err := store.Upsert(Record{ID: "after", Vector: []float32{0, 1, 0}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete("keep"); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.Get("keep"); ok {
		t.Fatalf("[%s] 恢复后删除未生效", label)
	}
}

// TestRecoveryFromWAL 模拟崩溃（不走 Close，不产生收尾快照），
// 完全依赖 WAL 重放恢复，结论必须与崩溃前已确认状态一致。
func TestRecoveryFromWAL(t *testing.T) {
	dir := t.TempDir()
	// 直接构造“崩溃”场景：写入后不 Close，只丢弃内存句柄。
	func() {
		store, err := Open(Config{DataDir: dir, MaxRecords: 100_000, MaxVectorDim: 8,
			MaxBatchSize: 500, MaxConcurrent: 64})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.BatchUpsert([]Record{
			{ID: "keep", Vector: []float32{1, 0, 0}, Metadata: map[string]string{"cat": "a"}},
			{ID: "upd", Vector: []float32{1, 0, 0}, Metadata: map[string]string{"cat": "a"}},
			{ID: "gone", Vector: []float32{1, 0, 0}, Metadata: map[string]string{"cat": "a"}},
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.Upsert(Record{ID: "upd", Vector: []float32{0, 1, 0},
			Metadata: map[string]string{"cat": "b"}}); err != nil {
			t.Fatal(err)
		}
		if err := store.Delete("gone"); err != nil {
			t.Fatal(err)
		}
		if err := store.BatchUpsert([]Record{
			{ID: "b1", Vector: []float32{0, 0, 1}, Metadata: map[string]string{"cat": "c"}},
			{ID: "b2", Vector: []float32{0, 0, 1}, Metadata: map[string]string{"cat": "c"}},
		}); err != nil {
			t.Fatal(err)
		}
		// 故意不 Close：模拟进程崩溃，WAL 是唯一恢复来源。
	}()

	if entries, err := readWAL(filepath.Join(dir, walFileName)); err != nil || len(entries) != 4 {
		t.Fatalf("WAL 应包含 4 条已确认原子变更: entries=%d err=%v", len(entries), err)
	} else {
		t.Logf("[WAL恢复] WAL 条目数=%d，无收尾快照时逐条重放", len(entries))
	}
	verifyRecoveredCount(t, dir, "WAL恢复", 4) // keep/upd/b1/b2，无 tail
}

// verifyRecoveredCount 是 verifyRecovered 的计数可参数化版本（崩溃场景无 tail）。
func verifyRecoveredCount(t *testing.T, dir, label string, wantRecords int) {
	t.Helper()
	store, err := Open(Config{DataDir: dir, MaxRecords: 100_000, MaxVectorDim: 8,
		MaxBatchSize: 500, MaxConcurrent: 64})
	if err != nil {
		t.Fatalf("[%s] 重新打开失败: %v", label, err)
	}
	defer store.Close()
	st := store.Stats()
	t.Logf("[%s] 恢复后 stats=%+v", label, st)
	if st.Records != wantRecords {
		t.Fatalf("[%s] 恢复记录数不一致: got=%d want=%d", label, st.Records, wantRecords)
	}
	if _, ok := store.Get("gone"); ok {
		t.Fatalf("[%s] 已删除记录 gone 在恢复后复活", label)
	}
	r, ok := store.Get("upd")
	if !ok || r.Vector[1] != 1 || r.Metadata["cat"] != "b" {
		t.Fatalf("[%s] upd 未恢复到最新状态: %+v ok=%v", label, r, ok)
	}
	res, err := store.Search(SearchRequest{Vector: []float32{0, 1, 0}, TopK: 5})
	if err != nil {
		t.Fatal(err)
	}
	logSearch(t, label+" 检索 (0,1,0)", SearchRequest{Vector: []float32{0, 1, 0}, TopK: 5}, res)
	if len(res.Hits) == 0 || res.Hits[0].ID != "upd" {
		t.Fatalf("[%s] 恢复后检索结论错误: %v", label, hitIDs(res))
	}
	res, err = store.Search(SearchRequest{Vector: []float32{0, 0, 1}, TopK: 10,
		Filter: &Filter{Equals: map[string]string{"cat": "c"}}})
	if err != nil {
		t.Fatal(err)
	}
	if got := hitIDs(res); fmt.Sprint(got) != "[b1 b2]" {
		t.Fatalf("[%s] 原子批次恢复不完整: got=%v want=[b1 b2]", label, got)
	}
	t.Logf("[%s] 删除不复活、更新不回退、批次完整，恢复一致", label)
}

// TestRecoveryFromSnapshotAndWAL 重建压缩后重启：快照 + WAL 尾巴恢复一致。
func TestRecoveryFromSnapshotAndWAL(t *testing.T) {
	dir := t.TempDir()
	openAndSeed(t, dir, true)
	if _, err := os.Stat(filepath.Join(dir, snapshotFileName)); err != nil {
		t.Fatalf("压缩后应存在快照: %v", err)
	}
	verifyRecovered(t, dir, "快照+WAL恢复")
}

// TestIdempotentReopen 同一数据目录被反复“恢复”（含重复重放），
// 结果不得错乱或重复：每次打开记录数与检索结论完全一致。
func TestIdempotentReopen(t *testing.T) {
	dir := t.TempDir()
	openAndSeed(t, dir, true)
	for round := 0; round < 3; round++ {
		label := fmt.Sprintf("重复恢复#%d", round+1)
		store, err := Open(Config{DataDir: dir, MaxRecords: 100_000, MaxVectorDim: 8,
			MaxBatchSize: 500, MaxConcurrent: 64})
		if err != nil {
			t.Fatal(err)
		}
		st := store.Stats()
		t.Logf("[%s] stats=%+v", label, st)
		if st.Records != 5 {
			t.Fatalf("[%s] 重复恢复导致记录数错乱: got=%d want=5", label, st.Records)
		}
		res, err := store.Search(SearchRequest{Vector: []float32{1, 0, 0}, TopK: 100})
		if err != nil {
			t.Fatal(err)
		}
		// 所有恢复轮次都必须看不到 gone，且 ID 集合一致。
		idSet := map[string]int{}
		for _, h := range res.Hits {
			idSet[h.ID]++
			if h.ID == "gone" {
				t.Fatalf("[%s] 删除记录在重复恢复后复活", label)
			}
			if idSet[h.ID] > 1 {
				t.Fatalf("[%s] 记录 %s 在结果中重复", label, h.ID)
			}
		}
		logSearch(t, label, SearchRequest{Vector: []float32{1, 0, 0}, TopK: 100}, res)
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestNoHalfBatchAfterRecovery 在 fsync 前的未确认批次（模拟为缺失最后一条日志）
// 重启后必须完全不可见。
func TestNoHalfBatchAfterRecovery(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Config{DataDir: dir, MaxRecords: 100, MaxVectorDim: 8,
		MaxBatchSize: 500, MaxConcurrent: 64})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BatchUpsert([]Record{
		{ID: "ok1", Vector: []float32{1, 0, 0}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// 手工向 WAL 追加一条截断的残缺 JSON（模拟崩溃时写到一半的未确认写入）。
	wf, err := os.OpenFile(filepath.Join(dir, walFileName), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wf.WriteString("{\"seq\":99,\"op\":\"upsert\",\"records\":[{\"id\":\"phantom\",\"vect"); err != nil {
		t.Fatal(err)
	}
	_ = wf.Sync()
	_ = wf.Close()

	store2, err := Open(Config{DataDir: dir, MaxRecords: 100, MaxVectorDim: 8,
		MaxBatchSize: 500, MaxConcurrent: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	if _, ok := store2.Get("phantom"); ok {
		t.Fatal("未确认的残缺批次在恢复后可见（半可见数据）")
	}
	if _, ok := store2.Get("ok1"); !ok {
		t.Fatal("已确认记录在恢复后丢失")
	}
	t.Logf("[未确认写入不可见] stats=%+v", store2.Stats())
}
