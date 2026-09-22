package vector

import (
	"strings"
	"sync"
	"testing"
)

// expectCode 断言错误码并打印判定依据。
func expectCode(t *testing.T, err error, want ErrorCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("期望错误码 %s，但未返回错误", want)
	}
	if got := errorCode(err); got != want {
		t.Fatalf("拒绝原因不可区分: got=%s want=%s err=%v", got, want, err)
	}
	t.Logf("[拒绝符合预期] code=%s msg=%q", want, err.Error())
}

// TestRejectionCodes 验证三类拒绝原因稳定可区分：
// 参数非法 / 容量不足 / 并发背压 / 索引不可用。
func TestRejectionCodes(t *testing.T) {
	store, err := Open(Config{MaxRecords: 3, MaxVectorDim: 4, MaxBatchSize: 2, MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if err := store.Upsert(Record{ID: "a", Vector: vecs(3, 1, 0, 0)}); err != nil {
		t.Fatal(err)
	}

	// 参数非法：维度不一致 / 空 ID / 超过维度上限 / 非法 topK。
	expectCode(t, store.Upsert(Record{ID: "b", Vector: []float32{1, 0}}), CodeInvalidArgument)
	expectCode(t, store.Upsert(Record{ID: "", Vector: vecs(3, 1)}), CodeInvalidArgument)
	expectCode(t, store.Upsert(Record{ID: "x", Vector: make([]float32, 5)}), CodeInvalidArgument)
	_, err = store.Search(SearchRequest{Vector: vecs(3, 1), TopK: 0})
	expectCode(t, err, CodeInvalidArgument)

	// 容量不足（MaxRecords）：已有 a，再写 2 个新 ID 达到 3 尚可，
	// 写第 4 个新 ID（单条）时 4 > 3 必须以 CAPACITY_EXCEEDED 拒绝。
	if err := store.BatchUpsert([]Record{
		{ID: "b", Vector: vecs(3, 1)},
		{ID: "c", Vector: vecs(3, 1)},
	}); err != nil {
		t.Fatalf("容量内应允许的批次被拒绝: %v", err)
	}
	expectCode(t, store.Upsert(Record{ID: "d", Vector: vecs(3, 1)}), CodeCapacityExceeded)

	// 超过单批条目上限（MaxBatchSize=2）同样是容量类拒绝。
	expectCode(t, store.BatchUpsert([]Record{
		{ID: "a", Vector: vecs(3, 1)}, // 已存在，不占新增容量
		{ID: "b", Vector: vecs(3, 1)},
		{ID: "c", Vector: vecs(3, 1)},
	}), CodeCapacityExceeded)

	// 并发背压：MaxConcurrent=1，占满唯一槽位时的写请求必须收到 TOO_BUSY，
	// 已接受的请求不受影响（不会静默丢弃）。
	store.sem <- struct{}{}
	expectCode(t, store.Upsert(Record{ID: "z", Vector: vecs(3, 1)}), CodeTooBusy)
	expectCode(t, store.BatchUpsert([]Record{{ID: "z2", Vector: vecs(3, 1)}}), CodeTooBusy)
	<-store.sem
	// 释放后恢复可写（更新已存在的 a，不新增容量），证明是背压拒绝而非静默丢弃。
	if err := store.Upsert(Record{ID: "a", Vector: vecs(3, 0, 1, 0)}); err != nil {
		t.Fatalf("释放并发槽后应可写: %v", err)
	}

	// 索引不可用：关闭后的请求。
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	expectCode(t, store.Upsert(Record{ID: "q", Vector: vecs(3, 1)}), CodeIndexUnavailable)
	_, err = store.Search(SearchRequest{Vector: vecs(3, 1), TopK: 1})
	expectCode(t, err, CodeIndexUnavailable)
}

// TestBatchAtomicity 批量中任一条目非法时整批拒绝，不留半可见数据。
func TestBatchAtomicity(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Config{DataDir: dir, MaxRecords: 100, MaxVectorDim: 8, MaxBatchSize: 10, MaxConcurrent: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	good := []Record{
		{ID: "g1", Vector: vecs(3, 1, 0, 0), Metadata: map[string]string{"cat": "ok"}},
		{ID: "g2", Vector: vecs(3, 0, 1, 0)},
		{ID: "g3", Vector: vecs(3, 0, 0, 1)},
	}
	if err := store.BatchUpsert(good); err != nil {
		t.Fatalf("合法批次被拒绝: %v", err)
	}

	// 坏批次：前两条合法，第三条维度非法 —— 整批必须拒绝。
	bad := []Record{
		{ID: "n1", Vector: vecs(3, 1)},
		{ID: "n2", Vector: vecs(3, 0, 1, 0)},
		{ID: "n3", Vector: []float32{1, 2}}, // 维度不一致
	}
	err = store.BatchUpsert(bad)
	expectCode(t, err, CodeInvalidArgument)
	if !strings.Contains(err.Error(), "n3") {
		t.Fatalf("拒绝原因未指明非法条目: %v", err)
	}

	// 判定依据：批次内任何记录都不可见，且总数仍为 3。
	for _, id := range []string{"n1", "n2", "n3"} {
		if _, ok := store.Get(id); ok {
			t.Fatalf("被拒绝批次留下半可见数据: %s 可见", id)
		}
	}
	if st := store.Stats(); st.Records != 3 {
		t.Fatalf("半批次污染数据，记录数=%d want=3", st.Records)
	}
	t.Logf("[原子性符合预期] 坏批次被整批拒绝，stats=%+v", store.Stats())

	// 批内重复 ID 同样整批拒绝。
	err = store.BatchUpsert([]Record{
		{ID: "dup", Vector: vecs(3, 1)},
		{ID: "dup", Vector: vecs(3, 0, 1, 0)},
	})
	expectCode(t, err, CodeInvalidArgument)

	// 并发混合坏批次，必须全部失败、全部不可见。
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		g := g
		go func() {
			defer wg.Done()
			batch := []Record{
				{ID: fmtID("p", g, 0), Vector: vecs(3, 1)},
				{ID: fmtID("p", g, 1), Vector: []float32{9}}, // 坏条目
			}
			if err := store.BatchUpsert(batch); errorCode(err) != CodeInvalidArgument {
				t.Errorf("协程 %d 期望 INVALID_ARGUMENT，got=%v", g, err)
			}
		}()
	}
	wg.Wait()
	if st := store.Stats(); st.Records != 3 {
		t.Fatalf("并发坏批次污染数据，记录数=%d want=3", st.Records)
	}
	t.Logf("[并发原子性符合预期] 4 个坏批次全部拒绝，stats=%+v", store.Stats())
}

func fmtID(prefix string, g, i int) string {
	return prefix + "-" + itoa(g) + "-" + itoa(i)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
