package vector

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// baselineHits 在固定数据集上返回期望命中集合（判定依据可复现）。
// 记录 rN 的向量 = (1, 0.1*N, 0)，查询 (1, 0, 0)；
// N 越大第二分量越大，余弦相似度越低，因此顺序恒为 r0, r1, ...。
func rebuildDataset(n int) []Record {
	rs := make([]Record, n)
	for i := 0; i < n; i++ {
		rs[i] = Record{
			ID:       fmt.Sprintf("r%03d", i),
			Vector:   []float32{1, float32(i) * 0.1, 0},
			Metadata: map[string]string{"grp": "base", "idx": fmt.Sprintf("%d", i)},
		}
	}
	return rs
}

// TestOnlineRebuildConcurrency 重建与大量读写并发：
//  1. 所有 Search 不失败，且任何一次命中都必须是“完整的一代”（topK 稳定）；
//  2. 重建成功，generation 单调推进；
//  3. 重建期间确认的写入最终全部可见、不丢失、不重复。
func TestOnlineRebuildConcurrency(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(Config{DataDir: dir, MaxRecords: 100_000, MaxVectorDim: 8,
		MaxBatchSize: 500, MaxConcurrent: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	const N = 200
	if err := store.BatchUpsert(rebuildDataset(N)); err != nil {
		t.Fatal(err)
	}

	stopCh := make(chan struct{})
	var stopWG sync.WaitGroup // 读者
	var mutWG sync.WaitGroup  // 写者 + 重建者
	var searchOK, searchReject atomic.Int64
	var mu sync.Mutex
	var genSeen = map[uint64]int{}
	var badResults []string

	// 读者：重建全程持续检索。
	for r := 0; r < 8; r++ {
		stopWG.Add(1)
		go func() {
			defer stopWG.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				res, err := store.Search(SearchRequest{
					Vector: []float32{1, 0, 0}, TopK: 5,
					Filter: &Filter{Equals: map[string]string{"grp": "base"}},
				})
				if err != nil {
					// 重建期间不允许出现不可用类错误；背压类错误可接受并计数。
					if code := errorCode(err); code == CodeTooBusy {
						searchReject.Add(1)
						continue
					}
					mu.Lock()
					badResults = append(badResults, "search 非背压错误: "+err.Error())
					mu.Unlock()
					return
				}
				searchOK.Add(1)
				// 判定依据：快照不可变，命中数恒为 5（base 记录数 N>=5），
				// 且首条恒为 r000（相似度最高）。若出现混合/撕裂，这里会失败。
				if len(res.Hits) != 5 || res.Hits[0].ID != "r000" {
					mu.Lock()
					badResults = append(badResults,
						fmt.Sprintf("撕裂结果 gen=%d n=%d hits=%v", res.Generation, len(res.Hits), res.Hits))
					mu.Unlock()
					return
				}
				for i := 1; i < len(res.Hits); i++ {
					if res.Hits[i-1].Score < res.Hits[i].Score {
						mu.Lock()
						badResults = append(badResults, "分数未降序，疑似新旧混合")
						mu.Unlock()
						return
					}
				}
				mu.Lock()
				genSeen[res.Generation]++
				mu.Unlock()
			}
		}()
	}

	// 写者：重建期间持续 upsert 新记录与更新旧记录、删除再写回。
	const writers = 4
	const perWriter = 50
	for w := 0; w < writers; w++ {
		mutWG.Add(1)
		go func(w int) {
			defer mutWG.Done()
			for i := 0; i < perWriter; i++ {
				id := fmt.Sprintf("w%02d-%03d", w, i)
				err := store.Upsert(Record{
					ID:       id,
					Vector:   []float32{0, 0, 1},
					Metadata: map[string]string{"grp": "new", "w": fmt.Sprintf("%d", w)},
				})
				if err != nil && errorCode(err) != CodeTooBusy {
					mu.Lock()
					badResults = append(badResults, "写入意外失败: "+err.Error())
					mu.Unlock()
					return
				}
				if err == nil {
					// 更新同一 ID（验证更新不返回旧向量）。
					if err := store.Upsert(Record{
						ID:       id,
						Vector:   []float32{0, 0, 2},
						Metadata: map[string]string{"grp": "new", "w": fmt.Sprintf("%d", w), "v": "2"},
					}); err != nil && errorCode(err) != CodeTooBusy {
						mu.Lock()
						badResults = append(badResults, "更新意外失败: "+err.Error())
						mu.Unlock()
						return
					}
				}
			}
		}(w)
	}

	// 重建者：连续多次重建，与读者、写者高度重叠。
	mutWG.Add(1)
	go func() {
		defer mutWG.Done()
		for i := 0; i < 5; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := store.Rebuild(ctx); err != nil {
				mu.Lock()
				badResults = append(badResults, "rebuild 失败: "+err.Error())
				mu.Unlock()
			}
			cancel()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// 等待全部写入与重建完成，再停止读者，做一次最终重建收敛。
	mutWG.Wait()
	close(stopCh)
	stopWG.Wait()

	if err := store.Rebuild(context.Background()); err != nil {
		t.Fatalf("最终重建失败: %v", err)
	}

	// 判定 1：无任何撕裂/非背压错误。
	for _, b := range badResults {
		t.Error(b)
	}
	// 判定 2：读者确实大量成功且观察到多个快照代（重建期间服务持续可用）。
	t.Logf("search 成功=%d 背压拒绝=%d 观察到的快照代=%v",
		searchOK.Load(), searchReject.Load(), genSeen)
	if searchOK.Load() == 0 {
		t.Fatal("重建期间没有任何成功检索")
	}
	if len(genSeen) < 2 {
		t.Fatalf("应观察到写入/重建产生的多个快照代，实际=%v", genSeen)
	}

	// 判定 3：最终过滤新记录，命中数 == 成功确认的新记录数，且无重复。
	res, err := store.Search(SearchRequest{
		Vector: []float32{0, 0, 1}, TopK: 100000,
		Filter: &Filter{Equals: map[string]string{"grp": "new"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, h := range res.Hits {
		if seen[h.ID] {
			t.Fatalf("结果出现重复记录: %s", h.ID)
		}
		seen[h.ID] = true
		if h.Metadata["v"] != "2" {
			t.Fatalf("记录 %s 返回了旧向量对应的旧元数据: %v", h.ID, h.Metadata)
		}
	}
	// 统计真实确认数（以 Get 为准，因为背压可能拒绝部分写入）。
	confirmed := 0
	for w := 0; w < writers; w++ {
		for i := 0; i < perWriter; i++ {
			if _, ok := store.Get(fmt.Sprintf("w%02d-%03d", w, i)); ok {
				confirmed++
			}
		}
	}
	logSearch(t, "重建后新记录检索", SearchRequest{TopK: 100000,
		Filter: &Filter{Equals: map[string]string{"grp": "new"}}}, res)
	if len(res.Hits) != confirmed {
		t.Fatalf("重建后记录数不一致：检索命中=%d 已确认=%d（已确认写入不得丢失/重复）",
			len(res.Hits), confirmed)
	}
	st := store.Stats()
	if st.Records != N+confirmed {
		t.Fatalf("总记录数不一致: got=%d want=%d", st.Records, N+confirmed)
	}
	t.Logf("[重建一致性符合预期] base=%d new=%d stats=%+v", N, confirmed, st)
}

// TestRebuildRejectsConcurrent 同一时刻只允许一个重建任务，第二个得到可区分拒绝。
func TestRebuildRejectsConcurrent(t *testing.T) {
	store, err := Open(Config{MaxRecords: 100, MaxVectorDim: 8, MaxBatchSize: 50, MaxConcurrent: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for i := 0; i < 100; i++ {
		_ = store.Upsert(Record{ID: fmt.Sprintf("r%d", i), Vector: []float32{1, float32(i) * 0.01, 0}})
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		store.mu.Lock()
		store.rebuilding = true
		close(entered)
		store.mu.Unlock()
		<-release
		store.mu.Lock()
		store.rebuilding = false
		store.mu.Unlock()
		close(done)
	}()
	<-entered
	err = store.Rebuild(context.Background())
	expectCode(t, err, CodeTooBusy)
	close(release)
	<-done // 等待标志确定复位后再发起重建，消除时序竞争
	// 正常重建仍然成功。
	if err := store.Rebuild(context.Background()); err != nil {
		t.Fatalf("并发解除后重建失败: %v", err)
	}
}
