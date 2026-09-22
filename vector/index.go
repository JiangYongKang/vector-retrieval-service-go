package vector

import (
	"sort"
	"sync/atomic"
)

// snapshot 是某一代索引的不可变快照：持有该时刻全部记录的只读副本。
// 快照构建时会深拷贝 vector / metadata，发布后任何 goroutine 都不得修改它，
// 因而读快照无需加锁，且永远不会读到“新旧混合”的部分结果。
type snapshot struct {
	gen   uint64
	items []*Record
}

func newSnapshot(gen uint64, items []*Record) *snapshot {
	return &snapshot{gen: gen, items: items}
}

// snapshotIndex 通过原子指针替换不可变快照，保证读永远看到某一代完整索引。
type snapshotIndex struct {
	cur atomic.Pointer[snapshot]
}

func newSnapshotIndex() *snapshotIndex { return &snapshotIndex{} }

func (idx *snapshotIndex) current() *snapshot { return idx.cur.Load() }

func (idx *snapshotIndex) swap(snap *snapshot) { idx.cur.Store(snap) }

// search 在当前不可变快照上执行带过滤的暴力余弦检索。
// 排序规则固定为“分数降序，分数相同按 ID 升序”，保证结论确定、可复现。
func (snap *snapshot) search(req SearchRequest) []SearchHit {
	hits := make([]SearchHit, 0, min(len(snap.items), req.TopK))
	for _, r := range snap.items {
		if !req.Filter.match(r.Metadata) {
			continue
		}
		score := cosineSimilarity(req.Vector, r.Vector)
		if score < req.MinScore {
			continue
		}
		hits = append(hits, SearchHit{
			ID:       r.ID,
			Score:    score,
			Metadata: cloneMetadata(r.Metadata),
		})
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		return hits[i].ID < hits[j].ID
	})
	if len(hits) > req.TopK {
		hits = hits[:req.TopK]
	}
	return hits
}
