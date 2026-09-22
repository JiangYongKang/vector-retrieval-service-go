package vector

// Record 是一条元数据 + 向量记录。元数据 map 是过滤与检索的唯一事实来源：
// 快照索引中的所有记录都由它派生，保证“过滤结果与元数据一致”。
type Record struct {
	ID       string            `json:"id"`
	Vector   []float32         `json:"vector"`
	Metadata map[string]string `json:"metadata"`
}

// Filter 支持 equals（key=value）与 in（key 属于候选集合）；
// 多个条件之间为 AND，单个 in 内部为 OR。
type Filter struct {
	Equals map[string]string   `json:"equals,omitempty"`
	In     map[string][]string `json:"in,omitempty"`
}

// match 判定一条元数据是否通过过滤器。所有指定的键值条件必须同时成立（AND）。
func (f *Filter) match(meta map[string]string) bool {
	if f == nil {
		return true
	}
	for k, want := range f.Equals {
		if got, ok := meta[k]; !ok || got != want {
			return false
		}
	}
	for k, candidates := range f.In {
		got, ok := meta[k]
		if !ok {
			return false
		}
		pass := false
		for _, c := range candidates {
			if got == c {
				pass = true
				break
			}
		}
		if !pass {
			return false
		}
	}
	return true
}

// SearchRequest 是相似度检索请求。
type SearchRequest struct {
	Vector   []float32 `json:"vector"`
	TopK     int       `json:"top_k"`
	Filter   *Filter   `json:"filter,omitempty"`
	MinScore float32   `json:"min_score,omitempty"`
}

// SearchHit 是单条命中，回传分数与元数据以便解释判定依据。
type SearchHit struct {
	ID       string            `json:"id"`
	Score    float32           `json:"score"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// SearchResult 携带命中列表与生成快照代号，便于解释结果来自哪个版本。
type SearchResult struct {
	Hits       []SearchHit `json:"hits"`
	Generation uint64      `json:"generation"`
	TookMicros int64       `json:"took_micros"`
}

// cloneMetadata 深拷贝元数据，用于发布不可变快照，避免外部 map 变更污染索引。
func cloneMetadata(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
