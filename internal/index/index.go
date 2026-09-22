// Package index provides immutable brute-force vector index snapshots.
//
// Consistency model: a published Snapshot is never mutated. Every upsert
// or delete produces a copy-on-write successor snapshot, so concurrent
// readers always observe one complete point-in-time view and never a mix
// of old and new data. An upsert appends a new slot and logically
// supersedes the previous slot for that ID; a delete marks the slot
// dead. Compact builds a fresh, contiguously packed snapshot containing
// only live slots: it changes storage layout but never the set of Search
// results.
package index

import (
	"math"
	"sort"

	"github.com/highcumontoa/vector-retrieval-service-go/internal/errs"
)

// Snapshot is an immutable vector index.
type Snapshot struct {
	dim     int
	ids     []string
	vectors [][]float32
	meta    []map[string]string
	alive   []bool
	pos     map[string]int // ID -> slot of its current (latest) record
	live    int
}

// NewSnapshot builds an empty snapshot expecting vectors of dimension dim.
func NewSnapshot(dim int) *Snapshot {
	return &Snapshot{dim: dim, pos: map[string]int{}}
}

// Dim returns the configured vector dimension.
func (s *Snapshot) Dim() int { return s.dim }

// Len returns the number of slots (including dead/superseded ones).
func (s *Snapshot) Len() int { return len(s.ids) }

// LiveLen returns the number of live records.
func (s *Snapshot) LiveLen() int { return s.live }

// clone returns a copy-on-write base with capacity for one more slot.
func (s *Snapshot) clone(extra int) *Snapshot {
	n := len(s.ids)
	cp := &Snapshot{
		dim:     s.dim,
		ids:     make([]string, n, n+extra),
		vectors: make([][]float32, n, n+extra),
		meta:    make([]map[string]string, n, n+extra),
		alive:   make([]bool, n, n+extra),
		pos:     make(map[string]int, len(s.pos)+extra),
		live:    s.live,
	}
	copy(cp.ids, s.ids)
	copy(cp.vectors, s.vectors)
	copy(cp.meta, s.meta)
	copy(cp.alive, s.alive)
	for k, v := range s.pos {
		cp.pos[k] = v
	}
	return cp
}

// cloneMeta copies a metadata map so callers cannot mutate snapshot
// contents through the map they passed in.
func cloneMeta(m map[string]string) map[string]string {
	if len(m) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// cloneVec copies a vector.
func cloneVec(v []float32) []float32 {
	out := make([]float32, len(v))
	copy(out, v)
	return out
}

// WithUpsert returns a new snapshot equal to s plus one upsert. If id
// already exists its previous slot is marked dead (superseded) and the
// new vector becomes the only searchable version.
func (s *Snapshot) WithUpsert(id string, vec []float32, meta map[string]string) *Snapshot {
	cp := s.clone(1)
	if idx, ok := cp.pos[id]; ok {
		// Supersede the old slot: updates never expose the old vector.
		if cp.alive[idx] {
			cp.alive[idx] = false
			cp.live--
		}
	}
	slot := len(cp.ids)
	cp.ids = append(cp.ids, id)
	cp.vectors = append(cp.vectors, cloneVec(vec))
	cp.meta = append(cp.meta, cloneMeta(meta))
	cp.alive = append(cp.alive, true)
	cp.pos[id] = slot
	cp.live++
	return cp
}

// WithDelete returns a new snapshot equal to s with id marked dead.
// Deleting an unknown or already-dead id is a no-op successor.
func (s *Snapshot) WithDelete(id string) *Snapshot {
	cp := s.clone(0)
	if idx, ok := cp.pos[id]; ok && cp.alive[idx] {
		cp.alive[idx] = false
		cp.live--
	}
	delete(cp.pos, id)
	return cp
}

// Hit is one scored search result.
type Hit struct {
	ID    string
	Score float32
}

// Search returns the top-k live records by cosine similarity, restricted
// to IDs present in allow (nil means every live record is a candidate).
// Result ordering: descending score, ties broken by ascending ID, which
// makes the output fully deterministic and reproducible.
func (s *Snapshot) Search(query []float32, k int, allow map[string]struct{}) ([]Hit, error) {
	if k < 0 {
		return nil, errs.New(errs.KindInvalid, "k must be non-negative")
	}
	if len(query) != s.dim {
		return nil, errs.New(errs.KindInvalid, "query dimension mismatch")
	}
	qnorm := norm(query)
	type scored struct {
		id    string
		score float32
	}
	var top []scored
	for i := range s.ids {
		if !s.alive[i] {
			continue
		}
		id := s.ids[i]
		if allow != nil {
			if _, ok := allow[id]; !ok {
				continue
			}
		}
		var score float32
		if qnorm > 0 {
			vn := norm(s.vectors[i])
			if vn > 0 {
				score = dot(query, s.vectors[i]) / (qnorm * vn)
			}
		}
		// Insert into sorted top-k (descending score, ascending ID).
		if len(top) < k || (k > 0 && better(score, id, top[len(top)-1].score, top[len(top)-1].id)) {
			top = append(top, scored{id, score})
			for j := len(top) - 1; j > 0 && better(top[j].score, top[j].id, top[j-1].score, top[j-1].id); j-- {
				top[j], top[j-1] = top[j-1], top[j]
			}
			if len(top) > k {
				top = top[:k]
			}
		}
	}
	out := make([]Hit, len(top))
	for i, t := range top {
		out[i] = Hit{ID: t.id, Score: t.score}
	}
	return out, nil
}

// better reports whether (scoreA,idA) must rank before (scoreB,idB).
func better(scoreA float32, idA string, scoreB float32, idB string) bool {
	if scoreA != scoreB {
		return scoreA > scoreB
	}
	return idA < idB
}

// Compact returns a fresh snapshot containing only the live records of s,
// packed contiguously (superseded/dead slot garbage from incremental
// writes is removed). The live record set, their vectors/metadata and
// therefore every Search result are unchanged.
func (s *Snapshot) Compact() *Snapshot {
	c := &Snapshot{
		dim:     s.dim,
		ids:     make([]string, 0, s.live),
		vectors: make([][]float32, 0, s.live),
		meta:    make([]map[string]string, 0, s.live),
		alive:   make([]bool, 0, s.live),
		pos:     make(map[string]int, s.live),
		live:    0,
	}
	// Iterate the position map but visit in original slot order for
	// determinism.
	order := make([]int, 0, s.live)
	for i := range s.ids {
		if s.alive[i] {
			order = append(order, i)
		}
	}
	sort.Ints(order)
	for _, i := range order {
		c.pos[s.ids[i]] = len(c.ids)
		c.ids = append(c.ids, s.ids[i])
		c.vectors = append(c.vectors, cloneVec(s.vectors[i]))
		c.meta = append(c.meta, cloneMeta(s.meta[i]))
		c.alive = append(c.alive, true)
		c.live++
	}
	return c
}

// Visit calls fn for every live slot in ascending slot order. If fn
// returns false the walk stops. Used by snapshot persistence/recovery.
func (s *Snapshot) Visit(fn func(id string, vec []float32, meta map[string]string) bool) {
	for i := range s.ids {
		if !s.alive[i] {
			continue
		}
		if !fn(s.ids[i], s.vectors[i], s.meta[i]) {
			return
		}
	}
}

// RecordAt returns the stored id/vector/meta for a slot (recovery aid).
func (s *Snapshot) slotAt(i int) (string, []float32, map[string]string, bool) {
	return s.ids[i], s.vectors[i], s.meta[i], s.alive[i]
}

func dot(a, b []float32) float32 {
	var d float32
	for i := range a {
		d += a[i] * b[i]
	}
	return d
}

func norm(a []float32) float32 {
	var s float64
	for _, v := range a {
		s += float64(v) * float64(v)
	}
	if s == 0 {
		return 0
	}
	return float32(math.Sqrt(s))
}
