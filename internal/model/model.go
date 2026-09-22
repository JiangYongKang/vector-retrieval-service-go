// Package model defines the core record and filter types for the vector
// retrieval service.
package model

// Record is the authoritative view of one indexed document: its metadata
// (ID, vector, filter attributes) as confirmed by a write.
type Record struct {
	ID     string
	Vector []float32
	Meta   map[string]string
}

// Filter narrows a similarity search to records whose metadata satisfies
// every equality constraint in Eq. All constraints must match (AND).
type Filter struct {
	Eq map[string]string
}

// Match reports whether r satisfies f. An empty filter matches everything.
// Semantics: every Eq entry must be present with the same value (AND of
// equality constraints). Filtering is always evaluated against the
// authoritative record metadata, so filtered hits are guaranteed to be
// consistent with the metadata state.
func (f Filter) Match(r *Record) bool {
	if r == nil {
		return false
	}
	for k, v := range f.Eq {
		if r.Meta[k] != v {
			return false
		}
	}
	return true
}

// SearchHit is one scored answer returned by a similarity search.
type SearchHit struct {
	ID    string
	Score float32
}
