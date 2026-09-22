package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/highcumontoa/vector-retrieval-service-go/internal/errs"
	"github.com/highcumontoa/vector-retrieval-service-go/internal/model"
)

// assertHitsConsistent verifies the explainable invariants every search
// answer must satisfy against the engine's authoritative state:
//   - every hit id exists (never deleted)
//   - ordering is non-increasing by score
//   - when a filter is given, every hit's metadata satisfies it
func assertHitsConsistent(t *testing.T, e *Engine, q []float32, k int, f model.Filter, phase string) {
	t.Helper()
	hits, view, err := e.SearchWithView(q, k, f)
	if err != nil {
		if errs.KindOf(err) == errs.KindUnavailable {
			return // allowed only during shutdown; not used here
		}
		t.Fatalf("[%s] search error: %v", phase, err)
	}
	for i, h := range hits {
		// Validate against the SAME immutable snapshot the hits came
		// from: a time-of-check race against a later delete cannot occur.
		rec, ok := view.Live[h.ID]
		if !ok {
			t.Fatalf("[%s] hit %q is not live in the answered snapshot gen=%d (deleted/stale)", phase, h.ID, view.Gen)
		}
		if len(f.Eq) > 0 && !f.Match(&rec) {
			t.Fatalf("[%s] hit %q metadata %v violates filter %v (gen=%d)", phase, h.ID, rec.Meta, f.Eq, view.Gen)
		}
		if i > 0 && hits[i-1].Score < h.Score {
			t.Fatalf("[%s] hits not sorted: %v", phase, hits)
		}
	}
}

func TestConcurrentWriteSearchRebuild(t *testing.T) {
	const dim = 4
	e, err := Open(context.Background(), Config{Dim: dim, MaxRecords: 2000, MaxConcurrent: 256, MaxBatchSize: 50})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var searchOK, rebuildOK int64
	var mu sync.Mutex

	// Writers: repeatedly upsert a rotating set of ids (update path) and
	// occasionally delete then re-create ids. Every vector has a known
	// dominant coordinate so scores are predictable.
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				id := fmt.Sprintf("w%d-k%03d", w, i%40)
				v := make([]float32, dim)
				v[i%dim] = 1
				err := e.Write([]model.Record{{
					ID:     id,
					Vector: v,
					Meta:   map[string]string{"owner": fmt.Sprintf("w%d", w), "even": fmt.Sprintf("%v", i%2 == 0)},
				}})
				if err != nil && errs.KindOf(err) != errs.KindCapacity {
					t.Errorf("writer unexpected error: %v", err)
					return
				}
				if i%17 == 0 {
					_ = e.Delete(fmt.Sprintf("w%d-k%03d", w, (i+1)%40))
				}
			}
		}(w)
	}

	// Searchers: continuously verify results agree with authoritative
	// metadata and never return deleted/stale ids.
	for s := 0; s < 4; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			q := make([]float32, dim)
			q[s%dim] = 1
			f := model.Filter{}
			if s%2 == 0 {
				f.Eq = map[string]string{"even": "true"}
			}
			for i := 0; i < 300; i++ {
				assertHitsConsistent(t, e, q, 10, f, fmt.Sprintf("searcher%d-iter%d", s, i))
				mu.Lock()
				searchOK++
				mu.Unlock()
			}
		}(s)
	}

	// Rebuilders: hammer online compaction while writes land. Reads and
	// writes must keep succeeding; snapshots must never mix old/new.
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				if err := e.Rebuild(ctx); err != nil {
					if errs.KindOf(err) != errs.KindUnavailable {
						t.Errorf("rebuilder unexpected error: %v", err)
					}
					return
				}
				mu.Lock()
				rebuildOK++
				mu.Unlock()
			}
		}(r)
	}

	wg.Wait()
	cancel()

	st := e.Stats()
	t.Logf("INPUT: 6 writers x200 ops, 4 searchers x300, 2 rebuilders x40; "+
		"RESULT searches=%d rebuilds=%d live=%d slots=%d gen=%d; "+
		"JUDGEMENT: all hits matched authoritative metadata, no stale/deleted ids, sorted scores",
		searchOK, rebuildOK, st.LiveRecords, st.IndexSlots, st.IndexGen)

	// Final compaction: slots must collapse to live count, proving no
	// duplicates survived.
	if err := e.Rebuild(context.Background()); err != nil {
		t.Fatal(err)
	}
	st = e.Stats()
	if st.IndexSlots != st.LiveRecords {
		t.Fatalf("after final rebuild slots=%d != live=%d (duplicate/garbage slots)", st.IndexSlots, st.LiveRecords)
	}
	// Live count must equal the number of distinct Get-able ids.
	seen := map[string]bool{}
	for w := 0; w < 6; w++ {
		for k := 0; k < 40; k++ {
			id := fmt.Sprintf("w%d-k%03d", w, k)
			if _, ok := e.Get(id); ok {
				if seen[id] {
					t.Fatalf("duplicate live id %s", id)
				}
				seen[id] = true
			}
		}
	}
	if len(seen) != st.LiveRecords {
		t.Fatalf("distinct ids=%d != stats live=%d", len(seen), st.LiveRecords)
	}
}

func TestBackpressureDistinguishable(t *testing.T) {
	// Capacity limit: distinct rejection reason.
	e, _ := Open(context.Background(), Config{Dim: 2, MaxRecords: 5, MaxConcurrent: 1})
	defer e.Close()
	if err := e.Write([]model.Record{
		{ID: "a", Vector: []float32{1, 0}},
		{ID: "b", Vector: []float32{1, 0}},
		{ID: "c", Vector: []float32{1, 0}},
		{ID: "d", Vector: []float32{1, 0}},
		{ID: "e", Vector: []float32{1, 0}},
	}); err != nil {
		t.Fatal(err)
	}
	err := e.Write([]model.Record{{ID: "f", Vector: []float32{1, 0}}})
	if errs.KindOf(err) != errs.KindCapacity {
		t.Fatalf("capacity: want KindCapacity got %v", err)
	}
	// Parameter invalidity: distinct reason, nothing accepted.
	err = e.Write([]model.Record{{ID: "g", Vector: []float32{1}}})
	if errs.KindOf(err) != errs.KindInvalid {
		t.Fatalf("invalid: want KindInvalid got %v", err)
	}
	// Unavailable after close: distinct reason.
	e.Close()
	if err := e.Write([]model.Record{{ID: "h", Vector: []float32{1, 0}}}); errs.KindOf(err) != errs.KindUnavailable {
		t.Fatalf("closed: want KindUnavailable got %v", err)
	}
	if _, err := e.Search([]float32{1, 0}, 1, model.Filter{}); errs.KindOf(err) != errs.KindUnavailable {
		t.Fatalf("closed search: want KindUnavailable got %v", err)
	}
	t.Logf("JUDGEMENT: capacity/invalid/unavailable produce distinct kinds: cap=%v invalid=%v closed=%v",
		errs.KindCapacity, errs.KindInvalid, errs.KindUnavailable)
}

func TestRecoveryAcrossManyRestarts(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	cfg := func() Config { return Config{Dir: dir, Dim: 3, MaxRecords: 500, MaxConcurrent: 64} }

	e, _ := Open(context.Background(), cfg())
	for i := 0; i < 50; i++ {
		if err := e.Write([]model.Record{{
			ID:     fmt.Sprintf("id%02d", i),
			Vector: []float32{float32(i), 1, 0},
			Meta:   map[string]string{"k": fmt.Sprintf("%d", i%5)},
		}}); err != nil {
			t.Fatal(err)
		}
		if i%3 == 0 {
			if err := e.Rebuild(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if i%7 == 0 {
			_ = e.Delete(fmt.Sprintf("id%02d", (i+2)%50))
		}
	}
	wantLive := e.Stats().LiveRecords
	e.Close()

	// Repeated recovery must be idempotent: state identical every time.
	for round := 0; round < 3; round++ {
		e2, err := Open(context.Background(), cfg())
		if err != nil {
			t.Fatal(err)
		}
		if got := e2.Stats().LiveRecords; got != wantLive {
			t.Fatalf("restart %d live=%d want %d", round, got, wantLive)
		}
		hits, _ := e2.Search([]float32{1, 0, 0}, 500, model.Filter{})
		if len(hits) != wantLive {
			t.Fatalf("restart %d total hits=%d want %d", round, len(hits), wantLive)
		}
		if e2.Stats().IndexSlots != e2.Stats().LiveRecords {
			// Last operation may not be a rebuild; compact now and check.
			if err := e2.Rebuild(context.Background()); err != nil {
				t.Fatal(err)
			}
			if e2.Stats().IndexSlots != e2.Stats().LiveRecords {
				t.Fatalf("restart %d duplicates after compaction", round)
			}
		}
		t.Logf("INPUT 50 upserts interleaved with rebuilds/deletes; ROUND %d RESULT live=%d hits=%d; JUDGEMENT identical across repeated recovery", round, wantLive, len(hits))
		e2.Close()
	}
}
