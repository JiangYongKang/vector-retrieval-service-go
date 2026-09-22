package engine

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/highcumontoa/vector-retrieval-service-go/internal/errs"
	"github.com/highcumontoa/vector-retrieval-service-go/internal/model"
)

func rec(id string, v []float32, m map[string]string) model.Record {
	return model.Record{ID: id, Vector: v, Meta: m}
}

func ids(h []model.SearchHit) []string {
	out := []string{}
	for _, x := range h {
		out = append(out, x.ID)
	}
	return out
}

func TestSmokeCRUDFilter(t *testing.T) {
	e, _ := Open(context.Background(), Config{Dim: 2})
	defer e.Close()
	if err := e.Write([]model.Record{
		rec("a", []float32{1, 0}, map[string]string{"g": "x"}),
		rec("b", []float32{0.9, 0.1}, map[string]string{"g": "y"}),
		rec("c", []float32{0.8, 0.2}, map[string]string{"g": "x"}),
	}); err != nil {
		t.Fatal(err)
	}
	// Update a away from query; the old vector [1,0] would score 1, while
	// the new vector [0,1] scores 0. Assert we never see the old vector.
	if err := e.Write([]model.Record{rec("a", []float32{0, 1}, map[string]string{"g": "x"})}); err != nil {
		t.Fatal(err)
	}
	h, err := e.Search([]float32{1, 0}, 3, model.Filter{Eq: map[string]string{"g": "x"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("filtered hits=%v", ids(h))
	if len(h) == 0 || h[0].ID != "c" {
		t.Fatalf("top hit must be c, got %v", ids(h))
	}
	scoreA := float32(-1)
	for _, x := range h {
		if x.ID == "a" {
			scoreA = x.Score
		}
	}
	if scoreA > 1e-6 {
		t.Fatalf("updated a returned old vector (score=%v, want ~0)", scoreA)
	}
	if len(h) > 2 {
		t.Fatalf("filter leaked non-matching records: %v", ids(h))
	}
	// Delete c.
	if err := e.Delete("c"); err != nil {
		t.Fatal(err)
	}
	h, _ = e.Search([]float32{1, 0}, 5, model.Filter{})
	for _, x := range h {
		if x.ID == "c" {
			t.Fatal("deleted c recalled")
		}
	}
	if st := e.Stats(); st.LiveRecords != 2 {
		t.Fatalf("live=%d want 2", st.LiveRecords)
	}
}

func TestSmokeBatchRejectNoPartial(t *testing.T) {
	e, _ := Open(context.Background(), Config{Dim: 2})
	defer e.Close()
	bad := []model.Record{
		rec("ok1", []float32{1, 0}, nil),
		rec("", []float32{1, 0}, nil), // invalid empty id
	}
	err := e.Write(bad)
	if errs.KindOf(err) != errs.KindInvalid {
		t.Fatalf("want invalid, got %v", err)
	}
	if _, ok := e.Get("ok1"); ok {
		t.Fatal("invalid batch left partially visible data")
	}
	badDim := []model.Record{
		rec("ok2", []float32{1, 0}, nil),
		rec("ok3", []float32{1, 0, 0}, nil),
	}
	if errs.KindOf(e.Write(badDim)) != errs.KindInvalid {
		t.Fatal("dim mismatch must be invalid_parameter")
	}
	if _, ok := e.Get("ok2"); ok {
		t.Fatal("dim-invalid batch left partial data")
	}
}

func TestSmokeCapacity(t *testing.T) {
	e, _ := Open(context.Background(), Config{Dim: 1, MaxRecords: 2})
	defer e.Close()
	if err := e.Write([]model.Record{rec("a", []float32{1}, nil), rec("b", []float32{1}, nil)}); err != nil {
		t.Fatal(err)
	}
	if err := e.Write([]model.Record{rec("c", []float32{1}, nil)}); errs.KindOf(err) != errs.KindCapacity {
		t.Fatalf("want capacity, got %v", err)
	}
	// Updating existing stays within capacity.
	if err := e.Write([]model.Record{rec("a", []float32{2}, nil)}); err != nil {
		t.Fatalf("update must be allowed: %v", err)
	}
}

func TestSmokeRebuildCompact(t *testing.T) {
	e, _ := Open(context.Background(), Config{Dim: 2})
	defer e.Close()
	for i := 0; i < 20; i++ {
		id := string(rune('a' + i))
		if err := e.Write([]model.Record{rec(id, []float32{float32(20 - i), float32(i)}, nil)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Delete("a", "b", "c"); err != nil {
		t.Fatal(err)
	}
	before, _ := e.Search([]float32{1, 0}, 10, model.Filter{})
	st0 := e.Stats()
	if err := e.Rebuild(context.Background()); err != nil {
		t.Fatal(err)
	}
	after, _ := e.Search([]float32{1, 0}, 10, model.Filter{})
	st1 := e.Stats()
	if len(before) != len(after) {
		t.Fatalf("results differ %v vs %v", ids(before), ids(after))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("rebuild changed results: %v vs %v", before, after)
		}
	}
	if st1.IndexSlots != st1.LiveRecords || st1.IndexSlots >= st0.IndexSlots {
		t.Fatalf("compaction did not pack slots: %+v -> %+v", st0, st1)
	}
}

func TestSmokeRestartRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	openE := func() *Engine {
		e, err := Open(context.Background(), Config{Dir: dir, Dim: 2})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	e := openE()
	if err := e.Write([]model.Record{
		rec("a", []float32{1, 0}, map[string]string{"g": "1"}),
		rec("b", []float32{0, 1}, nil),
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.Write([]model.Record{rec("a", []float32{0, 1}, nil)}); err != nil { // update
		t.Fatal(err)
	}
	if err := e.Delete("b"); err != nil {
		t.Fatal(err)
	}
	// Force a snapshot checkpoint, then add post-checkpoint data so the
	// test exercises snapshot + WAL-tail recovery together.
	if err := e.Rebuild(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.Write([]model.Record{rec("c", []float32{1, 1}, map[string]string{"g": "2"})}); err != nil {
		t.Fatal(err)
	}
	before, _ := e.Search([]float32{1, 0}, 5, model.Filter{})
	e.Close()

	e2 := openE()
	defer e2.Close()
	h, _ := e2.Search([]float32{1, 0}, 5, model.Filter{})
	t.Logf("recovered hits=%v", ids(h))
	if _, ok := e2.Get("a"); !ok {
		t.Fatal("a lost after restart")
	}
	if _, ok := e2.Get("b"); ok {
		t.Fatal("deleted b reappeared after restart")
	}
	if _, ok := e2.Get("c"); !ok {
		t.Fatal("post-checkpoint c lost after restart")
	}
	after, _ := e2.Search([]float32{1, 0}, 5, model.Filter{})
	if len(before) != len(after) {
		t.Fatalf("search changed across restart %v vs %v", ids(before), ids(after))
	}
	// Idempotent: reopen twice yields same state.
	e2.Close()
	e3 := openE()
	defer e3.Close()
	if st := e3.Stats(); st.LiveRecords != 2 {
		t.Fatalf("repeated recovery altered state: %+v", st)
	}
}
