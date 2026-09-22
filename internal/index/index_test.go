package index

import (
	"testing"
)

func TestDeterministicTieBreak(t *testing.T) {
	s := NewSnapshot(2)
	// b and a are the same vector -> equal scores; tie broken by id asc.
	s = s.WithUpsert("b", []float32{1, 0}, nil)
	s = s.WithUpsert("a", []float32{1, 0}, nil)
	h, err := s.Search([]float32{1, 0}, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if h[0].ID != "a" || h[1].ID != "b" || h[0].Score != h[1].Score {
		t.Fatalf("tie-break wrong: %+v", h)
	}
	t.Logf("INPUT two identical vectors a,b; RESULT %s,%s equal score; JUDGEMENT ties broken by ascending id => reproducible", h[0].ID, h[1].ID)
}

func TestCallerMutationIsolation(t *testing.T) {
	s := NewSnapshot(2)
	vec := []float32{1, 0}
	meta := map[string]string{"k": "v"}
	s = s.WithUpsert("a", vec, meta)
	// Mutate caller-owned data after insertion; snapshot must be unaffected.
	vec[0] = 0
	meta["k"] = "hacked"
	h, _ := s.Search([]float32{1, 0}, 1, nil)
	if len(h) != 1 || h[0].ID != "a" || h[0].Score < 0.99 {
		t.Fatalf("snapshot leaked caller mutation: %+v", h)
	}
	var gotMeta map[string]string
	s.Visit(func(id string, v []float32, m map[string]string) bool { gotMeta = m; return true })
	if gotMeta["k"] != "v" {
		t.Fatal("metadata mutation leaked into snapshot")
	}
	t.Log("JUDGEMENT snapshot stores defensive copies; caller mutation invisible")
}
