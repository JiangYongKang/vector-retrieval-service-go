package wal

import "testing"

func TestAppendReplayOrder(t *testing.T) {
	w, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	in := []Entry{
		{Op: OpUpsert, ID: "a", Vector: []float32{1, 2}, Meta: map[string]string{"k": "v"}},
		{Op: OpDelete, ID: "a"},
		{Op: OpUpsert, ID: "b", Vector: []float32{3}},
	}
	for _, e := range in {
		if _, err := w.Append(e); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	n, err := w.Replay(func(seq uint64, e Entry) error {
		got = append(got, e.ID)
		return nil
	})
	if err != nil || n != len(in) {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if len(got) != 3 || got[0] != "a" || got[1] != "a" || got[2] != "b" {
		t.Fatalf("order/contents wrong: %v", got)
	}
	t.Logf("INPUT %d entries; RESULT %v in order; JUDGEMENT strict append order preserved", len(in), got)
}
