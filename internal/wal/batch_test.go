package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBatchAtomicFrame(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "w.log")
	w, _ := Open(p)
	_, err := w.AppendBatch([]Entry{
		{Op: OpUpsert, ID: "a", Vector: []float32{1, 0}},
		{Op: OpUpsert, ID: "b", Vector: []float32{0, 1}},
		{Op: OpDelete, ID: "z"},
	})
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	w2, _ := Open(p)
	var got []string
	seqs := map[string]uint64{}
	n, err := w2.Replay(func(seq uint64, e Entry) error {
		got = append(got, string(e.Op)+e.ID)
		seqs[e.ID] = seq
		return nil
	})
	if err != nil || n != 1 {
		t.Fatalf("frames=%d err=%v", n, err)
	}
	if len(got) != 3 || seqs["a"] != 0 || seqs["z"] != 0 {
		t.Fatalf("batch not delivered with one seq: %v %v", got, seqs)
	}
	w2.Close()

	// Corrupt the tail of the batch frame: whole batch must vanish.
	fi, _ := os.Stat(p)
	f, _ := os.OpenFile(p, os.O_RDWR, 0o600)
	f.WriteAt([]byte{0xff, 0xff, 0xff, 0xff}, fi.Size()-3)
	f.Close()
	w3, _ := Open(p)
	n3, err := w3.Replay(func(seq uint64, e Entry) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if n3 != 0 {
		t.Fatalf("torn batch partially visible: %d entries", n3)
	}
}
