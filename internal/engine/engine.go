// Package engine coordinates the authoritative metadata map, the
// immutable index snapshots, the write-ahead log and persisted snapshots.
//
// Consistency rules (see docs for the full statement):
//
//   - The metadata map (records) is authoritative; the published index
//     Snapshot is its immutable, point-in-time queryable projection.
//   - A write is "confirmed" only after its whole batch is durable in the
//     WAL. It is then applied to memory in the same critical section, so
//     a confirmed write is never silently lost and an invalid batch is
//     rejected before any persistence or visibility (no half-visible
//     batch).
//   - Readers pin one Snapshot pointer for the whole query. Rebuild
//     swaps the pointer atomically, so a query sees either the complete
//     old index or the complete new one, never a mix.
//   - Recovery = latest snapshot + WAL prefix replay. Replay consists of
//     upserts/deletes keyed by ID and is therefore idempotent; replaying
//     entries already covered by a snapshot yields the same state.
package engine

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/highcumontoa/vector-retrieval-service-go/internal/errs"
	"github.com/highcumontoa/vector-retrieval-service-go/internal/index"
	"github.com/highcumontoa/vector-retrieval-service-go/internal/model"
	"github.com/highcumontoa/vector-retrieval-service-go/internal/wal"
)

// Default resource limits used when a Config field is left zero.
const (
	defaultMaxRecords    = 100_000
	defaultMaxConcurrent = 64
	defaultMaxBatchSize  = 1_000
)

// Config configures an Engine.
type Config struct {
	// Dir is the data directory for WAL and snapshots. Empty means
	// in-memory only (no persistence across restarts).
	Dir string
	// Dim is the mandatory vector dimensionality.
	Dim int
	// MaxRecords is the hard cap on live records (capacity limit).
	MaxRecords int
	// MaxConcurrent bounds mutating/rebuild operations accepted at once.
	MaxConcurrent int
	// MaxBatchSize bounds the records accepted in one Write.
	MaxBatchSize int
}

// Stats is a point-in-time operational summary.
type Stats struct {
	LiveRecords int    // live records in the authoritative map
	IndexSlots  int    // slots in the published snapshot (>= live before compaction)
	IndexGen    uint64 // published snapshot generation
	WALEntries  uint64 // WAL frames appended since start/base
	InFlight    int    // mutating/rebuild ops currently holding a slot
	Rebuilding  bool   // an online rebuild is in progress
	LastSnapSeq uint64 // WAL sequence covered by the last checkpoint snapshot
	Persisted   bool   // whether durability is enabled
}

// Engine is the service core. The zero value is not usable; use Open.
type Engine struct {
	cfg Config

	mu sync.RWMutex
	// Guarded by mu:
	closed     bool
	records    map[string]*model.Record // authoritative metadata
	snap       *index.Snapshot          // published immutable projection
	gen        uint64                   // bumped on every published snapshot
	walSeq     uint64                   // uncheckpointed WAL frames (resets on checkpoint)
	commitSeq  uint64                   // monotonic mutation counter across checkpoints/restarts
	snapSeq    uint64                   // commitSeq covered by the persisted snapshot
	rebuilding bool

	inflight chan struct{} // concurrency (backpressure) semaphore
	w        *wal.WAL
}

// Open creates an engine and restores state from cfg.Dir (atomic
// snapshot + idempotent WAL replay) when it contains data. With an empty
// Dir the engine runs fully in memory.
func Open(ctx context.Context, cfg Config) (*Engine, error) {
	if cfg.Dim <= 0 {
		return nil, errs.New(errs.KindInvalid, "dim must be positive")
	}
	if cfg.MaxRecords <= 0 {
		cfg.MaxRecords = defaultMaxRecords
	}
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = defaultMaxConcurrent
	}
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = defaultMaxBatchSize
	}

	walPath := ""
	if cfg.Dir != "" {
		if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
			return nil, fmt.Errorf("engine: create data dir: %w", err)
		}
		walPath = cfg.Dir + "/wal.log"
	}
	w, err := wal.Open(walPath)
	if err != nil {
		return nil, err
	}

	e := &Engine{
		cfg:      cfg,
		records:  map[string]*model.Record{},
		snap:     index.NewSnapshot(cfg.Dim),
		inflight: make(chan struct{}, cfg.MaxConcurrent),
		w:        w,
	}

	if err := e.recover(ctx); err != nil {
		w.Close()
		return nil, err
	}
	return e, nil
}

// recover loads the newest snapshot and replays the WAL on top of it.
// Snapshot load failure (missing/corrupt/partial) degrades gracefully to
// WAL-only replay. Replay is idempotent: upserts replace by ID and
// deletes remove by ID, so applying an entry already in the snapshot
// cannot duplicate or corrupt a record.
func (e *Engine) recover(ctx context.Context) error {
	base := index.NewSnapshot(e.cfg.Dim)
	var snapSeq uint64
	if e.cfg.Dir != "" {
		if s, seq, ok, err := loadSnapshot(e.cfg.Dir); err != nil {
			return err
		} else if ok {
			if s.Dim() != e.cfg.Dim {
				return errs.New(errs.KindInvalid,
					fmt.Sprintf("snapshot dim %d != configured dim %d", s.Dim(), e.cfg.Dim))
			}
			base = s
			snapSeq = seq
		}
	}

	frames, err := e.w.Replay(func(seq uint64, en wal.Entry) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		switch en.Op {
		case wal.OpUpsert:
			base = base.WithUpsert(en.ID, en.Vector, en.Meta)
		case wal.OpDelete:
			base = base.WithDelete(en.ID)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Derive the authoritative metadata map from the recovered snapshot.
	records := make(map[string]*model.Record, base.LiveLen())
	base.Visit(func(id string, vec []float32, meta map[string]string) bool {
		v := make([]float32, len(vec))
		copy(v, vec)
		records[id] = &model.Record{ID: id, Vector: v, Meta: meta}
		return true
	})

	e.mu.Lock()
	e.snap = base
	e.records = records
	e.snapSeq = snapSeq
	e.walSeq = uint64(frames)
	e.commitSeq = snapSeq + uint64(frames)
	e.gen = 1
	e.mu.Unlock()
	return nil
}

// acquireSlot reserves one concurrency slot or rejects with a
// capacity_exceeded error (backpressure) when the engine is saturated.
// The acquire is non-blocking: overload is reported distinctly rather
// than queued indefinitely.
func (e *Engine) acquireSlot() (func(), error) {
	select {
	case e.inflight <- struct{}{}:
		return func() { <-e.inflight }, nil
	default:
		return nil, errs.New(errs.KindCapacity,
			"concurrency limit exceeded, retry later")
	}
}

// Write atomically upserts a batch of records.
//
// Ordering and atomicity: every record is validated and the capacity
// decision is made BEFORE any persistence or in-memory change. The whole
// batch lands in one WAL frame; only once that frame is durable are the
// records applied and published. Hence a partially invalid batch is
// rejected as one unit with no half-visible data, and a durable failure
// never exposes unconfirmed data.
func (e *Engine) Write(records []model.Record) error {
	if len(records) == 0 {
		return errs.New(errs.KindInvalid, "empty batch")
	}
	if len(records) > e.cfg.MaxBatchSize {
		return errs.New(errs.KindInvalid,
			fmt.Sprintf("batch size %d exceeds limit %d", len(records), e.cfg.MaxBatchSize))
	}

	release, err := e.acquireSlot()
	if err != nil {
		return err
	}
	defer release()

	// Validate the entire batch up front. Rejection carries the offending
	// position so the reason is distinguishable.
	for i := range records {
		r := &records[i]
		if r.ID == "" {
			return errs.New(errs.KindInvalid, fmt.Sprintf("record[%d]: empty id", i))
		}
		if len(r.Vector) != e.cfg.Dim {
			return errs.New(errs.KindInvalid, fmt.Sprintf(
				"record[%d] id=%s: vector dim %d != configured dim %d",
				i, r.ID, len(r.Vector), e.cfg.Dim))
		}
	}

	entries := make([]wal.Entry, len(records))
	for i := range records {
		entries[i] = wal.Entry{
			Op:     wal.OpUpsert,
			ID:     records[i].ID,
			Vector: append([]float32(nil), records[i].Vector...),
			Meta:   cloneMeta(records[i].Meta),
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return errs.New(errs.KindUnavailable, "engine is closed")
	}

	// Capacity decision: count distinct batch IDs that are not already
	// live (updates to existing IDs do not grow the live set).
	netNew := 0
	seen := make(map[string]struct{}, len(records))
	for i := range records {
		id := records[i].ID
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		if _, exists := e.records[id]; !exists {
			netNew++
		}
	}
	if len(e.records)+netNew > e.cfg.MaxRecords {
		return errs.New(errs.KindCapacity, fmt.Sprintf(
			"capacity exceeded: live=%d netNew=%d limit=%d",
			len(e.records), netNew, e.cfg.MaxRecords))
	}

	// Durable first; apply only after the frame is confirmed on disk.
	seq, err := e.w.AppendBatch(entries)
	if err != nil {
		return errs.New(errs.KindUnavailable, "persist failed: "+err.Error())
	}
	e.walSeq = seq + 1
	e.commitSeq++

	for i := range records {
		r := &records[i]
		vec := append([]float32(nil), r.Vector...)
		meta := cloneMeta(r.Meta)
		e.records[r.ID] = &model.Record{ID: r.ID, Vector: vec, Meta: meta}
		e.snap = e.snap.WithUpsert(r.ID, vec, meta)
	}
	e.gen++
	return nil
}

// Delete atomically removes the given IDs. Deleting an unknown or
// already-deleted ID is not an error (idempotent). An empty ID anywhere
// rejects the whole call with invalid_parameter.
func (e *Engine) Delete(ids ...string) error {
	if len(ids) == 0 {
		return errs.New(errs.KindInvalid, "empty delete request")
	}
	release, err := e.acquireSlot()
	if err != nil {
		return err
	}
	defer release()

	for i, id := range ids {
		if id == "" {
			return errs.New(errs.KindInvalid, fmt.Sprintf("ids[%d]: empty id", i))
		}
	}

	entries := make([]wal.Entry, len(ids))
	for i, id := range ids {
		entries[i] = wal.Entry{Op: wal.OpDelete, ID: id}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return errs.New(errs.KindUnavailable, "engine is closed")
	}

	seq, err := e.w.AppendBatch(entries)
	if err != nil {
		return errs.New(errs.KindUnavailable, "persist failed: "+err.Error())
	}
	e.walSeq = seq + 1
	e.commitSeq++

	for _, id := range ids {
		if _, ok := e.records[id]; ok {
			delete(e.records, id)
			e.snap = e.snap.WithDelete(id)
		}
	}
	e.gen++
	return nil
}

// Search runs a filtered top-k cosine-similarity search against the
// currently published snapshot. The whole query pins one snapshot, so it
// observes a single point-in-time view. The candidate allow-list is built
// from the authoritative metadata, guaranteeing filtered hits agree with
// metadata state; deleted IDs are absent and updates expose only the new
// vector.
func (e *Engine) Search(query []float32, k int, f model.Filter) ([]model.SearchHit, error) {
	if k <= 0 {
		return nil, errs.New(errs.KindInvalid, "k must be positive")
	}
	if len(query) != e.cfg.Dim {
		return nil, errs.New(errs.KindInvalid, fmt.Sprintf(
			"query dim %d != configured dim %d", len(query), e.cfg.Dim))
	}

	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, errs.New(errs.KindUnavailable, "engine is closed")
	}
	snap := e.snap
	var allow map[string]struct{}
	if len(f.Eq) > 0 {
		allow = make(map[string]struct{}, len(e.records))
		for id, r := range e.records {
			if f.Match(r) {
				allow[id] = struct{}{}
			}
		}
	}
	e.mu.RUnlock()

	hits, err := snap.Search(query, k, allow)
	if err != nil {
		return nil, err
	}
	out := make([]model.SearchHit, len(hits))
	for i, h := range hits {
		out[i] = model.SearchHit{ID: h.ID, Score: h.Score}
	}
	return out, nil
}

// SnapshotView is the point-in-time live record set that a single search
// was answered from. Because the backing Snapshot is immutable, the view
// stays consistent after the read lock is released.
type SnapshotView struct {
	Gen  uint64
	Live map[string]model.Record
}

// SearchWithView behaves like Search but also returns the immutable live
// record set (and generation) of the exact snapshot the hits came from.
// It lets callers/tests validate that every hit is live and matches the
// filter against the SAME point-in-time state, with no time-of-check
// race against concurrent mutations.
func (e *Engine) SearchWithView(query []float32, k int, f model.Filter) ([]model.SearchHit, SnapshotView, error) {
	if k <= 0 {
		return nil, SnapshotView{}, errs.New(errs.KindInvalid, "k must be positive")
	}
	if len(query) != e.cfg.Dim {
		return nil, SnapshotView{}, errs.New(errs.KindInvalid, fmt.Sprintf(
			"query dim %d != configured dim %d", len(query), e.cfg.Dim))
	}

	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, SnapshotView{}, errs.New(errs.KindUnavailable, "engine is closed")
	}
	snap := e.snap
	gen := e.gen
	var allow map[string]struct{}
	if len(f.Eq) > 0 {
		allow = make(map[string]struct{}, len(e.records))
		for id, r := range e.records {
			if f.Match(r) {
				allow[id] = struct{}{}
			}
		}
	}
	e.mu.RUnlock()

	hits, err := snap.Search(query, k, allow)
	if err != nil {
		return nil, SnapshotView{}, err
	}
	view := SnapshotView{Gen: gen, Live: map[string]model.Record{}}
	snap.Visit(func(id string, vec []float32, meta map[string]string) bool {
		view.Live[id] = model.Record{ID: id, Vector: vec, Meta: meta}
		return true
	})
	out := make([]model.SearchHit, len(hits))
	for i, h := range hits {
		out[i] = model.SearchHit{ID: h.ID, Score: h.Score}
	}
	return out, view, nil
}

// Rebuild compacts/rebuilds the index online and then checkpoints a
// snapshot.
//
// Liveness: compaction runs on an immutable snapshot OUTSIDE any write
// lock, so concurrent reads and writes keep succeeding. The swap and the
// checkpoint (persist snapshot + truncate covered WAL prefix) happen in
// one write-lock critical section, which guarantees the persisted
// snapshot contains every record whose WAL prefix is discarded. If a
// write advanced the snapshot while compaction was running, the attempt
// is discarded and retried on the newer snapshot (optimistic, bounded).
// The pointer swap is atomic, therefore readers observe either the
// complete old or the complete new index and never a mixture. The brief
// fsync inside the critical section only delays (never fails) writes;
// readers, which take RLock, are unaffected.
func (e *Engine) Rebuild(ctx context.Context) error {
	release, err := e.acquireSlot()
	if err != nil {
		return err
	}
	defer release()

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return errs.New(errs.KindUnavailable, "engine is closed")
	}
	if e.rebuilding {
		e.mu.Unlock()
		return errs.New(errs.KindUnavailable, "another rebuild is in progress")
	}
	e.rebuilding = true
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		e.rebuilding = false
		e.mu.Unlock()
	}()

	const maxAttempts = 1000
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		e.mu.RLock()
		base := e.snap
		e.mu.RUnlock()

		candidate := base.Compact() // heavy work; no engine lock held

		e.mu.Lock()
		if e.closed {
			e.mu.Unlock()
			return errs.New(errs.KindUnavailable, "engine is closed")
		}
		if e.snap != base {
			// Writes advanced the snapshot; retry against the newer one
			// so the compacted index still contains every confirmed
			// write. Release the lock while we rebuild again.
			e.mu.Unlock()
			continue
		}

		// Publish the compacted index and checkpoint it atomically.
		var ckptErr error
		coveredSeq := e.commitSeq
		if e.cfg.Dir != "" && coveredSeq > e.snapSeq {
			if err := saveSnapshot(e.cfg.Dir, candidate, coveredSeq); err != nil {
				ckptErr = errs.New(errs.KindUnavailable, "snapshot persist failed: "+err.Error())
			} else if err := e.w.Truncate(); err != nil {
				ckptErr = errs.New(errs.KindUnavailable, "wal truncate failed: "+err.Error())
			}
		}
		if ckptErr == nil {
			e.snap = candidate
			e.gen++
			if e.cfg.Dir != "" && coveredSeq > e.snapSeq {
				e.snapSeq = coveredSeq
				e.walSeq = 0
			}
		}
		e.mu.Unlock()
		return ckptErr
	}
	return errs.New(errs.KindUnavailable,
		"rebuild could not converge under write load, retry")
}

// Stats returns a point-in-time operational summary.
func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return Stats{
		LiveRecords: len(e.records),
		IndexSlots:  e.snap.Len(),
		IndexGen:    e.gen,
		WALEntries:  e.walSeq,
		InFlight:    len(e.inflight),
		Rebuilding:  e.rebuilding,
		LastSnapSeq: e.snapSeq,
		Persisted:   e.cfg.Dir != "",
	}
}

// Get returns a copy of one record (used for explainability/tests).
func (e *Engine) Get(id string) (model.Record, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	r, ok := e.records[id]
	if !ok {
		return model.Record{}, false
	}
	return model.Record{
		ID:     r.ID,
		Vector: append([]float32(nil), r.Vector...),
		Meta:   cloneMeta(r.Meta),
	}, true
}

// Close flushes and releases durability state. Subsequent operations are
// rejected with index_unavailable.
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	w := e.w
	e.mu.Unlock()
	return w.Close()
}

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
