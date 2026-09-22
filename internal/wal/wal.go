// Package wal implements an append-only write-ahead log with length
// prefixing and CRC32 checksums. It makes confirmed writes durable and
// replayed idempotently during recovery.
//
// On-disk frame format (all integers big-endian):
//
//	+----------------+----------------------+-------------+
//	| payloadLen u32 | payload (payloadLen) | crc32 u32   |
//	+----------------+----------------------+-------------+
//
// payload:
//
//	op u8 | idLen u16 | id | vecLen u32 | float32... |
//	metaCount u32 | ( keyLen u16 | key | valLen u16 | val )...
//
// A frame is only visible after its full payload AND checksum land on
// disk. A crash can therefore leave at most a torn tail frame; replay
// stops at the first incomplete/incorrect frame, which makes recovery
// deterministic and repeatable.
package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"
)

// Op identifies a logged mutation.
type Op uint8

const (
	OpUpsert Op = 1
	OpDelete Op = 2
	// OpBatch wraps N entries in a single atomic frame. Replay delivers
	// all of them or none: a torn/corrupt batch frame discards the whole
	// batch, which is what guarantees that a crash cannot leave a batch
	// write half visible. A batch frame consumes one WAL sequence number.
	OpBatch Op = 3
)

// Entry is one logged mutation. For an upsert, Vector and Meta carry the
// record state; for a delete only ID is meaningful.
type Entry struct {
	Op     Op
	ID     string
	Vector []float32
	Meta   map[string]string
}

var (
	// ErrCorrupt is returned when a fully present frame fails its
	// checksum or its payload cannot be decoded.
	ErrCorrupt = errors.New("wal: corrupt frame")
)

// storage is the minimal backing medium. *os.File and memFile implement
// it, so the same code runs on disk and fully in memory.
type storage interface {
	io.Writer
	io.ReaderAt
	Stat() (size int64, err error)
	Sync() error
	Truncate(size int64) error
	Close() error
}

// WAL is an append-only log. Methods are safe for concurrent use.
type WAL struct {
	mu   sync.Mutex
	path string
	b    storage
	size int64  // physical bytes
	base uint64 // number of intact frames (sequence of next append)
}

// Open opens (creating if needed) the WAL at path and returns it ready
// for appends and reads. An empty path selects a purely in-memory log
// (useful for tests and ephemeral runs).
func Open(path string) (*WAL, error) {
	var b storage
	if path == "" {
		b = newMemFile()
	} else {
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
		if err != nil {
			return nil, fmt.Errorf("wal: open %s: %w", path, err)
		}
		fi, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, err
		}
		b = &osFile{f}
		return &WAL{path: path, b: b, size: fi.Size()}, nil
	}
	return &WAL{path: path, b: b}, nil
}

// Append durably persists one entry and returns its 0-based sequence
// number (equal to the number of frames present before the append).
func (w *WAL) Append(e Entry) (uint64, error) {
	if e.ID == "" {
		return 0, fmt.Errorf("wal: empty id")
	}
	payload, err := encodeEntry(e)
	if err != nil {
		return 0, err
	}
	return w.writeFrame(payload)
}

// AppendBatch atomically persists a whole batch in one frame and returns
// its single sequence number. On recovery the batch is delivered whole
// or discarded whole. An empty batch is rejected.
func (w *WAL) AppendBatch(entries []Entry) (uint64, error) {
	if len(entries) == 0 {
		return 0, fmt.Errorf("wal: empty batch")
	}
	for i := range entries {
		if entries[i].ID == "" {
			return 0, fmt.Errorf("wal: entry %d has empty id", i)
		}
	}
	payload := make([]byte, 0, 64)
	payload = append(payload, byte(OpBatch))
	payload = binary.BigEndian.AppendUint32(payload, uint32(len(entries)))
	for i := range entries {
		inner, err := encodeEntry(entries[i])
		if err != nil {
			return 0, fmt.Errorf("wal: batch entry %d: %w", i, err)
		}
		payload = binary.BigEndian.AppendUint32(payload, uint32(len(inner)))
		payload = append(payload, inner...)
	}
	return w.writeFrame(payload)
}

// writeFrame wraps payload with length and CRC and durably appends it as
// one frame. Frame publication is atomic under w.mu.
func (w *WAL) writeFrame(payload []byte) (uint64, error) {
	frame := make([]byte, 0, 4+len(payload)+4)
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(payload)))
	frame = append(frame, payload...)
	frame = binary.BigEndian.AppendUint32(frame, crc32.ChecksumIEEE(payload))

	w.mu.Lock()
	defer w.mu.Unlock()

	seq := w.base
	if n, err := w.b.Write(frame); err != nil {
		return 0, fmt.Errorf("wal: append: %w", err)
	} else if n != len(frame) {
		return 0, fmt.Errorf("wal: short write (%d/%d)", n, len(frame))
	}
	w.size += int64(len(frame))
	w.base++
	if err := w.b.Sync(); err != nil {
		return 0, fmt.Errorf("wal: sync: %w", err)
	}
	return seq, nil
}

// Replay feeds every intact entry to fn, strictly in append order. It
// stops at end-of-file or at the first torn (incomplete or checksum-bad)
// tail frame, physically truncates that unusable tail so later appends
// stay contiguous, and returns the number of intact entries replayed. A
// structurally malformed payload inside an intact frame returns ErrCorrupt.
func (w *WAL) Replay(fn func(seq uint64, e Entry) error) (int, error) {
	w.mu.Lock()
	b := w.b
	size := w.size
	w.mu.Unlock()

	var off int64
	count := 0
	goodEnd := off
	stopTorn := func() (int, error) {
		// Discard any unusable tail atomically under the log lock so a
		// later Append cannot be appended after garbage bytes.
		w.mu.Lock()
		defer w.mu.Unlock()
		if off < w.size {
			if err := b.Truncate(goodEnd); err != nil {
				return count, fmt.Errorf("wal: trim torn tail: %w", err)
			}
			w.size = goodEnd
			if err := b.Sync(); err != nil {
				return count, err
			}
		}
		w.base = uint64(count)
		return count, nil
	}
	var lenBuf [4]byte
	for off < size {
		// Header
		if _, err := io.ReadFull(io.NewSectionReader(readerAt(b), off, 4), lenBuf[:]); err != nil {
			// Torn header (partial or EOF): stop cleanly.
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				return stopTorn()
			}
			return count, err
		}
		payloadLen := int64(binary.BigEndian.Uint32(lenBuf[:]))
		if payloadLen <= 0 || off+4+payloadLen+4 > size {
			// Frame claims more than what is on disk: torn tail.
			return stopTorn()
		}
		buf := make([]byte, payloadLen+4)
		if _, err := io.ReadFull(io.NewSectionReader(readerAt(b), off+4, payloadLen+4), buf); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				return stopTorn()
			}
			return count, err
		}
		payload := buf[:payloadLen]
		gotCRC := binary.BigEndian.Uint32(buf[payloadLen:])
		if crc32.ChecksumIEEE(payload) != gotCRC {
			// Fully present but invalid: either real corruption or a
			// torn write. Treat as end-of-log; all prior entries are
			// intact and the unusable tail is discarded.
			return stopTorn()
		}
		entries, err := decodeFrame(payload)
		if err != nil {
			return count, fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
		// A batch frame is delivered entry-by-entry but counts as one
		// frame/sequence: all entries share seq=count and the frame is
		// consumed atomically.
		for _, ee := range entries {
			if err := fn(uint64(count), ee); err != nil {
				return count, err
			}
		}
		count++
		off += 4 + payloadLen + 4
		goodEnd = off
	}
	w.mu.Lock()
	w.base = uint64(count)
	w.mu.Unlock()
	return count, nil
}

// Truncate empties the log and forces the truncation to stable storage.
// It is called only after a snapshot covering the prefix is durable.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.b.Truncate(0); err != nil {
		return fmt.Errorf("wal: truncate: %w", err)
	}
	w.size = 0
	w.base = 0
	return w.b.Sync()
}

// Sync flushes buffered data to stable storage.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Sync()
}

// Close releases the backing storage.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Close()
}

// Size returns the current on-disk byte length.
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// ---------------------------------------------------------------------------
// encoding
// ---------------------------------------------------------------------------

func encodeEntry(e Entry) ([]byte, error) {
	if len(e.ID) > math.MaxUint16 {
		return nil, fmt.Errorf("wal: id too long")
	}
	if len(e.Vector) > math.MaxUint32/4 {
		return nil, fmt.Errorf("wal: vector too long")
	}
	buf := make([]byte, 0, 16+len(e.ID)+4*len(e.Vector))
	buf = append(buf, byte(e.Op))
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(e.ID)))
	buf = append(buf, e.ID...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(e.Vector)))
	for _, v := range e.Vector {
		buf = binary.BigEndian.AppendUint32(buf, math.Float32bits(v))
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(e.Meta)))
	for k, v := range e.Meta {
		if len(k) > math.MaxUint16 || len(v) > math.MaxUint16 {
			return nil, fmt.Errorf("wal: meta key/value too long")
		}
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(k)))
		buf = append(buf, k...)
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(v)))
		buf = append(buf, v...)
	}
	return buf, nil
}

// decodeFrame decodes one frame payload into one or more entries. A
// single-op frame yields one entry; an OpBatch frame yields all inner
// entries after validating every length prefix, so a truncated batch is
// rejected wholesale rather than partially delivered.
func decodeFrame(p []byte) ([]Entry, error) {
	if len(p) == 0 {
		return nil, errors.New("empty payload")
	}
	switch Op(p[0]) {
	case OpUpsert, OpDelete:
		e, err := decodeEntry(p)
		if err != nil {
			return nil, err
		}
		return []Entry{e}, nil
	case OpBatch:
		c := newCursor(p)
		if _, ok := c.byte(); !ok {
			return nil, errors.New("short batch op")
		}
		n, ok := c.uint32()
		if !ok {
			return nil, errors.New("short batch count")
		}
		out := make([]Entry, 0, n)
		for i := uint32(0); i < n; i++ {
			ln, ok := c.uint32()
			if !ok {
				return nil, errors.New("short batch inner length")
			}
			if c.i+int(ln) > len(c.p) {
				return nil, errors.New("truncated batch inner entry")
			}
			e, err := decodeEntry(c.p[c.i : c.i+int(ln)])
			if err != nil {
				return nil, err
			}
			c.i += int(ln)
			out = append(out, e)
		}
		if c.i != len(c.p) {
			return nil, errors.New("trailing bytes in batch frame")
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unknown op %d", p[0])
	}
}

func decodeEntry(p []byte) (Entry, error) {
	var e Entry
	r := newCursor(p)
	op, ok := r.byte()
	if !ok {
		return e, errors.New("short op")
	}
	e.Op = Op(op)
	id, ok := r.varBytes()
	if !ok {
		return e, errors.New("short id")
	}
	e.ID = string(id)
	n, ok := r.uint32()
	if !ok {
		return e, errors.New("short vector length")
	}
	e.Vector = make([]float32, n)
	for i := range e.Vector {
		bits, ok := r.uint32()
		if !ok {
			return e, errors.New("short vector payload")
		}
		e.Vector[i] = math.Float32frombits(bits)
	}
	mn, ok := r.uint32()
	if !ok {
		return e, errors.New("short meta count")
	}
	if mn > 0 {
		e.Meta = make(map[string]string, mn)
	}
	for i := uint32(0); i < mn; i++ {
		k, ok := r.varBytes()
		if !ok {
			return e, errors.New("short meta key")
		}
		v, ok := r.varBytes()
		if !ok {
			return e, errors.New("short meta value")
		}
		e.Meta[string(k)] = string(v)
	}
	if e.Op != OpUpsert && e.Op != OpDelete {
		return e, fmt.Errorf("unknown op %d", e.Op)
	}
	return e, nil
}

type cursor struct {
	p []byte
	i int
}

func newCursor(p []byte) *cursor { return &cursor{p: p} }

func (c *cursor) byte() (byte, bool) {
	if c.i >= len(c.p) {
		return 0, false
	}
	v := c.p[c.i]
	c.i++
	return v, true
}

func (c *cursor) uint16() (uint16, bool) {
	if c.i+2 > len(c.p) {
		return 0, false
	}
	v := binary.BigEndian.Uint16(c.p[c.i:])
	c.i += 2
	return v, true
}

func (c *cursor) uint32() (uint32, bool) {
	if c.i+4 > len(c.p) {
		return 0, false
	}
	v := binary.BigEndian.Uint32(c.p[c.i:])
	c.i += 4
	return v, true
}

func (c *cursor) varBytes() ([]byte, bool) {
	n, ok := c.uint16()
	if !ok || c.i+int(n) > len(c.p) {
		return nil, false
	}
	v := c.p[c.i : c.i+int(n)]
	c.i += int(n)
	return v, true
}

// ---------------------------------------------------------------------------
// backing media
// ---------------------------------------------------------------------------

// readerAt narrows storage to io.ReaderAt.
func readerAt(b storage) io.ReaderAt { return b }

// osFile adapts *os.File to the storage interface.
type osFile struct{ f *os.File }

func (o *osFile) Write(p []byte) (int, error) { return o.f.Write(p) }
func (o *osFile) ReadAt(p []byte, off int64) (int, error) {
	return o.f.ReadAt(p, off)
}
func (o *osFile) Stat() (int64, error) {
	fi, err := o.f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}
func (o *osFile) Sync() error { return o.f.Sync() }
func (o *osFile) Truncate(size int64) error {
	return o.f.Truncate(size)
}
func (o *osFile) Close() error { return o.f.Close() }

// memFile is a bytes-backed storage used when path is empty.
type memFile struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func newMemFile() *memFile { return &memFile{} }

func (m *memFile) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.buf.Write(p)
}
func (m *memFile) ReadAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data := m.buf.Bytes()
	if off >= int64(len(data)) {
		return 0, io.EOF
	}
	n := copy(p, data[off:])
	if n < len(p) {
		return n, io.ErrUnexpectedEOF
	}
	return n, nil
}
func (m *memFile) Stat() (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(m.buf.Len()), nil
}
func (m *memFile) Sync() error { return nil }
func (m *memFile) Truncate(size int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if size < int64(m.buf.Len()) {
		m.buf.Truncate(int(size))
	}
	return nil
}
func (m *memFile) Close() error { return nil }
