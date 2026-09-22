package engine

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/highcumontoa/vector-retrieval-service-go/internal/index"
)

// Persisted snapshot format (big-endian):
//
//	magic      [4]byte "VSNP"
//	version    u8  (=1)
//	dim        u32
//	seq        u64   // WAL sequence of the last mutation included
//	count      u32   // number of live records
//	records[count]:
//	  idLen u16 | id | vecLen u32 | float32... |
//	  metaCount u32 | ( keyLen u16 | key | valLen u16 | val )...
//	crc32      u32   // over everything preceding
//
// Snapshots are written to a temp file, fsynced, then atomically renamed,
// so a crash can never leave a half-written snapshot in place: recovery
// either sees the previous complete snapshot or the new one.
const (
	snapshotName    = "snapshot.bin"
	snapshotTmpName = "snapshot.bin.tmp"
	snapMagic       = "VSNP"
	snapVersion     = uint8(1)
)

func snapPath(dir string) string    { return filepath.Join(dir, snapshotName) }
func snapTmpPath(dir string) string { return filepath.Join(dir, snapshotTmpName) }

// saveSnapshot atomically persists snap together with the WAL sequence
// seq (the last mutation covered by this snapshot).
func saveSnapshot(dir string, snap *index.Snapshot, seq uint64) error {
	buf, err := encodeSnapshot(snap, seq)
	if err != nil {
		return err
	}
	tmp := snapTmpPath(dir)
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("snapshot: create: %w", err)
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return fmt.Errorf("snapshot: write: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("snapshot: sync: %w", err)
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, snapPath(dir)); err != nil {
		return fmt.Errorf("snapshot: rename: %w", err)
	}
	// Ensure the directory entry for the rename is durable.
	if df, err := os.Open(dir); err == nil {
		_ = df.Sync()
		df.Close()
	}
	return nil
}

// loadSnapshot reads and verifies the snapshot in dir. A missing snapshot
// is reported with ok=false and no error; a corrupt/partial snapshot is
// likewise treated as absent (recovery falls back to the WAL), so a
// crashed save can never poison startup.
func loadSnapshot(dir string) (snap *index.Snapshot, seq uint64, ok bool, err error) {
	f, err := os.Open(snapPath(dir))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, false, nil
		}
		return nil, 0, false, err
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, 0, false, err
	}
	if len(data) < 4+1+4+8+4+4 {
		return nil, 0, false, nil
	}
	if string(data[:4]) != snapMagic {
		return nil, 0, false, nil
	}
	storedCRC := binary.BigEndian.Uint32(data[len(data)-4:])
	if crc32.ChecksumIEEE(data[:len(data)-4]) != storedCRC {
		return nil, 0, false, nil
	}
	p := data[4 : len(data)-4]
	c := &snapCursor{p: p}
	version, _ := c.byte()
	if version != snapVersion {
		return nil, 0, false, nil
	}
	dim, good := c.uint32()
	if !good {
		return nil, 0, false, nil
	}
	seq, good = c.uint64()
	if !good {
		return nil, 0, false, nil
	}
	count, good := c.uint32()
	if !good {
		return nil, 0, false, nil
	}
	s := index.NewSnapshot(int(dim))
	for i := uint32(0); i < count; i++ {
		id, ok := c.varBytes()
		if !ok {
			return nil, 0, false, nil
		}
		vn, ok := c.uint32()
		if !ok {
			return nil, 0, false, nil
		}
		vec := make([]float32, vn)
		for j := range vec {
			bits, ok := c.uint32()
			if !ok {
				return nil, 0, false, nil
			}
			vec[j] = math.Float32frombits(bits)
		}
		mn, ok := c.uint32()
		if !ok {
			return nil, 0, false, nil
		}
		meta := map[string]string{}
		for j := uint32(0); j < mn; j++ {
			k, ok := c.varBytes()
			if !ok {
				return nil, 0, false, nil
			}
			v, ok := c.varBytes()
			if !ok {
				return nil, 0, false, nil
			}
			meta[string(k)] = string(v)
		}
		s = s.WithUpsert(string(id), vec, meta)
	}
	return s, seq, true, nil
}

func encodeSnapshot(snap *index.Snapshot, seq uint64) ([]byte, error) {
	buf := make([]byte, 0, 4096)
	buf = append(buf, snapMagic...)
	buf = append(buf, snapVersion)
	buf = binary.BigEndian.AppendUint32(buf, uint32(snap.Dim()))
	buf = binary.BigEndian.AppendUint64(buf, seq)
	buf = binary.BigEndian.AppendUint32(buf, uint32(snap.LiveLen()))
	encErr := error(nil)
	snap.Visit(func(id string, vec []float32, meta map[string]string) bool {
		if len(id) > math.MaxUint16 {
			encErr = fmt.Errorf("snapshot: id too long")
			return false
		}
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(id)))
		buf = append(buf, id...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(vec)))
		for _, v := range vec {
			buf = binary.BigEndian.AppendUint32(buf, math.Float32bits(v))
		}
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(meta)))
		for k, v := range meta {
			if len(k) > math.MaxUint16 || len(v) > math.MaxUint16 {
				encErr = fmt.Errorf("snapshot: meta too long")
				return false
			}
			buf = binary.BigEndian.AppendUint16(buf, uint16(len(k)))
			buf = append(buf, k...)
			buf = binary.BigEndian.AppendUint16(buf, uint16(len(v)))
			buf = append(buf, v...)
		}
		return true
	})
	if encErr != nil {
		return nil, encErr
	}
	buf = binary.BigEndian.AppendUint32(buf, crc32.ChecksumIEEE(buf))
	return buf, nil
}

type snapCursor struct {
	p []byte
	i int
}

func (c *snapCursor) byte() (byte, bool) {
	if c.i >= len(c.p) {
		return 0, false
	}
	v := c.p[c.i]
	c.i++
	return v, true
}
func (c *snapCursor) uint32() (uint32, bool) {
	if c.i+4 > len(c.p) {
		return 0, false
	}
	v := binary.BigEndian.Uint32(c.p[c.i:])
	c.i += 4
	return v, true
}
func (c *snapCursor) uint64() (uint64, bool) {
	if c.i+8 > len(c.p) {
		return 0, false
	}
	v := binary.BigEndian.Uint64(c.p[c.i:])
	c.i += 8
	return v, true
}
func (c *snapCursor) varBytes() ([]byte, bool) {
	if c.i+2 > len(c.p) {
		return nil, false
	}
	n := int(binary.BigEndian.Uint16(c.p[c.i:]))
	c.i += 2
	if c.i+n > len(c.p) {
		return nil, false
	}
	v := c.p[c.i : c.i+n]
	c.i += n
	return v, true
}
