package vector

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// snapshotFile 是快照文件首行的头：记录该快照覆盖的 WAL 序列号与记录数。
type snapshotHeader struct {
	Seq     uint64 `json:"seq"`
	Records int    `json:"records"`
}

// saveSnapshot 原子落盘快照：先写同目录临时文件并 fsync，再 rename 覆盖。
// rename 在本地文件系统上是原子的，因此重启时只会看到“完整的旧快照”或
// “完整的新快照”，绝不会读到写了一半的快照文件。
func saveSnapshot(path string, seq uint64, records []*Record) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".snapshot-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	enc := json.NewEncoder(tmp)
	if err := enc.Encode(snapshotHeader{Seq: seq, Records: len(records)}); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	for _, r := range records {
		if err := enc.Encode(r); err != nil {
			_ = tmp.Close()
			cleanup()
			return err
		}
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		cleanup()
		return err
	}
	// fsync 目录，保证 rename 元数据落盘。
	if d, derr := os.Open(dir); derr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// loadSnapshot 读取最近一次快照；文件不存在时返回 seq=0、nil 切片且无错误，
// 表示需要完全依赖 WAL 重放恢复。
func loadSnapshot(path string) (uint64, []*Record, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil, nil
		}
		return 0, nil, err
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	var hdr snapshotHeader
	if err := dec.Decode(&hdr); err != nil {
		return 0, nil, err
	}
	records := make([]*Record, 0, hdr.Records)
	for i := 0; i < hdr.Records; i++ {
		var r Record
		if err := dec.Decode(&r); err != nil {
			return 0, nil, err
		}
		records = append(records, &r)
	}
	return hdr.Seq, records, nil
}
