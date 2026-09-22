package vector

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
)

// walEntry 是预写日志中一条原子的已确认变更。
// upsert 的原子单元可以是单条或一整个批次（Records）；delete 携带 ID。
// 以整批为一条日志，崩溃恢复时要么整批生效、要么整批不可见，不存在半批次。
type walEntry struct {
	Seq     uint64   `json:"seq"`
	Op      string   `json:"op"` // upsert / delete
	Records []Record `json:"records,omitempty"`
	IDs     []string `json:"ids,omitempty"`
}

const (
	opUpsert = "upsert"
	opDelete = "delete"
)

// walWriter 以追加方式顺序写 WAL。每条记录 Sync 落盘后才算“已确认”，
// 因此已确认的写入在崩溃 / 重启后不会丢失。
type walWriter struct {
	path string
	f    *os.File
	w    *bufio.Writer
	enc  *json.Encoder
}

// openWAL 以 append 方式打开（不存在则创建，权限 0o600）。
func openWAL(path string) (*walWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	bw := bufio.NewWriter(f)
	return &walWriter{path: path, f: f, w: bw, enc: json.NewEncoder(bw)}, nil
}

// append 编码一条日志，flush 后 fsync；返回成功即表示持久化已确认。
func (ww *walWriter) append(e walEntry) error {
	if err := ww.enc.Encode(e); err != nil {
		return err
	}
	if err := ww.w.Flush(); err != nil {
		return err
	}
	return ww.f.Sync()
}

// truncate 在快照安全落盘后清空 WAL，实现日志压缩。
func (ww *walWriter) truncate() error {
	if err := ww.f.Close(); err != nil {
		return err
	}
	f, err := os.OpenFile(ww.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	ww.f = f
	ww.w = bufio.NewWriter(f)
	ww.enc = json.NewEncoder(ww.w)
	return nil
}

func (ww *walWriter) close() error {
	if err := ww.w.Flush(); err != nil {
		_ = ww.f.Close()
		return err
	}
	return ww.f.Close()
}

// readWAL 顺序读出全部日志条目，用于重启重放。
// 末尾若存在未写完整的半行（崩溃造成），直接忽略：它从未被确认。
func readWAL(path string) ([]walEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	dec := json.NewDecoder(bufio.NewReader(f))
	var out []walEntry
	for {
		var e walEntry
		err := dec.Decode(&e)
		if err == io.EOF {
			break
		}
		if err != nil {
			// 尾部残缺记录：停止读取，已完整解码的前缀仍然有效。
			break
		}
		out = append(out, e)
	}
	return out, nil
}
