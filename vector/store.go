package vector

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// 文件名约定。
const (
	walFileName      = "wal.log"
	snapshotFileName = "snapshot.json"
)

// Config 定义本地服务的资源上限。
type Config struct {
	DataDir       string // 本地数据目录（空目录表示纯内存，不持久化）
	MaxRecords    int    // 最大记录数
	MaxVectorDim  int    // 最大向量维度
	MaxBatchSize  int    // 单次批量写入最大条目数
	MaxConcurrent int    // 读写最大并发数（背压阈值）
}

// DefaultConfig 给出可直接本地运行的默认上限。
func DefaultConfig() Config {
	return Config{
		DataDir:       "data",
		MaxRecords:    100_000,
		MaxVectorDim:  4096,
		MaxBatchSize:  500,
		MaxConcurrent: 64,
	}
}

// Stats 暴露当前版本号、记录数与重建状态，用于可解释性与测试判定。
type Stats struct {
	Seq        uint64 `json:"seq"`
	Generation uint64 `json:"generation"`
	Records    int    `json:"records"`
	Rebuilding bool   `json:"rebuilding"`
}

// Store 是向量检索引擎：
//   - meta 是唯一事实来源（map + 固定维度），过滤与索引都由它派生；
//   - index 持有不可变快照，原子指针切换，读永远看到某一完整代；
//   - 每条 / 每批变更先 fsync WAL 并发布快照后才算确认。
type Store struct {
	cfg Config

	mu         sync.RWMutex
	seq        uint64
	gen        uint64
	dim        int
	meta       map[string]*Record
	rebuilding bool
	closed     bool

	index *snapshotIndex
	wal   *walWriter

	sem chan struct{} // 读写并发背压信号量
}

// Open 从数据目录恢复（快照 + WAL 重放）并构建初始索引快照。
func Open(cfg Config) (*Store, error) {
	s := &Store{
		cfg:   cfg,
		meta:  make(map[string]*Record),
		index: newSnapshotIndex(),
	}
	if cfg.MaxConcurrent > 0 {
		s.sem = make(chan struct{}, cfg.MaxConcurrent)
	}

	if cfg.DataDir != "" {
		// 首次本地运行时自动创建数据目录。
		if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
			return nil, newError(CodeIndexUnavailable, "创建数据目录失败: %v", err)
		}
		if err := s.recover(); err != nil {
			return nil, err
		}
		ww, err := openWAL(filepath.Join(cfg.DataDir, walFileName))
		if err != nil {
			return nil, err
		}
		s.wal = ww
	}

	s.publishLocked()
	return s, nil
}

// recover 重放快照与 WAL。重放操作天然幂等（upsert 覆盖、delete 删除），
// 因此即使同一批日志被重放多次，结果也不会错乱或重复。
func (s *Store) recover() error {
	snapSeq, records, err := loadSnapshot(filepath.Join(s.cfg.DataDir, snapshotFileName))
	if err != nil {
		return err
	}
	for _, r := range records {
		cp := deepCopyRecord(*r)
		s.meta[cp.ID] = &cp
		if len(cp.Vector) > s.dim {
			s.dim = len(cp.Vector)
		}
	}
	s.seq = snapSeq

	entries, err := readWAL(filepath.Join(s.cfg.DataDir, walFileName))
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Seq <= snapSeq {
			continue // 快照已覆盖（压缩后残留的旧条目）
		}
		s.applyEntry(e)
		if e.Seq > s.seq {
			s.seq = e.Seq
		}
	}
	return nil
}

// applyEntry 在不加锁的恢复路径中重放单条原子变更。
func (s *Store) applyEntry(e walEntry) {
	switch e.Op {
	case opUpsert:
		for i := range e.Records {
			r := deepCopyRecord(e.Records[i])
			s.meta[r.ID] = &r
			if len(r.Vector) > s.dim {
				s.dim = len(r.Vector)
			}
		}
	case opDelete:
		for _, id := range e.IDs {
			delete(s.meta, id)
		}
	}
}

// acquire 申请并发配额；拿不到立即拒绝（背压），不阻塞排队造成静默超时。
func (s *Store) acquire() error {
	if s.sem == nil {
		return nil
	}
	select {
	case s.sem <- struct{}{}:
		return nil
	default:
		return newError(CodeTooBusy, "并发数已达上限 %d，请稍后重试", s.cfg.MaxConcurrent)
	}
}

func (s *Store) release() {
	if s.sem != nil {
		<-s.sem
	}
}

func (s *Store) checkOpenLocked() error {
	if s.closed {
		return newError(CodeIndexUnavailable, "服务已关闭，索引暂不可用")
	}
	return nil
}

// Upsert 写入或更新单条向量记录。
func (s *Store) Upsert(r Record) error {
	if err := s.acquire(); err != nil {
		return err
	}
	defer s.release()
	return s.commitUpsert([]Record{r})
}

// BatchUpsert 原子批量写入：先完整校验整批，再持久化、再应用。
// 任一条目非法则整批拒绝，不会留下任何半可见数据。
func (s *Store) BatchUpsert(rs []Record) error {
	if err := s.acquire(); err != nil {
		return err
	}
	defer s.release()
	return s.commitUpsert(rs)
}

func (s *Store) commitUpsert(rs []Record) error {
	if len(rs) == 0 {
		return newError(CodeInvalidArgument, "批量写入不能为空")
	}
	if s.cfg.MaxBatchSize > 0 && len(rs) > s.cfg.MaxBatchSize {
		return newError(CodeCapacityExceeded,
			"批量条目数 %d 超过上限 %d", len(rs), s.cfg.MaxBatchSize)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpenLocked(); err != nil {
		return err
	}

	// 1) 完整校验：维度、ID、数值、批内重复 ID、容量。全部通过才进入提交。
	dim := s.dim
	for i := range rs {
		r := &rs[i]
		if strings.TrimSpace(r.ID) == "" {
			return newError(CodeInvalidArgument, "第 %d 条记录的 id 不能为空", i)
		}
		if len(r.Vector) == 0 {
			return newError(CodeInvalidArgument, "记录 %q 的向量不能为空", r.ID)
		}
		if s.cfg.MaxVectorDim > 0 && len(r.Vector) > s.cfg.MaxVectorDim {
			return newError(CodeInvalidArgument,
				"记录 %q 的向量维度 %d 超过上限 %d", r.ID, len(r.Vector), s.cfg.MaxVectorDim)
		}
		if dim == 0 {
			dim = len(r.Vector)
		} else if len(r.Vector) != dim {
			return newError(CodeInvalidArgument,
				"记录 %q 的向量维度 %d 与索引维度 %d 不一致", r.ID, len(r.Vector), dim)
		}
		for k, v := range r.Vector {
			if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
				return newError(CodeInvalidArgument,
					"记录 %q 的向量第 %d 维为 NaN/Inf", r.ID, k)
			}
		}
	}
	seen := make(map[string]struct{}, len(rs))
	for i := range rs {
		id := rs[i].ID
		if _, dup := seen[id]; dup {
			return newError(CodeInvalidArgument,
				"批次内存在重复 id %q（第 %d 条），无法原子应用", id, i)
		}
		seen[id] = struct{}{}
	}
	if s.cfg.MaxRecords > 0 {
		added := 0
		for id := range seen {
			if _, exists := s.meta[id]; !exists {
				added++
			}
		}
		if len(s.meta)+added > s.cfg.MaxRecords {
			return newError(CodeCapacityExceeded,
				"容量不足：现有 %d 条 + 新增 %d 条将超过上限 %d",
				len(s.meta), added, s.cfg.MaxRecords)
		}
	}

	// 2) 深拷贝，防止调用方在提交后修改切片/map 污染内部状态。
	recs := make([]Record, len(rs))
	for i := range rs {
		recs[i] = deepCopyRecord(rs[i])
	}

	// 3) 先持久化（fsync 确认），再发布内存状态：确认即不丢。
	s.seq++
	if s.wal != nil {
		if err := s.wal.append(walEntry{Seq: s.seq, Op: opUpsert, Records: recs}); err != nil {
			return newError(CodeIndexUnavailable, "WAL 持久化失败: %v", err)
		}
	}
	for i := range recs {
		r := recs[i]
		cp := r
		s.meta[r.ID] = &cp
	}
	if s.dim == 0 {
		s.dim = dim
	}
	s.publishLocked()
	return nil
}

// Delete 删除一条或多条记录。删除立即发布快照，已删除记录不会再被召回。
func (s *Store) Delete(id string) error {
	if err := s.acquire(); err != nil {
		return err
	}
	defer s.release()
	if strings.TrimSpace(id) == "" {
		return newError(CodeInvalidArgument, "id 不能为空")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpenLocked(); err != nil {
		return err
	}
	if _, ok := s.meta[id]; !ok {
		return newError(CodeNotFound, "记录 %q 不存在", id)
	}
	s.seq++
	if s.wal != nil {
		if err := s.wal.append(walEntry{Seq: s.seq, Op: opDelete, IDs: []string{id}}); err != nil {
			return newError(CodeIndexUnavailable, "WAL 持久化失败: %v", err)
		}
	}
	delete(s.meta, id)
	s.publishLocked()
	return nil
}

// Get 读取单条当前记录（深拷贝返回）。
func (s *Store) Get(id string) (Record, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.meta[id]
	if !ok {
		return Record{}, false
	}
	return deepCopyRecord(*r), true
}

// Search 在当前已发布的不可变快照上检索；重建期间也不会失败或看到混合数据。
func (s *Store) Search(req SearchRequest) (*SearchResult, error) {
	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	if req.TopK <= 0 {
		return nil, newError(CodeInvalidArgument, "top_k 必须为正整数")
	}
	if len(req.Vector) == 0 {
		return nil, newError(CodeInvalidArgument, "查询向量不能为空")
	}
	if s.cfg.MaxVectorDim > 0 && len(req.Vector) > s.cfg.MaxVectorDim {
		return nil, newError(CodeInvalidArgument,
			"查询向量维度 %d 超过上限 %d", len(req.Vector), s.cfg.MaxVectorDim)
	}
	s.mu.RLock()
	dim := s.dim
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil, newError(CodeIndexUnavailable, "服务已关闭，索引暂不可用")
	}
	if dim != 0 && len(req.Vector) != dim {
		return nil, newError(CodeInvalidArgument,
			"查询向量维度 %d 与索引维度 %d 不一致", len(req.Vector), dim)
	}
	snap := s.index.current()
	if snap == nil {
		return nil, newError(CodeIndexUnavailable, "索引尚未就绪")
	}

	start := time.Now()
	hits := snap.search(req)
	if hits == nil {
		hits = []SearchHit{}
	}
	return &SearchResult{
		Hits:       hits,
		Generation: snap.gen,
		TookMicros: time.Since(start).Microseconds(),
	}, nil
}

// Rebuild 在线重建 / 压缩索引。重建期间读写照常服务于旧快照；
// 完成后一次性原子切换到新快照，绝无新旧混合。
// 重建结束后顺带把当前数据做快照并压缩 WAL。
func (s *Store) Rebuild(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return newError(CodeIndexUnavailable, "服务已关闭，索引暂不可用")
	}
	if s.rebuilding {
		s.mu.Unlock()
		return newError(CodeTooBusy, "已有重建任务在执行中")
	}
	s.rebuilding = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.rebuilding = false
		s.mu.Unlock()
	}()

	// 复制循环：锁外构建新索引；若构建期间有写入，则基于最新数据再构建一轮，
	// 直到捕获到稳定数据后在锁内完成最终交换。
	var snap *snapshot
	for {
		if err := ctx.Err(); err != nil {
			return newError(CodeTooBusy, "重建被取消: %v", err)
		}
		s.mu.RLock()
		closed := s.closed
		seq, gen := s.seq, s.gen
		items := snapshotItemsLocked(s.meta) // 锁内深拷贝
		s.mu.RUnlock()
		if closed {
			return newError(CodeIndexUnavailable, "服务已关闭，索引暂不可用")
		}

		snap = newSnapshot(gen+1, items)

		s.mu.Lock()
		if s.seq == seq {
			// 构建期间没有新写入：正式发布这一代快照。
			s.gen++
			s.index.swap(snap)
			dirRef := s.cfg.DataDir
			seqRef := s.seq
			// snap.items 在锁内完成深拷贝，锁外落盘不会与写入产生数据竞争。
			snapItems := snap.items
			s.mu.Unlock()

			// 锁外做磁盘快照（耗时 IO 不阻塞读写）。此期间允许新写入，
			// 它们会携带更大的 seq 追加进 WAL。
			if dirRef != "" {
				if err := saveSnapshot(filepath.Join(dirRef, snapshotFileName),
					seqRef, snapItems); err != nil {
					// 磁盘快照失败不影响已发布的内存索引，仅记为不可用类错误，
					// 重启时仍可依赖完整 WAL 重放恢复。
					return newError(CodeIndexUnavailable, "重建已发布但快照落盘失败: %v", err)
				}
				// 压缩 WAL 必须持写锁（与 append 串行，安全替换文件句柄），
				// 且仅当 seq 未推进时才允许清空：否则快照之后的新写入会被误删。
				s.mu.Lock()
				if s.seq != seqRef {
					s.mu.Unlock()
					return nil // 有更新的写入，保留 WAL，等下一次重建再压缩
				}
				walRef := s.wal
				var truncErr error
				if walRef != nil {
					truncErr = walRef.truncate()
				}
				s.mu.Unlock()
				if truncErr != nil {
					return newError(CodeIndexUnavailable, "WAL 压缩失败: %v", truncErr)
				}
			}
			return nil
		}
		s.mu.Unlock()
		// 期间有写入，丢弃本轮结果重试。
	}
}

// Stats 返回当前版本号、快照代号、记录数与重建标记。
func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Stats{
		Seq:        s.seq,
		Generation: s.gen,
		Records:    len(s.meta),
		Rebuilding: s.rebuilding,
	}
}

// Close 持久化快照后关闭 WAL；关闭后的请求得到 INDEX_UNAVAILABLE。
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	dir := s.cfg.DataDir
	seq := s.seq
	wal := s.wal
	// 持锁完成深拷贝快照，随后解锁落盘；关闭标志已置位，不会再有新写入。
	items := snapshotItemsLocked(s.meta)
	s.mu.Unlock()

	if dir != "" {
		_ = saveSnapshot(filepath.Join(dir, snapshotFileName), seq, items)
		// 尽力快照；WAL 仍包含全部已确认变更，是恢复的完整事实来源。
	}
	if wal != nil {
		return wal.close()
	}
	return nil
}

// publishLocked 在持写锁时依据当前 meta 构建并原子发布新一代快照。
func (s *Store) publishLocked() {
	s.gen++
	s.index.swap(newSnapshot(s.gen, snapshotItemsLocked(s.meta)))
}

// snapshotItemsLocked 深拷贝当前全部记录，并按 ID 排序，
// 使每一代快照的物理顺序确定，检索结论可复现。
func snapshotItemsLocked(meta map[string]*Record) []*Record {
	ids := make([]string, 0, len(meta))
	for id := range meta {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	items := make([]*Record, 0, len(ids))
	for _, id := range ids {
		r := deepCopyRecord(*meta[id])
		items = append(items, &r)
	}
	return items
}

// deepCopyRecord 深拷贝向量与元数据。
func deepCopyRecord(r Record) Record {
	out := Record{ID: r.ID}
	if r.Vector != nil {
		out.Vector = slices.Clone(r.Vector)
	}
	out.Metadata = cloneMetadata(r.Metadata)
	return out
}
