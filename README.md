# 向量检索服务（Go，本地可运行）

一个零外部依赖的本地向量检索服务：用内存结构 + 本地文件（快照 + WAL）模拟索引与元数据，
保证**增量写入、在线重建、重启恢复**三种场景下检索结论一致且可解释。

- 向量写入 / 更新 / 删除，带元数据过滤（`equals` / `in`）的余弦相似度 Top-K 检索
- 不可变快照索引 + 原子切换：读写与重建并发时绝不返回新旧混合结果
- 预写日志（WAL，每条/每批 `fsync`）+ 原子快照：已确认写入重启不丢、不重，恢复幂等
- 批量写入整批校验、整批提交：坏条目不会留下半可见数据
- 资源上限与背压：容量、维度、批量大小、并发数超限均**立即、显式、可区分**地拒绝
- HTTP API + 可复现的单元测试（`-race` 下验证无数据竞争），日志打印输入、命中与判定依据

---

## 1. 构建与本地运行

需要 Go 1.25+（开发环境验证于 Go 1.26）。

```bash
# 构建
go build ./...

# 启动（全部参数可选，以下为默认值）
go run ./cmd/server \
  -addr 127.0.0.1:8080 \
  -data-dir ./data \
  -max-records 100000 \
  -max-dim 4096 \
  -max-batch 500 \
  -max-concurrent 64
```

`-data-dir` 传空字符串表示纯内存模式（不持久化，进程退出数据消失）。

### HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET  | `/healthz` | 健康检查 |
| POST | `/v1/vectors` | 写入或更新单条记录 |
| POST | `/v1/vectors/batch` | 原子批量写入/更新 |
| GET  | `/v1/vectors/{id}` | 读取单条记录 |
| DELETE | `/v1/vectors/{id}` | 删除记录 |
| POST | `/v1/search` | 带过滤的相似度检索 |
| POST | `/v1/rebuild` | 在线重建/压缩索引 |
| GET  | `/v1/stats` | 版本号 `seq`、快照代 `generation`、记录数、重建标记 |

快速体验：

```bash
curl -s -X POST localhost:8080/v1/vectors \
  -d '{"id":"a","vector":[1,0,0],"metadata":{"cat":"x"}}'
curl -s -X POST localhost:8080/v1/vectors \
  -d '{"id":"b","vector":[0,1,0],"metadata":{"cat":"y"}}'

curl -s -X POST localhost:8080/v1/search \
  -d '{"vector":[1,0,0],"top_k":5,"min_score":0.0,
       "filter":{"equals":{"cat":"x"},"in":{"cat":["x","z"]}}}'
# -> {"hits":[{"id":"a","score":1,"metadata":{"cat":"x"}}],
#      "generation":3,"took_micros":2}
```

检索响应里的 `generation` 是**生成结果的快照代号**，可用于解释“这条结论来自哪一代索引”。

### 错误码（拒绝原因可区分）

所有拒绝都返回稳定错误码，HTTP 状态码一一对应，绝不静默丢弃已接受的写入：

| code | HTTP | 触发场景 |
| --- | --- | --- |
| `INVALID_ARGUMENT`  | 400 | ID 为空、向量空/维度不一致/超维度上限/NaN、TopK 非法、批内重复 ID、JSON 非法 |
| `NOT_FOUND`         | 404 | 删除/读取不存在的记录 |
| `CAPACITY_EXCEEDED` | 507 | 记录数超过 `max-records`，或批次超过 `max-batch` |
| `TOO_BUSY`          | 429 | 并发数达到 `max-concurrent`（背压，可重试）；或已有重建在执行 |
| `INDEX_UNAVAILABLE` | 503 | 服务已关闭、索引尚未就绪、持久化 IO 失败 |

错误响应体形如：`{"error":{"code":"CAPACITY_EXCEEDED","message":"容量不足：…"}}`。

---

## 2. 索引与元数据的一致性规则

1. **元数据是唯一事实来源（source of truth）。**
   内存中的 `meta map[id]*Record` 保存全部已确认记录；检索索引只是它派生的只读视图。
   任何过滤判定都直接针对记录的 `Metadata`，因此“过滤结果与元数据一致”。

2. **索引是不可变快照，原子指针切换（MVCC 风格）。**
   每次已确认的写/更新/删除都会：在写锁内深拷贝当前全部记录 → 构建新快照
   （记录按 ID 排序）→ `atomic.Pointer.Store` 一次性发布。检索开始时取一次当前快照指针，
   全程在该快照上扫描。因此：
   - 检索要么完整看到变更前的一代，要么完整看到变更后的一代，**不存在新旧混合/撕裂**；
   - 已删除记录在快照发布后立刻不可召回；更新发布后只可能返回新向量。

3. **排序是确定性的。** 分数（余弦相似度）降序；分数相同按 ID 升序。
   快照物理顺序按 ID 排序，相同输入在任何时刻、任何重启后结论都可复现。
   零范数向量的相似度定义为 0（不产生 NaN）。

4. **维度统一。** 第一条记录确定索引维度，之后维度不一致的写入以
   `INVALID_ARGUMENT` 拒绝；查询向量维度不符同样拒绝。

5. **单条 / 批量写入的提交顺序固定为：先完整校验 → 深拷贝 → WAL fsync → 更新内存并发布快照。**
   批量是**一个原子单元**：
   - 先校验整批（空 ID、维度、NaN、批内重复 ID、容量），任一不合法整批拒绝，错误信息指明非法条目；
   - 整批写为**一条 WAL 记录**，恢复时要么整批生效、要么整批不存在，不会半批可见。

---

## 3. 在线重建与压缩语义

`POST /v1/rebuild`（`Store.Rebuild`）在**不停服**的情况下重建索引：

1. 重建开始时置 `rebuilding` 标志；同一时刻只允许一个重建任务，并发重建返回 `TOO_BUSY`。
2. 重建在**读锁/锁外**深拷贝数据并构建新快照，期间读写请求继续服务于旧快照，
   不会失败（仅可能在并发超限时得到可重试的 `TOO_BUSY`）。
3. 构建完成后取写锁：若构建期间 `seq` 未推进，立即原子切换到新快照并结束；
   若期间有写入，则丢弃本轮结果、基于最新数据重试，直到捕获到稳定一代再发布。
   因而切换点上的快照必然包含此前全部已确认写入，切换是原子的，读永远看不到混合态。
4. 压缩：新快照发布后，把该代数据写入 `snapshot.json`（临时文件 + fsync + rename），
   随后在写锁保护下**仅当 `seq` 未推进**才清空 WAL。若快照落盘后已有更新的写入，
   则保留 WAL，等下一次重建压缩——快照之后确认的写入绝不会被误删。

---

## 4. 重启恢复语义

数据目录包含两个文件：

- `wal.log`：JSON Lines 追加日志，每条是一次原子变更（upsert 批次 / delete），
  追加时 flush + `fsync`；
- `snapshot.json`：重建/关闭时原子落盘的快照，首行头记录覆盖到的 `seq` 与记录数，
  其后每行一条记录。

恢复流程（`Open`）：

1. 读取最近快照（不存在则从空状态开始）；
2. 顺序重放 WAL，只应用 `seq > 快照seq` 的条目；
3. 重放操作天然**幂等**：upsert 按 ID 覆盖（更新不会回退、不会重复）、delete 幂等删除。
   因此同一目录被反复打开/重复恢复，结果不会错乱或重复；
4. 发布第 1 代快照后开始服务。崩溃时 WAL 末尾若有写了一半的残缺行，直接忽略
   ——它从未完成 fsync，即从未被确认；已确认的前缀全部生效。

**确认语义**：写请求在 WAL `fsync` 成功并发布内存快照后才返回 `ack`。
即“已确认 ⇒ 已持久化 ⇒ 重启后仍在”；未返回成功的写入不承诺保留，恢复后不可见。

---

## 5. 资源上限与背压

- 容量：`meta` 中的记录数（只统计新增 ID，更新不占新容量）超过 `max-records` 时，
  写入以 `CAPACITY_EXCEEDED` 拒绝；批量在应用前统一预估新增数。
- 参数：任何非法输入在触及状态前以 `INVALID_ARGUMENT` 拒绝。
- 并发：写/检索通过容量为 `max-concurrent` 的信号量限流，槽满时**立即**返回
  `TOO_BUSY`（不排队、不静默超时、不丢弃已接受的请求），调用方退避重试即可。
- 并发安全：所有状态变更经同一把写锁串行化；读走 RLock 或原子快照；
  深拷贝隔离调用方切片/map。`go test -race` 验证无数据竞争。

---

## 6. 本地验证方法

```bash
# 全部单测（含增删改查一致性、并发在线重建、WAL/快照重启恢复、重复恢复幂等、
# 未确认半批次不可见、拒绝码区分、HTTP 状态码映射）
go test ./... -race -count=1 -v

# 反复运行确认并发场景可复现、无 flaky
go test ./... -race -count=5
```

测试与需求的对应关系：

| 测试 | 覆盖的需求点 |
| --- | --- |
| `TestCRUDConsistency` | 写入/更新/删除一致性、删除不召回、更新不回旧向量、过滤与元数据一致、确定性排序 |
| `TestRejectionCodes` | 四类拒绝原因稳定可区分（非法/容量/背压/不可用），背压释放后可恢复 |
| `TestBatchAtomicity` | 坏批次/批内重复 ID 整批拒绝、不留半可见数据；并发坏批次同样原子 |
| `TestOnlineRebuildConcurrency` | 重建与 8 读者 + 4 写者并发：读不失败不撕裂、代际单调、确认写入不丢不重、更新不留旧值 |
| `TestRebuildRejectsConcurrent` | 并发重建显式 `TOO_BUSY`，解除后重建成功 |
| `TestRecoveryFromWAL` | 模拟崩溃、不做收尾快照，纯 WAL 重放恢复一致 |
| `TestRecoveryFromSnapshotAndWAL` | 重建压缩后“快照 + WAL 尾巴”恢复一致 |
| `TestIdempotentReopen` | 同一目录重复恢复多轮，记录数/结果完全一致，删除不复活、无重复 |
| `TestNoHalfBatchAfterRecovery` | WAL 尾部残缺（未确认写入）重启后不可见，已确认数据不丢 |
| `TestHTTPCodes` / `TestHTTPSearchFlow` | HTTP 层 400/404/429/507 映射、背压、检索链路与 `generation` 解释字段 |

单测日志会打印每次检索的**输入向量、TopK、过滤条件、命中 ID/分数/元数据、快照代号
与判定依据**，可用 `go test -run <用例名> -v` 查看，例如：

```
[初始检索] 输入: query=[1 0 0] topK=3 ...
[初始检索] 命中 gen=4: #1 id=a score=1.000000 metadata=map[cat:x] ...
判定依据：与查询向量余弦相似度降序，同分按 ID 升序
```

### 手动端到端（重启恢复）

```bash
go run ./cmd/server -data-dir /tmp/vrs-data -addr 127.0.0.1:18080 &
curl -s -X POST 127.0.0.1:18080/v1/vectors -d '{"id":"a","vector":[1,0,0]}'
curl -s -X POST 127.0.0.1:18080/v1/rebuild          # 触发快照 + WAL 压缩
kill %1                                             # 重启（或 kill -9 模拟崩溃）
go run ./cmd/server -data-dir /tmp/vrs-data -addr 127.0.0.1:18080 &
curl -s 127.0.0.1:18080/v1/stats                    # records 与重启前一致
```

---

## 7. 目录结构

```
vector/
  errors.go       # 稳定错误码与错误类型
  model.go        # Record / Filter / SearchRequest/SearchHit，过滤匹配
  distance.go     # 余弦相似度
  index.go        # 不可变快照 + 原子指针索引 + 快照检索
  wal.go          # fsync 追加 WAL（批次原子记录）与重放读取
  persistence.go  # 原子快照落盘/加载（temp + fsync + rename）
  store.go        # 引擎：校验、提交、背压、在线重建、恢复、关闭
  server.go       # HTTP API 与错误码 -> 状态码映射
cmd/server/       # 本地可运行入口（参数化上限、优雅关闭）
```

> 说明：检索实现为全量暴力扫描，面向本地小规模数据，重点是一致性语义的正确性，
> 不依赖任何外部向量库或数据库。
