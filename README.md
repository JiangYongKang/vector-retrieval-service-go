# Vector Retrieval Service (Go, local)

A locally-runnable vector similarity retrieval service with **local-filesystem
durability** (append-only WAL + atomic snapshots), designed so that
incremental writes, online index rebuild and crash recovery always converge
to the **same, explainable** retrieval result.

No external services are required. The index is a brute-force cosine index
built from immutable snapshots; the goal is correctness and explainability,
not raw scale.

---

## 1. Build & run

```bash
go build ./...
go test -race ./...          # full suite (unit + concurrency + recovery)

go run ./cmd/server \
  -addr 127.0.0.1:8080 \
  -data-dir ./data \
  -dim 8 \
  -max-records 100000 \
  -max-concurrent 64 \
  -max-batch 1000
```

`-data-dir ""` runs fully in memory (no persistence across restarts).

### HTTP API

| Method & path       | Body (JSON)                                                        | Meaning |
|---------------------|--------------------------------------------------------------------|---------|
| `POST /v1/records`  | `{"records":[{"id","vector":[float... ],"meta":{"k":"v"}}]}`      | Batch upsert (atomic) |
| `POST /v1/delete`   | `{"ids":["a","b"]}`                                                | Batch delete (idempotent) |
| `POST /v1/search`   | `{"vector":[...],"k":10,"filter":{"k":"v"}}`                       | Filtered top-k cosine search |
| `POST /v1/rebuild`  | `{}`                                                               | Online compaction + checkpoint |
| `GET  /v1/stats`    | –                                                                  | Live operational stats |
| `GET  /healthz`     | –                                                                  | Liveness |

Filter semantics: every `key=value` must be present and equal in the
record's authoritative metadata (AND of equalities). Empty filter matches
all records.

### Quick local verification

```bash
# write
curl -s -X POST localhost:8080/v1/records -d '{"records":[
  {"id":"d1","vector":[1,0,0],"meta":{"cat":"doc"}},
  {"id":"d2","vector":[0.9,0.1,0],"meta":{"cat":"doc"}}]}'

# filtered search
curl -s -X POST localhost:8080/v1/search \
  -d '{"vector":[1,0,0],"k":3,"filter":{"cat":"doc"}}'

# online rebuild/compaction
curl -s -X POST localhost:8080/v1/rebuild -d '{}'

# delete, then confirm it can never be recalled
curl -s -X POST localhost:8080/v1/delete -d '{"ids":["d2"]}'

# restart the process -> state is recovered; d2 stays deleted
curl -s localhost:8080/v1/stats
```

### Distinguishable rejection statuses

Every rejection is a typed error mapped to a stable HTTP code/code-string:

| Condition                                   | HTTP | `error` code           |
|---------------------------------------------|------|------------------------|
| Bad parameter (dim, empty id, k, JSON)      | 400  | `invalid_parameter`    |
| Record-capacity or concurrency limit hit    | 429  | `capacity_exceeded`    |
| Shutting down / persistence unavailable     | 503  | `index_unavailable`    |

The response body always includes a human-readable `message` pinpointing the
offending record position or the limit that was reached.

---

## 2. Architecture

```
            ┌──────────────────────────────────────────────┐
  HTTP ───▶ │ engine                                       │
            │  ┌────────────────────┐   ┌───────────────┐  │
            │  │ records map        │   │ index.Snapshot│  │
            │  │ (AUTHORITATIVE     │◀──┤ (immutable,   │  │
            │  │  metadata+vector)  │   │  queryable    │  │
            │  └────────────────────┘   │  projection)   │  │
            │            │ append        └───────────────┘  │
            │            ▼                  ▲ COW publish   │
            │  ┌────────────────────┐       │ (atomic swap) │
            │  │ WAL (CRC frames,   │──────▶│               │
            │  │ batch = 1 frame)   │       └───────────────┘
            │  └────────────────────┐
            │  ┌────────────────────┴┐  snapshot.bin (atomic rename)
            │  │ snapshot checkpoint ├──▶ compaction + fsync
            │  └─────────────────────┘
            └──────────────────────────────────────────────┘
```

Packages:

- `internal/model`   – record, filter (AND-equality), search hit types.
- `internal/errs`    – typed `invalid_parameter / capacity_exceeded /
  index_unavailable` errors.
- `internal/wal`     – append-only log; length-prefixed **CRC32 frames**;
  a whole batch is one frame; torn tails are detected and trimmed.
- `internal/index`   – immutable copy-on-write snapshot; cosine top-k;
  `Compact()` packs only live slots without changing any answer.
- `internal/engine`  – validates, durably appends, applies, publishes;
  enforces limits/backpressure; online rebuild; snapshot+WAL recovery.
- `internal/server`  – HTTP mapping and error-code translation.

---

## 3. Index ⇄ metadata consistency rules

1. **The in-memory `records` map is authoritative.** The published
   `index.Snapshot` is always a projection of it, never the other way
   around. A search candidate allow-list is built from this map, so
   filtered hits are guaranteed to agree with current metadata.
2. **A published snapshot is never mutated.** Every upsert/delete builds a
   copy-on-write *successor*. A query pins one snapshot pointer for its
   whole duration, so it observes one complete point-in-time view.
3. **Update never returns the old vector.** An upsert for an existing id
   marks the old slot dead and appends a new slot; only the new vector is
   live and searchable.
4. **Delete is not recallable.** A delete marks the id's slot dead and
   removes it from the authoritative map; no later search can return it.
5. **Atomic batch, no half-visible data.** *All* records are validated and
   the capacity decision is made **before** anything is persisted or made
   visible. The whole batch is one WAL frame; only after the frame is
   durable are records applied and published. Any invalid record rejects
   the entire batch with a position-specific `invalid_parameter` reason and
   leaves zero visible state.
6. **Deterministic ranking.** Hits are ordered by descending cosine score,
   ties broken by ascending id, so identical inputs always yield identical,
   reproducible output.

---

## 4. Resource limits & backpressure

- `MaxRecords` caps the **live** record count. Updates to existing ids do
  not grow the set; a batch that would exceed the cap (counting only
  distinct *new* ids) is rejected as one unit with `capacity_exceeded` and
  is not partially applied.
- `MaxConcurrent` bounds mutating/rebuild operations. Slot acquisition is
  **non-blocking**: when saturated, the request is rejected immediately as
  `capacity_exceeded` rather than queued indefinitely (explicit backpressure).
- `MaxBatchSize` bounds a single request.
- **An accepted write is never silently dropped.** A write is confirmed to
  the client only after its batch frame is fsync'd; if persistence fails the
  data is not exposed and the client gets `index_unavailable` so it can retry.

---

## 5. Online rebuild / compaction semantics

- Rebuild compacts the current snapshot into a fresh, contiguously packed
  snapshot containing only live records. **Storage layout changes; the set
  of search answers does not** (compaction is verified result-equivalent).
- The expensive compaction runs on an **immutable snapshot without holding
  the write lock**, so concurrent reads and writes continue to succeed.
- Publication is an **atomic pointer swap** inside the write lock. A reader
  therefore sees either the entire old index or the entire new index, never
  a mixture.
- If a commit lands while compaction is running, the candidate is discarded
  and the rebuild **retries against the newer snapshot** (optimistic,
  bounded), guaranteeing the compacted index still contains every confirmed
  write up to the swap instant.
- After publishing, rebuild writes `snapshot.bin` (temp file + fsync +
  atomic rename) and truncates the now-covered WAL prefix **in the same
  critical section**. No commit can be truncated out of existence. The fsync
  briefly delays (never fails) writes; readers (read-lock) are unaffected.

---

## 6. Durability and restart-recovery semantics

- **WAL frame:** `len u32 | payload | crc32 u32`. A batch is a single
  `OpBatch` frame holding N inner entries. A crash can leave at most a torn
  tail; on open, replay stops at the first incomplete/CRC-bad frame and
  physically trims it, so a partial batch can never become half-visible.
- **Snapshot:** `magic | version | dim | coveredSeq | count | records... | crc`.
  Written atomically (temp + fsync + rename), so recovery sees either the
  old complete snapshot or the new one — never a half-written file. A
  corrupt/partial snapshot is ignored and recovery falls back to the WAL.
- **Recovery = latest snapshot ⊕ replay of the surviving WAL prefix.**
- **Replay is idempotent.** It consists only of upserts (replace-by-id) and
  deletes (remove-by-id). Replaying an entry already contained in the
  snapshot yields the same record set — no duplication, no corruption, no
  ordering drift. Reopening the same directory repeatedly converges to the
  same state (covered by `TestRecoveryAcrossManyRestarts`).
- **No loss / no duplication of confirmed writes:** a write is part of the
  recovered state iff its batch frame was durable before acknowledgement.
  Snapshot checkpoints only remove WAL bytes that the snapshot already
  contains.

---

## 7. Test map (reproducibility & explainability)

Tests log **input → hit results → judgement basis**.

- `internal/engine/smoke_test.go`
  - `TestSmokeCRUDFilter` – update hides old vector; delete not recalled;
    filter matches metadata.
  - `TestSmokeBatchRejectNoPartial` – one invalid entry rejects the batch,
    no partial visibility.
  - `TestSmokeCapacity` – `MaxRecords` rejection; updates still allowed.
  - `TestSmokeRebuildCompact` – answers identical pre/post compaction,
    slots collapse.
  - `TestSmokeRestartRecovery` – snapshot + WAL-tail recovery; delete
    persists; post-checkpoint write survives; repeated reopen is stable.
- `internal/engine/concurrent_test.go`
  - `TestConcurrentWriteSearchRebuild` – many writers/searchers/rebuilders
    with `-race`; each search is validated against the **same immutable
    snapshot view** it was answered from (no time-of-check race).
  - `TestBackpressureDistinguishable` – capacity vs invalid vs unavailable
    produce distinct error kinds.
  - `TestRecoveryAcrossManyRestarts` – repeated idempotent recovery.
- `internal/wal/batch_test.go` – batch frame atomicity and torn-batch
  discard.
- `internal/server/server_test.go` – HTTP end-to-end incl. status-code
  mapping.

```bash
go test -race -count=3 ./...        # stress determinism under the race detector
```
