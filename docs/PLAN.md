# Project plan: flink-hashtag-trends

> **Single source of truth.** Read before working on any phase; update the *Progress log* and
> *Deviations* sections at the end of every phase. If an implementation choice contradicts
> this file, change the file first (with a reason) and then the code.

## 1. Goal

Learn Apache Flink internals hands-on by building a small, real, distributed
trending-hashtags system on a single Mac, and publish it as a clean GitHub portfolio repo.

The owner is **new to Flink**. Every phase must therefore produce:
1. working code,
2. a runnable scenario that makes a Flink behaviour *visible*,
3. a concepts doc `docs/NN-<topic>.md` explaining what happens inside and what to look at.

## 2. Working agreement

- Work **one phase at a time**. Stop at the end of each phase so the owner can run and explore it.
- Don't start the next phase without the owner's go-ahead.
- Commit only when the owner asks; one or more commits per phase, conventional-commit style
  (`feat(flink-job): ...`). Never push or create a GitHub remote without explicit approval.
- Verify every version pin or API against the real artifact or docs before using it, and don't rely on memory.
- Keep the VM within budget (§5). New containers must have a `mem_limit`.
- Docs are written for a Flink beginner: concrete, with "what to look at in the UI" and real
  observations from running it (not invented expectations).

## 3. Fixed decisions

| Topic | Decision | Why |
|---|---|---|
| Host | macOS (Apple Silicon, 8 CPU, 16 GB) | owner's laptop |
| Virtualisation | **One** Lima VM `flink-lab` (6 CPU, 8 GiB, 40 GiB), `base: template:docker` (rootless Docker) | a multi-VM cluster adds RAM and networking cost but little Flink learning |
| Orchestration | Docker Compose, project name `hashtags` | simple, fast iteration; failover = `docker kill` |
| Docker access | Makefile exports `DOCKER_HOST=unix://$HOME/.lima/flink-lab/sock/docker.sock` | never touch Docker Desktop's context |
| Mounts | only the repo dir, read-only, at the same path | compose bind-mounts resolve identically |
| Source | Kafka (KRaft, single broker) | replayable; offsets in checkpoints make recovery demos meaningful |
| Flink job language | **Java 17**, DataStream API | Flink has no Go API; Beam-Go/StateFun rejected (abstraction / unmaintained) |
| Generator + API | **Go** | polyglot profile, natural fit |
| Build | Maven multi-module, fat jar via shade; Flink deps `provided` | jar is submitted via REST, no Java in VM |
| Job submission | REST from the host: `/jars/upload` → `/jars/{id}/run` | mirrors real deployments, no `docker exec` |
| Flink cluster | 1 JobManager + 2 named TaskManagers × 2 slots, `parallelism.default: 2` | named TMs so scenarios can kill one |
| Restart strategy | `exponential-delay` (set explicitly) | visible, predictable |

### Pinned versions (verified 2026-10-04)

| Component | Version | Note |
|---|---|---|
| Flink | **2.2.1** (`flink:2.2.1-java17`) | 2.3.0 exists but the Kafka connector supports only ≤2.2 |
| flink-connector-kafka | **5.0.0-2.2** | supports Flink 2.1/2.2 |
| Kafka | `apache/kafka:4.3.1` | |
| Redis | `redis:8.6.7-alpine` | no persistence (`--save ""`), 256 MB LRU |
| Go | 1.26 (host) | |
| Java | 17 (host, for Maven build) | |

## 4. Architecture

```
 generator (Go, host) ──► Kafka topic `posts` (6 partitions, key = user_id)
                                  │
                                  ▼
 ┌──────────────────────── Flink job "hashtag-trends" ─────────────────────────┐
 │ KafkaSource (+ watermarks per partition, idleness)                          │
 │   → parse JSON → extract hashtags (flatMap, normalise lowercase)            │
 │   → keyBy(tag)                         ← network shuffle between TMs        │
 │   → sliding event-time window count    ← late events → side output         │
 │   → keyBy(windowEnd) → top-K           ← stage 2 of two-stage aggregation   │
 │   → RedisSink (custom, Sink v2)                                             │
 └─────────────────────────────────────────────────────────────────────────────┘
        │ checkpoints (phase 3)                         │
        ▼                                               ▼
     MinIO / S3                                       Redis ──► trending API (Go) ──► web page
```

Kafka key = `user_id` on purpose: posts about one hashtag land on *all* partitions, so the
`keyBy(tag)` shuffle is real and visible.

### Event schema (Kafka `posts`, JSON value)

```json
{ "post_id": "uuid", "user_id": "u-123", "text": "loving #flink and #Redis today",
  "lang": "en", "event_time": 1759550000000 }
```
`event_time` = epoch ms, set by the generator, possibly in the past (out-of-order / late).
Hashtags are extracted **in Flink** from `text` (regex `#[\p{L}\p{N}_]+`, lower-cased).

### Redis data model

All writes must be **idempotent**: write absolute values computed by Flink and never `INCR`,
so replays after a failure overwrite instead of double counting. Phase 5 explains why.

| Key | Type | Phase | Content |
|---|---|---|---|
| `hashtag:totals` | HASH tag → count | 1 | running total per tag (from keyed state) |
| `trending:{size}:{windowEndMs}` | ZSET tag → count | 2/4 | top-K for one window, `EXPIRE` ~1h |
| `trending:{size}:latest` | STRING | 2/4 | windowEndMs of newest complete window |
| `late:events` | STREAM (`MAXLEN ~1000`) | 2 | late records from the side output |
| `stats:late` | HASH | 2 | counters for late events per tag (absolute) |

### Repo layout (target)

```
flink-hashtag-trends/
  Makefile                 single entry point (make help)
  CLAUDE.md                pointer to this plan for AI-assisted sessions
  infra/
    lima/flink-lab.yaml
    docker-compose.yml
    prometheus/, grafana/  (phase 5/7)
    scripts/               smoke.sh, submit-job.sh, ...
  generator/               Go module: cmd/generator, internal/{gen,kafka}
  flink/                   Maven parent pom
    redis-sink/            reusable Sink v2 Redis connector module (+ tests)
    hashtag-job/           the pipeline: model, operators, config, Main
  api/                     Go module: trending HTTP API + static page
  scenarios/               NN-name.sh, each prints what to watch
  docs/                    PLAN.md, NN-topic.md, architecture diagram
  .github/workflows/ci.yml
```

## 5. Resource budget (VM 8 GiB)

| Container | mem_limit | Phase |
|---|---|---|
| kafka | 768m (heap 384m) | 0 |
| jobmanager | 1152m (process 1024m) | 0 |
| taskmanager-1/2 | 1408m each (process 1280m) | 0 |
| redis | 320m | 0 |
| minio | ~384m | 3 |
| prometheus + grafana | ~512m | 5 |
| api | ~64m | 7 |
| **total** | **~6.0 GiB** | headroom for OS + docker |

The generator runs on the host by default. `make down` stops the VM to free its RAM.
Don't run Docker Desktop at the same time.

## 6. Phases

Each phase: deliverables → acceptance checks (all must pass before calling it done) → doc.

### Phase 0: Cluster anatomy ✅
- Lima VM, compose (Kafka, JM, 2 TMs, Redis), Makefile, smoke script, `docs/00-cluster-anatomy.md`.
- Accept: `make up` healthy; `make smoke` passes; TM kill/restart observed and documented.

### Phase 1: End-to-end pipeline
- **generator/** (Go, `franz-go` client): flags `--rate`, `--tags` (vocabulary size),
  `--zipf` (skew), `--users`; JSON events above; graceful shutdown; prints throughput.
  `make gen` runs it on the host against `localhost:29092`.
- **flink/** Maven parent + `hashtag-job` module: KafkaSource (group `hashtag-trends`,
  committed offsets) → parse → extract tags → `keyBy(tag)` → keyed running count
  (`ValueState<Long>`) → simple Redis sink (Sink v2 `SinkWriter`, Jedis, sync) writing
  `HSET hashtag:totals`.
- Job config via CLI args with defaults (bootstrap servers, topic, redis host, parallelism).
- `make build` (mvn package), `make job` (submit via REST script), `make job-cancel`.
- Unit tests: hashtag extraction, JSON parsing; MiniCluster test of the pipeline with a
  test source and a collecting sink.
- `docs/01-first-pipeline.md`: sources, operators, keyBy and hash partitioning / key groups,
  chaining visible in the UI, records in/out per subtask, what "processing time" means here.
- Accept: generator at 1k ev/s → `HGETALL hashtag:totals` grows; UI shows shuffle between
  source and keyed operator on both TMs; tests green.

### Phase 2: Event time and windows
- Generator adds `--out-of-order-ms`, `--late-fraction`, `--late-ms`.
- WatermarkStrategy `forBoundedOutOfOrderness` at the source (per-partition) + `withIdleness`.
- Sliding window (default size 60s, slide 10s, configurable) counting per tag;
  `allowedLateness` small (e.g. 10s); late beyond that → side output → `late:events` / `stats:late`.
- Window results → `trending:{size}:{end}` (all tags of window, before top-K) + `latest` pointer.
- Tests: watermark/window behaviour with a controlled test source (deterministic timestamps).
- `scenarios/02-late-data.sh`: crank lateness, watch the side output and watermark lag in the UI.
- `docs/02-event-time.md`: event vs processing time, watermarks, per-partition watermarks and
  idleness, window lifecycle (assign → trigger → fire → purge), allowed lateness, side outputs.
- Accept: windows emit only after watermark passes window end; late events visible in Redis.

### Phase 3: State, checkpoints, recovery
- Add MinIO (or alternative; see open question Q1) + bucket init; Flink S3 filesystem plugin.
- RocksDB state backend, incremental checkpoints every 10s, EXACTLY_ONCE barriers,
  retained on cancellation; Kafka offsets committed on checkpoint.
- `scenarios/03-failover.sh`: run generator, kill a TM mid-stream, restore, compare Redis
  totals against a ground-truth count from the generator (generator logs per-tag counts).
- `docs/03-state-checkpoints.md`: keyed state, state backends, checkpoint barriers and
  alignment, what's in a checkpoint (the UI checkpoint tab), restore flow, why idempotent sink
  writes + replay give correct results.
- Accept: after a TM kill the job restores from the latest checkpoint and totals match ground truth.

### Phase 4: Top-K and skew
- Stage 1: per-tag window count (from phase 2). Stage 2: `keyBy(windowEnd)` +
  `KeyedProcessFunction` buffering counts until the event-time timer at `windowEnd`,
  emit top N (default 10) → ZSET.
- Generator `--viral-tag` / `--viral-share` (or HTTP control endpoint) to create a hot key.
- Salting variant (`keyBy(tag + "#" + salt)` partial counts → merge by tag), toggled by flag.
- `scenarios/04-hot-key.sh`: observe one subtask's busy time/backpressure skyrocket; enable salting; compare.
- `docs/04-topk-skew.md`: timers, two-stage aggregation, data skew, salting trade-offs.
- Accept: skew visible in subtask metrics; salting evens it out; top-K correct in tests.

### Phase 5: Redis sink deep dive, backpressure, metrics
- `redis-sink` module: rewrite the sink on `AsyncSinkBase` (flink-connector-base) with
  Lettuce async: batching, max in-flight requests, buffer limits, retries; idempotent commands.
- Integration tests with Testcontainers Redis (configure for Lima rootless Docker socket).
- Prometheus + Grafana in compose; Flink Prometheus reporter; basic dashboard
  (throughput, busy/backpressured time, checkpoint duration, watermark lag).
- `scenarios/05-backpressure.sh`: slow Redis (e.g. `DEBUG SLEEP` loop or `tc netem` delay) and
  watch backpressure propagate to the source; Kafka consumer lag grows; recover.
- `docs/05-sinks-backpressure.md`: Sink v2 API, delivery guarantees (at-least-once vs
  exactly-once, why Redis can't do two-phase commit, and why idempotent writes are enough),
  credit-based flow control, how backpressure is measured.
- Accept: backpressure visible in the UI and Grafana; sink tests green.

### Phase 6: Savepoints and rescaling
- `make savepoint`, `make job-from-savepoint p=4` targets; stable operator `uid()`s on every
  stateful operator (add in phase 1 already); `setMaxParallelism` fixed.
- `scenarios/06-rescale.sh`: savepoint at p=2, stop, restart at p=4 (needs ≥4 slots: OK),
  verify state carried over.
- `docs/06-savepoints-rescaling.md`: savepoint vs checkpoint, uids, key groups and max
  parallelism, how state is redistributed.
- Accept: rescaled job continues with correct totals.

### Phase 7: Polish
- `api/` Go service: `GET /trending?window=60s&k=10`, `GET /tags/{tag}`, static auto-refreshing
  page; in compose with mem_limit.
- Grafana dashboard JSON committed; architecture diagram in README.
- GitHub Actions: `mvn -B verify`, `go test ./...`, `docker compose config`.
- README: screenshots/GIF, what-I-learned section, links to each doc.
- Accept: fresh clone → `make vm-create up build job gen` works; CI green.

## 7. Open questions / risks

- **Q1 MinIO images:** MinIO changed its community distribution in 2025. At phase 3,
  check that a pinned `minio/minio` arm64 image is still pullable; otherwise choose an
  S3-compatible alternative (e.g. SeaweedFS, Garage) or a shared Docker volume with `file://`.
- **Q2 Testcontainers + rootless Lima Docker:** may need `DOCKER_HOST` and
  `TESTCONTAINERS_RYUK_DISABLED=true`; verify in phase 5.
- **Q3 Flink 2.x API changes:** e.g. `SinkFunction`/`SourceFunction` removed, `ParameterTool`
  location, `Time` → `Duration` in window APIs. Check against the 2.2 docs and not 1.x examples.
- **Q4 Throughput on laptop:** target ~1–5k events/s; enough to see backpressure without
  overheating. Tune in phase 5.

## 8. Progress log

| Date | Phase | Notes |
|---|---|---|
| 2026-10-04 | 0 | VM + compose up; smoke job; TM kill test: immediate restart attempt hit dead TM (`RecipientUnreachableException`), heartbeat removal ~30s, job waited in CREATED for slots, recovered on TM start. |
| 2026-10-04 | 0 | Fixed smoke.sh: `smoke-stop` now cancels RESTARTING/CREATED jobs too (p=6 exercise jobs were un-cancellable and starved slots); `smoke` tracks its own JobID. |

## 9. Deviations from plan

| Date | Change | Reason |
|---|---|---|
| 2026-10-04 | VM memory 10 → 8 GiB | owner asked about RAM impact on 16 GB host |
| 2026-10-04 | MinIO moved to phase 3, Prometheus/Grafana to phase 5 | add components when first used |
| 2026-10-04 | Flink 2.2.1 instead of 2.3.0 | Kafka connector supports ≤2.2 |
