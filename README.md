# flink-hashtag-trends

A small but real **distributed trending-hashtags system** that runs entirely on a laptop:
a Go event generator → **Kafka** → an **Apache Flink** cluster → **Redis** → a Go trending API.

It's built as a hands-on lab: every Flink feature it uses comes with a script that triggers it,
plus a write-up of what happens inside the cluster.

```
 generator (Go) ──► Kafka ──► Flink: JobManager + 2 TaskManagers ──► Redis ──► trending API (Go)
                                       │
                                       └── checkpoints ──► MinIO (S3)
```

## Status

| Phase | Topic | State |
|---|---|---|
| 0 | Cluster anatomy: VM, Kafka, Flink, Redis, smoke job | ✅ |
| 1 | End to end: generator → Kafka → Flink count → Redis | ✅ |
| 2 | Event time, watermarks, sliding windows, late data | ✅ |
| 3 | Keyed state, RocksDB, checkpoints, failure recovery | ⏳ |
| 4 | Two-stage top-K, hot-key skew, salting | |
| 5 | Custom Redis Sink (Sink v2), delivery guarantees, backpressure | |
| 6 | Savepoints and rescaling | |
| 7 | Trending API/page, Grafana, CI | |

## Run it

Requirements: macOS (Apple Silicon or Intel), [Lima](https://lima-vm.io) ≥ 2.0, Docker CLI.
Everything runs in a single Lima VM (6 CPU / 8 GiB), so nothing is installed on the host.

```bash
make vm-create   # one-time: Ubuntu VM with rootless Docker
make up          # start VM + services, wait for health, create topics
make smoke       # check services, run a sample Flink job
make smoke-stop
make build       # job jar (with tests) + generator binary
make reset job   # clean slate, submit the hashtag job
make gen DURATION=30s
make verify      # Redis totals == generator's ground truth?
make trending    # top hashtags of the newest 60 s window
make scenario-02 # watch watermarks, windows and late data live
make help        # all targets
make down        # stop everything and the VM
```

| Endpoint | URL |
|---|---|
| Flink UI | http://localhost:8081 |
| Kafka (from host) | `localhost:29092` |
| Redis | `localhost:6379` |

## Layout

```
infra/       Lima VM config, docker-compose, helper scripts (job submit/reset/verify)
generator/   Go: synthetic posts → Kafka, skewed hashtags, ground-truth output
flink/       Maven: redis-sink (Sink v2 connector) + hashtag-job (the pipeline)
scenarios/   scripts that trigger one Flink behaviour each and print what to watch
docs/        PLAN.md (full plan + progress) and one concept guide per phase
```

## Versions

Flink 2.2.1 (Java 17) · Kafka 4.3.1 (KRaft) · Redis 8.6 · flink-connector-kafka 5.0.0-2.2
