# 01 · The first pipeline: Kafka → Flink → Redis

> **Goal of this phase:** a real end-to-end job you can feed, watch and verify. You'll see
> sources, chaining, the `keyBy` shuffle and keyed state, and what happens to a stateful job
> that has **no checkpoints** when a machine dies.

## What runs

```
 generator (Go, on your Mac)
   │  1000 posts/s, key = user_id, JSON value
   ▼
 Kafka topic `posts` (6 partitions)
   │
   ▼  Flink job "hashtag-trends"
 ┌───────────────────────── TASK A ─────────────────────────┐        ┌─────────── TASK B ───────────┐
 │ kafka-posts ──► parse-post ──► extract-hashtags          │──HASH──►│ running-count ──► redis-totals│
 │ (KafkaSource)   (JSON→Post)    (Post→"flink","redis")    │ (tag)  │ (ValueState)     (HSET)       │
 └──────────────────────────────────────────────────────────┘        └──────────────────────────────┘
                                                                                  │
                                                                     Redis HASH hashtag:totals
```

| Piece | Code | Flink concept |
|---|---|---|
| Source | `HashtagTrendsJob` → `KafkaSource` | split enumerator + readers, offsets |
| Parse | `ops/ParsePost` | `RichFlatMapFunction`, custom metric `malformedPosts` |
| Extract | `ops/ExtractHashtags` | stateless 1→N `flatMap` |
| Shuffle | `.keyBy(tag -> tag)` | hash partitioning via **key groups** |
| Count | `ops/RunningCount` | `KeyedProcessFunction` + `ValueState<Long>` |
| Sink | `redis-sink` module: `RedisSink` / `RedisSinkWriter` | Sink v2: one writer per subtask |

## Run it

```bash
make up                  # if not already running
make build               # mvn package (runs tests) + go build
make reset               # empty topic, clear Redis (needed for exact verification)
make job                 # submit via REST (P=2 by default; try P=4)
make gen DURATION=30s    # 1000 posts/s for 30s; writes ground truth on exit
make totals              # top hashtags in Redis
make verify              # compare Redis with the generator's ground truth
```
What we got:
```
tags in truth: 200   tags in redis: 200
posts-with-tag total: truth=44900 redis=44900
✅ every tag matches exactly
```

## Walkthrough of each piece

### 1. Source: how Kafka partitions become parallel readers
`KafkaSource` has two halves:
- an **enumerator** on the JobManager that discovers the topic's partitions (*splits*) and
  assigns them to readers;
- one **reader** per source subtask on the TaskManagers that actually polls Kafka.

With 6 partitions and parallelism 2, each reader got 3 partitions: subtask 1 read 15,130 records
and subtask 2 read 14,850. With parallelism 4 the split is 2/2/1/1, which is uneven.
**Rule:** keep source parallelism a divisor of the partition count. More readers than partitions just leaves readers idle.

The starting position is `committedOffsets(EARLIEST)`: resume from the consumer group's committed
offsets, or start at the beginning if none exist. Remember that for the failure experiment below.

### 2. Parse and extract: cheap, stateless, chained
Both are ordinary functions that run in the source's thread. `ParsePost` never throws on bad
input: it drops the record and increments the custom metric `malformedPosts`, visible under the
task's **Metrics** tab. One poison message must never crash-loop a streaming job.

`ExtractHashtags` emits each *distinct* tag of a post in lower case, so `"#Flink #flink"` counts once.
The generator deliberately capitalises some tags to make sure we handle that.

### 3. keyBy: key groups, not "hash mod parallelism"
```
keyGroup = murmurHash(tag.hashCode()) % maxParallelism      // maxParallelism = 128
subtask  = keyGroup * parallelism / maxParallelism           // contiguous ranges of key groups
```
At parallelism 2, subtask 1 owns key groups 0–63 and subtask 2 owns 64–127. A tag's key group
never changes. When parallelism changes, whole key-group ranges move between subtasks (phase 6).
That's why `setMaxParallelism(128)` is fixed in code: it can't be changed once state exists.

### 4. Keyed state
```java
count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Types.LONG));
...
Long current = count.value();   // reads the counter of the *current* key only
```
There's no `Map<String, Long>` in our code. Flink keeps one value per key and scopes the handle
to whatever key the current record has. Right now the state lives on the **JVM heap** of the
TaskManager (`HashMapStateBackend`, the default), and nothing saves it anywhere else.

### 5. Sink v2
`RedisSink` is serialized and shipped with the job; each sink subtask calls `createWriter()`
and gets its own `RedisSinkWriter` with its own Redis connection. This phase's writer does one
synchronous `HSET` round trip per record. That's simple and slow, and phase 5 replaces it with an
async, batching writer. Writes are **absolute values** (`HSET flink 1234`), never `HINCRBY`.
The failure experiment shows why.

### Serialization, even when chained
The job sets `pipeline.generic-types: false`, which makes Flink refuse to fall back to Kryo
(slow, schema-less) for any type it can't analyse. Our `Post` and `TagCount` Java records
pass, because Flink 2.x handles records with its POJO serializer. Records are serialized
whenever they cross the HASH exchange. Even between *chained* operators, Flink copies each
record by default (object reuse is off), so serializer cost matters there too.

## What we observed (real numbers)

**Plan:** exactly the two tasks drawn above.
```
TASK: Source: kafka-posts +- parse-post +- extract-hashtags | p 2
TASK: running-count +- redis-totals: Writer                 | p 2
    <- input via HASH
```

**Subtasks after 30 s:**
```
Source: kafka-posts -> parse-post -> extract-hashtags
  subtask 1/2 on taskmanager-1 | in 15130 out 22568
  subtask 2/2 on taskmanager-1 | in 14850 out 22332
running-count -> redis-totals: Writer
  subtask 1/2 on taskmanager-1 | in 19592
  subtask 2/2 on taskmanager-1 | in 25308
```
Three things to notice:
1. **~1.5 records out per post in**: `extract-hashtags` is a 1→N operator (0–3 tags per post).
2. **All subtasks ran on taskmanager-1.** With parallelism 2 the scheduler filled TM-1's two
   slots and left TM-2 idle, so this "shuffle" never left the machine. Run `make job P=4` to
   use both TaskManagers and see real cross-machine traffic. (Flink can be told to spread
   instead: `cluster.evenly-spread-out-slots: true`.)
3. **Skew already:** counting subtask 2 did 29% more work. `#flink` alone was 10,400 of the
   44,900 tag occurrences, and all of them land on one subtask. Phase 4 deals with this.

**Kafka has no consumer group** for `hashtag-trends`
(`kafka-consumer-groups.sh --describe` → *GroupIdNotFoundException*). `KafkaSource` commits
offsets **only when a checkpoint completes**, and we have no checkpoints yet.

## Experiment: kill the TaskManager that holds all the state

With the generator running at 1000 posts/s, we killed `taskmanager-1` (which held every subtask)
and polled `HGET hashtag:totals flink` every 2 s:

| t | flink in Redis | What's happening |
|---|---|---|
| 0–12 s | 11,143 → 14,758 | normal |
| 12 s | | `docker kill hashtags-taskmanager-1-1` |
| 14–26 s | 14,792 (frozen) | failure detection, then restart on taskmanager-2's free slots |
| 28 s | **41** | state is gone; source restarts from offset 0 (no committed offsets) |
| 30–38 s | 10,949 → 24,284 | replaying the whole topic at ~5× normal speed |
| end | **24,284** | equals ground truth (10,400 + 13,884); all 200 tags verified ✅ |

Why the result was still correct:
- Kafka still held **every** post since the beginning, and the job re-read all of it.
- Recounting from zero over the full history rebuilds exactly the same totals.
- Writes are absolute, so the replay **overwrote** Redis. `HINCRBY` would have double counted.

Why this is still bad:
- **Readers saw `#flink` drop from 14,792 to 41**, a visible regression in the product.
- Recovery cost was proportional to *all history*, not to the time since the failure.
  With a week of data and 24 h retention, the totals would also be **wrong**.

The fix is **checkpoints** (phase 3): a periodic, consistent snapshot of *both* the Kafka
offsets and the keyed state. After a failure Flink rewinds to the last snapshot, a few seconds
back instead of to the beginning of time.

## Exercises

1. **Use both machines:** `make job-cancel && make reset && make job P=4 && make gen DURATION=30s`.
   Which subtasks are on which TM now? Open the HASH edge's subtasks: are bytes in roughly
   equal? Does `make verify` still pass?
2. **Crank the skew:** `make gen DURATION=30s ARGS="-zipf 2.0"`. Compare `records in` across
   the counting subtasks. How lopsided can it get?
3. **Poison message:** send a broken record and find the `malformedPosts` metric in the UI:
   ```bash
   echo 'not json at all' | docker compose -f infra/docker-compose.yml -p hashtags exec -T kafka \
     /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic posts
   ```
   (Task A → Metrics → add `...parse-post.malformedPosts`.)
4. **Restart without failure:** `make job-cancel && make job`. What happens to the Redis
   totals, and why? (Hint: offsets.)

## Key takeaways
- Source parallelism should divide the partition count; extra readers sit idle.
- Cheap stateless steps chain into the source; `keyBy` forces exactly one shuffle.
- Keyed state is per key and lives with the subtask that owns the key's key group.
- The scheduler packs slots by default, so a shuffle can be entirely local.
- Without checkpoints, a failure loses all state and replays from the start. That's correct only by
  luck (full retention + idempotent writes), and readers see the regression.
