# 00 · Cluster anatomy

> **Goal of this phase:** have a real Flink cluster running locally, know what each piece is,
> and see with your own eyes how a job is spread across machines and what happens when one dies.

## The 30-second version

You write a program that describes a **dataflow** (source → transformations → sink).
You don't run it like a normal program: you **submit** it to a Flink cluster, which cuts it
into pieces and runs those pieces in parallel on worker processes.

```
           you ──submit job──►  ┌──────────────── JobManager ────────────────┐
                                │ Dispatcher      accepts jobs, serves REST/UI│
                                │ ResourceManager knows every slot in cluster │
                                │ JobMaster (1/job) schedules tasks, restarts │
                                │                 them, coordinates checkpoints│
                                └──────────┬───────────────────┬──────────────┘
                                 heartbeats + task deployment (RPC)
                                ┌──────────▼─────────┐ ┌───────▼────────────┐
                                │ TaskManager-1      │ │ TaskManager-2      │
                                │ [slot 0] [slot 1]  │ │ [slot 0] [slot 1]  │
                                └─────────▲──────────┘ └─────────▲──────────┘
                                          └──── data (Netty) ────┘
```

| Piece | What it is | In this repo |
|---|---|---|
| **JobManager** | The coordinator. One per cluster. Never touches your data records. | `jobmanager` container, UI on :8081 |
| **TaskManager** | A worker JVM. Runs your operators and moves data between them. | `taskmanager-1`, `taskmanager-2` |
| **Slot** | A fixed share of a TaskManager's memory. The unit of scheduling. | 2 per TaskManager → 4 total |
| **Parallelism** | How many copies (*subtasks*) of each operator run. | default 2, the smoke job uses 4 |

Two separate channels connect them, which matters later:
- **Control plane** (RPC via Pekko): JM ↔ TM. Deploy task, heartbeat, "take a checkpoint".
- **Data plane** (Netty TCP): TM ↔ TM. Your records flow here when data must move between
  subtasks (e.g. after `keyBy`). This is where *backpressure* happens (phase 5).

## From your code to running tasks

1. **StreamGraph / JobGraph:** Flink turns your code into a graph of operators. Operators that
   can run in the same thread without a network hop get **chained** into one *task*
   (e.g. `Flat Map -> Sink`). Chaining avoids serialization and is the #1 free performance win.
2. **ExecutionGraph:** the JobMaster expands every task by its parallelism. A job with 2 tasks
   at parallelism 4 = **8 subtasks**.
3. **Scheduling:** subtasks go into slots. Thanks to **slot sharing**, one slot can hold one
   subtask *of each task*, i.e. a full vertical slice of the pipeline. So those 8 subtasks
   need only **4 slots**, not 8. Rule of thumb: *slots needed = max parallelism in the job.*

## Worked example: chaining, the network hop and slots

### Vocabulary

| Term | What it is | Count in the example below |
|---|---|---|
| **Operator** | One step you wrote (source, map, sink, ...) | 3 |
| **Task** | A chain of operators fused to run in one thread | 2 |
| **Subtask** | One parallel copy of a task = **one thread** | 2 tasks × p4 = 8 |
| **Slot** | A fixed share of one TaskManager's resources that subtasks are placed in | 4 (2 per TM) |

### The StateMachineExample, as Flink actually built it

```java
env.fromSource(generator, ...)        // fake events: (sourceAddress, eventType)
   .keyBy(Event::sourceAddress)       // all events of one address must meet in one place
   .flatMap(new StateMachineMapper()) // per-address state, flags invalid transitions
   .print();
```

3 operators became **2 tasks**. The plan from `GET /jobs/<id>/plan`:
```
TASK: Flat Map +- Sink: Print to Std. Out | parallelism 4
    <- input via HASH
TASK: Source: Events Generator Source     | parallelism 4
```

```
TASK A                               TASK B
┌──────────────────────┐    HASH     ┌──────────────────────────────────┐
│ Source: Events Gen   │ ──────────► │ Flat Map  ──►  Sink: Print       │
└──────────────────────┘ (network)   └──────────────────────────────────┘
                                         ▲ chained: plain method call
```

- **Flat Map → Sink is chained:** each Flat Map copy hands its output to exactly one Sink
  copy, with the same parallelism and no reshuffling. So the hand-off is a method call: nothing
  is serialized, buffered or copied.
- **Source → Flat Map is not chained**, because `keyBy` is in between. Any source copy can
  produce any address, but each address must reach the one copy holding its state. Every
  record is therefore **serialized → hashed to a target copy → written into a network buffer
  (32 KB) → flushed when full or after the buffer timeout (100 ms) → deserialized**.
  That is the *network hop*: the most expensive thing a record goes through, and the
  place where backpressure shows up (phase 5).

Live subtask metrics (`GET /jobs/<id>/vertices/<vid>`) confirm it:
```
Source: Events Generator Source
  subtask 1/4 on taskmanager-1 | records out 15220
  subtask 3/4 on taskmanager-2 | records out 15196   ...
Flat Map -> Sink
  subtask 1/4 on taskmanager-1 | records in 14967  bytes in 329457
  subtask 3/4 on taskmanager-2 | records in 15059  bytes in 331380   ...
```
- Flat Map has **bytes in**: records arrived serialized, so there is an exchange.
- Each Flat Map copy receives from **all 4** source copies. About half comes from the other TM
  over TCP; the other half comes from the same TM over a local channel (still serialized and
  buffered, just no socket).
- The Sink shows no records: metrics only count at task boundaries, and Flat Map→Sink never
  leaves the thread.

**What breaks a chain:** `keyBy`, `rebalance`/`shuffle`/`broadcast`, a change of parallelism
between operators, or explicit `disableChaining()` / `startNewChain()`.

**Slot sharing:** each slot holds one subtask of each task, so 8 subtasks fit in 4 slots:
```
taskmanager-1                                 taskmanager-2
┌─ slot ───────────┐ ┌─ slot ───────────┐    ┌─ slot ───────────┐ ┌─ slot ───────────┐
│ Source 1/4       │ │ Source 2/4       │    │ Source 3/4       │ │ Source 4/4       │
│ FlatMap→Sink 1/4 │ │ FlatMap→Sink 2/4 │    │ FlatMap→Sink 3/4 │ │ FlatMap→Sink 4/4 │
└──────────────────┘ └──────────────────┘    └──────────────────┘ └──────────────────┘
```
A slot isolates **memory**, not CPU: all threads on a TM share its cores.

### The same ideas applied to our hashtag job (phase 1)

```java
KafkaSource("posts")            // operator 1
  .map(parseJson)               // operator 2
  .flatMap(extractHashtags)     // operator 3: one post → 0..n tags
  .keyBy(tag -> tag)            // not an operator: a rule for how data moves
  .process(runningCount)        // operator 4: ValueState<Long> per tag
  .sinkTo(redisSink)            // operator 5: HSET hashtag:totals
```
At parallelism 4 this becomes **2 tasks, 8 subtasks, 4 slots**:
```
TASK A (chained)                                     TASK B (chained)
[Kafka Source → Parse → Extract tags]  ──HASH(tag)──►  [Running count → Redis sink]
```

Following one post through it:
1. Kafka message key `u-42` lands in **partition 3**. 6 partitions over 4 source copies split
   2/2/1/1 (a real imbalance; parallelism 3 or 6 would divide evenly). Say partition 3 belongs
   to **Source 2/4 on taskmanager-1**.
2. In the **same thread**, the post `"loving #flink and #Redis"` is parsed and `flink` and
   `redis` are extracted. No hop.
3. **keyBy = the hop.** Each tag is hashed into one of 128 **key groups** (the default max
   parallelism); each counting copy owns 32 of them. `flink` might belong to *Count 3/4 on
   taskmanager-2* (TCP to the other machine); `redis` to *Count 1/4 on taskmanager-1*
   (local channel).
4. *Count 3/4* is the **only** place in the cluster holding `flink`'s total. It increments it,
   and the chained sink writes `HSET hashtag:totals flink 1234` in the same thread.

Why this shape:
- Parse and extract are cheap and work on one record at a time → chain them to the source.
- Counting needs every occurrence of a tag in one place → exactly one `keyBy` hop is unavoidable.
- Ownership is by key group, not `hash % parallelism`. That lets phase 6 rescale by moving
  whole key groups instead of rehashing every key.
- A viral tag (40% of traffic) overloads the single copy that owns it while others idle →
  the skew problem of phase 4.

## Memory: why `1280m` is not all heap

A TaskManager's `taskmanager.memory.process.size` is split into regions. Open
**Task Managers → taskmanager-1 → Metrics** in the UI to see them:

- **Framework / task heap:** normal Java objects (yours and Flink's).
- **Managed memory:** off-heap, used by RocksDB state (phase 3) and batch sorting.
- **Network buffers:** the queues between subtasks. When they fill up, that is backpressure.
- **JVM metaspace + overhead:** class metadata, threads, native allocations.

That's also why each container's `mem_limit` is slightly above the Flink process size.

## Hands-on

```bash
make up          # start VM + services (first run pulls images)
make smoke       # health checks + start Flink's StateMachineExample at parallelism 4
```

### Exercise 1: read the cluster
Open http://localhost:8081.
- **Overview:** 2 TaskManagers, 4 slots. After `make smoke`, 0 slots are free. Why exactly 4?
- **Running Jobs → State machine job:** the graph has 2 boxes. Which operators got chained?
- Click a box → **SubTasks** tab: which TaskManager runs each subtask? Are they spread evenly?
- **Task Managers → one TM → Metrics:** find managed memory and network buffer sizes.

### Exercise 2: kill a machine
In a second terminal, point your Docker CLI at the VM first:
```bash
export DOCKER_HOST=unix://$HOME/.lima/flink-lab/sock/docker.sock
docker kill hashtags-taskmanager-2-1
```
Watch the job page and **Exceptions** tab for ~40 s. What we observed when building this:

1. The job **restarts immediately** (the data connection to TM-2 broke), but the JobMaster
   still believes TM-2 exists and tries to redeploy onto it, which fails with
   `RecipientUnreachableException`.
2. ~30 s later TM-2's missing **heartbeats** time out (worst case `heartbeat.timeout`, 50 s
   by default). The ResourceManager drops it: **1 TaskManager, 2 slots**.
3. The job sits in **CREATED**: it needs 4 slots and only 2 exist, so it waits.
   (The restart strategy, `exponential-delay`, keeps retrying with growing pauses.)

Now bring the "machine" back:
```bash
docker start hashtags-taskmanager-2-1
```
Within seconds TM-2 registers, slots appear, and the job goes back to **RUNNING**.

> **Question to think about:** the job restarted *from scratch*. Every key's state machine
> began again with empty state. In a real system that would lose data. Phase 3 adds
> **checkpoints** so a restart resumes from a consistent snapshot instead.

### Exercise 3: parallelism vs slots
```bash
make smoke-stop
docker compose -f infra/docker-compose.yml -p hashtags exec jobmanager \
  flink run -d -p 6 /opt/flink/examples/streaming/StateMachineExample.jar
```
What happens with parallelism 6 on a 4-slot cluster? (Hint: look at the job state and wait.)
Cancel it with `make smoke-stop`.

## Cleanup
```bash
make smoke-stop  # cancel jobs
make down        # stop containers and the VM (frees its 8 GiB)
```

## Key takeaways
- JobManager = brain (scheduling, failure handling, checkpoints); TaskManagers = muscle.
- Slots bound how much runs in parallel; slot sharing means *max parallelism*, not sum.
- Chaining fuses operators into one thread; data only crosses the network between tasks.
- Failure detection = connection loss (fast) + heartbeat timeout (slow, authoritative).
- Without checkpoints, a restart throws state away. → That's the motivation for phase 3.
