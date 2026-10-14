# 02 · Event time, watermarks and windows

> **Goal of this phase:** turn "all-time counts" into "**trending**": counts per hashtag over the
> last 60 s, refreshed every 10 s, measured by **when posts were written**, not when they
> happened to arrive. You'll see watermarks move, windows fire, late data get diverted, an
> empty partition freeze time, and a replay produce byte-identical results.

## The problem

Phase 1 ignored the `event_time` field completely. "Trending" needs a clock, and there are two:

| Clock | Meaning | Problem |
|---|---|---|
| **Processing time** | the Flink machine's clock when a record arrives | a backlog, a replay or a slow network moves posts into the *wrong* minute |
| **Event time** | `event_time`, set when the post was written | records arrive out of order, so when is a minute "complete"? |

In the phase 1 failure test, Flink re-read ~25 s of posts in ~8 s. With processing-time windows,
those would have landed in "the current minute" as a fake spike. With event time each post
falls in the minute it was written, no matter when it is processed. The price is that
Flink must decide when a window has seen "enough" data. That decision is the **watermark**.

## The new topology

```
                        WatermarkStrategy: per partition, bound 5s, idleness 30s
                                    │
Kafka ──► [ kafka-posts (parse JSON, assign timestamps, emit watermarks) → extract-hashtags ]
                                    │
            ┌───────────keyBy(tag)──┴──keyBy(tag)────────────────┐
            ▼                                                    ▼
 [ running-count → redis-totals ]          [ window-count (sliding 60s/10s, lateness 10s) → redis-windows ]
   phase 1, unchanged                          │ side output: tags too late for every window
                                               ├────────► [ late-tags → redis-late-events ]   (chained)
                                               └─keyBy──► [ late-count → redis-late-totals ]
```

Code:
- `HashtagTrendsJob.watermarks()`: the watermark strategy.
- `ops/PostDeserializer`: parsing moved into the source; see below for why.
- `Pipeline.build()`: the window, `allowedLateness`, `sideOutputLateData`.
- `ops/CountAggregate` + `ops/EmitWindowCount`: incremental counting per window.
- `RedisMappings`: Redis layout.

Redis now also contains:

| Key | Type | Content |
|---|---|---|
| `trending:60s:<windowEndMs>` | ZSET | tag → count for that window (expires after 1 h) |
| `trending:60s:windows` | ZSET | index of window ends; the newest is the "current" trending window |
| `late:events` | STREAM | the most recent ~1000 late occurrences: tag, event_time, watermark, behind_ms |
| `stats:late` | HASH | late occurrences per tag (absolute counts) |

## Concept 1: watermarks

A **watermark** is a special marker that flows through the stream next to the records and says
*"event time has reached T; no record older than T should still arrive."*

We use *bounded out-of-orderness*:
```
watermark = (largest event time seen so far) − 5s − 1ms        emitted every 200 ms
```
So if the newest post Flink has seen was written at 12:01:05.000, the watermark says 12:00:59.999.

Three rules matter in practice:

1. **Watermarks are generated in the source, per Kafka partition.** That's why parsing moved
   from a separate `parse-post` operator into the source's `PostDeserializer`: the source must
   read `event_time` to assign timestamps. Each partition (split) gets its own watermark, and
   the reader emits the **minimum**. Had we generated watermarks *after* the source, partitions
   would be mixed together first, and a slow partition's posts would look "late" compared with a
   fast one.
2. **Every operator's event time is the minimum over its inputs.** `window-count` receives
   watermarks from both source subtasks and advances only as fast as the slower one.
3. **No watermark = no event time.** Until every input has produced one, the watermark is
   "none" (`Long.MIN_VALUE`) and nothing event-time-based can fire (see Concept 4).

**Watch it:** `make watermarks` prints each task's watermark and its lag behind the wall clock:
```
Source: kafka-posts -> extract-hashtags   emits  01:13:10  lag per subtask  #0:5.2s  #1:5.2s
window-count -> ...                       input  01:13:10  lag per subtask  #0:5.3s  #1:5.3s
```
The lag is about 5 s: exactly the bound, plus the 200 ms emit interval and up to 2 s of
metric refresh.

> **Gotcha we hit:** the UI's / REST `…/watermarks` endpoint showed ~16 s lag while windows
> were clearly firing on time. That endpoint serves a cached value. The per-vertex
> `…/metrics?get=N.currentInputWatermark` endpoint is fresh, so our script uses it. We also
> lowered `metrics.fetcher.update-interval` from 10 s to 2 s in `docker-compose.yml`.

## Concept 2: sliding windows

`SlidingEventTimeWindows.of(60s, 10s)`: every 10 s of event time, a window covering the
previous 60 s. Each post belongs to **6** overlapping windows.

```
event time ──►  :00     :10     :20     :30     :40     :50   1:00    1:10
window A        [──────────────── 60s ──────────────────────)
window B                [──────────────── 60s ──────────────────────)
```

A window's lifecycle:
1. **assign:** a post with event time T is added to every window covering T;
2. **accumulate:** `CountAggregate.add()` keeps one `Long` per (tag, window). No list of posts is stored;
3. **fire:** when the watermark passes the window end, `EmitWindowCount` emits
   `(tag, start, end, count)` → `ZADD trending:60s:<end>`;
4. **clean up:** once the watermark passes *end + allowed lateness*, the state is deleted.

Unlike phase 1's totals, window state does **not** grow forever.

**What we saw** (`make trending` during a run):
```
wall 01:04:44   newest window 01:03:40 → 01:04:40   flink 20479  kafka 9868  redis 6240
```
The newest finished window always ends about 5 s (the bound) before the wall clock. A window
ending at 01:04:40 can't fire until a post stamped ≥ 01:04:45 has been seen.

## Concept 3: late data, in three tiers

A record is *late for a window* if it arrives after that window fired. What happens depends on how late:

| How late | What Flink does | In our job |
|---|---|---|
| within the 5 s bound | not late at all: the watermark waited for it | counted normally |
| window fired, but < end + **allowed lateness** (10 s) | added; the window **fires again** with the corrected count | `ZADD` overwrites the score: an absolute value, so it's safe |
| past end + allowed lateness for **some** of its windows | **silently dropped** from those windows, counted in the still-open ones | ⚠️ no trace |
| past end + allowed lateness for **all** its windows | sent to the **side output** | `late:events`, `stats:late` |

Two real observations:

**Correction firing:** after the generator stopped, the `01:03:40–01:04:40` window's `flink`
went from **20,479 to 20,501**. Late posts arrived within the 10 s grace period and the window
fired again.

**The sliding-window surprise:** our first run made 2% of posts **30 s late** and got **zero**
late events. A post written 30 s ago still belongs to the *newer* windows that cover it (a
window ending 30 s from now includes it), so it's only too late for the *older* windows.
Flink drops it from those without telling anyone. It reaches the side output only once it is
older than roughly *window size + allowed lateness + bound* (≈ 75 s here). With **90 s** late
posts, the side output fills:
```
late event: photography  event_time 1792007067687  watermark 1792007155886  behind_ms 88199
late per tag: flink 285, kafka 126, redis 89 ...        (≈2% of each tag, as generated)
```
**Lesson:** with sliding windows, the side output undercounts lateness. If you need every
dropped (record, window) pair, add a custom trigger or count per window yourself.

## Concept 4: an empty partition freezes time

`scenarios/02b-idle-partitions.sh` writes only to partitions 0–2 of 6.
```
A) --idleness 0
  t= 8s .. 40s   windows written: 0   window-count watermark none
  offsets: posts:0:13266 posts:1:13346 posts:2:13368 posts:3:0 posts:4:0 posts:5:0
B) --idleness 10s
  t= 8s          windows written: 0   watermark none
  t=16s          windows written: 5   watermark 01:17:17  lag 5.1s
  t=24s ..       windows keep firing every 10 s
```
In **A**, 40,000 posts flowed and **no window ever fired**. Each source reader owns three
partitions (reader 0 had partitions 1, 3 and 5; reader 1 had 0, 2 and 4). Partitions 3, 4 and 5
never produced a watermark, so the minimum stayed at "none".

In **B**, after 10 s without data those partitions were marked **idle** and excluded from the
minimum. This happens in production whenever a topic has more partitions than active
producers, or a source has more parallelism than partitions.

The trade-off: if an idle partition suddenly gets data that is *older* than the watermark,
that data is late. Idleness trades completeness for progress.

Related: when **all** traffic stops, the watermark stops too, so the last windows don't fire
until new data arrives. Streams have no "end".

## Concept 5: replay gives identical results

The phase 1 replay made `#flink` drop from 14,792 to 41 and climb back. Now we took a finished
window, deleted it from Redis, and restarted the job (it re-reads everything from offset 0):
```
window: trending:60s:1792006970000   200 tags
... cancel, DEL key, resubmit, replay ...
IDENTICAL after replay (200 tags)
```
Event time makes the output a function of the **data**, not of when or how fast it is processed.
That's the property that makes recovery and backfills safe.

(The all-time totals branch still behaves as in phase 1, since it has no notion of time. With
replays it also ended exact: `make verify` → 120,003 occurrences, all tags match.)

## Testing event time deterministically

`PipelineTest` doesn't use the real 200 ms periodic watermarks, because a fast in-memory source
would make timing random. `TestPosts.watermarkPerEvent()` emits a watermark after **every**
record, so each test is an exact timeline:
```
a@1s a@2s b@6s | a@11s → watermark 10.999s closes windows ending at 5s and 10s
               | a@3s  → both its windows are closed → side output, watermark 10999
               | a@12s
```
Four tests cover: firing on the watermark, late data → side output, allowed lateness →
re-firing with a corrected count (2 then 3), and out-of-order within the bound → still counted.

## Run it

```bash
make build
make scenario-02     # messy event times; watch watermark, windows, late events (~90 s)
make scenario-02b    # empty partitions with and without idleness (~90 s)

# or by hand
make reset job
make gen DURATION=60s ARGS="-out-of-order 3s -late-fraction 0.02 -late 90s"
make watermarks      # each task's event time + lag
make trending        # newest window's top 10
make late            # late events and per-tag counts
```
In the UI, open the job, click `window-count`, then the **Watermarks** tab, and compare
with `make watermarks`.

## Exercises

1. **Tighter bound:** `make job JOB_ARGS="--out-of-orderness 1s"` with
   `ARGS="-out-of-order 3s"`. On-time posts now miss windows. Do they show up in `late:events`?
   (Think about Concept 3 before checking.)
2. **Tumbling windows:** make `--window-slide` equal to `--window-size` (`10s`/`10s`). Now each
   post is in one window. Re-run with 30 s late posts: how many late events now, and why?
3. **No grace:** `--allowed-lateness 0`. Watch a window's score: does it ever change after
   it first appears?
4. **Parallelism vs partitions:** `make job P=4`. Each reader gets 1–2 partitions. Run the
   idleness scenario's generator (`ARGS="-partitions 0"`) and predict which readers block the
   watermark.

## Key takeaways
- Event time = correctness under disorder and replay. Watermarks = the moment you decide to stop waiting.
- Generate watermarks **in the source** so they are tracked per partition.
- An operator's event time is the minimum over its inputs. One silent input stops everything;
  idleness is the escape hatch, at the cost of possible late data.
- Late data has tiers: within bound (fine), within allowed lateness (re-fire), beyond it
  (dropped per window, side output only when all windows are closed).
- Write absolute values to the sink so re-fires and replays overwrite, never accumulate.
