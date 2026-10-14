#!/usr/bin/env python3
"""Print every task's event-time watermark and how far it trails the wall clock.

Usage: watermarks.py [job-name]      (default: hashtag-trends)

For the source task this is the watermark each subtask *emits*; for every other task the
watermark each subtask *receives* (the minimum over its non-idle input channels). The first column
is the task's slowest subtask. "none" means no watermark yet, so nothing there can fire in event
time. An *idle* source subtask keeps reporting its last value; downstream tasks ignore it.

Reads the per-vertex metrics endpoint: the dedicated /watermarks endpoint serves a cached value
that was observed to be ~10 s stale.
"""
import json
import re
import sys
import time
import urllib.request

FLINK = "http://localhost:8081"
NO_WATERMARK = -(2**63) + 1  # Long.MIN_VALUE (+1 for float rounding): "nothing received yet"


def get(path):
    with urllib.request.urlopen(FLINK + path) as r:
        return json.load(r)


name = sys.argv[1] if len(sys.argv) > 1 else "hashtag-trends"
jobs = [j for j in get("/jobs/overview")["jobs"] if j["name"] == name and j["state"] == "RUNNING"]
if not jobs:
    sys.exit(f"no RUNNING job named {name}")
jid = jobs[0]["jid"]

for v in get(f"/jobs/{jid}")["vertices"]:
    base = f"/jobs/{jid}/vertices/{v['id']}/metrics"
    ids = [m["id"] for m in get(base)]
    if v["name"].startswith("Source"):
        wanted = [i for i in ids if re.fullmatch(r"\d+\.Source__[^.]+\.currentOutputWatermark", i)]
        kind = "emits"
    else:
        wanted = [i for i in ids if re.fullmatch(r"\d+\.currentInputWatermark", i)]
        kind = "input"
    metrics = get(base + "?get=" + ",".join(wanted)) if wanted else []
    per_subtask = sorted((int(m["id"].split(".")[0]), int(float(m["value"]))) for m in metrics)
    now = time.time() * 1000

    def lag(wm):
        return "none" if wm <= NO_WATERMARK else f"{(now - wm) / 1000:.1f}s"

    low = min((wm for _, wm in per_subtask), default=NO_WATERMARK)
    shown = "none" if low <= NO_WATERMARK else time.strftime("%H:%M:%S", time.localtime(low / 1000))
    subtasks = "  ".join(f"#{i}:{lag(wm)}" for i, wm in per_subtask)
    print(f"{v['name'][:50]:<50} {kind:<6} {shown:<9} lag per subtask  {subtasks}")
