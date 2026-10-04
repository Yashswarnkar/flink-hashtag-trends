#!/usr/bin/env python3
"""Compare Flink's totals in Redis (hashtag:totals) with the generator's ground truth file.

Usage: verify-totals.py TRUTH_JSON
Exit code 0 when every tag matches exactly.
"""
import json
import subprocess
import sys

truth = json.load(open(sys.argv[1]))
out = subprocess.run(
    ["docker", "compose", "-f", "infra/docker-compose.yml", "-p", "hashtags",
     "exec", "-T", "redis", "redis-cli", "--no-raw", "HGETALL", "hashtag:totals"],
    check=True, capture_output=True, text=True).stdout
# --no-raw prints numbered lines: 1) "tag" 2) "count" ...
values = [line.split(") ", 1)[1].strip('"') for line in out.splitlines() if ") " in line]
redis = {values[i]: int(values[i + 1]) for i in range(0, len(values), 2)}

missing = sorted(set(truth) - set(redis))
extra = sorted(set(redis) - set(truth))
wrong = sorted(t for t in set(truth) & set(redis) if truth[t] != redis[t])

print(f"tags in truth: {len(truth)}   tags in redis: {len(redis)}")
print(f"posts-with-tag total: truth={sum(truth.values())} redis={sum(redis.values())}")
for t in wrong[:10]:
    print(f"  MISMATCH {t}: truth={truth[t]} redis={redis[t]}")
for t in missing[:10]:
    print(f"  MISSING  {t}: truth={truth[t]}")
for t in extra[:10]:
    print(f"  EXTRA    {t}: redis={redis[t]}")
if missing or extra or wrong:
    print("❌ totals differ (is the job still catching up? was the state reset before generating?)")
    sys.exit(1)
print("✅ every tag matches exactly")
