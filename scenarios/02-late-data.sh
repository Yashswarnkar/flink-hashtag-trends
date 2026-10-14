#!/usr/bin/env bash
# Scenario 02: event time with messy data.
#
# Every post is backdated by up to 3 s (jitter) and 2% are 90 s late (offline phones).
# Watch:  - the watermark trailing the wall clock by ~the out-of-orderness bound (5 s)
#         - a window appearing in Redis only once the watermark passes its end
#         - 90 s-late posts ending up in the side output (late:events), not in any window
# Also open http://localhost:8081 → the job → window-count → Watermarks tab.
set -euo pipefail
cd "$(dirname "$0")/.."
export DOCKER_HOST="unix://$HOME/.lima/flink-lab/sock/docker.sock"
redis() { docker exec hashtags-redis-1 redis-cli "$@"; }

DURATION=${DURATION:-90}

echo "▶ clean slate + submit job (window 60s/10s, out-of-orderness 5s, allowed lateness 10s)"
./infra/scripts/reset.sh >/dev/null
./infra/scripts/job.sh submit 2

echo "▶ generator: 1000 posts/s for ${DURATION}s, jitter 3s, 2% of posts 90s late"
./generator/bin/generator -rate 1000 -duration "${DURATION}s" \
  -out-of-order 3s -late-fraction 0.02 -late 90s -truth-out /tmp/hashtags-truth.json \
  > /tmp/scenario-02-generator.log 2>&1 &
gen=$!
trap 'kill $gen 2>/dev/null || true' EXIT

for ((t = 10; t <= DURATION; t += 10)); do
  sleep 10
  wm=$(./infra/scripts/watermarks.py | grep '^window-count' | sed -E 's/.*input +([^ ]+) .*#0:([^ ]+).*/watermark \1 (lag \2)/' || true)
  # `|| true`: with pipefail, `head` closing the pipe early would otherwise abort the script.
  newest=$(./infra/scripts/trending.sh 60s 1 | head -1 | awk '{print $4}' || true)
  printf '%s  t=%3ss  %-32s  newest window ends %-8s  late events %s\n' \
    "$(date +%T)" "$t" "$wm" "${newest:-none}" "$(redis XLEN late:events)"
done
wait $gen || true

echo; echo "▶ top of the newest window"; ./infra/scripts/trending.sh 60s 5
echo; echo "▶ late per tag (top 5)"; redis HGETALL stats:late | paste - - | sort -k2 -nr | head -5 || true
echo; echo "▶ one late event"; redis XREVRANGE late:events + - COUNT 1 | paste - - - - - - - - - | cut -f3- || true
