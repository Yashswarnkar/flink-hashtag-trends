#!/usr/bin/env bash
# Scenario 02b: an empty Kafka partition freezes event time.
#
# The generator writes only to partitions 0,1,2 of 6. A Kafka source's watermark is the minimum
# over its partitions, and an empty partition has no watermark at all.
#   A) --idleness 0   : no watermark anywhere → no window ever fires, though data flows
#   B) --idleness 10s : empty partitions are declared idle after 10 s and ignored → windows fire
set -euo pipefail
cd "$(dirname "$0")/.."
export DOCKER_HOST="unix://$HOME/.lima/flink-lab/sock/docker.sock"
redis() { docker exec hashtags-redis-1 redis-cli "$@"; }

watch() {   # $1 = seconds to watch
  for ((t = 8; t <= $1; t += 8)); do
    sleep 8
    printf '  t=%2ss  windows written: %-3s %s\n' "$t" "$(redis ZCARD trending:60s:windows)" \
      "$(./infra/scripts/watermarks.py | grep '^window-count' | sed -E 's/.*input +/window-count watermark /' || true)"
  done
}

./infra/scripts/reset.sh >/dev/null

echo "▶ A) idleness disabled"
./infra/scripts/job.sh submit 2 --idleness 0 | head -1 || true
./generator/bin/generator -rate 1000 -duration 40s -partitions 0,1,2 >/dev/null 2>&1 &
watch 40; wait
docker compose -f infra/docker-compose.yml -p hashtags exec -T kafka \
  /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic posts | tr '\n' ' '; echo

echo; echo "▶ B) idleness 10s (job restarts and re-reads the same data)"
./infra/scripts/job.sh cancel >/dev/null; sleep 2
./infra/scripts/job.sh submit 2 --idleness 10s | head -1 || true
./generator/bin/generator -rate 1000 -duration 40s -partitions 0,1,2 >/dev/null 2>&1 &
watch 40; wait
