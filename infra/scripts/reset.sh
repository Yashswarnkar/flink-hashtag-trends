#!/usr/bin/env bash
# Start from a clean slate: no jobs, an empty `posts` topic, no consumer offsets, empty Redis.
# Needed before comparing Redis against a generator's --truth-out file.
set -euo pipefail
cd "$(dirname "$0")/../.."

COMPOSE=(docker compose -f infra/docker-compose.yml -p hashtags)
KAFKA=(/opt/kafka/bin)

./infra/scripts/job.sh cancel
sleep 2

"${COMPOSE[@]}" exec -T kafka "${KAFKA[0]}/kafka-topics.sh" --bootstrap-server kafka:9092 \
  --delete --topic posts --if-exists
# Topic deletion is asynchronous; wait until it is really gone before re-creating it.
for _ in $(seq 1 30); do
  "${COMPOSE[@]}" exec -T kafka "${KAFKA[0]}/kafka-topics.sh" --bootstrap-server kafka:9092 --list \
    | grep -qx posts || break
  sleep 1
done
"${COMPOSE[@]}" exec -T kafka "${KAFKA[0]}/kafka-consumer-groups.sh" --bootstrap-server kafka:9092 \
  --delete --group hashtag-trends >/dev/null 2>&1 || true
make --no-print-directory topics >/dev/null
echo "topic posts recreated, consumer group offsets removed"

# Redis holds nothing but this job's results.
"${COMPOSE[@]}" exec -T redis redis-cli FLUSHDB >/dev/null
echo "redis cleared"
