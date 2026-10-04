#!/usr/bin/env bash
# Checks that every service is reachable from the Mac, then runs Flink's bundled
# StateMachineExample (a keyed, stateful streaming job) so there is something
# to look at in the UI.
#
#   ./infra/scripts/smoke.sh          check + start the example job
#   ./infra/scripts/smoke.sh stop     cancel all running jobs
set -euo pipefail

FLINK=http://localhost:8081
COMPOSE=(docker compose -f "$(dirname "$0")/../docker-compose.yml" -p hashtags)

ok()   { printf '  \033[32m✔\033[0m %s\n' "$*"; }
fail() { printf '  \033[31m✘\033[0m %s\n' "$*"; exit 1; }

# Every job that is not finished yet: RUNNING, but also RESTARTING/CREATED ones that are
# stuck waiting for slots (e.g. parallelism higher than the cluster has).
active_jobs() {
  curl -fs "$FLINK/jobs/overview" | python3 -c \
    'import json,sys; [print(j["jid"]) for j in json.load(sys.stdin)["jobs"] if j["state"] not in ("FINISHED","CANCELED","FAILED")]'
}

job_state() {
  curl -fs "$FLINK/jobs/$1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])'
}

if [[ "${1:-}" == "stop" ]]; then
  for jid in $(active_jobs); do
    curl -fs -X PATCH "$FLINK/jobs/$jid?mode=cancel" >/dev/null && ok "cancelled $jid"
  done
  exit 0
fi

echo "Services"
overview=$(curl -fs "$FLINK/overview") || fail "Flink REST not reachable on $FLINK"
read -r tms slots < <(python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(d["taskmanagers"], d["slots-total"])' "$overview")
[[ "$tms" == 2 ]] || fail "expected 2 TaskManagers, found $tms"
ok "Flink: $tms TaskManagers, $slots slots"
nc -z localhost 29092 2>/dev/null && ok "Kafka reachable on localhost:29092" || fail "Kafka not reachable"
[[ "$("${COMPOSE[@]}" exec -T redis redis-cli ping)" == PONG ]] && ok "Redis PONG" || fail "Redis not responding"

echo "Example job"
[[ -z "$(active_jobs)" ]] || fail "other jobs are still active and hold slots; run: make smoke-stop"
jid=$("${COMPOSE[@]}" exec -T jobmanager flink run -d -p 4 /opt/flink/examples/streaming/StateMachineExample.jar 2>/dev/null \
  | sed -n 's/.*JobID \([0-9a-f]*\).*/\1/p')
[[ -n "$jid" ]] || fail "job submission failed"
for _ in $(seq 1 30); do
  [[ "$(job_state "$jid")" == RUNNING ]] && break
  sleep 1
done
[[ "$(job_state "$jid")" == RUNNING ]] || fail "job $jid did not reach RUNNING"
ok "StateMachineExample RUNNING with parallelism 4"
echo
echo "  Open $FLINK/#/job/running/$jid/overview"
echo "  Stop it with: make smoke-stop"
