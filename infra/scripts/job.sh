#!/usr/bin/env bash
# Manage the hashtag-trends job through Flink's REST API (the same API the UI uses).
#
#   job.sh submit [parallelism]   upload the fat jar and start the job
#   job.sh cancel                 cancel every unfinished job
#   job.sh list                   show jobs and their state
set -euo pipefail

FLINK=http://localhost:8081
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
JAR="$ROOT/flink/hashtag-job/target/hashtag-job.jar"
JOB_NAME=hashtag-trends

json() { python3 -c "import json,sys; d=json.load(sys.stdin); $1"; }

active_jobs() {
  curl -fs "$FLINK/jobs/overview" \
    | json '[print(j["jid"], j["name"]) for j in d["jobs"] if j["state"] not in ("FINISHED","CANCELED","FAILED")]'
}

case "${1:-}" in
  submit)
    parallelism="${2:-2}"
    [[ -f "$JAR" ]] || { echo "no jar at $JAR; run: make build" >&2; exit 1; }
    if active_jobs | grep -qw "$JOB_NAME"; then
      echo "$JOB_NAME is already active; run: make job-cancel" >&2; exit 1
    fi
    # Remove jars uploaded earlier so the JobManager's upload dir doesn't grow forever.
    for id in $(curl -fs "$FLINK/jars" | json '[print(f["id"]) for f in d["files"]]'); do
      curl -fs -X DELETE "$FLINK/jars/$id" >/dev/null
    done
    jar_id=$(curl -fs -X POST -H "Expect:" -F "jarfile=@$JAR" "$FLINK/jars/upload" \
      | json 'print(d["filename"].rsplit("/",1)[-1])')
    job_id=$(curl -fs -X POST "$FLINK/jars/$jar_id/run" -H 'Content-Type: application/json' \
      -d "{\"parallelism\": $parallelism}" | json 'print(d["jobid"])')
    echo "submitted $JOB_NAME (parallelism $parallelism): $FLINK/#/job/running/$job_id/overview"
    ;;
  cancel)
    active_jobs | while read -r jid name; do
      curl -fs -X PATCH "$FLINK/jobs/$jid?mode=cancel" >/dev/null && echo "cancelled $name ($jid)"
    done
    ;;
  list)
    curl -fs "$FLINK/jobs/overview" | json '[print("%-11s %-20s %s" % (j["state"], j["name"], j["jid"])) for j in d["jobs"]]'
    ;;
  *)
    sed -n '2,7p' "$0"; exit 1 ;;
esac
