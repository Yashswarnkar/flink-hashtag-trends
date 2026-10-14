#!/usr/bin/env bash
# Print the top hashtags of the newest window Flink has written to Redis.
#   trending.sh [label] [k]     label defaults to 60s, k to 10
set -euo pipefail
cd "$(dirname "$0")/../.."

label="${1:-60s}"
k="${2:-10}"
redis() { docker compose -f infra/docker-compose.yml -p hashtags exec -T redis redis-cli "$@"; }

end=$(redis ZREVRANGE "trending:$label:windows" 0 0)
if [[ -z "$end" ]]; then
  echo "no windows yet (has the watermark passed the end of the first window?)"
  exit 0
fi
size_s=${label%s}
fmt() { python3 -c 'import sys,datetime; print(datetime.datetime.fromtimestamp(int(sys.argv[1])/1000).strftime("%H:%M:%S"))' "$1"; }
echo "window $(fmt $((end - size_s * 1000))) → $(fmt "$end")  (key trending:$label:$end)"
redis ZREVRANGE "trending:$label:$end" 0 $((k - 1)) WITHSCORES | paste - - | awk '{printf "  %-14s %s\n", $1, $2}'
