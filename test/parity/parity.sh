#!/usr/bin/env bash
# Sends the same corpus through the reference collector and the candidate, then
# compares what each wrote to its ClickHouse, table by table.
#   parity.sh <out_dir>
set -euo pipefail
out=$(realpath -m "${1:-out}")
cd "$(dirname "$0")"

tables=(signoz_index_v3 signoz_error_index_v2 traces_v3_resource tag_attributes_v2 span_attributes_keys
  dependency_graph_minutes_v2 top_level_operations trace_summary)

ch() {
  local body code
  body=$(curl -s -w '\n%{http_code}' --data-binary "$2" "http://localhost:$1/")
  code=${body##*$'\n'}
  body=${body%$'\n'*}
  body=${body%$'\n'}
  if [ "$code" != 200 ]; then echo "clickhouse :$1 $code for: $2"$'\n'"$body" >&2; return 1; fi
  [ -n "$body" ] && printf '%s\n' "$body"; return 0
}

docker compose up -d --build reference-collector candidate-collector
for port in 13133 23133/health/status; do
  for _ in $(seq 120); do curl -sf "localhost:$port" >/dev/null && break; sleep 2; done
  curl -sf "localhost:$port" >/dev/null
done

t0=$(( $(date +%s) * 1000000000 ))
rm -rf "$out" && mkdir -p "$out/reference" "$out/candidate"
python3 -I ../observe/corpus.py "$t0" "$out/corpus"
for port in 14318 24318; do
  code=$(curl -s -o "$out/response" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
    --data-binary @"$out/corpus/traces.json" "localhost:$port/v1/traces")
  if [ "$code" != 200 ]; then echo "POST :$port/v1/traces: $code $(cat "$out/response")" >&2; exit 1; fi
done
rm -f "$out/response"
sleep 20

for side in reference:18123 candidate:28123; do
  name=${side%%:*} port=${side##*:}
  for t in "${tables[@]}"; do
    ch "$port" "SELECT * FROM signoz_traces.$t FORMAT JSONEachRow" > "$out/$name/$t.jsonl"
  done
done
python3 -I compare.py "$out" "${tables[@]}"
