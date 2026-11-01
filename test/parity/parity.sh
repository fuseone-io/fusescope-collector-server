#!/usr/bin/env bash
# Sends the same corpus through the reference collector and the candidate, then
# compares what each wrote to its ClickHouse, table by table.
#   parity.sh <out_dir>
set -euo pipefail
out=$(realpath -m "${1:-out}")
cd "$(dirname "$0")"

tables=(signoz_traces.signoz_index_v3 signoz_traces.signoz_error_index_v2 signoz_traces.traces_v3_resource
  signoz_traces.tag_attributes_v2 signoz_traces.span_attributes_keys signoz_traces.dependency_graph_minutes_v2
  signoz_traces.top_level_operations signoz_traces.trace_summary
  signoz_logs.logs_v2 signoz_logs.logs_v2_resource signoz_logs.tag_attributes_v2
  signoz_logs.logs_attribute_keys signoz_logs.logs_resource_keys
  signoz_metrics.time_series_v4 signoz_metrics.samples_v4 signoz_metrics.metadata
  signoz_metrics.samples_v4_agg_5m signoz_metrics.samples_v4_agg_30m
  signoz_metrics.time_series_v4_6hrs signoz_metrics.time_series_v4_1day signoz_metrics.time_series_v4_1week)

ch() {
  local body code
  body=$(curl -s -w '\n%{http_code}' --data-binary "$2" "http://localhost:$1/")
  code=${body##*$'\n'}
  body=${body%$'\n'*}
  body=${body%$'\n'}
  if [ "$code" != 200 ]; then echo "clickhouse :$1 $code for: $2"$'\n'"$body" >&2; return 1; fi
  [ -n "$body" ] && printf '%s\n' "$body"; return 0
}

# Start from empty databases, also when a previous local run left containers up.
docker compose down -v --remove-orphans
docker compose up -d --build reference-collector candidate-collector
for port in 13133 23133/health/status; do
  for _ in $(seq 120); do curl -sf "localhost:$port" >/dev/null && break; sleep 2; done
  curl -sf "localhost:$port" >/dev/null || { echo "health localhost:$port: $(curl -s -w ' %{http_code}' "localhost:$port")" >&2; exit 1; }
done

t0=$(( $(date +%s) * 1000000000 ))
rm -rf "$out" && mkdir -p "$out/reference" "$out/candidate"
python3 -I ../observe/corpus.py "$t0" "$out/corpus"
# send_corpus PORT... posts the whole corpus. The protobuf probes go 3s after the
# JSON, past both collectors' 1s flush: a batch holding both would let the probes'
# tag values dedupe against older spans (traces spec §4) on one side only. Each
# call thus produces the same batches, and a second call resends them.
send_corpus() {
  local port signal code
  for port in "$@"; do
    for signal in traces logs metrics; do
      code=$(curl -s -o "$out/response" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        --data-binary @"$out/corpus/$signal.json" "localhost:$port/v1/$signal")
      if [ "$code" != 200 ]; then echo "POST :$port/v1/$signal: $code $(cat "$out/response")" >&2; exit 1; fi
    done
  done
  sleep 3
  for port in "$@"; do
    for signal in traces logs; do
      code=$(curl -s -o "$out/response" -w '%{http_code}' -X POST -H 'Content-Type: application/x-protobuf' \
        --data-binary @"$out/corpus/$signal-invalid-utf8.pb" "localhost:$port/v1/$signal")
      if [ "$code" != 200 ]; then echo "POST :$port/v1/$signal (invalid UTF-8): $code $(cat "$out/response")" >&2; exit 1; fi
    done
  done
  rm -f "$out/response"
}
send_corpus 14318 24318
sleep 20

for side in reference:18123 candidate:28123; do
  name=${side%%:*} port=${side##*:}
  for t in "${tables[@]}"; do
    # Rows a ReplacingMergeTree merges away are compared merged: the reference also
    # skips keys it wrote in an earlier batch, the candidate repeats them.
    engine=$(ch "$port" "SELECT engine FROM system.tables WHERE database = '${t%%.*}' AND name = '${t#*.}'")
    final=; case $engine in *ReplacingMergeTree) final=FINAL;; esac
    ch "$port" "SELECT * FROM $t $final FORMAT JSONEachRow" > "$out/$name/$t.jsonl"
  done
done
python3 -I compare.py "$out" "${tables[@]}" || { echo "parity: differences, see report" >&2; exit 1; }

# A retry resends the same batch: the deduplication token must keep every table,
# including materialized-view targets, unchanged.
counts() { for t in "${tables[@]}"; do echo "$t $(ch 28123 "SELECT count() FROM $t")"; done; }
counts > "$out/candidate-counts-before.txt"
send_corpus 24318
sleep 20
counts > "$out/candidate-counts-after.txt"
if ! diff "$out/candidate-counts-before.txt" "$out/candidate-counts-after.txt" >&2; then
  echo "retry: resending the same batch added rows" >&2
  exit 1
fi
