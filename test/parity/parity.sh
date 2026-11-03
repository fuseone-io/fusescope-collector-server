#!/usr/bin/env bash
# Sends the same corpus through the reference collector and the candidate, then
# compares what each wrote to its ClickHouse, table by table.
#   parity.sh <out_dir>
# PARITY_{REFERENCE,CANDIDATE}_{OTLP,HEALTH,CLICKHOUSE}_PORT move the host ports
# (compose.yaml), COMPOSE_PROJECT_NAME the project.
set -euo pipefail
out=$(realpath -m "${1:-out}")
cd "$(dirname "$0")"
export PARITY_REFERENCE_OTLP_PORT=${PARITY_REFERENCE_OTLP_PORT:-14318}
export PARITY_CANDIDATE_OTLP_PORT=${PARITY_CANDIDATE_OTLP_PORT:-24318}
export PARITY_REFERENCE_HEALTH_PORT=${PARITY_REFERENCE_HEALTH_PORT:-13133}
export PARITY_CANDIDATE_HEALTH_PORT=${PARITY_CANDIDATE_HEALTH_PORT:-23133}
export PARITY_REFERENCE_CLICKHOUSE_PORT=${PARITY_REFERENCE_CLICKHOUSE_PORT:-18123}
export PARITY_CANDIDATE_CLICKHOUSE_PORT=${PARITY_CANDIDATE_CLICKHOUSE_PORT:-28123}
ref_otlp=$PARITY_REFERENCE_OTLP_PORT cand_otlp=$PARITY_CANDIDATE_OTLP_PORT
ref_ch=$PARITY_REFERENCE_CLICKHOUSE_PORT cand_ch=$PARITY_CANDIDATE_CLICKHOUSE_PORT

# Both collectors must run the same managed processors.
managed() { sed -n '/# >>> managed processors/,/# <<< managed processors/p' "$1"; }
if [ -z "$(managed reference-collector.yaml)" ] || ! diff <(managed reference-collector.yaml) <(managed candidate-collector.yaml) >&2; then
  echo "parity: the managed processors of reference-collector.yaml and candidate-collector.yaml differ" >&2
  exit 1
fi

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
for port in "$PARITY_REFERENCE_HEALTH_PORT" "$PARITY_CANDIDATE_HEALTH_PORT/health/status"; do
  for _ in $(seq 120); do curl -sf "localhost:$port" >/dev/null && break; sleep 2; done
  curl -sf "localhost:$port" >/dev/null || { echo "health localhost:$port: $(curl -s -w ' %{http_code}' "localhost:$port")" >&2; exit 1; }
done

t0=$(( $(date +%s) * 1000000000 ))
rm -rf "$out" && mkdir -p "$out/reference" "$out/candidate"
python3 -I ../observe/corpus.py "$t0" "$out/corpus"
python3 -I managed_corpus.py "$t0" "$out/corpus"
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
  # Spans of an already flushed series and minute, after both span-metrics flushes (10s).
  sleep 12
  for port in "$@"; do
    code=$(curl -s -o "$out/response" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
      --data-binary @"$out/corpus/traces-late.json" "localhost:$port/v1/traces")
    if [ "$code" != 200 ]; then echo "POST :$port/v1/traces (late): $code $(cat "$out/response")" >&2; exit 1; fi
  done
  rm -f "$out/response"
}
send_corpus "$ref_otlp" "$cand_otlp"
sleep 20

for side in "reference:$ref_ch" "candidate:$cand_ch"; do
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

# Sending equal telemetry again is a new request, not a retry: both collectors
# must write it again. Retries of one request are deduplicated (unit tests of the
# exporter); MergeTree tables are counted, as Replacing ones merge repeats.
merge_tables=(signoz_traces.signoz_index_v3 signoz_traces.signoz_error_index_v2 signoz_logs.logs_v2 signoz_metrics.samples_v4)
counts() { for t in "${merge_tables[@]}"; do echo "$t $(ch "$1" "SELECT count() FROM $t")"; done; }
for port in "$ref_ch" "$cand_ch"; do counts $port > "$out/counts-before-$port.txt"; done
send_corpus "$ref_otlp" "$cand_otlp"
sleep 20
for port in "$ref_ch" "$cand_ch"; do
  counts $port > "$out/counts-after-$port.txt"
  if ! awk 'NR == FNR { before[$1] = $2; next } $2 != 2 * before[$1] { print "resend: " FILENAME ": " $1 " has " $2 " rows, want " 2 * before[$1]; bad = 1 } END { exit bad }' \
      "$out/counts-before-$port.txt" "$out/counts-after-$port.txt" >&2; then
    exit 1
  fi
done
