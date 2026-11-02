#!/usr/bin/env bash
# Runs the reference collector as a black box, sends the corpus, and writes what
# ClickHouse holds afterwards: DDL for every table and the rows of every table
# that received data.
#   observe.sh <out_dir>
set -euo pipefail
out=$(realpath -m "${1:-out}")
cd "$(dirname "$0")"
ch() {
  local body code
  body=$(curl -s -w '\n%{http_code}' --data-binary "$1" "http://localhost:8123/")
  code=${body##*$'\n'}
  body=${body%$'\n'*}
  body=${body%$'\n'}
  if [ "$code" != 200 ]; then echo "clickhouse $code for: $1"$'\n'"$body" >&2; return 1; fi
  [ -n "$body" ] && printf '%s\n' "$body"; return 0
}

docker compose up -d reference-collector
for _ in $(seq 120); do curl -sf localhost:13133 >/dev/null && break; sleep 2; done
curl -sf localhost:13133 >/dev/null

t0=$(( $(date +%s) * 1000000000 ))
rm -rf "$out" && mkdir -p "$out/ddl" "$out/rows"
python3 -I corpus.py "$t0" "$out/corpus"
for signal in traces logs metrics; do
  code=$(curl -s -o "$out/$signal.response" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
    --data-binary @"$out/corpus/$signal.json" "localhost:4318/v1/$signal")
  if [ "$code" != 200 ]; then echo "POST /v1/$signal: $code $(cat "$out/$signal.response")" >&2; exit 1; fi
  rm "$out/$signal.response"
done
# The protobuf probes go 3s later, past the reference's 1s batch timeout, so they
# always form a second batch (golden tests replay them as one).
sleep 3
# Invalid UTF-8 may be refused; the status is part of the observation.
utf8_status=""
for signal in traces logs; do
  utf8_status+=" $signal=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/x-protobuf' \
    --data-binary @"$out/corpus/$signal-invalid-utf8.pb" "localhost:4318/v1/$signal")"
done
sleep 30

{
  echo "t0_unix_nanos: $t0"
  echo "reference_image: ghcr.io/fuseone-io/fusescope-otel-collector:${REFERENCE_VERSION:-0.145.0}"
  echo "clickhouse: $(ch 'SELECT version()')"
  echo "invalid_utf8_status:$utf8_status"
} > "$out/run.yaml"

ch "SYSTEM FLUSH LOGS" >/dev/null
ch "SELECT DISTINCT arrayStringConcat(tables, ',') AS tables, replaceRegexpAll(query, '(?is)\\s+(VALUES|FORMAT)\\s.*$', '') AS statement
    FROM system.query_log WHERE type = 'QueryFinish' AND query_kind = 'Insert' AND has(databases, 'system') = 0
    ORDER BY tables, statement FORMAT JSONEachRow" > "$out/inserts.jsonl"

ch "SELECT database, name, engine FROM system.tables WHERE database LIKE 'signoz%' ORDER BY database, name FORMAT TSV" \
  > "$out/tables.tsv"
while IFS=$'\t' read -r db table engine; do
  [ -n "$db" ] || continue
  ch "SHOW CREATE TABLE \`$db\`.\`$table\` FORMAT TSVRaw" > "$out/ddl/$db.$table.sql"
  case "$engine" in
    *MergeTree)
      rows=$(ch "SELECT count() FROM \`$db\`.\`$table\`")
      # Every row is recorded: a truncated table reads as rows the reference never wrote.
      if [ "$rows" -gt 100000 ]; then echo "$db.$table has $rows rows; shrink the corpus" >&2; exit 1; fi
      if [ "$rows" -gt 0 ]; then
        ch "SELECT * FROM \`$db\`.\`$table\` ORDER BY ALL FORMAT JSONEachRow" > "$out/rows/$db.$table.jsonl" 2>/dev/null \
          || ch "SELECT * FROM \`$db\`.\`$table\` FORMAT JSONEachRow" > "$out/rows/$db.$table.jsonl"
      fi
      ;;
  esac
done < "$out/tables.tsv"
echo "observed $(wc -l < "$out/tables.tsv") tables, $(ls "$out/rows" | wc -l) with rows"
