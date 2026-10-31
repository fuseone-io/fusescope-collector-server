#!/usr/bin/env bash
# Runs the reference collector as a black box, sends the corpus, and writes what
# ClickHouse holds afterwards: DDL for every table and the rows of every table
# that received data.
#   observe.sh <out_dir>
set -euo pipefail
cd "$(dirname "$0")"
out=$(realpath -m "${1:-out}")
ch() {
  local body code
  body=$(curl -s -w '\n%{http_code}' --data-binary "$1" "http://localhost:8123/")
  code=${body##*$'\n'}
  body=${body%$'\n'*}
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
sleep 30

{
  echo "t0_unix_nanos: $t0"
  echo "reference_image: ghcr.io/fuseone-io/fusescope-otel-collector:${REFERENCE_VERSION:-0.145.0}"
  echo "clickhouse: $(ch 'SELECT version()')"
} > "$out/run.yaml"

ch "SELECT database, name, engine FROM system.tables WHERE database LIKE 'signoz%' ORDER BY database, name FORMAT TSV" \
  > "$out/tables.tsv"
while IFS=$'\t' read -r db table engine; do
  [ -n "$db" ] || continue
  ch "SHOW CREATE TABLE \`$db\`.\`$table\` FORMAT TSVRaw" > "$out/ddl/$db.$table.sql"
  case "$engine" in
    *MergeTree)
      rows=$(ch "SELECT count() FROM \`$db\`.\`$table\`")
      if [ "$rows" -gt 0 ]; then
        ch "SELECT * FROM \`$db\`.\`$table\` ORDER BY ALL LIMIT 1000 FORMAT JSONEachRow" > "$out/rows/$db.$table.jsonl" 2>/dev/null \
          || ch "SELECT * FROM \`$db\`.\`$table\` LIMIT 1000 FORMAT JSONEachRow" > "$out/rows/$db.$table.jsonl"
      fi
      ;;
  esac
done < "$out/tables.tsv"
echo "observed $(wc -l < "$out/tables.tsv") tables, $(ls "$out/rows" | wc -l) with rows"
