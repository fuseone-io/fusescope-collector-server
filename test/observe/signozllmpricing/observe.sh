#!/usr/bin/env bash
# Runs the reference collector as a black box once per case of cases.py: starts
# it with that case's signozllmpricing configuration, records whether it starts,
# sends the corpus and records what the processor passed on (file exporter).
# Nothing here reads the collector's source (ADR 0002).
#   observe.sh <out_dir>      # needs Docker, curl, python3
set -euo pipefail
out=$(realpath -m "${1:-out}")
cd "$(dirname "$0")"
export COMPOSE_PROJECT_NAME=observe-llmpricing
work=$(mktemp -d)
trap 'docker compose down -t 1 >/dev/null 2>&1 || true; rm -rf "$work"' EXIT

rm -rf "$out" && mkdir -p "$out/outputs" "$out/logs"
python3 -I cases.py "$out"

# Log lines worth keeping, without timestamps and instance IDs.
clean_log() {
  grep -iE '"level":"(warn|error|fatal)"|[[:space:]](warn|error|fatal)[[:space:]]|^Error|panic' \
    | grep -v 'opamp' \
    | sed -E 's/[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z//g; s/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/<uuid>/g; s/"timestamp":"[^"]*",?//g; s/,"stacktrace":.*$/}/; s/"NumCPU": [0-9]+/"NumCPU": <n>/g' \
    || true
}

results="$out/results.yaml"
echo "reference_image: ghcr.io/fuseone-io/fusescope-otel-collector:${REFERENCE_VERSION:-0.145.0}" > "$results"
echo "cases:" >> "$results"
for cfg in "$out"/configs/*.yaml; do
  name=$(basename "$cfg" .yaml)
  case_out="$work/$name" && mkdir -p "$case_out" && chmod 777 "$case_out"
  export CASE_CONFIG="$cfg" CASE_OUT="$case_out"
  docker compose up -d --force-recreate reference >/dev/null 2>&1
  state=refused
  for _ in $(seq 40); do
    if curl -sf localhost:24433 >/dev/null; then state=started; break; fi
    [ "$(docker compose ps -a --format '{{.State}}' reference)" = exited ] && break
    sleep 0.5
  done
  echo "  $name:" >> "$results"
  echo "    start: $state" >> "$results"
  if [ "$state" = started ]; then
    for signal in traces logs metrics; do
      grep -q "\"$signal\"" "$cfg" || continue
      code=$(curl -s -o "$work/response" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
        --data-binary @"$out/corpus/$signal.json" "localhost:24418/v1/$signal")
      echo "    post_$signal: $code $(cat "$work/response")" >> "$results"
    done
    sleep 3
    docker compose stop -t 5 reference >/dev/null 2>&1
    [ -s "$case_out/output.jsonl" ] && cp "$case_out/output.jsonl" "$out/outputs/$name.jsonl"
  fi
  docker compose logs --no-color --no-log-prefix reference 2>&1 | clean_log > "$out/logs/$name.log"
  [ -s "$out/logs/$name.log" ] || rm "$out/logs/$name.log"
  docker compose down -t 1 >/dev/null 2>&1
done
echo "observed $(grep -c 'start:' "$results") cases"
