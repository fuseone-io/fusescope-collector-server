#!/usr/bin/env bash
# Runs the reference collector as a black box with signozspanmapper configs,
# sends each case's spans and records what the file exporter wrote, then starts
# it with each startup config to see which ones it refuses.
#   observe.sh <out_dir>
set -euo pipefail
out=$(realpath -m "${1:-out}")
here=$(cd "$(dirname "$0")" && pwd)
# Under the harness, not /tmp: the Docker daemon may not see the caller's /tmp.
work=$here/.work
rm -rf "$work"
mkdir -p "$work"
compose() { docker compose -f "$here/compose.yaml" "$@"; }
cleanup() {
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT
export OBSERVE_DIR=$work

python3 -I "$here/cases.py" generate "$work"
mkdir -p "$work/out"
chmod 0777 "$work/out"

compose up -d reference-collector
for _ in $(seq 60); do curl -sf localhost:14133 >/dev/null && break; sleep 1; done
curl -sf localhost:14133 >/dev/null
for f in "$work"/requests/*.json; do
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
    --data-binary @"$f" localhost:14318/v1/traces)
  if [ "$code" != 200 ]; then echo "POST $(basename "$f"): $code" >&2; exit 1; fi
done
sleep 3
compose stop reference-collector >/dev/null
python3 -I "$here/cases.py" collect "$work"

# A config is accepted when the collector logs that it is ready, refused when it exits first.
: > "$work/startup.jsonl"
for f in "$work"/startup/*.yaml; do
  name=$(basename "$f" .yaml)
  export STARTUP_CASE=$name
  compose up -d startup >/dev/null 2>&1
  id=$(compose ps -aq startup)
  result=timeout
  for _ in $(seq 30); do
    if docker logs "$id" 2>&1 | grep -q 'Everything is ready'; then result=accepted; break; fi
    if [ "$(docker inspect -f '{{.State.Status}}' "$id")" = exited ]; then result=refused; break; fi
    sleep 1
  done
  docker logs "$id" > "$work/startup.log" 2>&1
  python3 -I "$here/cases.py" startup-line "$name" "$result" "$work/startup.log" >> "$work/startup.jsonl"
  compose rm -sf startup >/dev/null 2>&1
done

rm -rf "$out"
mkdir -p "$out"
cp "$work/cases.json" "$work/outputs.json" "$work/startup.jsonl" "$out/"
echo "reference_image: ghcr.io/fuseone-io/fusescope-otel-collector:${REFERENCE_VERSION:-0.145.0}" > "$out/run.yaml"
echo "observed into $out"
