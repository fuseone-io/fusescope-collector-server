#!/usr/bin/env bash
X() { kubectl --context kind-fs-observe -n fs "$@"; }
CH() { X exec chi-fs-clickhouse-cluster-0-0-0 -c clickhouse -- clickhouse-client -q "$1" 2>/dev/null; }
TG=ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:v0.139.0; EP=fs-fusescope-otel-collector:4317
until [ "$(date -u +%H%M)" = 2212 ]; do sleep 20; done
echo "== meter at $(date -u +%T) for rows without service.name"
CH "SELECT metric_name, toDateTime(intDiv(unix_milli,1000),'UTC') t, value, labels FROM signoz_meter.distributed_samples WHERE unix_milli > toUnixTimestamp('2026-11-05 21:30:00','UTC')*1000 ORDER BY t, metric_name FORMAT TSV"
until [ "$(date -u +%H)" = 00 ] && [ "$(date -u +%M)" -ge 01 ]; do sleep 30; done
echo "== identical obs-w21 set again at $(date -u +%T), after the 00:00 boundary"
X run w00-traces --restart=Never --image=$TG -- traces --otlp-endpoint=$EP --otlp-insecure --rate=5 --traces=10 --service=obs-w21 --otlp-attributes='deployment.environment="obs"' >/dev/null
for i in $(seq 60); do s=$(X get pod w00-traces -o jsonpath='{.status.phase}'); case "$s" in Pending|Running) sleep 3;; *) break;; esac; done; echo "  sender: $s"
sleep 10
CH "SELECT toDateTime(intDiv(unix_milli,1000),'UTC') t, attributes['name'] n, count() FROM signoz_metadata.distributed_attributes_metadata WHERE resource_attributes['service.name']='obs-w21' GROUP BY 1,2 ORDER BY 1,2 FORMAT TSV"
