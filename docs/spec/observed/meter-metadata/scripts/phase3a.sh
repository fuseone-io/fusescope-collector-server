#!/usr/bin/env bash
X() { kubectl --context kind-fs-observe -n fs "$@"; }
CH() { X exec chi-fs-clickhouse-cluster-0-0-0 -c clickhouse -- clickhouse-client -q "$1" 2>/dev/null; }
TG=ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:v0.139.0; EP=fs-fusescope-otel-collector:4317
run() { X run "$@" >/dev/null; }
echo "== window probe at $(date -u +%T)"
run w21-traces --restart=Never --image=$TG -- traces --otlp-endpoint=$EP --otlp-insecure --rate=5 --traces=10 --service=obs-w21 --otlp-attributes='deployment.environment="obs"'
# no resource attributes at all: one log and one span over OTLP/HTTP JSON
NOW=$(date +%s)000000000
LOG='{"resourceLogs":[{"resource":{},"scopeLogs":[{"logRecords":[{"timeUnixNano":"'$NOW'","body":{"stringValue":"obs-nosvc"},"attributes":[{"key":"marker","value":{"stringValue":"obs-nosvc"}}]}]}]}]}'
SPAN='{"resourceSpans":[{"resource":{},"scopeSpans":[{"spans":[{"traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174","name":"obs-nosvc","kind":1,"startTimeUnixNano":"'$NOW'","endTimeUnixNano":"'$NOW'","attributes":[{"key":"marker","value":{"stringValue":"obs-nosvc"}}]}]}]}]}'
run w21-nosvc --restart=Never --image=curlimages/curl:8.10.1 --command -- sh -c "curl -sS -o /dev/null -w 'logs %{http_code} ' -H 'Content-Type: application/json' -d '$LOG' http://fs-fusescope-otel-collector:4318/v1/logs; curl -sS -o /dev/null -w 'traces %{http_code}\n' -H 'Content-Type: application/json' -d '$SPAN' http://fs-fusescope-otel-collector:4318/v1/traces"
for i in $(seq 60); do s=$(X get pods w21-traces w21-nosvc -o jsonpath='{.items[*].status.phase}' 2>/dev/null); case "$s" in *Pending*|*Running*) sleep 3;; *) break;; esac; done; echo "  senders: $s; $(X logs w21-nosvc)"
sleep 10
CH "SELECT data_source, toDateTime(intDiv(unix_milli,1000),'UTC') t, toString(resource_attributes) r, toString(attributes) a FROM signoz_metadata.distributed_attributes_metadata WHERE resource_attributes['service.name']='obs-w21' OR attributes['marker']='obs-nosvc' OR has(mapValues(attributes),'obs-nosvc') FORMAT TSV"
