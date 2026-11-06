#!/usr/bin/env bash
X() { kubectl --context kind-fs-observe -n fs "$@"; }
CH() { X exec chi-fs-clickhouse-cluster-0-0-0 -c clickhouse -- clickhouse-client -q "$1" 2>/dev/null; }
TG=ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:v0.139.0; EP=fs-fusescope-otel-collector:4317
run() { X run "$@" >/dev/null; }
echo "== control and probes at $(date -u +%T) (restarted 20:10:50)"
run c-control-traces --restart=Never --image=$TG -- traces --otlp-endpoint=$EP --otlp-insecure --rate=5 --traces=10 --service=obs-control --otlp-attributes='deployment.environment="obs"'
run c-control-logs --restart=Never --image=$TG -- logs --otlp-endpoint=$EP --otlp-insecure --rate=5 --logs=10 --service=obs-control-logs --body=control --otlp-attributes='deployment.environment="obs"'
# size probes: bodies of 10 and 1010 characters, 10 records each
run c-size10 --restart=Never --image=$TG -- logs --otlp-endpoint=$EP --otlp-insecure --rate=5 --logs=10 --service=obs-size-10 --body=$(printf 'x%.0s' $(seq 10))
run c-size1010 --restart=Never --image=$TG -- logs --otlp-endpoint=$EP --otlp-insecure --rate=5 --logs=10 --service=obs-size-1010 --body=$(printf 'x%.0s' $(seq 1010))
# points vs series: 3 workers send to 3 series? telemetrygen metrics: --workers sets parallel series
run c-series --restart=Never --image=$TG -- metrics --otlp-endpoint=$EP --otlp-insecure --rate=5 --metrics=10 --workers=3 --service=obs-series
for i in $(seq 60); do s=$(X get pods c-control-traces c-control-logs c-size10 c-size1010 c-series -o jsonpath='{.items[*].status.phase}' 2>/dev/null); case "$s" in *Pending*|*Running*) sleep 3;; *) break;; esac; done; echo "  senders: $s"
echo "-- metrics series sent by c-series (from the metrics table): $(CH "SELECT uniqExact(fingerprint) FROM signoz_metrics.distributed_time_series_v4 WHERE JSONExtractString(labels,'service.name')='obs-series'") series, $(CH "SELECT count() FROM signoz_metrics.distributed_samples_v4 s WHERE s.fingerprint IN (SELECT fingerprint FROM signoz_metrics.distributed_time_series_v4 WHERE JSONExtractString(labels,'service.name')='obs-series')") points"
until [ "$(date -u +%H%M)" -ge 2114 ]; do sleep 30; done
echo "== read at $(date -u +%T)"
CH "SELECT metric_name, toDateTime(intDiv(unix_milli,1000),'UTC') t, value, JSONExtractString(labels,'service.name') svc, JSONExtractString(labels,'deployment.environment') env, JSONExtractString(labels,'connector_id') cid FROM signoz_meter.distributed_samples ORDER BY t, svc, metric_name FORMAT TSV"
echo "-- other meter names: $(CH "SELECT groupUniqArray(metric_name) FROM signoz_meter.distributed_samples")"
