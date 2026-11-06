#!/usr/bin/env bash
X() { kubectl --context kind-fs-observe -n fs "$@"; }
CH() { X exec chi-fs-clickhouse-cluster-0-0-0 -c clickhouse -- clickhouse-client -q "$1" 2>/dev/null; }
TG=ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:v0.139.0; EP=fs-fusescope-otel-collector:4317
wait_pods() { for i in $(seq 60); do s=$(X get pods "$@" -o jsonpath='{.items[*].status.phase}' 2>/dev/null); case "$s" in *Pending*|*Running*) sleep 3;; *) break;; esac; done; echo "  senders: $s"; }
send() { # name signal service resource-attrs telemetry-attrs
  local args=(--otlp-endpoint=$EP --otlp-insecure --rate=5 --service=$3)
  [ -n "$4" ] && args+=(--otlp-attributes="$4"); [ -n "$5" ] && args+=(--telemetry-attributes="$5")
  case $2 in traces) args=(traces "${args[@]}" --traces=10);; logs) args=(logs "${args[@]}" --logs=10 --body="$1 line");; metrics) args=(metrics "${args[@]}" --metrics=10);; esac
  X run "$1" --restart=Never --image=$TG -- "${args[@]}" >/dev/null
}
until [ "$(date -u +%H%M)" -ge 2005 ]; do sleep 20; done
echo "== round A input at $(date -u +%T)"
# identical to the first input
send a-same-traces traces obs-traces 'deployment.environment="obs"' 'obs.key="tv"'
send a-same-logs logs obs-logs 'deployment.environment="obs"' 'obs.key="lv"'
send a-same-metrics metrics obs-metrics 'deployment.environment="obs"' 'obs.key="mv"'
# same service, new marker
send a-new-traces traces obs-traces 'deployment.environment="obs"' 'obs.key="tv-new"'
# missing dimensions: no deployment.environment, no data attributes
send a-bare-traces traces obs-bare '' ''
send a-bare-logs logs obs-bare-logs '' ''
wait_pods a-same-traces a-same-logs a-same-metrics a-new-traces a-bare-traces a-bare-logs
until [ "$(date -u +%H%M)" -ge 2010 ]; do sleep 20; done
echo "== read at $(date -u +%T) (collector started 19:02:22)"
echo "-- attributes_metadata, UTC"
CH "SELECT data_source, toDateTime(intDiv(unix_milli,1000),'UTC') t, resource_attributes, attributes, resource_fingerprint, attrs_fingerprint FROM signoz_metadata.distributed_attributes_metadata WHERE resource_attributes['service.name'] LIKE 'obs%' ORDER BY data_source, resource_attributes['service.name'], t, toString(attributes) FORMAT TSV"
echo "-- attributes_metadata, final (deduplicated) count by (source, service, hour): $(CH "SELECT count() FROM signoz_metadata.distributed_attributes_metadata FINAL WHERE resource_attributes['service.name'] LIKE 'obs%'") of $(CH "SELECT count() FROM signoz_metadata.distributed_attributes_metadata WHERE resource_attributes['service.name'] LIKE 'obs%'")"
echo "-- meter samples"
CH "SELECT metric_name, temporality, type, is_monotonic, unit, description, toDateTime(intDiv(unix_milli,1000),'UTC') t, value, labels FROM signoz_meter.distributed_samples ORDER BY metric_name, unix_milli FORMAT TSV"
echo "-- meter count: $(CH 'SELECT count() FROM signoz_meter.distributed_samples')"
# restart test: marked input, then restart before the next flush
echo "== restart-test input at $(date -u +%T)"
send r-traces traces obs-restart 'deployment.environment="obs"' 'obs.key="restart"'
send r-logs logs obs-restart-logs 'deployment.environment="obs"' 'obs.key="restart"'
wait_pods r-traces r-logs
sleep 30
X rollout restart deploy/fs-fusescope-otel-collector >/dev/null; X rollout status deploy/fs-fusescope-otel-collector --timeout=5m | tail -1
echo "== collector restarted, now started=$(X get pod -l app.kubernetes.io/component=otel-collector --field-selector=status.phase=Running -o jsonpath='{.items[0].status.containerStatuses[0].state.running.startedAt}')"
