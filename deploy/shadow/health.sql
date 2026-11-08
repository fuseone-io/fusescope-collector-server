-- fusescope-collector's own telemetry over the last 10 minutes (CUTOVER.md §4, "Health thresholds"):
-- the increase of each refusal, failure and insert error counter, and the latest queue size and
-- capacity (all exporters together; queues.sql per exporter) and memory. Read-only:
--   docker exec -i fusescope-demo-clickhouse-1 clickhouse-client --readonly 1 < shadow/health.sql
-- A counter absent from the output has not been reported in the window, which for the
-- send_failed and enqueue_failed counters means none.
SELECT metric_name, round(sum(v), 2) AS value
FROM (
  SELECT s.metric_name AS metric_name, s.fingerprint,
         if(s.metric_name IN ('otelcol_exporter_queue_size', 'otelcol_exporter_queue_capacity', 'otelcol_process_memory_rss'),
            argMax(s.value, s.unix_milli), max(s.value) - min(s.value)) AS v
  FROM signoz_metrics.samples_v4 AS s
  WHERE s.unix_milli >= (toUnixTimestamp(now()) - 600) * 1000
    AND (s.metric_name LIKE 'otelcol\_receiver\_refused\_%' OR s.metric_name LIKE 'otelcol\_receiver\_failed\_%'
         OR s.metric_name LIKE 'otelcol\_exporter\_send\_failed\_%' OR s.metric_name LIKE 'otelcol\_exporter\_enqueue\_failed\_%'
         OR s.metric_name = 'otelcol_exporter_fusescope_clickhouse_insert_errors'
         OR s.metric_name = 'otelcol_receiver_rum_requests_refused'
         OR s.metric_name IN ('otelcol_exporter_queue_size', 'otelcol_exporter_queue_capacity', 'otelcol_process_memory_rss'))
    AND s.fingerprint IN (
      SELECT fingerprint FROM signoz_metrics.time_series_v4
      WHERE JSONExtractString(labels, 'service.name') = 'fusescope-collector'
        AND unix_milli >= (toUnixTimestamp(now()) - 7200) * 1000)
  GROUP BY s.metric_name, s.fingerprint)
GROUP BY metric_name ORDER BY metric_name
