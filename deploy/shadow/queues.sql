-- fusescope-collector's exporter queues over the last 10 minutes, per exporter and signal: the
-- largest size, the capacity (batches for fusescope_clickhouse, bytes for otlp/*) and the share
-- used (CUTOVER.md, "Abort thresholds"). Read-only:
--   docker exec -i fusescope-demo-clickhouse-1 clickhouse-client --readonly 1 < shadow/queues.sql
SELECT JSONExtractString(ts.labels, 'exporter') AS exporter, JSONExtractString(ts.labels, 'data_type') AS signal,
       maxIf(s.value, s.metric_name = 'otelcol_exporter_queue_size') AS size,
       maxIf(s.value, s.metric_name = 'otelcol_exporter_queue_capacity') AS capacity,
       round(100 * size / nullIf(capacity, 0), 1) AS used_pct
FROM signoz_metrics.samples_v4 AS s
INNER JOIN (SELECT DISTINCT fingerprint, labels FROM signoz_metrics.time_series_v4
            WHERE metric_name IN ('otelcol_exporter_queue_size', 'otelcol_exporter_queue_capacity')
              AND JSONExtractString(labels, 'service.name') = 'fusescope-collector'
              AND unix_milli >= (toUnixTimestamp(now()) - 7200) * 1000) AS ts USING fingerprint
WHERE s.metric_name IN ('otelcol_exporter_queue_size', 'otelcol_exporter_queue_capacity')
  AND s.unix_milli >= (toUnixTimestamp(now()) - 600) * 1000
GROUP BY exporter, signal ORDER BY exporter, signal
