-- One collector's resources and throughput from its own telemetry (CUTOVER.md §6), read-only.
-- Parameters: svc, the collector's service.name; since, the window start in UTC. A restart starts
-- new series, so counters are taken per series and hour (max - min) and then summed.
--   docker exec -i fusescope-demo-clickhouse-1 clickhouse-client --readonly 1 \
--     --param_svc=fusescope-collector --param_since='2026-11-06 00:00:00' < shadow/perf.sql
WITH
  toUnixTimestamp(toDateTime({since:String}, 'UTC')) * 1000 AS t0,
  (toUnixTimestamp(now()) - 300) * 1000 AS t1,
  samples AS (
    SELECT s.metric_name AS m, s.fingerprint AS fp, toStartOfHour(toDateTime(intDiv(s.unix_milli, 1000))) AS h, s.value AS v
    FROM signoz_metrics.samples_v4 AS s
    WHERE s.unix_milli >= t0 AND s.unix_milli < t1
      AND s.metric_name IN ('otelcol_process_cpu_seconds', 'otelcol_process_runtime_total_alloc_bytes',
                            'otelcol_receiver_accepted_spans', 'otelcol_receiver_accepted_log_records',
                            'otelcol_receiver_accepted_metric_points', 'otelcol_process_memory_rss',
                            'otelcol_process_runtime_heap_alloc_bytes')
      AND s.fingerprint IN (SELECT fingerprint FROM signoz_metrics.time_series_v4
                            WHERE resource_attrs['service.name'] = {svc:String})),
  hourly AS (
    SELECT m, h, sum(inc) AS inc FROM (SELECT m, h, fp, greatest(max(v) - min(v), 0) AS inc FROM samples GROUP BY m, h, fp)
    GROUP BY m, h),
  hours AS (SELECT count(DISTINCT h) AS n FROM hourly WHERE m = 'otelcol_process_cpu_seconds')
SELECT
  (SELECT n FROM hours) AS hours,
  round((SELECT sum(inc) FROM hourly WHERE m = 'otelcol_process_cpu_seconds') / (3600 * hours) * 100, 2) AS cpu_pct_avg,
  round((SELECT quantile(0.95)(inc) FROM hourly WHERE m = 'otelcol_process_cpu_seconds') / 3600 * 100, 2) AS cpu_pct_p95_hour,
  round((SELECT avg(v) FROM samples WHERE m = 'otelcol_process_memory_rss') / 1048576, 1) AS rss_mib_avg,
  round((SELECT max(v) FROM samples WHERE m = 'otelcol_process_memory_rss') / 1048576, 1) AS rss_mib_max,
  round((SELECT avg(v) FROM samples WHERE m = 'otelcol_process_runtime_heap_alloc_bytes') / 1048576, 1) AS heap_mib_avg,
  round((SELECT sum(inc) FROM hourly WHERE m = 'otelcol_process_runtime_total_alloc_bytes') / hours / 1048576, 1) AS alloc_mib_per_hour,
  round((SELECT sum(inc) FROM hourly WHERE m = 'otelcol_receiver_accepted_spans') / hours) AS spans_per_hour,
  round((SELECT sum(inc) FROM hourly WHERE m = 'otelcol_receiver_accepted_log_records') / hours) AS logs_per_hour,
  round((SELECT sum(inc) FROM hourly WHERE m = 'otelcol_receiver_accepted_metric_points') / hours) AS points_per_hour
FORMAT Vertical
