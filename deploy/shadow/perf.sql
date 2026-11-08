-- One collector's resources and throughput from its own telemetry (CUTOVER.md §6), read-only.
-- Parameters, in UTC and on whole hours: svc, the collector's service.name; since and until, the
-- window [since, until). Counters are summed sample to sample per series: each delta is a sample
-- minus the series' sample before it, across hour boundaries, with the last sample before
-- `since` as the base (looked up to an hour back); a drop is a restart and counts the sample
-- itself. A series' first sample has no base and adds nothing. Gauges are averaged over the window.
--   docker exec -i fusescope-demo-clickhouse-1 clickhouse-client --readonly 1 --param_svc=fusescope-collector \
--     --param_since='2026-11-06 00:00:00' --param_until='2026-11-08 20:00:00' < shadow/perf.sql
WITH
  toDateTime({since:String}, 'UTC') AS t_since,
  toDateTime({until:String}, 'UTC') AS t_until,
  dateDiff('hour', t_since, t_until) AS hours,
  fps AS (SELECT DISTINCT fingerprint FROM signoz_metrics.time_series_v4 WHERE resource_attrs['service.name'] = {svc:String}),
  counters AS (
    SELECT m, ms, if(rn = 1, 0, if(v >= prev, v - prev, v)) AS delta
    FROM (
      SELECT s.metric_name AS m, s.unix_milli AS ms, s.value AS v,
             lagInFrame(s.value) OVER w AS prev, row_number() OVER w AS rn
      FROM signoz_metrics.samples_v4 AS s
      WHERE s.metric_name IN ('otelcol_process_cpu_seconds', 'otelcol_process_runtime_total_alloc_bytes',
                              'otelcol_receiver_accepted_spans', 'otelcol_receiver_accepted_log_records',
                              'otelcol_receiver_accepted_metric_points')
        AND s.fingerprint IN fps
        AND s.unix_milli >= (toUnixTimestamp(t_since) - 3600) * 1000 AND s.unix_milli < toUnixTimestamp(t_until) * 1000
      WINDOW w AS (PARTITION BY s.metric_name, s.fingerprint ORDER BY s.unix_milli ROWS BETWEEN 1 PRECEDING AND CURRENT ROW))
    WHERE ms >= toUnixTimestamp(t_since) * 1000),
  gauges AS (
    SELECT s.metric_name AS m, s.value AS v FROM signoz_metrics.samples_v4 AS s
    WHERE s.metric_name IN ('otelcol_process_memory_rss', 'otelcol_process_runtime_heap_alloc_bytes')
      AND s.fingerprint IN fps
      AND s.unix_milli >= toUnixTimestamp(t_since) * 1000 AND s.unix_milli < toUnixTimestamp(t_until) * 1000),
  hourly_cpu AS (
    SELECT toStartOfHour(toDateTime(intDiv(ms, 1000))) AS h, sum(delta) AS d FROM counters
    WHERE m = 'otelcol_process_cpu_seconds' GROUP BY h)
SELECT
  hours,
  (SELECT count() FROM hourly_cpu) AS hours_with_samples,
  round((SELECT sum(delta) FROM counters WHERE m = 'otelcol_process_cpu_seconds') / (3600 * hours) * 100, 2) AS cpu_pct_avg,
  round((SELECT quantile(0.95)(d) FROM hourly_cpu) / 3600 * 100, 2) AS cpu_pct_p95_hour,
  round((SELECT avg(v) FROM gauges WHERE m = 'otelcol_process_memory_rss') / 1048576, 1) AS rss_mib_avg,
  round((SELECT max(v) FROM gauges WHERE m = 'otelcol_process_memory_rss') / 1048576, 1) AS rss_mib_max,
  round((SELECT avg(v) FROM gauges WHERE m = 'otelcol_process_runtime_heap_alloc_bytes') / 1048576, 1) AS heap_mib_avg,
  round((SELECT sum(delta) FROM counters WHERE m = 'otelcol_process_runtime_total_alloc_bytes') / hours / 1048576, 1) AS alloc_mib_per_hour,
  round((SELECT sum(delta) FROM counters WHERE m = 'otelcol_receiver_accepted_spans') / hours) AS spans_per_hour,
  round((SELECT sum(delta) FROM counters WHERE m = 'otelcol_receiver_accepted_log_records') / hours) AS logs_per_hour,
  round((SELECT sum(delta) FROM counters WHERE m = 'otelcol_receiver_accepted_metric_points') / hours) AS points_per_hour
FORMAT Vertical
