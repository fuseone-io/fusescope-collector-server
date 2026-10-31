CREATE MATERIALIZED VIEW signoz_metrics.samples_v4_reduced_last_60s_mv
REFRESH EVERY 1 MINUTE RANDOMIZE FOR 10 SECOND APPEND TO signoz_metrics.samples_v4_reduced_last_60s
(
    `env` LowCardinality(String),
    `temporality` LowCardinality(String),
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64,
    `unix_milli` Int64,
    `sum_last` Float64,
    `min` Float64,
    `max` Float64,
    `sum_values` Float64,
    `count_series` UInt64,
    `count_samples` UInt64,
    `computed_at` DateTime
)
DEFINER = default SQL SECURITY DEFINER
AS SELECT
    env,
    temporality,
    metric_name,
    reduced_fingerprint,
    bucket_unix_milli AS unix_milli,
    sum(last) AS sum_last,
    min(min_value) AS min,
    max(max_value) AS max,
    sum(sum_value) AS sum_values,
    count() AS count_series,
    sum(num_values) AS count_samples,
    now() AS computed_at
FROM
(
    SELECT
        env,
        temporality,
        metric_name,
        reduced_fingerprint,
        fingerprint,
        intDiv(unix_milli, 60000) * 60000 AS bucket_unix_milli,
        argMax(value, unix_milli) AS last,
        min(value) AS min_value,
        max(value) AS max_value,
        sum(value) AS sum_value,
        count(value) AS num_values
    FROM signoz_metrics.samples_v4_buffer
    PREWHERE reduced_fingerprint != 0
    WHERE (bitAnd(flags, 1) = 0) AND ((temporality = 'Unspecified') OR ((temporality = 'Cumulative') AND (is_monotonic = false))) AND (unix_milli < (toUnixTimestamp(now() - toIntervalSecond(120)) * 1000)) AND (unix_milli >= (toUnixTimestamp(now() - toIntervalMinute(11)) * 1000))
    GROUP BY
        env,
        temporality,
        metric_name,
        reduced_fingerprint,
        fingerprint,
        bucket_unix_milli
)
WHERE bucket_unix_milli >= (toUnixTimestamp(now() - toIntervalMinute(10)) * 1000)
GROUP BY
    env,
    temporality,
    metric_name,
    reduced_fingerprint,
    bucket_unix_milli

