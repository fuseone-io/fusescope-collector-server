CREATE MATERIALIZED VIEW signoz_metrics.samples_v4_reduced_last_30m_mv
REFRESH EVERY 30 MINUTE RANDOMIZE FOR 2 MINUTE APPEND TO signoz_metrics.samples_v4_reduced_last_30m
(
    `env` LowCardinality(String),
    `temporality` LowCardinality(String),
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64,
    `unix_milli` Int64,
    `last` Float64,
    `min` Float64,
    `max` Float64,
    `sum` Float64,
    `sum_values` Float64,
    `count` UInt64,
    `sum_count_series` UInt64,
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
    last,
    min,
    max,
    sum,
    sum_values,
    count,
    sum_count_series,
    count_samples,
    now() AS computed_at
FROM
(
    SELECT
        env,
        temporality,
        metric_name,
        reduced_fingerprint,
        intDiv(unix_milli, 1800000) * 1800000 AS bucket_unix_milli,
        argMax(last, unix_milli) AS last,
        min(min) AS min,
        max(max) AS max,
        sum(sum) AS sum,
        sum(sum_values) AS sum_values,
        sum(count) AS count,
        sum(sum_count_series) AS sum_count_series,
        sum(count_samples) AS count_samples
    FROM signoz_metrics.samples_v4_reduced_last_5m
    FINAL
    WHERE (unix_milli >= (toUnixTimestamp(now() - toIntervalMinute(180)) * 1000)) AND (unix_milli < (toUnixTimestamp(now() - toIntervalMinute(40)) * 1000))
    GROUP BY
        env,
        temporality,
        metric_name,
        reduced_fingerprint,
        bucket_unix_milli
)
WHERE (bucket_unix_milli >= (toUnixTimestamp(now() - toIntervalMinute(150)) * 1000)) AND ((bucket_unix_milli + 1800000) <= (toUnixTimestamp(now() - toIntervalMinute(40)) * 1000))
