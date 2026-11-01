CREATE MATERIALIZED VIEW signoz_metrics.samples_v4_reduced_sum_5m_mv
REFRESH EVERY 5 MINUTE RANDOMIZE FOR 30 SECOND APPEND TO signoz_metrics.samples_v4_reduced_sum_5m
(
    `env` LowCardinality(String),
    `temporality` LowCardinality(String),
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64,
    `unix_milli` Int64,
    `sum` Float64,
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
    sum,
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
        intDiv(unix_milli, 300000) * 300000 AS bucket_unix_milli,
        sum(sum) AS sum,
        count() AS count,
        sum(count_series) AS sum_count_series,
        sum(count_samples) AS count_samples
    FROM signoz_metrics.samples_v4_reduced_sum_60s
    FINAL
    WHERE (unix_milli >= (toUnixTimestamp(now() - toIntervalMinute(40)) * 1000)) AND (unix_milli < (toUnixTimestamp(now() - toIntervalMinute(12)) * 1000))
    GROUP BY
        env,
        temporality,
        metric_name,
        reduced_fingerprint,
        bucket_unix_milli
)
WHERE (bucket_unix_milli >= (toUnixTimestamp(now() - toIntervalMinute(35)) * 1000)) AND ((bucket_unix_milli + 300000) <= (toUnixTimestamp(now() - toIntervalMinute(12)) * 1000))
