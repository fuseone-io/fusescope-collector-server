CREATE MATERIALIZED VIEW signoz_metrics.samples_v4_reduced_sum_60s_delta_mv
REFRESH EVERY 1 MINUTE RANDOMIZE FOR 10 SECOND APPEND TO signoz_metrics.samples_v4_reduced_sum_60s
(
    `env` LowCardinality(String),
    `temporality` LowCardinality(String),
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64,
    `unix_milli` Int64,
    `sum` Float64,
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
    sum(series_sum) AS sum,
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
        sum(value) AS series_sum,
        count(value) AS num_values
    FROM signoz_metrics.samples_v4_buffer
    PREWHERE reduced_fingerprint != 0
    WHERE (bitAnd(flags, 1) = 0) AND (temporality = 'Delta') AND (unix_milli < (toUnixTimestamp(now() - toIntervalSecond(120)) * 1000)) AND (unix_milli >= (toUnixTimestamp(now() - toIntervalMinute(11)) * 1000))
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
