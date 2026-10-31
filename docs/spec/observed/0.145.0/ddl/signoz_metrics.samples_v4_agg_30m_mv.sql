CREATE MATERIALIZED VIEW signoz_metrics.samples_v4_agg_30m_mv TO signoz_metrics.samples_v4_agg_30m
(
    `env` LowCardinality(String),
    `temporality` LowCardinality(String),
    `metric_name` LowCardinality(String),
    `fingerprint` UInt64,
    `unix_milli` Int64,
    `last` SimpleAggregateFunction(anyLast, Float64),
    `min` SimpleAggregateFunction(min, Float64),
    `max` SimpleAggregateFunction(max, Float64),
    `sum` Float64,
    `count` UInt64
)
AS SELECT
    env,
    temporality,
    metric_name,
    fingerprint,
    intDiv(unix_milli, 1800000) * 1800000 AS unix_milli,
    anyLast(last) AS last,
    min(min) AS min,
    max(max) AS max,
    sum(sum) AS sum,
    sum(count) AS count
FROM signoz_metrics.samples_v4_agg_5m
GROUP BY
    env,
    temporality,
    metric_name,
    fingerprint,
    unix_milli
