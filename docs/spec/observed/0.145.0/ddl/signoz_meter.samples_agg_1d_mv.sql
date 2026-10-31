CREATE MATERIALIZED VIEW signoz_meter.samples_agg_1d_mv TO signoz_meter.samples_agg_1d
(
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT 'default',
    `unit` LowCardinality(String) DEFAULT 'default',
    `type` LowCardinality(String) DEFAULT 'default',
    `is_monotonic` Bool DEFAULT false,
    `labels` String CODEC(ZSTD(5)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(DoubleDelta, ZSTD(1)),
    `last` SimpleAggregateFunction(anyLast, Float64) CODEC(ZSTD(1)),
    `min` SimpleAggregateFunction(min, Float64) CODEC(ZSTD(1)),
    `max` SimpleAggregateFunction(max, Float64) CODEC(ZSTD(1)),
    `sum` SimpleAggregateFunction(sum, Float64) CODEC(ZSTD(1)),
    `count` SimpleAggregateFunction(sum, UInt64) CODEC(ZSTD(1))
)
AS SELECT
    temporality,
    metric_name,
    description,
    unit,
    type,
    is_monotonic,
    labels,
    fingerprint,
    intDiv(unix_milli, 86400000) * 86400000 AS unix_milli,
    anyLast(value) AS last,
    min(value) AS min,
    max(value) AS max,
    sum(value) AS sum,
    count(*) AS count
FROM signoz_meter.samples
GROUP BY
    temporality,
    metric_name,
    fingerprint,
    description,
    unit,
    type,
    is_monotonic,
    labels,
    unix_milli

