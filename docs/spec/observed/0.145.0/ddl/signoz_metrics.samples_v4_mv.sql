CREATE MATERIALIZED VIEW signoz_metrics.samples_v4_mv TO signoz_metrics.samples_v4
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(DoubleDelta, ZSTD(1)),
    `value` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `flags` UInt32 DEFAULT 0 CODEC(ZSTD(1)),
    `inserted_at_unix_milli` Int64 CODEC(ZSTD(1))
)
AS SELECT
    env,
    temporality,
    metric_name,
    fingerprint,
    unix_milli,
    value,
    flags,
    inserted_at_unix_milli
FROM signoz_metrics.samples_v4_buffer
WHERE reduced_fingerprint = 0

