CREATE TABLE signoz_metrics.updated_metadata
(
    `metric_name` LowCardinality(String) CODEC(ZSTD(1)),
    `temporality` LowCardinality(String) CODEC(ZSTD(1)),
    `is_monotonic` Bool CODEC(ZSTD(1)),
    `type` LowCardinality(String) CODEC(ZSTD(1)),
    `description` LowCardinality(String) CODEC(ZSTD(1)),
    `unit` LowCardinality(String) CODEC(ZSTD(1)),
    `created_at` Int64 CODEC(ZSTD(1))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY metric_name
SETTINGS index_granularity = 8192
