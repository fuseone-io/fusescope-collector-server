CREATE TABLE signoz_metrics.metric_reduction_rules
(
    `metric_name` LowCardinality(String) CODEC(ZSTD(1)),
    `labels` Array(LowCardinality(String)) CODEC(ZSTD(1)),
    `match_type` LowCardinality(String) DEFAULT 'drop' CODEC(ZSTD(1)),
    `effective_from_unix_milli` Int64 CODEC(ZSTD(1)),
    `deleted` Bool DEFAULT false CODEC(ZSTD(1)),
    `updated_at` DateTime64(3) DEFAULT now64(3) CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', updated_at)
ORDER BY metric_name
SETTINGS index_granularity = 8192
