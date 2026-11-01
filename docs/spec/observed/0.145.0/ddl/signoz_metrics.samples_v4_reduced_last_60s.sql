CREATE TABLE signoz_metrics.samples_v4_reduced_last_60s
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64 CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `sum_last` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `min` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `max` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `sum_values` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `count_series` UInt64 CODEC(ZSTD(1)),
    `count_samples` UInt64 CODEC(ZSTD(1)),
    `computed_at` DateTime DEFAULT now() CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', computed_at)
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (env, temporality, metric_name, reduced_fingerprint, unix_milli)
TTL toDateTime(unix_milli / 1000) + toIntervalSecond(2592000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, min_age_to_force_merge_seconds = 7200, min_age_to_force_merge_on_partition_only = 1
