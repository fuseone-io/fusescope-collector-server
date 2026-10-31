CREATE TABLE signoz_metrics.distributed_samples_v4_reduced_sum_5m
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64 CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `sum` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `count` UInt64 CODEC(ZSTD(1)),
    `sum_count_series` UInt64 CODEC(ZSTD(1)),
    `count_samples` UInt64 CODEC(ZSTD(1)),
    `computed_at` DateTime DEFAULT now() CODEC(ZSTD(1))
)
ENGINE = Distributed('cluster', 'signoz_metrics', 'samples_v4_reduced_sum_5m', cityHash64(env, temporality, metric_name, reduced_fingerprint))
