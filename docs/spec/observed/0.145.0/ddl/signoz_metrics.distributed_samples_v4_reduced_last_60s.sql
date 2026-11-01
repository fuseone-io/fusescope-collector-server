CREATE TABLE signoz_metrics.distributed_samples_v4_reduced_last_60s
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
ENGINE = Distributed('cluster', 'signoz_metrics', 'samples_v4_reduced_last_60s', cityHash64(env, temporality, metric_name, reduced_fingerprint))
