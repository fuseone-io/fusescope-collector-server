CREATE TABLE signoz_metrics.distributed_samples_v4_buffer
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `reduced_fingerprint` UInt64 DEFAULT 0 CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(DoubleDelta, ZSTD(1)),
    `value` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `flags` UInt32 DEFAULT 0 CODEC(ZSTD(1)),
    `inserted_at_unix_milli` Int64 CODEC(ZSTD(1))
)
ENGINE = Distributed('cluster', 'signoz_metrics', 'samples_v4_buffer', cityHash64(env, temporality, metric_name, if(reduced_fingerprint != 0, reduced_fingerprint, fingerprint)))
