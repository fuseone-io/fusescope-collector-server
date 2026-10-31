CREATE TABLE signoz_metrics.distributed_time_series_v4_buffer
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `unit` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `type` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `reduced_fingerprint` UInt64 DEFAULT 0 CODEC(ZSTD(1)),
    `is_reduced` Bool DEFAULT false CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `labels` String CODEC(ZSTD(5)),
    `attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `scope_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `resource_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `__normalized` Bool DEFAULT true CODEC(ZSTD(1)),
    `inserted_at_unix_milli` Int64 CODEC(ZSTD(1))
)
ENGINE = Distributed('cluster', 'signoz_metrics', 'time_series_v4_buffer', cityHash64(env, temporality, metric_name, if(reduced_fingerprint != 0, reduced_fingerprint, fingerprint)))

