CREATE TABLE signoz_metrics.distributed_time_series_v2
(
    `metric_name` LowCardinality(String),
    `fingerprint` UInt64 CODEC(ZSTD(1)),
    `timestamp_ms` Int64 CODEC(DoubleDelta, LZ4),
    `labels` String CODEC(ZSTD(5)),
    `temporality` LowCardinality(String) DEFAULT 'Unspecified' CODEC(ZSTD(5)),
    `description` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `unit` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `type` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1))
)
ENGINE = Distributed('cluster', 'signoz_metrics', 'time_series_v2', cityHash64(metric_name, fingerprint))
