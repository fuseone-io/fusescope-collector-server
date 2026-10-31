CREATE TABLE signoz_metrics.distributed_samples_v2
(
    `metric_name` LowCardinality(String),
    `fingerprint` UInt64 CODEC(ZSTD(1)),
    `timestamp_ms` Int64 CODEC(DoubleDelta, LZ4),
    `value` Float64 CODEC(Gorilla(8), LZ4)
)
ENGINE = Distributed('cluster', 'signoz_metrics', 'samples_v2', cityHash64(metric_name, fingerprint))

