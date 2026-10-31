CREATE TABLE signoz_traces.distributed_usage_explorer
(
    `timestamp` DateTime64(9) CODEC(DoubleDelta, LZ4),
    `service_name` LowCardinality(String) CODEC(ZSTD(1)),
    `count` UInt64 CODEC(T64, ZSTD(1))
)
ENGINE = Distributed('cluster', 'signoz_traces', 'usage_explorer', cityHash64(rand()))

