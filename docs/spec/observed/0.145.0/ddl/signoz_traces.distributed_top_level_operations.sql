CREATE TABLE signoz_traces.distributed_top_level_operations
(
    `name` LowCardinality(String) CODEC(ZSTD(1)),
    `serviceName` LowCardinality(String) CODEC(ZSTD(1)),
    `time` DateTime DEFAULT now() CODEC(ZSTD(1))
)
ENGINE = Distributed('cluster', 'signoz_traces', 'top_level_operations', cityHash64(rand()))
