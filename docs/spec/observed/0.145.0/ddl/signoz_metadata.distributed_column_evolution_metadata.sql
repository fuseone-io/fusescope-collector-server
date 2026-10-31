CREATE TABLE signoz_metadata.distributed_column_evolution_metadata
(
    `signal` String CODEC(ZSTD(1)),
    `column_name` String CODEC(ZSTD(1)),
    `column_type` String CODEC(ZSTD(1)),
    `field_context` String CODEC(ZSTD(1)),
    `field_name` String CODEC(ZSTD(1)),
    `version` UInt32 CODEC(DoubleDelta, ZSTD(1)),
    `release_time` SimpleAggregateFunction(min, Float64) CODEC(ZSTD(1))
)
ENGINE = Distributed('cluster', 'signoz_metadata', 'column_evolution_metadata', cityHash64(signal, column_name))
