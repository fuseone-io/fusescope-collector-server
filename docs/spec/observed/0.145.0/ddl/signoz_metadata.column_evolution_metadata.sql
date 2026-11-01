CREATE TABLE signoz_metadata.column_evolution_metadata
(
    `signal` String CODEC(ZSTD(1)),
    `column_name` String CODEC(ZSTD(1)),
    `column_type` String CODEC(ZSTD(1)),
    `field_context` String CODEC(ZSTD(1)),
    `field_name` String CODEC(ZSTD(1)),
    `version` UInt32 CODEC(DoubleDelta, ZSTD(1)),
    `release_time` SimpleAggregateFunction(min, Float64) CODEC(ZSTD(1))
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(release_time / 1000000000)
ORDER BY (signal, column_name, column_type, field_context, field_name, version)
SETTINGS index_granularity = 8192
