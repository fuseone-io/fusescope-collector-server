CREATE TABLE signoz_metadata.field_keys
(
    `signal` String CODEC(ZSTD(1)),
    `field_context` String CODEC(ZSTD(1)),
    `field_name` String CODEC(ZSTD(1)),
    `field_data_type` String CODEC(ZSTD(1)),
    `last_seen` UInt64 CODEC(DoubleDelta, LZ4)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(last_seen / 1000000000)
ORDER BY (signal, field_context, field_name, field_data_type)
TTL toDateTime(last_seen / 1000000000) + toIntervalSecond(1296000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
