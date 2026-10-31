CREATE TABLE signoz_metadata.distributed_field_keys
(
    `signal` String CODEC(ZSTD(1)),
    `field_context` String CODEC(ZSTD(1)),
    `field_name` String CODEC(ZSTD(1)),
    `field_data_type` String CODEC(ZSTD(1)),
    `last_seen` UInt64 CODEC(DoubleDelta, LZ4)
)
ENGINE = Distributed('cluster', 'signoz_metadata', 'field_keys', cityHash64(signal, field_context, field_name))
