CREATE TABLE signoz_metrics.metadata
(
    `temporality` LowCardinality(String) CODEC(ZSTD(1)),
    `metric_name` LowCardinality(String) CODEC(ZSTD(1)),
    `description` String CODEC(ZSTD(1)),
    `unit` LowCardinality(String) CODEC(ZSTD(1)),
    `type` LowCardinality(String) CODEC(ZSTD(1)),
    `is_monotonic` Bool CODEC(ZSTD(1)),
    `attr_name` LowCardinality(String) CODEC(ZSTD(1)),
    `attr_type` LowCardinality(String) CODEC(ZSTD(1)),
    `attr_datatype` LowCardinality(String) CODEC(ZSTD(1)),
    `attr_string_value` String CODEC(ZSTD(1)),
    `first_reported_unix_milli` SimpleAggregateFunction(min, UInt64) CODEC(ZSTD(1)),
    `last_reported_unix_milli` SimpleAggregateFunction(max, UInt64) CODEC(ZSTD(1))
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(last_reported_unix_milli / 1000)
ORDER BY (temporality, metric_name, attr_name, attr_type, attr_datatype, attr_string_value)
TTL toDateTime(last_reported_unix_milli / 1000) + toIntervalSecond(2592000)
SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192
