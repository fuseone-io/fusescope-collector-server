CREATE TABLE signoz_traces.span_attributes_keys
(
    `tagKey` LowCardinality(String) CODEC(ZSTD(1)),
    `tagType` Enum8('tag' = 1, 'resource' = 2, 'scope' = 3) CODEC(ZSTD(1)),
    `dataType` Enum8('string' = 1, 'bool' = 2, 'float64' = 3) CODEC(ZSTD(1)),
    `isColumn` Bool CODEC(ZSTD(1)),
    `timestamp` DateTime DEFAULT toDateTime(now())
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (tagKey, tagType, dataType, isColumn)
TTL timestamp + toIntervalDay(15)
SETTINGS index_granularity = 8192

