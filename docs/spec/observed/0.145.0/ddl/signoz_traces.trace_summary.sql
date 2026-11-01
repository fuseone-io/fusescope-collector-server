CREATE TABLE signoz_traces.trace_summary
(
    `trace_id` String CODEC(ZSTD(1)),
    `start` SimpleAggregateFunction(min, DateTime64(9)) CODEC(ZSTD(1)),
    `end` SimpleAggregateFunction(max, DateTime64(9)) CODEC(ZSTD(1)),
    `num_spans` SimpleAggregateFunction(sum, UInt64) CODEC(ZSTD(1))
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(end)
ORDER BY trace_id
TTL toDateTime(end) + toIntervalSecond(1296000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
