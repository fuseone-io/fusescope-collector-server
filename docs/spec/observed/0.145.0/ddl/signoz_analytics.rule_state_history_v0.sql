CREATE TABLE signoz_analytics.rule_state_history_v0
(
    `_retention_days` UInt32 DEFAULT 180,
    `rule_id` LowCardinality(String),
    `rule_name` LowCardinality(String),
    `overall_state` LowCardinality(String),
    `overall_state_changed` Bool,
    `state` LowCardinality(String),
    `state_changed` Bool,
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `fingerprint` UInt64 CODEC(ZSTD(1)),
    `value` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `labels` String CODEC(ZSTD(5))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (rule_id, unix_milli)
TTL toDateTime(unix_milli / 1000) + toIntervalDay(_retention_days)
SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192

