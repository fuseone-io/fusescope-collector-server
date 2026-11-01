CREATE TABLE signoz_logs.logs_resource_keys
(
    `name` String,
    `datatype` String,
    `timestamp` DateTime DEFAULT toDateTime(now())
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (name, datatype)
TTL timestamp + toIntervalDay(15)
SETTINGS index_granularity = 8192
