CREATE TABLE signoz_analytics.schema_migrations_v2
(
    `migration_id` UInt64,
    `status` String,
    `error` String,
    `created_at` DateTime64(9),
    `updated_at` DateTime64(9)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PRIMARY KEY migration_id
ORDER BY migration_id
SETTINGS index_granularity = 8192
