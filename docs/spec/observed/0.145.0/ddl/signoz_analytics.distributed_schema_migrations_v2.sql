CREATE TABLE signoz_analytics.distributed_schema_migrations_v2
(
    `migration_id` UInt64,
    `status` String,
    `error` String,
    `created_at` DateTime64(9),
    `updated_at` DateTime64(9)
)
ENGINE = Distributed('cluster', 'signoz_analytics', 'schema_migrations_v2', rand())

