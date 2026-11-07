CREATE DATABASE IF NOT EXISTS {db:analytics} ON CLUSTER {cluster}
;

CREATE DATABASE IF NOT EXISTS {db:logs} ON CLUSTER {cluster}
;

CREATE DATABASE IF NOT EXISTS {db:metadata} ON CLUSTER {cluster}
;

CREATE DATABASE IF NOT EXISTS {db:meter} ON CLUSTER {cluster}
;

CREATE DATABASE IF NOT EXISTS {db:metrics} ON CLUSTER {cluster}
;

CREATE DATABASE IF NOT EXISTS {db:traces} ON CLUSTER {cluster}
;

CREATE TABLE IF NOT EXISTS {db:analytics}.rule_state_history_v0 ON CLUSTER {cluster}
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
;

CREATE TABLE IF NOT EXISTS {db:analytics}.schema_migrations_v2 ON CLUSTER {cluster}
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
;

CREATE TABLE IF NOT EXISTS {db:logs}.logs_attribute_keys ON CLUSTER {cluster}
(
    `name` String,
    `datatype` String,
    `timestamp` DateTime DEFAULT toDateTime(now())
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (name, datatype)
TTL timestamp + toIntervalDay(15)
SETTINGS index_granularity = 8192
;

CREATE TABLE IF NOT EXISTS {db:logs}.logs_resource_keys ON CLUSTER {cluster}
(
    `name` String,
    `datatype` String,
    `timestamp` DateTime DEFAULT toDateTime(now())
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (name, datatype)
TTL timestamp + toIntervalDay(15)
SETTINGS index_granularity = 8192
;

CREATE TABLE IF NOT EXISTS {db:logs}.logs_v2 ON CLUSTER {cluster}
(
    `ts_bucket_start` UInt64 CODEC(DoubleDelta, LZ4),
    `resource_fingerprint` String CODEC(ZSTD(1)),
    `timestamp` UInt64 CODEC(DoubleDelta, LZ4),
    `observed_timestamp` UInt64 CODEC(DoubleDelta, LZ4),
    `id` String CODEC(ZSTD(1)),
    `trace_id` String CODEC(ZSTD(1)),
    `span_id` String CODEC(ZSTD(1)),
    `trace_flags` UInt32,
    `severity_text` LowCardinality(String) CODEC(ZSTD(1)),
    `severity_number` UInt8,
    `body` String CODEC(ZSTD(2)),
    `body_v2` JSON(max_dynamic_paths = 0, message String) CODEC(ZSTD(1)),
    `body_promoted` JSON CODEC(ZSTD(1)),
    `attributes_string` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `attributes_number` Map(LowCardinality(String), Float64) CODEC(ZSTD(1)),
    `attributes_bool` Map(LowCardinality(String), Bool) CODEC(ZSTD(1)),
    `resources_string` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `scope_name` String CODEC(ZSTD(1)),
    `scope_version` String CODEC(ZSTD(1)),
    `scope_string` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `_retention_days` UInt16 DEFAULT 15,
    `_retention_days_cold` UInt16 DEFAULT 0,
    `resource` JSON(max_dynamic_paths = 100) CODEC(ZSTD(1)),
    `inserted_at` DateTime64(3) CODEC(Delta(8), ZSTD(1)),
    `created_at` DateTime64(3) DEFAULT now64(3) CODEC(Delta(8), ZSTD(1)),
    INDEX id_minmax id TYPE minmax GRANULARITY 1,
    INDEX severity_number_idx severity_number TYPE set(25) GRANULARITY 4,
    INDEX severity_text_idx severity_text TYPE set(25) GRANULARITY 4,
    INDEX trace_flags_idx trace_flags TYPE bloom_filter GRANULARITY 4,
    INDEX body_index_v2_token lower(body) TYPE tokenbf_v1(10000, 2, 0) GRANULARITY 1,
    INDEX body_index_v2_ngram lower(body) TYPE ngrambf_v1(4, 15000, 3, 0) GRANULARITY 1,
    INDEX scope_name_idx scope_name TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 4,
    INDEX attributes_string_idx_key mapKeys(attributes_string) TYPE tokenbf_v1(1024, 2, 0) GRANULARITY 1,
    INDEX attributes_string_idx_val mapValues(attributes_string) TYPE ngrambf_v1(4, 5000, 2, 0) GRANULARITY 1,
    INDEX attributes_number_idx_key mapKeys(attributes_number) TYPE tokenbf_v1(1024, 2, 0) GRANULARITY 1,
    INDEX attributes_number_idx_val mapValues(attributes_number) TYPE bloom_filter GRANULARITY 1,
    INDEX attributes_bool_idx_key mapKeys(attributes_bool) TYPE tokenbf_v1(1024, 2, 0) GRANULARITY 1,
    INDEX trace_id_idx trace_id TYPE tokenbf_v1(10000, 5, 0) GRANULARITY 1,
    INDEX span_id_idx span_id TYPE tokenbf_v1(5000, 5, 0) GRANULARITY 1,
    INDEX body_v2_string_ngram_idx lower(toString(body_v2)) TYPE ngrambf_v1(4, 15000, 3, 0) GRANULARITY 1,
    INDEX body_v2_string_token_idx lower(toString(body_v2)) TYPE tokenbf_v1(10000, 2, 0) GRANULARITY 1,
    INDEX body_v2_paths_ngram_idx JSONAllPaths(body_v2) TYPE ngrambf_v1(4, 15000, 3, 0) GRANULARITY 1,
    INDEX body_v2_paths_token_idx JSONAllPaths(body_v2) TYPE tokenbf_v1(10000, 2, 0) GRANULARITY 1
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY (toDate(timestamp / 1000000000), _retention_days, _retention_days_cold)
ORDER BY (ts_bucket_start, resource_fingerprint, severity_text, timestamp, id)
TTL toDateTime(timestamp / 1000000000) + toIntervalDay(_retention_days)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, object_serialization_version = 'v3', object_shared_data_serialization_version = 'advanced'
;

CREATE TABLE IF NOT EXISTS {db:logs}.logs_v2_resource ON CLUSTER {cluster}
(
    `labels` String CODEC(ZSTD(5)),
    `fingerprint` String CODEC(ZSTD(1)),
    `seen_at_ts_bucket_start` Int64 CODEC(Delta(8), ZSTD(1)),
    `_retention_days` UInt16 DEFAULT 15,
    `_retention_days_cold` UInt16 DEFAULT 0,
    INDEX idx_labels lower(labels) TYPE ngrambf_v1(4, 1024, 3, 0) GRANULARITY 1,
    INDEX idx_labels_v1 labels TYPE ngrambf_v1(4, 1024, 3, 0) GRANULARITY 1
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY (toDate(seen_at_ts_bucket_start), _retention_days, _retention_days_cold)
ORDER BY (labels, fingerprint, seen_at_ts_bucket_start)
TTL (toDateTime(seen_at_ts_bucket_start) + toIntervalDay(_retention_days)) + toIntervalSecond(1800)
SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192
;

CREATE TABLE IF NOT EXISTS {db:logs}.schema_migrations_v2 ON CLUSTER {cluster}
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
;

CREATE TABLE IF NOT EXISTS {db:logs}.tag_attributes_v2 ON CLUSTER {cluster}
(
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `tag_key` String CODEC(ZSTD(1)),
    `tag_type` LowCardinality(String) CODEC(ZSTD(1)),
    `tag_data_type` LowCardinality(String) CODEC(ZSTD(1)),
    `string_value` String CODEC(ZSTD(1)),
    `number_value` Nullable(Float64) CODEC(ZSTD(1)),
    INDEX string_value_index string_value TYPE ngrambf_v1(4, 1024, 3, 0) GRANULARITY 1,
    INDEX number_value_index number_value TYPE minmax GRANULARITY 1
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (tag_key, tag_type, tag_data_type, string_value, number_value)
TTL toDateTime(unix_milli / 1000) + toIntervalSecond(1296000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, allow_nullable_key = 1
;

CREATE TABLE IF NOT EXISTS {db:logs}.usage ON CLUSTER {cluster}
(
    `tenant` String CODEC(ZSTD(1)),
    `collector_id` String CODEC(ZSTD(1)),
    `exporter_id` String CODEC(ZSTD(1)),
    `timestamp` DateTime CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (tenant, collector_id, exporter_id, timestamp)
TTL timestamp + toIntervalDay(3)
SETTINGS index_granularity = 8192
;

CREATE TABLE IF NOT EXISTS {db:metadata}.attributes_metadata ON CLUSTER {cluster}
(
    `unix_milli` UInt64,
    `data_source` String,
    `resource_fingerprint` UInt64,
    `attrs_fingerprint` UInt64,
    `resource_attributes` Map(LowCardinality(String), String),
    `attributes` Map(LowCardinality(String), String),
    INDEX idx_resource_attributes_map_keys mapKeys(resource_attributes) TYPE tokenbf_v1(1024, 2, 0) GRANULARITY 1,
    INDEX idx_attributes_map_keys mapKeys(attributes) TYPE tokenbf_v1(1024, 2, 0) GRANULARITY 1,
    INDEX idx_resource_attributes_map_values mapValues(resource_attributes) TYPE ngrambf_v1(4, 5000, 2, 0) GRANULARITY 1,
    INDEX idx_attributes_map_values mapValues(attributes) TYPE ngrambf_v1(4, 5000, 2, 0) GRANULARITY 1
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (data_source, unix_milli, resource_fingerprint, attrs_fingerprint)
TTL toDateTime(unix_milli / 1000) + toIntervalSecond(2592000)
SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192
;

CREATE TABLE IF NOT EXISTS {db:metadata}.column_evolution_metadata ON CLUSTER {cluster}
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
;

CREATE TABLE IF NOT EXISTS {db:metadata}.field_keys ON CLUSTER {cluster}
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
;

CREATE TABLE IF NOT EXISTS {db:metadata}.schema_migrations_v2 ON CLUSTER {cluster}
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
;

CREATE TABLE IF NOT EXISTS {db:meter}.samples ON CLUSTER {cluster}
(
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT 'default',
    `unit` LowCardinality(String) DEFAULT 'default',
    `type` LowCardinality(String) DEFAULT 'default',
    `is_monotonic` Bool DEFAULT false,
    `labels` String CODEC(ZSTD(5)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(DoubleDelta, ZSTD(1)),
    `value` Float64 CODEC(Gorilla(8), ZSTD(1))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toYYYYMM(toDateTime(intDiv(unix_milli, 1000)))
ORDER BY (temporality, metric_name, fingerprint, toDayOfMonth(toDateTime(intDiv(unix_milli, 1000))))
TTL toDateTime(intDiv(unix_milli, 1000)) + toIntervalYear(1)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
;

CREATE TABLE IF NOT EXISTS {db:meter}.samples_agg_1d ON CLUSTER {cluster}
(
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT 'default',
    `unit` LowCardinality(String) DEFAULT 'default',
    `type` LowCardinality(String) DEFAULT 'default',
    `is_monotonic` Bool DEFAULT false,
    `labels` String CODEC(ZSTD(5)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(DoubleDelta, ZSTD(1)),
    `last` SimpleAggregateFunction(anyLast, Float64) CODEC(ZSTD(1)),
    `min` SimpleAggregateFunction(min, Float64) CODEC(ZSTD(1)),
    `max` SimpleAggregateFunction(max, Float64) CODEC(ZSTD(1)),
    `sum` SimpleAggregateFunction(sum, Float64) CODEC(ZSTD(1)),
    `count` SimpleAggregateFunction(sum, UInt64) CODEC(ZSTD(1))
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toYYYYMM(toDateTime(intDiv(unix_milli, 1000)))
ORDER BY (temporality, metric_name, fingerprint, toDayOfMonth(toDateTime(intDiv(unix_milli, 1000))))
TTL toDateTime(intDiv(unix_milli, 1000)) + toIntervalYear(1)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
;

CREATE TABLE IF NOT EXISTS {db:meter}.schema_migrations_v2 ON CLUSTER {cluster}
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
;

CREATE TABLE IF NOT EXISTS {db:metrics}.exp_hist ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(DoubleDelta, ZSTD(1)),
    `count` UInt64 CODEC(ZSTD(1)),
    `sum` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `min` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `max` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `sketch` AggregateFunction(quantilesDD(0.01, 0.5, 0.75, 0.9, 0.95, 0.99), UInt64) CODEC(ZSTD(1)),
    `flags` UInt32 DEFAULT 0 CODEC(ZSTD(1)),
    `inserted_at_unix_milli` Int64 CODEC(ZSTD(1))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (env, temporality, metric_name, fingerprint, unix_milli)
TTL toDateTime(unix_milli / 1000) + toIntervalSecond(2592000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
;

CREATE TABLE IF NOT EXISTS {db:metrics}.metadata ON CLUSTER {cluster}
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
;

CREATE TABLE IF NOT EXISTS {db:metrics}.metric_reduction_rules ON CLUSTER {cluster}
(
    `metric_name` LowCardinality(String) CODEC(ZSTD(1)),
    `labels` Array(LowCardinality(String)) CODEC(ZSTD(1)),
    `match_type` LowCardinality(String) DEFAULT 'drop' CODEC(ZSTD(1)),
    `effective_from_unix_milli` Int64 CODEC(ZSTD(1)),
    `deleted` Bool DEFAULT false CODEC(ZSTD(1)),
    `updated_at` DateTime64(3) DEFAULT now64(3) CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', updated_at)
ORDER BY metric_name
SETTINGS index_granularity = 8192
;

CREATE TABLE IF NOT EXISTS {db:metrics}.samples_v2 ON CLUSTER {cluster}
(
    `metric_name` LowCardinality(String),
    `fingerprint` UInt64 CODEC(ZSTD(1)),
    `timestamp_ms` Int64 CODEC(DoubleDelta, LZ4),
    `value` Float64 CODEC(Gorilla(8), LZ4)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(timestamp_ms / 1000)
ORDER BY (metric_name, fingerprint, timestamp_ms)
TTL toDateTime(timestamp_ms / 1000) + toIntervalSecond(2592000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
;

CREATE TABLE IF NOT EXISTS {db:metrics}.samples_v4 ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(DoubleDelta, ZSTD(1)),
    `value` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `flags` UInt32 DEFAULT 0 CODEC(ZSTD(1)),
    `inserted_at_unix_milli` Int64 CODEC(ZSTD(1))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (env, temporality, metric_name, fingerprint, unix_milli)
TTL toDateTime(unix_milli / 1000) + toIntervalSecond(2592000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
;

CREATE TABLE IF NOT EXISTS {db:metrics}.samples_v4_agg_30m ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `fingerprint` UInt64 CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `last` SimpleAggregateFunction(anyLast, Float64) CODEC(ZSTD(1)),
    `min` SimpleAggregateFunction(min, Float64) CODEC(ZSTD(1)),
    `max` SimpleAggregateFunction(max, Float64) CODEC(ZSTD(1)),
    `sum` SimpleAggregateFunction(sum, Float64) CODEC(ZSTD(1)),
    `count` SimpleAggregateFunction(sum, UInt64) CODEC(ZSTD(1))
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (env, temporality, metric_name, fingerprint, unix_milli)
TTL toDateTime(unix_milli / 1000) + toIntervalSecond(2592000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
;

CREATE TABLE IF NOT EXISTS {db:metrics}.samples_v4_agg_5m ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `fingerprint` UInt64 CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `last` SimpleAggregateFunction(anyLast, Float64) CODEC(ZSTD(1)),
    `min` SimpleAggregateFunction(min, Float64) CODEC(ZSTD(1)),
    `max` SimpleAggregateFunction(max, Float64) CODEC(ZSTD(1)),
    `sum` SimpleAggregateFunction(sum, Float64) CODEC(ZSTD(1)),
    `count` SimpleAggregateFunction(sum, UInt64) CODEC(ZSTD(1))
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (env, temporality, metric_name, fingerprint, unix_milli)
TTL toDateTime(unix_milli / 1000) + toIntervalSecond(2592000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
;

CREATE TABLE IF NOT EXISTS {db:metrics}.samples_v4_buffer ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `reduced_fingerprint` UInt64 DEFAULT 0 CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(DoubleDelta, ZSTD(1)),
    `value` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `flags` UInt32 DEFAULT 0 CODEC(ZSTD(1)),
    `inserted_at_unix_milli` Int64 CODEC(ZSTD(1)),
    INDEX idx_unix_milli unix_milli TYPE minmax GRANULARITY 1
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (env, temporality, metric_name, fingerprint, unix_milli)
TTL toDateTime(unix_milli / 1000) + toIntervalSecond(90000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
;

CREATE TABLE IF NOT EXISTS {db:metrics}.samples_v4_reduced_last_30m ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64 CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `last` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `min` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `max` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `sum` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `sum_values` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `count` UInt64 CODEC(ZSTD(1)),
    `sum_count_series` UInt64 CODEC(ZSTD(1)),
    `count_samples` UInt64 CODEC(ZSTD(1)),
    `computed_at` DateTime DEFAULT now() CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', computed_at)
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (env, temporality, metric_name, reduced_fingerprint, unix_milli)
TTL toDateTime(unix_milli / 1000) + toIntervalSecond(2592000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, min_age_to_force_merge_seconds = 7200, min_age_to_force_merge_on_partition_only = 1
;

CREATE TABLE IF NOT EXISTS {db:metrics}.samples_v4_reduced_last_5m ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64 CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `last` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `min` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `max` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `sum` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `sum_values` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `count` UInt64 CODEC(ZSTD(1)),
    `sum_count_series` UInt64 CODEC(ZSTD(1)),
    `count_samples` UInt64 CODEC(ZSTD(1)),
    `computed_at` DateTime DEFAULT now() CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', computed_at)
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (env, temporality, metric_name, reduced_fingerprint, unix_milli)
TTL toDateTime(unix_milli / 1000) + toIntervalSecond(2592000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, min_age_to_force_merge_seconds = 7200, min_age_to_force_merge_on_partition_only = 1
;

CREATE TABLE IF NOT EXISTS {db:metrics}.samples_v4_reduced_last_60s ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64 CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `sum_last` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `min` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `max` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `sum_values` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `count_series` UInt64 CODEC(ZSTD(1)),
    `count_samples` UInt64 CODEC(ZSTD(1)),
    `computed_at` DateTime DEFAULT now() CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', computed_at)
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (env, temporality, metric_name, reduced_fingerprint, unix_milli)
TTL toDateTime(unix_milli / 1000) + toIntervalSecond(2592000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, min_age_to_force_merge_seconds = 7200, min_age_to_force_merge_on_partition_only = 1
;

CREATE TABLE IF NOT EXISTS {db:metrics}.samples_v4_reduced_sum_30m ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64 CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `sum` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `count` UInt64 CODEC(ZSTD(1)),
    `sum_count_series` UInt64 CODEC(ZSTD(1)),
    `count_samples` UInt64 CODEC(ZSTD(1)),
    `computed_at` DateTime DEFAULT now() CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', computed_at)
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (env, temporality, metric_name, reduced_fingerprint, unix_milli)
TTL toDateTime(unix_milli / 1000) + toIntervalSecond(2592000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, min_age_to_force_merge_seconds = 7200, min_age_to_force_merge_on_partition_only = 1
;

CREATE TABLE IF NOT EXISTS {db:metrics}.samples_v4_reduced_sum_5m ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64 CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `sum` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `count` UInt64 CODEC(ZSTD(1)),
    `sum_count_series` UInt64 CODEC(ZSTD(1)),
    `count_samples` UInt64 CODEC(ZSTD(1)),
    `computed_at` DateTime DEFAULT now() CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', computed_at)
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (env, temporality, metric_name, reduced_fingerprint, unix_milli)
TTL toDateTime(unix_milli / 1000) + toIntervalSecond(2592000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, min_age_to_force_merge_seconds = 7200, min_age_to_force_merge_on_partition_only = 1
;

CREATE TABLE IF NOT EXISTS {db:metrics}.samples_v4_reduced_sum_60s ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64 CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `sum` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `count_series` UInt64 CODEC(ZSTD(1)),
    `count_samples` UInt64 CODEC(ZSTD(1)),
    `computed_at` DateTime DEFAULT now() CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', computed_at)
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (env, temporality, metric_name, reduced_fingerprint, unix_milli)
TTL toDateTime(unix_milli / 1000) + toIntervalSecond(2592000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, min_age_to_force_merge_seconds = 7200, min_age_to_force_merge_on_partition_only = 1
;

CREATE TABLE IF NOT EXISTS {db:metrics}.schema_migrations_v2 ON CLUSTER {cluster}
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
;

CREATE TABLE IF NOT EXISTS {db:metrics}.time_series_v2 ON CLUSTER {cluster}
(
    `metric_name` LowCardinality(String),
    `fingerprint` UInt64 CODEC(ZSTD(1)),
    `timestamp_ms` Int64 CODEC(DoubleDelta, LZ4),
    `labels` String CODEC(ZSTD(5)),
    `temporality` LowCardinality(String) DEFAULT 'Unspecified' CODEC(ZSTD(5)),
    `description` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `unit` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `type` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1)),
    INDEX temporality_index temporality TYPE SET(3) GRANULARITY 1
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(timestamp_ms / 1000)
ORDER BY (metric_name, fingerprint)
SETTINGS index_granularity = 8192
;

CREATE TABLE IF NOT EXISTS {db:metrics}.time_series_v4 ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `unit` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `type` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `labels` String CODEC(ZSTD(5)),
    `attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `scope_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `resource_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `__normalized` Bool DEFAULT true CODEC(ZSTD(1)),
    `inserted_at_unix_milli` Int64 CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (env, temporality, metric_name, fingerprint, unix_milli)
TTL toDateTime(unix_milli / 1000) + toIntervalSecond(2592000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
;

CREATE TABLE IF NOT EXISTS {db:metrics}.time_series_v4_1day ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `unit` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `type` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `labels` String CODEC(ZSTD(5)),
    `attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `scope_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `resource_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `__normalized` Bool DEFAULT true CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (env, temporality, metric_name, fingerprint, unix_milli)
TTL toDateTime(unix_milli / 1000) + toIntervalSecond(2592000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
;

CREATE TABLE IF NOT EXISTS {db:metrics}.time_series_v4_1week ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `unit` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `type` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `labels` String CODEC(ZSTD(5)),
    `attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `scope_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `resource_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `__normalized` Bool DEFAULT true CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (env, temporality, metric_name, fingerprint, unix_milli)
TTL toDateTime(unix_milli / 1000) + toIntervalSecond(2592000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
;

CREATE TABLE IF NOT EXISTS {db:metrics}.time_series_v4_6hrs ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `unit` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `type` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `labels` String CODEC(ZSTD(5)),
    `attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `scope_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `resource_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `__normalized` Bool DEFAULT true CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (env, temporality, metric_name, fingerprint, unix_milli)
TTL toDateTime(unix_milli / 1000) + toIntervalSecond(2592000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
;

CREATE TABLE IF NOT EXISTS {db:metrics}.time_series_v4_buffer ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `unit` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `type` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `reduced_fingerprint` UInt64 DEFAULT 0 CODEC(ZSTD(1)),
    `is_reduced` Bool DEFAULT false CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `labels` String CODEC(ZSTD(5)),
    `attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `scope_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `resource_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `__normalized` Bool DEFAULT true CODEC(ZSTD(1)),
    `inserted_at_unix_milli` Int64 CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (env, temporality, metric_name, fingerprint, unix_milli, is_reduced)
TTL toDateTime(unix_milli / 1000) + toIntervalSecond(90000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
;

CREATE TABLE IF NOT EXISTS {db:metrics}.time_series_v4_reduced ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `unit` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `type` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `labels` String CODEC(ZSTD(5)),
    `attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `scope_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `resource_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `__normalized` Bool DEFAULT true CODEC(ZSTD(1)),
    `inserted_at_unix_milli` Int64 CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (env, temporality, metric_name, fingerprint, unix_milli)
TTL toDateTime(unix_milli / 1000) + toIntervalSecond(2592000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, min_age_to_force_merge_seconds = 7200, min_age_to_force_merge_on_partition_only = 1
;

CREATE TABLE IF NOT EXISTS {db:metrics}.time_series_v4_reduced_1day ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `unit` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `type` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `labels` String CODEC(ZSTD(5)),
    `attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `scope_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `resource_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `__normalized` Bool DEFAULT true CODEC(ZSTD(1)),
    `inserted_at_unix_milli` Int64 CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (env, temporality, metric_name, fingerprint, unix_milli)
TTL toDateTime(unix_milli / 1000) + toIntervalSecond(2592000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, min_age_to_force_merge_seconds = 7200, min_age_to_force_merge_on_partition_only = 1
;

CREATE TABLE IF NOT EXISTS {db:metrics}.updated_metadata ON CLUSTER {cluster}
(
    `metric_name` LowCardinality(String) CODEC(ZSTD(1)),
    `temporality` LowCardinality(String) CODEC(ZSTD(1)),
    `is_monotonic` Bool CODEC(ZSTD(1)),
    `type` LowCardinality(String) CODEC(ZSTD(1)),
    `description` LowCardinality(String) CODEC(ZSTD(1)),
    `unit` LowCardinality(String) CODEC(ZSTD(1)),
    `created_at` Int64 CODEC(ZSTD(1))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY metric_name
SETTINGS index_granularity = 8192
;

CREATE TABLE IF NOT EXISTS {db:metrics}.usage ON CLUSTER {cluster}
(
    `tenant` String CODEC(ZSTD(1)),
    `collector_id` String CODEC(ZSTD(1)),
    `exporter_id` String CODEC(ZSTD(1)),
    `timestamp` DateTime CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (tenant, collector_id, exporter_id, timestamp)
TTL timestamp + toIntervalDay(3)
SETTINGS index_granularity = 8192
;

CREATE TABLE IF NOT EXISTS {db:traces}.dependency_graph_minutes_v2 ON CLUSTER {cluster}
(
    `src` LowCardinality(String) CODEC(ZSTD(1)),
    `dest` LowCardinality(String) CODEC(ZSTD(1)),
    `duration_quantiles_state` AggregateFunction(quantiles(0.5, 0.75, 0.9, 0.95, 0.99), Float64) CODEC(Default),
    `error_count` SimpleAggregateFunction(sum, UInt64) CODEC(T64, ZSTD(1)),
    `total_count` SimpleAggregateFunction(sum, UInt64) CODEC(T64, ZSTD(1)),
    `timestamp` DateTime CODEC(DoubleDelta, LZ4),
    `deployment_environment` LowCardinality(String) CODEC(ZSTD(1)),
    `k8s_cluster_name` LowCardinality(String) CODEC(ZSTD(1)),
    `k8s_namespace_name` LowCardinality(String) CODEC(ZSTD(1))
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(timestamp)
ORDER BY (timestamp, src, dest, deployment_environment, k8s_cluster_name, k8s_namespace_name)
TTL toDateTime(timestamp) + toIntervalSecond(1296000)
SETTINGS index_granularity = 8192
;

CREATE TABLE IF NOT EXISTS {db:traces}.durationSort ON CLUSTER {cluster}
(
    `timestamp` DateTime64(9) CODEC(DoubleDelta, LZ4),
    `traceID` FixedString(32) CODEC(ZSTD(1)),
    `spanID` String CODEC(ZSTD(1)),
    `parentSpanID` String CODEC(ZSTD(1)),
    `serviceName` LowCardinality(String) CODEC(ZSTD(1)),
    `name` LowCardinality(String) CODEC(ZSTD(1)),
    `kind` Int8 CODEC(T64, ZSTD(1)),
    `durationNano` UInt64 CODEC(T64, ZSTD(1)),
    `statusCode` Int16 CODEC(T64, ZSTD(1)),
    `httpMethod` LowCardinality(String) CODEC(ZSTD(1)),
    `httpUrl` LowCardinality(String) CODEC(ZSTD(1)),
    `httpRoute` LowCardinality(String) CODEC(ZSTD(1)),
    `httpHost` LowCardinality(String) CODEC(ZSTD(1)),
    `hasError` Bool CODEC(T64, ZSTD(1)),
    `rpcSystem` LowCardinality(String) CODEC(ZSTD(1)),
    `rpcService` LowCardinality(String) CODEC(ZSTD(1)),
    `rpcMethod` LowCardinality(String) CODEC(ZSTD(1)),
    `responseStatusCode` LowCardinality(String) CODEC(ZSTD(1)),
    `stringTagMap` Map(String, String) CODEC(ZSTD(1)),
    `numberTagMap` Map(String, Float64) CODEC(ZSTD(1)),
    `boolTagMap` Map(String, Bool) CODEC(ZSTD(1)),
    `isRemote` LowCardinality(String) CODEC(ZSTD(1)),
    `statusMessage` String CODEC(ZSTD(1)),
    `statusCodeString` String CODEC(ZSTD(1)),
    `spanKind` String CODEC(ZSTD(1)),
    INDEX idx_service serviceName TYPE bloom_filter GRANULARITY 4,
    INDEX idx_name name TYPE bloom_filter GRANULARITY 4,
    INDEX idx_kind kind TYPE minmax GRANULARITY 4,
    INDEX idx_duration durationNano TYPE minmax GRANULARITY 1,
    INDEX idx_hasError hasError TYPE set(2) GRANULARITY 1,
    INDEX idx_httpRoute httpRoute TYPE bloom_filter GRANULARITY 4,
    INDEX idx_httpUrl httpUrl TYPE bloom_filter GRANULARITY 4,
    INDEX idx_httpHost httpHost TYPE bloom_filter GRANULARITY 4,
    INDEX idx_httpMethod httpMethod TYPE bloom_filter GRANULARITY 4,
    INDEX idx_timestamp timestamp TYPE minmax GRANULARITY 1,
    INDEX idx_rpcMethod rpcMethod TYPE bloom_filter GRANULARITY 4,
    INDEX idx_responseStatusCode responseStatusCode TYPE set(0) GRANULARITY 1
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(timestamp)
ORDER BY (durationNano, timestamp)
TTL toDateTime(timestamp) + toIntervalSecond(1296000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
;

CREATE TABLE IF NOT EXISTS {db:traces}.schema_migrations_v2 ON CLUSTER {cluster}
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
;

CREATE TABLE IF NOT EXISTS {db:traces}.signoz_error_index_v2 ON CLUSTER {cluster}
(
    `timestamp` DateTime64(9) CODEC(DoubleDelta, LZ4),
    `errorID` FixedString(32) CODEC(ZSTD(1)),
    `groupID` FixedString(32) CODEC(ZSTD(1)),
    `traceID` FixedString(32) CODEC(ZSTD(1)),
    `spanID` String CODEC(ZSTD(1)),
    `serviceName` LowCardinality(String) CODEC(ZSTD(1)),
    `exceptionType` LowCardinality(String) CODEC(ZSTD(1)),
    `exceptionMessage` String CODEC(ZSTD(1)),
    `exceptionStacktrace` String CODEC(ZSTD(1)),
    `exceptionEscaped` Bool CODEC(T64, ZSTD(1)),
    `resourceTagsMap` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    INDEX idx_error_id errorID TYPE bloom_filter GRANULARITY 4,
    INDEX idx_resourceTagsMapKeys mapKeys(resourceTagsMap) TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_resourceTagsMapValues mapValues(resourceTagsMap) TYPE bloom_filter(0.01) GRANULARITY 64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(timestamp)
ORDER BY (timestamp, groupID)
TTL toDateTime(timestamp) + toIntervalSecond(1296000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
;

CREATE TABLE IF NOT EXISTS {db:traces}.signoz_index_v2 ON CLUSTER {cluster}
(
    `timestamp` DateTime64(9) CODEC(DoubleDelta, LZ4),
    `traceID` FixedString(32) CODEC(ZSTD(1)),
    `spanID` String CODEC(ZSTD(1)),
    `parentSpanID` String CODEC(ZSTD(1)),
    `serviceName` LowCardinality(String) CODEC(ZSTD(1)),
    `name` LowCardinality(String) CODEC(ZSTD(1)),
    `kind` Int8 CODEC(T64, ZSTD(1)),
    `durationNano` UInt64 CODEC(T64, ZSTD(1)),
    `statusCode` Int16 CODEC(T64, ZSTD(1)),
    `externalHttpMethod` LowCardinality(String) CODEC(ZSTD(1)),
    `externalHttpUrl` LowCardinality(String) CODEC(ZSTD(1)),
    `dbSystem` LowCardinality(String) CODEC(ZSTD(1)),
    `dbName` LowCardinality(String) CODEC(ZSTD(1)),
    `dbOperation` LowCardinality(String) CODEC(ZSTD(1)),
    `peerService` LowCardinality(String) CODEC(ZSTD(1)),
    `events` Array(String) CODEC(ZSTD(2)),
    `httpMethod` LowCardinality(String) CODEC(ZSTD(1)),
    `httpUrl` LowCardinality(String) CODEC(ZSTD(1)),
    `httpRoute` LowCardinality(String) CODEC(ZSTD(1)),
    `httpHost` LowCardinality(String) CODEC(ZSTD(1)),
    `msgSystem` LowCardinality(String) CODEC(ZSTD(1)),
    `msgOperation` LowCardinality(String) CODEC(ZSTD(1)),
    `hasError` Bool CODEC(T64, ZSTD(1)),
    `rpcSystem` LowCardinality(String) CODEC(ZSTD(1)),
    `rpcService` LowCardinality(String) CODEC(ZSTD(1)),
    `rpcMethod` LowCardinality(String) CODEC(ZSTD(1)),
    `responseStatusCode` LowCardinality(String) CODEC(ZSTD(1)),
    `stringTagMap` Map(String, String) CODEC(ZSTD(1)),
    `numberTagMap` Map(String, Float64) CODEC(ZSTD(1)),
    `boolTagMap` Map(String, Bool) CODEC(ZSTD(1)),
    `resourceTagsMap` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `isRemote` LowCardinality(String) CODEC(ZSTD(1)),
    `statusMessage` String CODEC(ZSTD(1)),
    `statusCodeString` String CODEC(ZSTD(1)),
    `spanKind` String CODEC(ZSTD(1)),
    INDEX idx_service serviceName TYPE bloom_filter GRANULARITY 4,
    INDEX idx_name name TYPE bloom_filter GRANULARITY 4,
    INDEX idx_kind kind TYPE minmax GRANULARITY 4,
    INDEX idx_duration durationNano TYPE minmax GRANULARITY 1,
    INDEX idx_hasError hasError TYPE set(2) GRANULARITY 1,
    INDEX idx_httpRoute httpRoute TYPE bloom_filter GRANULARITY 4,
    INDEX idx_httpUrl httpUrl TYPE bloom_filter GRANULARITY 4,
    INDEX idx_httpHost httpHost TYPE bloom_filter GRANULARITY 4,
    INDEX idx_httpMethod httpMethod TYPE bloom_filter GRANULARITY 4,
    INDEX idx_timestamp timestamp TYPE minmax GRANULARITY 1,
    INDEX idx_rpcMethod rpcMethod TYPE bloom_filter GRANULARITY 4,
    INDEX idx_responseStatusCode responseStatusCode TYPE set(0) GRANULARITY 1,
    INDEX idx_resourceTagsMapKeys mapKeys(resourceTagsMap) TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_resourceTagsMapValues mapValues(resourceTagsMap) TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_statusCodeString statusCodeString TYPE set(3) GRANULARITY 4,
    INDEX idx_spanKind spanKind TYPE set(5) GRANULARITY 4,
    INDEX idx_stringTagMapKeys mapKeys(stringTagMap) TYPE tokenbf_v1(1024, 2, 0) GRANULARITY 1,
    INDEX idx_stringTagMapValues mapValues(stringTagMap) TYPE ngrambf_v1(4, 5000, 2, 0) GRANULARITY 1,
    INDEX idx_numberTagMapKeys mapKeys(numberTagMap) TYPE tokenbf_v1(1024, 2, 0) GRANULARITY 1,
    INDEX idx_numberTagMapValues mapValues(numberTagMap) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_boolTagMapKeys mapKeys(boolTagMap) TYPE tokenbf_v1(1024, 2, 0) GRANULARITY 1,
    INDEX idx_resourceTagMapKeys mapKeys(resourceTagsMap) TYPE tokenbf_v1(1024, 2, 0) GRANULARITY 1,
    INDEX idx_resourceTagMapValues mapValues(resourceTagsMap) TYPE ngrambf_v1(4, 5000, 2, 0) GRANULARITY 1,
    PROJECTION timestampSort
    (
        SELECT *
        ORDER BY timestamp
    )
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(timestamp)
PRIMARY KEY (serviceName, hasError, toStartOfHour(timestamp), name)
ORDER BY (serviceName, hasError, toStartOfHour(timestamp), name, timestamp)
TTL toDateTime(timestamp) + toIntervalSecond(1296000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
;

CREATE TABLE IF NOT EXISTS {db:traces}.signoz_index_v3 ON CLUSTER {cluster}
(
    `ts_bucket_start` UInt64 CODEC(DoubleDelta, LZ4),
    `resource_fingerprint` String CODEC(ZSTD(1)),
    `timestamp` DateTime64(9) CODEC(DoubleDelta, LZ4),
    `trace_id` FixedString(32) CODEC(ZSTD(1)),
    `span_id` String CODEC(ZSTD(1)),
    `trace_state` String CODEC(ZSTD(1)),
    `parent_span_id` String CODEC(ZSTD(1)),
    `flags` UInt32 CODEC(T64, ZSTD(1)),
    `name` LowCardinality(String) CODEC(ZSTD(1)),
    `kind` Int8 CODEC(T64, ZSTD(1)),
    `kind_string` String CODEC(ZSTD(1)),
    `duration_nano` UInt64 CODEC(T64, ZSTD(1)),
    `status_code` Int16 CODEC(T64, ZSTD(1)),
    `status_message` String CODEC(ZSTD(1)),
    `status_code_string` String CODEC(ZSTD(1)),
    `attributes_string` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `attributes_number` Map(LowCardinality(String), Float64) CODEC(ZSTD(1)),
    `attributes_bool` Map(LowCardinality(String), Bool) CODEC(ZSTD(1)),
    `attributes` JSON(max_dynamic_paths = 0) CODEC(ZSTD(1)),
    `attributes_promoted` JSON CODEC(ZSTD(1)),
    `resources_string` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `events` Array(String) CODEC(ZSTD(2)),
    `links` String CODEC(ZSTD(1)),
    `response_status_code` LowCardinality(String) CODEC(ZSTD(1)),
    `external_http_url` LowCardinality(String) CODEC(ZSTD(1)),
    `http_url` LowCardinality(String) CODEC(ZSTD(1)),
    `external_http_method` LowCardinality(String) CODEC(ZSTD(1)),
    `http_method` LowCardinality(String) CODEC(ZSTD(1)),
    `http_host` LowCardinality(String) CODEC(ZSTD(1)),
    `db_name` LowCardinality(String) CODEC(ZSTD(1)),
    `db_operation` LowCardinality(String) CODEC(ZSTD(1)),
    `has_error` Bool CODEC(T64, ZSTD(1)),
    `is_remote` LowCardinality(String) CODEC(ZSTD(1)),
    `resource_string_service$$name` LowCardinality(String) DEFAULT resources_string['service.name'] CODEC(ZSTD(1)),
    `attribute_string_http$$route` LowCardinality(String) DEFAULT attributes_string['http.route'] CODEC(ZSTD(1)),
    `attribute_string_messaging$$system` LowCardinality(String) DEFAULT attributes_string['messaging.system'] CODEC(ZSTD(1)),
    `attribute_string_messaging$$operation` LowCardinality(String) DEFAULT attributes_string['messaging.operation'] CODEC(ZSTD(1)),
    `attribute_string_db$$system` LowCardinality(String) DEFAULT attributes_string['db.system'] CODEC(ZSTD(1)),
    `attribute_string_rpc$$system` LowCardinality(String) DEFAULT attributes_string['rpc.system'] CODEC(ZSTD(1)),
    `attribute_string_rpc$$service` LowCardinality(String) DEFAULT attributes_string['rpc.service'] CODEC(ZSTD(1)),
    `attribute_string_rpc$$method` LowCardinality(String) DEFAULT attributes_string['rpc.method'] CODEC(ZSTD(1)),
    `attribute_string_peer$$service` LowCardinality(String) DEFAULT attributes_string['peer.service'] CODEC(ZSTD(1)),
    `traceID` FixedString(32) ALIAS trace_id,
    `spanID` String ALIAS span_id,
    `parentSpanID` String ALIAS parent_span_id,
    `spanKind` String ALIAS kind_string,
    `durationNano` UInt64 ALIAS duration_nano,
    `statusCode` Int16 ALIAS status_code,
    `statusMessage` String ALIAS status_message,
    `statusCodeString` String ALIAS status_code_string,
    `references` String ALIAS links,
    `responseStatusCode` LowCardinality(String) ALIAS response_status_code,
    `externalHttpUrl` LowCardinality(String) ALIAS external_http_url,
    `httpUrl` LowCardinality(String) ALIAS http_url,
    `externalHttpMethod` LowCardinality(String) ALIAS external_http_method,
    `httpMethod` LowCardinality(String) ALIAS http_method,
    `httpHost` LowCardinality(String) ALIAS http_host,
    `dbName` LowCardinality(String) ALIAS db_name,
    `dbOperation` LowCardinality(String) ALIAS db_operation,
    `hasError` Bool ALIAS has_error,
    `isRemote` LowCardinality(String) ALIAS is_remote,
    `serviceName` LowCardinality(String) ALIAS `resource_string_service$$name`,
    `httpRoute` LowCardinality(String) ALIAS `attribute_string_http$$route`,
    `msgSystem` LowCardinality(String) ALIAS `attribute_string_messaging$$system`,
    `msgOperation` LowCardinality(String) ALIAS `attribute_string_messaging$$operation`,
    `dbSystem` LowCardinality(String) ALIAS `attribute_string_db$$system`,
    `rpcSystem` LowCardinality(String) ALIAS `attribute_string_rpc$$system`,
    `rpcService` LowCardinality(String) ALIAS `attribute_string_rpc$$service`,
    `rpcMethod` LowCardinality(String) ALIAS `attribute_string_rpc$$method`,
    `peerService` LowCardinality(String) ALIAS `attribute_string_peer$$service`,
    `resource_string_service$$name_exists` Bool DEFAULT if(mapContains(resources_string, 'service.name') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_string_http$$route_exists` Bool DEFAULT if(mapContains(attributes_string, 'http.route') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_string_messaging$$system_exists` Bool DEFAULT if(mapContains(attributes_string, 'messaging.system') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_string_messaging$$operation_exists` Bool DEFAULT if(mapContains(attributes_string, 'messaging.operation') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_string_db$$system_exists` Bool DEFAULT if(mapContains(attributes_string, 'db.system') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_string_rpc$$system_exists` Bool DEFAULT if(mapContains(attributes_string, 'rpc.system') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_string_rpc$$service_exists` Bool DEFAULT if(mapContains(attributes_string, 'rpc.service') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_string_rpc$$method_exists` Bool DEFAULT if(mapContains(attributes_string, 'rpc.method') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_string_peer$$service_exists` Bool DEFAULT if(mapContains(attributes_string, 'peer.service') != 0, true, false) CODEC(ZSTD(1)),
    `resource` JSON(max_dynamic_paths = 100) CODEC(ZSTD(1)),
    `scope` JSON(max_dynamic_paths = 0, attributes JSON(max_dynamic_paths = 0), name String, version String) CODEC(ZSTD(1)),
    `inserted_at` DateTime64(3) CODEC(Delta(8), ZSTD(1)),
    `created_at` DateTime64(3) DEFAULT now64(3) CODEC(Delta(8), ZSTD(1)),
    `attribute_string_gen_ai$$request$$model` LowCardinality(String) DEFAULT attributes_string['gen_ai.request.model'] CODEC(ZSTD(1)),
    `attribute_string_gen_ai$$request$$model_exists` Bool DEFAULT if(mapContains(attributes_string, 'gen_ai.request.model') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_string_gen_ai$$provider$$name` LowCardinality(String) DEFAULT attributes_string['gen_ai.provider.name'] CODEC(ZSTD(1)),
    `attribute_string_gen_ai$$provider$$name_exists` Bool DEFAULT if(mapContains(attributes_string, 'gen_ai.provider.name') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_string_gen_ai$$tool$$name` LowCardinality(String) DEFAULT attributes_string['gen_ai.tool.name'] CODEC(ZSTD(1)),
    `attribute_string_gen_ai$$tool$$name_exists` Bool DEFAULT if(mapContains(attributes_string, 'gen_ai.tool.name') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_string_gen_ai$$agent$$name` LowCardinality(String) DEFAULT attributes_string['gen_ai.agent.name'] CODEC(ZSTD(1)),
    `attribute_string_gen_ai$$agent$$name_exists` Bool DEFAULT if(mapContains(attributes_string, 'gen_ai.agent.name') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_number_signoz$$gen_ai$$usage$$tokens$$cost` Float64 DEFAULT attributes_number['signoz.gen_ai.usage.tokens.cost'] CODEC(ZSTD(1)),
    `attribute_number_signoz$$gen_ai$$usage$$tokens$$cost_exists` Bool DEFAULT if(mapContains(attributes_number, 'signoz.gen_ai.usage.tokens.cost') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_number_gen_ai$$usage$$input_tokens` Float64 DEFAULT attributes_number['gen_ai.usage.input_tokens'] CODEC(ZSTD(1)),
    `attribute_number_gen_ai$$usage$$input_tokens_exists` Bool DEFAULT if(mapContains(attributes_number, 'gen_ai.usage.input_tokens') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_number_gen_ai$$usage$$output_tokens` Float64 DEFAULT attributes_number['gen_ai.usage.output_tokens'] CODEC(ZSTD(1)),
    `attribute_number_gen_ai$$usage$$output_tokens_exists` Bool DEFAULT if(mapContains(attributes_number, 'gen_ai.usage.output_tokens') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_number_gen_ai$$usage$$cache_read$$input_tokens` Float64 DEFAULT attributes_number['gen_ai.usage.cache_read.input_tokens'] CODEC(ZSTD(1)),
    `attribute_number_gen_ai$$usage$$cache_read$$input_tokens_exists` Bool DEFAULT if(mapContains(attributes_number, 'gen_ai.usage.cache_read.input_tokens') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_number_gen_ai$$usage$$cache_creation$$input_tokens` Float64 DEFAULT attributes_number['gen_ai.usage.cache_creation.input_tokens'] CODEC(ZSTD(1)),
    `attribute_number_gen_ai$$usage$$cache_creation$$input_tokens_exists` Bool DEFAULT if(mapContains(attributes_number, 'gen_ai.usage.cache_creation.input_tokens') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_number_signoz$$gen_ai$$usage$$input_tokens$$cost` Float64 DEFAULT attributes_number['signoz.gen_ai.usage.input_tokens.cost'] CODEC(ZSTD(1)),
    `attribute_number_signoz$$gen_ai$$usage$$input_tokens$$cost_exists` Bool DEFAULT if(mapContains(attributes_number, 'signoz.gen_ai.usage.input_tokens.cost') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_number_signoz$$gen_ai$$usage$$output_tokens$$cost` Float64 DEFAULT attributes_number['signoz.gen_ai.usage.output_tokens.cost'] CODEC(ZSTD(1)),
    `attribute_number_signoz$$gen_ai$$usage$$output_tokens$$cost_exists` Bool DEFAULT if(mapContains(attributes_number, 'signoz.gen_ai.usage.output_tokens.cost') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_number_signoz$$gen_ai$$usage$$cache_read$$input_tokens$$cost` Float64 DEFAULT attributes_number['signoz.gen_ai.usage.cache_read.input_tokens.cost'] CODEC(ZSTD(1)),
    `attribute_number_signoz$$gen_ai$$usage$$cache_read$$input_tokens$$cost_exists` Bool DEFAULT if(mapContains(attributes_number, 'signoz.gen_ai.usage.cache_read.input_tokens.cost') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_number_signoz$$gen_ai$$usage$$cache_write$$input_tokens$$cost` Float64 DEFAULT attributes_number['signoz.gen_ai.usage.cache_write.input_tokens.cost'] CODEC(ZSTD(1)),
    `attribute_number_signoz$$gen_ai$$usage$$cache_write$$input_tokens$$cost_exists` Bool DEFAULT if(mapContains(attributes_number, 'signoz.gen_ai.usage.cache_write.input_tokens.cost') != 0, true, false) CODEC(ZSTD(1)),
    INDEX idx_trace_id trace_id TYPE tokenbf_v1(10000, 5, 0) GRANULARITY 1,
    INDEX idx_span_id span_id TYPE tokenbf_v1(5000, 5, 0) GRANULARITY 1,
    INDEX idx_duration duration_nano TYPE minmax GRANULARITY 1,
    INDEX idx_name name TYPE ngrambf_v1(4, 5000, 2, 0) GRANULARITY 1,
    INDEX idx_kind kind TYPE minmax GRANULARITY 4,
    INDEX idx_http_route `attribute_string_http$$route` TYPE bloom_filter GRANULARITY 4,
    INDEX idx_http_url http_url TYPE bloom_filter GRANULARITY 4,
    INDEX idx_http_host http_host TYPE bloom_filter GRANULARITY 4,
    INDEX idx_http_method http_method TYPE bloom_filter GRANULARITY 4,
    INDEX idx_timestamp timestamp TYPE minmax GRANULARITY 1,
    INDEX idx_rpc_method `attribute_string_rpc$$method` TYPE bloom_filter GRANULARITY 4,
    INDEX idx_response_statusCode response_status_code TYPE set(0) GRANULARITY 1,
    INDEX idx_status_code_string status_code_string TYPE set(3) GRANULARITY 4,
    INDEX idx_kind_string kind_string TYPE set(5) GRANULARITY 4,
    INDEX attributes_string_idx_key mapKeys(attributes_string) TYPE tokenbf_v1(1024, 2, 0) GRANULARITY 1,
    INDEX attributes_string_idx_val mapValues(attributes_string) TYPE ngrambf_v1(4, 5000, 2, 0) GRANULARITY 1,
    INDEX attributes_number_idx_key mapKeys(attributes_number) TYPE tokenbf_v1(1024, 2, 0) GRANULARITY 1,
    INDEX attributes_number_idx_val mapValues(attributes_number) TYPE bloom_filter GRANULARITY 1,
    INDEX attributes_bool_idx_key mapKeys(attributes_bool) TYPE tokenbf_v1(1024, 2, 0) GRANULARITY 1,
    INDEX resources_string_idx_key mapKeys(resources_string) TYPE tokenbf_v1(1024, 2, 0) GRANULARITY 1,
    INDEX resources_string_idx_val mapValues(resources_string) TYPE ngrambf_v1(4, 5000, 2, 0) GRANULARITY 1,
    INDEX attributes_paths_tokenbf JSONAllPaths(attributes) TYPE tokenbf_v1(1024, 2, 0) GRANULARITY 1,
    INDEX attributes_promoted_paths_tokenbf JSONAllPaths(attributes_promoted) TYPE tokenbf_v1(1024, 2, 0) GRANULARITY 1,
    INDEX idx_gen_ai_request_model `attribute_string_gen_ai$$request$$model` TYPE bloom_filter GRANULARITY 1,
    INDEX idx_gen_ai_provider_name `attribute_string_gen_ai$$provider$$name` TYPE bloom_filter GRANULARITY 1,
    INDEX idx_gen_ai_span_exists (`attribute_string_gen_ai$$request$$model_exists`, `attribute_string_gen_ai$$tool$$name_exists`, `attribute_string_gen_ai$$agent$$name_exists`) TYPE set(8) GRANULARITY 1,
    INDEX idx_attributes_http_route CAST(attributes.http.route, 'String') TYPE bloom_filter GRANULARITY 4,
    INDEX idx_attributes_rpc_method CAST(attributes.rpc.method, 'String') TYPE bloom_filter GRANULARITY 4,
    INDEX idx_attributes_gen_ai_request_model CAST(attributes.gen_ai.request.model, 'String') TYPE bloom_filter GRANULARITY 1,
    INDEX idx_attributes_gen_ai_provider_name CAST(attributes.gen_ai.provider.name, 'String') TYPE bloom_filter GRANULARITY 1
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(timestamp)
ORDER BY (ts_bucket_start, resource_fingerprint, has_error, name, timestamp)
TTL toDateTime(timestamp) + toIntervalSecond(1296000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, object_serialization_version = 'v3', object_shared_data_serialization_version = 'advanced'
;

CREATE TABLE IF NOT EXISTS {db:traces}.signoz_spans ON CLUSTER {cluster}
(
    `timestamp` DateTime64(9) CODEC(DoubleDelta, LZ4),
    `traceID` FixedString(32) CODEC(ZSTD(1)),
    `model` String CODEC(ZSTD(9))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(timestamp)
ORDER BY traceID
TTL toDateTime(timestamp) + toIntervalSecond(1296000)
SETTINGS index_granularity = 1024, ttl_only_drop_parts = 1
;

CREATE TABLE IF NOT EXISTS {db:traces}.span_attributes ON CLUSTER {cluster}
(
    `timestamp` DateTime CODEC(DoubleDelta, ZSTD(1)),
    `tagKey` LowCardinality(String) CODEC(ZSTD(1)),
    `tagType` Enum8('tag' = 1, 'resource' = 2) CODEC(ZSTD(1)),
    `dataType` Enum8('string' = 1, 'bool' = 2, 'float64' = 3) CODEC(ZSTD(1)),
    `stringTagValue` String CODEC(ZSTD(1)),
    `float64TagValue` Nullable(Float64) CODEC(ZSTD(1)),
    `isColumn` Bool CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (tagKey, tagType, dataType, stringTagValue, float64TagValue, isColumn)
TTL toDateTime(timestamp) + toIntervalSecond(172800)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, allow_nullable_key = 1
;

CREATE TABLE IF NOT EXISTS {db:traces}.span_attributes_keys ON CLUSTER {cluster}
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
;

CREATE TABLE IF NOT EXISTS {db:traces}.tag_attributes_v2 ON CLUSTER {cluster}
(
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `tag_key` String CODEC(ZSTD(1)),
    `tag_type` LowCardinality(String) CODEC(ZSTD(1)),
    `tag_data_type` LowCardinality(String) CODEC(ZSTD(1)),
    `string_value` String CODEC(ZSTD(1)),
    `number_value` Nullable(Float64) CODEC(ZSTD(1)),
    INDEX string_value_index string_value TYPE ngrambf_v1(4, 1024, 3, 0) GRANULARITY 1,
    INDEX number_value_index number_value TYPE minmax GRANULARITY 1
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(unix_milli / 1000)
ORDER BY (tag_key, tag_type, tag_data_type, string_value, number_value)
TTL toDateTime(unix_milli / 1000) + toIntervalSecond(1296000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, allow_nullable_key = 1
;

CREATE TABLE IF NOT EXISTS {db:traces}.top_level_operations ON CLUSTER {cluster}
(
    `name` LowCardinality(String) CODEC(ZSTD(1)),
    `serviceName` LowCardinality(String) CODEC(ZSTD(1)),
    `time` DateTime DEFAULT now() CODEC(ZSTD(1))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (serviceName, name)
TTL time + toIntervalMonth(1)
SETTINGS index_granularity = 8192
;

CREATE TABLE IF NOT EXISTS {db:traces}.trace_summary ON CLUSTER {cluster}
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
;

CREATE TABLE IF NOT EXISTS {db:traces}.traces_v3_resource ON CLUSTER {cluster}
(
    `labels` String CODEC(ZSTD(5)),
    `fingerprint` String CODEC(ZSTD(1)),
    `seen_at_ts_bucket_start` Int64 CODEC(Delta(8), ZSTD(1)),
    INDEX idx_labels lower(labels) TYPE ngrambf_v1(4, 1024, 3, 0) GRANULARITY 1,
    INDEX idx_labels_v1 labels TYPE ngrambf_v1(4, 1024, 3, 0) GRANULARITY 1
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(seen_at_ts_bucket_start)
ORDER BY (labels, fingerprint, seen_at_ts_bucket_start)
TTL (toDateTime(seen_at_ts_bucket_start) + toIntervalSecond(1296000)) + toIntervalSecond(1800)
SETTINGS ttl_only_drop_parts = 1, index_granularity = 8192
;

CREATE TABLE IF NOT EXISTS {db:traces}.usage ON CLUSTER {cluster}
(
    `tenant` String CODEC(ZSTD(1)),
    `collector_id` String CODEC(ZSTD(1)),
    `exporter_id` String CODEC(ZSTD(1)),
    `timestamp` DateTime CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (tenant, collector_id, exporter_id, timestamp)
TTL timestamp + toIntervalDay(3)
SETTINGS index_granularity = 8192
;

CREATE TABLE IF NOT EXISTS {db:traces}.usage_explorer ON CLUSTER {cluster}
(
    `timestamp` DateTime64(9) CODEC(DoubleDelta, LZ4),
    `service_name` LowCardinality(String) CODEC(ZSTD(1)),
    `count` UInt64 CODEC(T64, ZSTD(1))
)
ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toDate(timestamp)
ORDER BY (timestamp, service_name)
TTL toDateTime(timestamp) + toIntervalSecond(1296000)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
;

CREATE TABLE IF NOT EXISTS {db:analytics}.distributed_rule_state_history_v0 ON CLUSTER {cluster}
(
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
ENGINE = Distributed('{cluster}', '{db:analytics}', 'rule_state_history_v0', cityHash64(rule_id, rule_name, fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:analytics}.distributed_schema_migrations_v2 ON CLUSTER {cluster}
(
    `migration_id` UInt64,
    `status` String,
    `error` String,
    `created_at` DateTime64(9),
    `updated_at` DateTime64(9)
)
ENGINE = Distributed('{cluster}', '{db:analytics}', 'schema_migrations_v2', rand())
;

CREATE TABLE IF NOT EXISTS {db:logs}.distributed_logs_attribute_keys ON CLUSTER {cluster}
(
    `name` String,
    `datatype` String
)
ENGINE = Distributed('{cluster}', '{db:logs}', 'logs_attribute_keys', cityHash64(datatype))
;

CREATE TABLE IF NOT EXISTS {db:logs}.distributed_logs_resource_keys ON CLUSTER {cluster}
(
    `name` String,
    `datatype` String
)
ENGINE = Distributed('{cluster}', '{db:logs}', 'logs_resource_keys', cityHash64(datatype))
;

CREATE TABLE IF NOT EXISTS {db:logs}.distributed_logs_v2 ON CLUSTER {cluster}
(
    `ts_bucket_start` UInt64 CODEC(DoubleDelta, LZ4),
    `resource_fingerprint` String CODEC(ZSTD(1)),
    `timestamp` UInt64 CODEC(DoubleDelta, LZ4),
    `observed_timestamp` UInt64 CODEC(DoubleDelta, LZ4),
    `id` String CODEC(ZSTD(1)),
    `trace_id` String CODEC(ZSTD(1)),
    `span_id` String CODEC(ZSTD(1)),
    `trace_flags` UInt32,
    `severity_text` LowCardinality(String) CODEC(ZSTD(1)),
    `severity_number` UInt8,
    `body` String CODEC(ZSTD(2)),
    `body_v2` JSON(max_dynamic_paths = 0, message String) CODEC(ZSTD(1)),
    `body_promoted` JSON CODEC(ZSTD(1)),
    `attributes_string` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `attributes_number` Map(LowCardinality(String), Float64) CODEC(ZSTD(1)),
    `attributes_bool` Map(LowCardinality(String), Bool) CODEC(ZSTD(1)),
    `resources_string` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `scope_name` String CODEC(ZSTD(1)),
    `scope_version` String CODEC(ZSTD(1)),
    `scope_string` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `_retention_days` UInt16 DEFAULT 15,
    `_retention_days_cold` UInt16 DEFAULT 0,
    `resource` JSON(max_dynamic_paths = 100) CODEC(ZSTD(1)),
    `inserted_at` DateTime64(3) CODEC(Delta(8), ZSTD(1)),
    `created_at` DateTime64(3) DEFAULT now64(3) CODEC(Delta(8), ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:logs}', 'logs_v2', cityHash64(id))
;

CREATE TABLE IF NOT EXISTS {db:logs}.distributed_logs_v2_resource ON CLUSTER {cluster}
(
    `labels` String CODEC(ZSTD(5)),
    `fingerprint` String CODEC(ZSTD(1)),
    `seen_at_ts_bucket_start` Int64 CODEC(Delta(8), ZSTD(1)),
    `_retention_days` UInt16 DEFAULT 15,
    `_retention_days_cold` UInt16 DEFAULT 0
)
ENGINE = Distributed('{cluster}', '{db:logs}', 'logs_v2_resource', cityHash64(labels, fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:logs}.distributed_schema_migrations_v2 ON CLUSTER {cluster}
(
    `migration_id` UInt64,
    `status` String,
    `error` String,
    `created_at` DateTime64(9),
    `updated_at` DateTime64(9)
)
ENGINE = Distributed('{cluster}', '{db:logs}', 'schema_migrations_v2', rand())
;

CREATE TABLE IF NOT EXISTS {db:logs}.distributed_tag_attributes_v2 ON CLUSTER {cluster}
(
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `tag_key` String CODEC(ZSTD(1)),
    `tag_type` LowCardinality(String) CODEC(ZSTD(1)),
    `tag_data_type` LowCardinality(String) CODEC(ZSTD(1)),
    `string_value` String CODEC(ZSTD(1)),
    `number_value` Nullable(Float64) CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:logs}', 'tag_attributes_v2', cityHash64(rand()))
;

CREATE TABLE IF NOT EXISTS {db:logs}.distributed_usage ON CLUSTER {cluster}
(
    `tenant` String CODEC(ZSTD(1)),
    `collector_id` String CODEC(ZSTD(1)),
    `exporter_id` String CODEC(ZSTD(1)),
    `timestamp` DateTime CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:logs}', 'usage', cityHash64(rand()))
;

CREATE TABLE IF NOT EXISTS {db:metadata}.distributed_attributes_metadata ON CLUSTER {cluster}
(
    `unix_milli` UInt64,
    `data_source` String,
    `resource_fingerprint` UInt64,
    `attrs_fingerprint` UInt64,
    `resource_attributes` Map(LowCardinality(String), String),
    `attributes` Map(LowCardinality(String), String)
)
ENGINE = Distributed('{cluster}', '{db:metadata}', 'attributes_metadata', cityHash64(data_source, resource_fingerprint, attrs_fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:metadata}.distributed_column_evolution_metadata ON CLUSTER {cluster}
(
    `signal` String CODEC(ZSTD(1)),
    `column_name` String CODEC(ZSTD(1)),
    `column_type` String CODEC(ZSTD(1)),
    `field_context` String CODEC(ZSTD(1)),
    `field_name` String CODEC(ZSTD(1)),
    `version` UInt32 CODEC(DoubleDelta, ZSTD(1)),
    `release_time` SimpleAggregateFunction(min, Float64) CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metadata}', 'column_evolution_metadata', cityHash64(signal, column_name))
;

CREATE TABLE IF NOT EXISTS {db:metadata}.distributed_field_keys ON CLUSTER {cluster}
(
    `signal` String CODEC(ZSTD(1)),
    `field_context` String CODEC(ZSTD(1)),
    `field_name` String CODEC(ZSTD(1)),
    `field_data_type` String CODEC(ZSTD(1)),
    `last_seen` UInt64 CODEC(DoubleDelta, LZ4)
)
ENGINE = Distributed('{cluster}', '{db:metadata}', 'field_keys', cityHash64(signal, field_context, field_name))
;

CREATE TABLE IF NOT EXISTS {db:metadata}.distributed_schema_migrations_v2 ON CLUSTER {cluster}
(
    `migration_id` UInt64,
    `status` String,
    `error` String,
    `created_at` DateTime64(9),
    `updated_at` DateTime64(9)
)
ENGINE = Distributed('{cluster}', '{db:metadata}', 'schema_migrations_v2', rand())
;

CREATE TABLE IF NOT EXISTS {db:meter}.distributed_samples ON CLUSTER {cluster}
(
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT 'default',
    `unit` LowCardinality(String) DEFAULT 'default',
    `type` LowCardinality(String) DEFAULT 'default',
    `is_monotonic` Bool DEFAULT false,
    `labels` String CODEC(ZSTD(5)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(DoubleDelta, ZSTD(1)),
    `value` Float64 CODEC(Gorilla(8), ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:meter}', 'samples', cityHash64(temporality, metric_name, fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:meter}.distributed_samples_agg_1d ON CLUSTER {cluster}
(
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT 'default',
    `unit` LowCardinality(String) DEFAULT 'default',
    `type` LowCardinality(String) DEFAULT 'default',
    `is_monotonic` Bool DEFAULT false,
    `labels` String CODEC(ZSTD(5)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(DoubleDelta, ZSTD(1)),
    `last` SimpleAggregateFunction(anyLast, Float64) CODEC(ZSTD(1)),
    `min` SimpleAggregateFunction(min, Float64) CODEC(ZSTD(1)),
    `max` SimpleAggregateFunction(max, Float64) CODEC(ZSTD(1)),
    `sum` SimpleAggregateFunction(sum, Float64) CODEC(ZSTD(1)),
    `count` SimpleAggregateFunction(sum, UInt64) CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:meter}', 'samples_agg_1d', cityHash64(temporality, metric_name, fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:meter}.distributed_schema_migrations_v2 ON CLUSTER {cluster}
(
    `migration_id` UInt64,
    `status` String,
    `error` String,
    `created_at` DateTime64(9),
    `updated_at` DateTime64(9)
)
ENGINE = Distributed('{cluster}', '{db:meter}', 'schema_migrations_v2', rand())
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_exp_hist ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(DoubleDelta, ZSTD(1)),
    `count` UInt64 CODEC(ZSTD(1)),
    `sum` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `min` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `max` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `sketch` AggregateFunction(quantilesDD(0.01, 0.5, 0.75, 0.9, 0.95, 0.99), UInt64) CODEC(ZSTD(1)),
    `flags` UInt32 DEFAULT 0 CODEC(ZSTD(1)),
    `inserted_at_unix_milli` Int64 CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'exp_hist', cityHash64(env, temporality, metric_name, fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_metadata ON CLUSTER {cluster}
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
ENGINE = Distributed('{cluster}', '{db:metrics}', 'metadata', rand())
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_metric_reduction_rules ON CLUSTER {cluster}
(
    `metric_name` LowCardinality(String) CODEC(ZSTD(1)),
    `labels` Array(LowCardinality(String)) CODEC(ZSTD(1)),
    `match_type` LowCardinality(String) DEFAULT 'drop' CODEC(ZSTD(1)),
    `effective_from_unix_milli` Int64 CODEC(ZSTD(1)),
    `deleted` Bool DEFAULT false CODEC(ZSTD(1)),
    `updated_at` DateTime64(3) DEFAULT now64(3) CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'metric_reduction_rules', cityHash64(metric_name))
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_samples_v2 ON CLUSTER {cluster}
(
    `metric_name` LowCardinality(String),
    `fingerprint` UInt64 CODEC(ZSTD(1)),
    `timestamp_ms` Int64 CODEC(DoubleDelta, LZ4),
    `value` Float64 CODEC(Gorilla(8), LZ4)
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'samples_v2', cityHash64(metric_name, fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_samples_v4 ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(DoubleDelta, ZSTD(1)),
    `value` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `flags` UInt32 DEFAULT 0 CODEC(ZSTD(1)),
    `inserted_at_unix_milli` Int64 CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'samples_v4', cityHash64(env, temporality, metric_name, fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_samples_v4_agg_30m ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `fingerprint` UInt64 CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `last` SimpleAggregateFunction(anyLast, Float64) CODEC(ZSTD(1)),
    `min` SimpleAggregateFunction(min, Float64) CODEC(ZSTD(1)),
    `max` SimpleAggregateFunction(max, Float64) CODEC(ZSTD(1)),
    `sum` SimpleAggregateFunction(sum, Float64) CODEC(ZSTD(1)),
    `count` SimpleAggregateFunction(sum, UInt64) CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'samples_v4_agg_30m', cityHash64(env, temporality, metric_name, fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_samples_v4_agg_5m ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `fingerprint` UInt64 CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `last` SimpleAggregateFunction(anyLast, Float64) CODEC(ZSTD(1)),
    `min` SimpleAggregateFunction(min, Float64) CODEC(ZSTD(1)),
    `max` SimpleAggregateFunction(max, Float64) CODEC(ZSTD(1)),
    `sum` SimpleAggregateFunction(sum, Float64) CODEC(ZSTD(1)),
    `count` SimpleAggregateFunction(sum, UInt64) CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'samples_v4_agg_5m', cityHash64(env, temporality, metric_name, fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_samples_v4_buffer ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `reduced_fingerprint` UInt64 DEFAULT 0 CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(DoubleDelta, ZSTD(1)),
    `value` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `flags` UInt32 DEFAULT 0 CODEC(ZSTD(1)),
    `inserted_at_unix_milli` Int64 CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'samples_v4_buffer', cityHash64(env, temporality, metric_name, if(reduced_fingerprint != 0, reduced_fingerprint, fingerprint)))
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_samples_v4_reduced_last_30m ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64 CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `last` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `min` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `max` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `sum` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `sum_values` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `count` UInt64 CODEC(ZSTD(1)),
    `sum_count_series` UInt64 CODEC(ZSTD(1)),
    `count_samples` UInt64 CODEC(ZSTD(1)),
    `computed_at` DateTime DEFAULT now() CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'samples_v4_reduced_last_30m', cityHash64(env, temporality, metric_name, reduced_fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_samples_v4_reduced_last_5m ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64 CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `last` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `min` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `max` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `sum` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `sum_values` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `count` UInt64 CODEC(ZSTD(1)),
    `sum_count_series` UInt64 CODEC(ZSTD(1)),
    `count_samples` UInt64 CODEC(ZSTD(1)),
    `computed_at` DateTime DEFAULT now() CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'samples_v4_reduced_last_5m', cityHash64(env, temporality, metric_name, reduced_fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_samples_v4_reduced_last_60s ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64 CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `sum_last` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `min` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `max` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `sum_values` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `count_series` UInt64 CODEC(ZSTD(1)),
    `count_samples` UInt64 CODEC(ZSTD(1)),
    `computed_at` DateTime DEFAULT now() CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'samples_v4_reduced_last_60s', cityHash64(env, temporality, metric_name, reduced_fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_samples_v4_reduced_sum_30m ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64 CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `sum` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `count` UInt64 CODEC(ZSTD(1)),
    `sum_count_series` UInt64 CODEC(ZSTD(1)),
    `count_samples` UInt64 CODEC(ZSTD(1)),
    `computed_at` DateTime DEFAULT now() CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'samples_v4_reduced_sum_30m', cityHash64(env, temporality, metric_name, reduced_fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_samples_v4_reduced_sum_5m ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64 CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `sum` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `count` UInt64 CODEC(ZSTD(1)),
    `sum_count_series` UInt64 CODEC(ZSTD(1)),
    `count_samples` UInt64 CODEC(ZSTD(1)),
    `computed_at` DateTime DEFAULT now() CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'samples_v4_reduced_sum_5m', cityHash64(env, temporality, metric_name, reduced_fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_samples_v4_reduced_sum_60s ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64 CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `sum` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `count_series` UInt64 CODEC(ZSTD(1)),
    `count_samples` UInt64 CODEC(ZSTD(1)),
    `computed_at` DateTime DEFAULT now() CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'samples_v4_reduced_sum_60s', cityHash64(env, temporality, metric_name, reduced_fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_schema_migrations_v2 ON CLUSTER {cluster}
(
    `migration_id` UInt64,
    `status` String,
    `error` String,
    `created_at` DateTime64(9),
    `updated_at` DateTime64(9)
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'schema_migrations_v2', rand())
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_time_series_v2 ON CLUSTER {cluster}
(
    `metric_name` LowCardinality(String),
    `fingerprint` UInt64 CODEC(ZSTD(1)),
    `timestamp_ms` Int64 CODEC(DoubleDelta, LZ4),
    `labels` String CODEC(ZSTD(5)),
    `temporality` LowCardinality(String) DEFAULT 'Unspecified' CODEC(ZSTD(5)),
    `description` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `unit` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `type` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'time_series_v2', cityHash64(metric_name, fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_time_series_v4 ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `unit` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `type` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `labels` String CODEC(ZSTD(5)),
    `attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `scope_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `resource_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `__normalized` Bool DEFAULT true CODEC(ZSTD(1)),
    `inserted_at_unix_milli` Int64 CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'time_series_v4', cityHash64(env, temporality, metric_name, fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_time_series_v4_1day ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `unit` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `type` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `labels` String CODEC(ZSTD(5)),
    `attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `scope_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `resource_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `__normalized` Bool DEFAULT true CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'time_series_v4_1day', cityHash64(env, temporality, metric_name, fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_time_series_v4_1week ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `unit` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `type` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `labels` String CODEC(ZSTD(5)),
    `attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `scope_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `resource_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `__normalized` Bool DEFAULT true CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'time_series_v4_1week', cityHash64(env, temporality, metric_name, fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_time_series_v4_6hrs ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `unit` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `type` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `labels` String CODEC(ZSTD(5)),
    `attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `scope_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `resource_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `__normalized` Bool DEFAULT true CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'time_series_v4_6hrs', cityHash64(env, temporality, metric_name, fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_time_series_v4_buffer ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `unit` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `type` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `reduced_fingerprint` UInt64 DEFAULT 0 CODEC(ZSTD(1)),
    `is_reduced` Bool DEFAULT false CODEC(ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `labels` String CODEC(ZSTD(5)),
    `attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `scope_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `resource_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `__normalized` Bool DEFAULT true CODEC(ZSTD(1)),
    `inserted_at_unix_milli` Int64 CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'time_series_v4_buffer', cityHash64(env, temporality, metric_name, if(reduced_fingerprint != 0, reduced_fingerprint, fingerprint)))
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_time_series_v4_reduced ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `unit` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `type` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `labels` String CODEC(ZSTD(5)),
    `attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `scope_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `resource_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `__normalized` Bool DEFAULT true CODEC(ZSTD(1)),
    `inserted_at_unix_milli` Int64 CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'time_series_v4_reduced', cityHash64(env, temporality, metric_name, fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_time_series_v4_reduced_1day ON CLUSTER {cluster}
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `unit` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `type` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `labels` String CODEC(ZSTD(5)),
    `attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `scope_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `resource_attrs` Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    `__normalized` Bool DEFAULT true CODEC(ZSTD(1)),
    `inserted_at_unix_milli` Int64 CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'time_series_v4_reduced_1day', cityHash64(env, temporality, metric_name, fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_updated_metadata ON CLUSTER {cluster}
(
    `metric_name` LowCardinality(String) CODEC(ZSTD(1)),
    `temporality` LowCardinality(String) CODEC(ZSTD(1)),
    `is_monotonic` Bool CODEC(ZSTD(1)),
    `type` LowCardinality(String) CODEC(ZSTD(1)),
    `description` LowCardinality(String) CODEC(ZSTD(1)),
    `unit` LowCardinality(String) CODEC(ZSTD(1)),
    `created_at` Int64 CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'updated_metadata', cityHash64(metric_name))
;

CREATE TABLE IF NOT EXISTS {db:metrics}.distributed_usage ON CLUSTER {cluster}
(
    `tenant` String CODEC(ZSTD(1)),
    `collector_id` String CODEC(ZSTD(1)),
    `exporter_id` String CODEC(ZSTD(1)),
    `timestamp` DateTime CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:metrics}', 'usage', cityHash64(rand()))
;

CREATE TABLE IF NOT EXISTS {db:traces}.distributed_dependency_graph_minutes_v2 ON CLUSTER {cluster}
(
    `src` LowCardinality(String) CODEC(ZSTD(1)),
    `dest` LowCardinality(String) CODEC(ZSTD(1)),
    `duration_quantiles_state` AggregateFunction(quantiles(0.5, 0.75, 0.9, 0.95, 0.99), Float64) CODEC(Default),
    `error_count` SimpleAggregateFunction(sum, UInt64) CODEC(T64, ZSTD(1)),
    `total_count` SimpleAggregateFunction(sum, UInt64) CODEC(T64, ZSTD(1)),
    `timestamp` DateTime CODEC(DoubleDelta, LZ4),
    `deployment_environment` LowCardinality(String) CODEC(ZSTD(1)),
    `k8s_cluster_name` LowCardinality(String) CODEC(ZSTD(1)),
    `k8s_namespace_name` LowCardinality(String) CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:traces}', 'dependency_graph_minutes_v2', cityHash64(rand()))
;

CREATE TABLE IF NOT EXISTS {db:traces}.distributed_schema_migrations_v2 ON CLUSTER {cluster}
(
    `migration_id` UInt64,
    `status` String,
    `error` String,
    `created_at` DateTime64(9),
    `updated_at` DateTime64(9)
)
ENGINE = Distributed('{cluster}', '{db:traces}', 'schema_migrations_v2', rand())
;

CREATE TABLE IF NOT EXISTS {db:traces}.distributed_signoz_error_index_v2 ON CLUSTER {cluster}
(
    `timestamp` DateTime64(9) CODEC(DoubleDelta, LZ4),
    `errorID` FixedString(32) CODEC(ZSTD(1)),
    `groupID` FixedString(32) CODEC(ZSTD(1)),
    `traceID` FixedString(32) CODEC(ZSTD(1)),
    `spanID` String CODEC(ZSTD(1)),
    `serviceName` LowCardinality(String) CODEC(ZSTD(1)),
    `exceptionType` LowCardinality(String) CODEC(ZSTD(1)),
    `exceptionMessage` String CODEC(ZSTD(1)),
    `exceptionStacktrace` String CODEC(ZSTD(1)),
    `exceptionEscaped` Bool CODEC(T64, ZSTD(1)),
    `resourceTagsMap` Map(LowCardinality(String), String) CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:traces}', 'signoz_error_index_v2', cityHash64(groupID))
;

CREATE TABLE IF NOT EXISTS {db:traces}.distributed_signoz_index_v2 ON CLUSTER {cluster}
(
    `timestamp` DateTime64(9) CODEC(DoubleDelta, LZ4),
    `traceID` FixedString(32) CODEC(ZSTD(1)),
    `spanID` String CODEC(ZSTD(1)),
    `parentSpanID` String CODEC(ZSTD(1)),
    `serviceName` LowCardinality(String) CODEC(ZSTD(1)),
    `name` LowCardinality(String) CODEC(ZSTD(1)),
    `kind` Int8 CODEC(T64, ZSTD(1)),
    `durationNano` UInt64 CODEC(T64, ZSTD(1)),
    `statusCode` Int16 CODEC(T64, ZSTD(1)),
    `externalHttpMethod` LowCardinality(String) CODEC(ZSTD(1)),
    `externalHttpUrl` LowCardinality(String) CODEC(ZSTD(1)),
    `dbSystem` LowCardinality(String) CODEC(ZSTD(1)),
    `dbName` LowCardinality(String) CODEC(ZSTD(1)),
    `dbOperation` LowCardinality(String) CODEC(ZSTD(1)),
    `peerService` LowCardinality(String) CODEC(ZSTD(1)),
    `events` Array(String) CODEC(ZSTD(2)),
    `httpMethod` LowCardinality(String) CODEC(ZSTD(1)),
    `httpUrl` LowCardinality(String) CODEC(ZSTD(1)),
    `httpRoute` LowCardinality(String) CODEC(ZSTD(1)),
    `httpHost` LowCardinality(String) CODEC(ZSTD(1)),
    `msgSystem` LowCardinality(String) CODEC(ZSTD(1)),
    `msgOperation` LowCardinality(String) CODEC(ZSTD(1)),
    `hasError` Bool CODEC(T64, ZSTD(1)),
    `rpcSystem` LowCardinality(String) CODEC(ZSTD(1)),
    `rpcService` LowCardinality(String) CODEC(ZSTD(1)),
    `rpcMethod` LowCardinality(String) CODEC(ZSTD(1)),
    `responseStatusCode` LowCardinality(String) CODEC(ZSTD(1)),
    `stringTagMap` Map(String, String) CODEC(ZSTD(1)),
    `numberTagMap` Map(String, Float64) CODEC(ZSTD(1)),
    `boolTagMap` Map(String, Bool) CODEC(ZSTD(1)),
    `resourceTagsMap` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `isRemote` LowCardinality(String) CODEC(ZSTD(1)),
    `statusMessage` String CODEC(ZSTD(1)),
    `statusCodeString` String CODEC(ZSTD(1)),
    `spanKind` String CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:traces}', 'signoz_index_v2', cityHash64(traceID))
;

CREATE TABLE IF NOT EXISTS {db:traces}.distributed_signoz_index_v3 ON CLUSTER {cluster}
(
    `ts_bucket_start` UInt64 CODEC(DoubleDelta, LZ4),
    `resource_fingerprint` String CODEC(ZSTD(1)),
    `timestamp` DateTime64(9) CODEC(DoubleDelta, LZ4),
    `trace_id` FixedString(32) CODEC(ZSTD(1)),
    `span_id` String CODEC(ZSTD(1)),
    `trace_state` String CODEC(ZSTD(1)),
    `parent_span_id` String CODEC(ZSTD(1)),
    `flags` UInt32 CODEC(T64, ZSTD(1)),
    `name` LowCardinality(String) CODEC(ZSTD(1)),
    `kind` Int8 CODEC(T64, ZSTD(1)),
    `kind_string` String CODEC(ZSTD(1)),
    `duration_nano` UInt64 CODEC(T64, ZSTD(1)),
    `status_code` Int16 CODEC(T64, ZSTD(1)),
    `status_message` String CODEC(ZSTD(1)),
    `status_code_string` String CODEC(ZSTD(1)),
    `attributes_string` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `attributes_number` Map(LowCardinality(String), Float64) CODEC(ZSTD(1)),
    `attributes_bool` Map(LowCardinality(String), Bool) CODEC(ZSTD(1)),
    `attributes` JSON(max_dynamic_paths = 0) CODEC(ZSTD(1)),
    `attributes_promoted` JSON CODEC(ZSTD(1)),
    `resources_string` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `events` Array(String) CODEC(ZSTD(2)),
    `links` String CODEC(ZSTD(1)),
    `response_status_code` LowCardinality(String) CODEC(ZSTD(1)),
    `external_http_url` LowCardinality(String) CODEC(ZSTD(1)),
    `http_url` LowCardinality(String) CODEC(ZSTD(1)),
    `external_http_method` LowCardinality(String) CODEC(ZSTD(1)),
    `http_method` LowCardinality(String) CODEC(ZSTD(1)),
    `http_host` LowCardinality(String) CODEC(ZSTD(1)),
    `db_name` LowCardinality(String) CODEC(ZSTD(1)),
    `db_operation` LowCardinality(String) CODEC(ZSTD(1)),
    `has_error` Bool CODEC(T64, ZSTD(1)),
    `is_remote` LowCardinality(String) CODEC(ZSTD(1)),
    `resource_string_service$$name` LowCardinality(String) DEFAULT resources_string['service.name'] CODEC(ZSTD(1)),
    `attribute_string_http$$route` LowCardinality(String) DEFAULT attributes_string['http.route'] CODEC(ZSTD(1)),
    `attribute_string_messaging$$system` LowCardinality(String) DEFAULT attributes_string['messaging.system'] CODEC(ZSTD(1)),
    `attribute_string_messaging$$operation` LowCardinality(String) DEFAULT attributes_string['messaging.operation'] CODEC(ZSTD(1)),
    `attribute_string_db$$system` LowCardinality(String) DEFAULT attributes_string['db.system'] CODEC(ZSTD(1)),
    `attribute_string_rpc$$system` LowCardinality(String) DEFAULT attributes_string['rpc.system'] CODEC(ZSTD(1)),
    `attribute_string_rpc$$service` LowCardinality(String) DEFAULT attributes_string['rpc.service'] CODEC(ZSTD(1)),
    `attribute_string_rpc$$method` LowCardinality(String) DEFAULT attributes_string['rpc.method'] CODEC(ZSTD(1)),
    `attribute_string_peer$$service` LowCardinality(String) DEFAULT attributes_string['peer.service'] CODEC(ZSTD(1)),
    `traceID` FixedString(32) ALIAS trace_id,
    `spanID` String ALIAS span_id,
    `parentSpanID` String ALIAS parent_span_id,
    `spanKind` String ALIAS kind_string,
    `durationNano` UInt64 ALIAS duration_nano,
    `statusCode` Int16 ALIAS status_code,
    `statusMessage` String ALIAS status_message,
    `statusCodeString` String ALIAS status_code_string,
    `references` String ALIAS links,
    `responseStatusCode` LowCardinality(String) ALIAS response_status_code,
    `externalHttpUrl` LowCardinality(String) ALIAS external_http_url,
    `httpUrl` LowCardinality(String) ALIAS http_url,
    `externalHttpMethod` LowCardinality(String) ALIAS external_http_method,
    `httpMethod` LowCardinality(String) ALIAS http_method,
    `httpHost` LowCardinality(String) ALIAS http_host,
    `dbName` LowCardinality(String) ALIAS db_name,
    `dbOperation` LowCardinality(String) ALIAS db_operation,
    `hasError` Bool ALIAS has_error,
    `isRemote` LowCardinality(String) ALIAS is_remote,
    `serviceName` LowCardinality(String) ALIAS `resource_string_service$$name`,
    `httpRoute` LowCardinality(String) ALIAS `attribute_string_http$$route`,
    `msgSystem` LowCardinality(String) ALIAS `attribute_string_messaging$$system`,
    `msgOperation` LowCardinality(String) ALIAS `attribute_string_messaging$$operation`,
    `dbSystem` LowCardinality(String) ALIAS `attribute_string_db$$system`,
    `rpcSystem` LowCardinality(String) ALIAS `attribute_string_rpc$$system`,
    `rpcService` LowCardinality(String) ALIAS `attribute_string_rpc$$service`,
    `rpcMethod` LowCardinality(String) ALIAS `attribute_string_rpc$$method`,
    `peerService` LowCardinality(String) ALIAS `attribute_string_peer$$service`,
    `resource_string_service$$name_exists` Bool DEFAULT if(mapContains(resources_string, 'service.name') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_string_http$$route_exists` Bool DEFAULT if(mapContains(attributes_string, 'http.route') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_string_messaging$$system_exists` Bool DEFAULT if(mapContains(attributes_string, 'messaging.system') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_string_messaging$$operation_exists` Bool DEFAULT if(mapContains(attributes_string, 'messaging.operation') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_string_db$$system_exists` Bool DEFAULT if(mapContains(attributes_string, 'db.system') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_string_rpc$$system_exists` Bool DEFAULT if(mapContains(attributes_string, 'rpc.system') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_string_rpc$$service_exists` Bool DEFAULT if(mapContains(attributes_string, 'rpc.service') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_string_rpc$$method_exists` Bool DEFAULT if(mapContains(attributes_string, 'rpc.method') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_string_peer$$service_exists` Bool DEFAULT if(mapContains(attributes_string, 'peer.service') != 0, true, false) CODEC(ZSTD(1)),
    `resource` JSON(max_dynamic_paths = 100) CODEC(ZSTD(1)),
    `scope` JSON(max_dynamic_paths = 0, attributes JSON(max_dynamic_paths = 0), name String, version String) CODEC(ZSTD(1)),
    `inserted_at` DateTime64(3) CODEC(Delta(8), ZSTD(1)),
    `created_at` DateTime64(3) DEFAULT now64(3) CODEC(Delta(8), ZSTD(1)),
    `attribute_string_gen_ai$$request$$model` LowCardinality(String) DEFAULT attributes_string['gen_ai.request.model'] CODEC(ZSTD(1)),
    `attribute_string_gen_ai$$request$$model_exists` Bool DEFAULT if(mapContains(attributes_string, 'gen_ai.request.model') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_string_gen_ai$$provider$$name` LowCardinality(String) DEFAULT attributes_string['gen_ai.provider.name'] CODEC(ZSTD(1)),
    `attribute_string_gen_ai$$provider$$name_exists` Bool DEFAULT if(mapContains(attributes_string, 'gen_ai.provider.name') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_string_gen_ai$$tool$$name` LowCardinality(String) DEFAULT attributes_string['gen_ai.tool.name'] CODEC(ZSTD(1)),
    `attribute_string_gen_ai$$tool$$name_exists` Bool DEFAULT if(mapContains(attributes_string, 'gen_ai.tool.name') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_string_gen_ai$$agent$$name` LowCardinality(String) DEFAULT attributes_string['gen_ai.agent.name'] CODEC(ZSTD(1)),
    `attribute_string_gen_ai$$agent$$name_exists` Bool DEFAULT if(mapContains(attributes_string, 'gen_ai.agent.name') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_number_signoz$$gen_ai$$usage$$tokens$$cost` Float64 DEFAULT attributes_number['signoz.gen_ai.usage.tokens.cost'] CODEC(ZSTD(1)),
    `attribute_number_signoz$$gen_ai$$usage$$tokens$$cost_exists` Bool DEFAULT if(mapContains(attributes_number, 'signoz.gen_ai.usage.tokens.cost') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_number_gen_ai$$usage$$input_tokens` Float64 DEFAULT attributes_number['gen_ai.usage.input_tokens'] CODEC(ZSTD(1)),
    `attribute_number_gen_ai$$usage$$input_tokens_exists` Bool DEFAULT if(mapContains(attributes_number, 'gen_ai.usage.input_tokens') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_number_gen_ai$$usage$$output_tokens` Float64 DEFAULT attributes_number['gen_ai.usage.output_tokens'] CODEC(ZSTD(1)),
    `attribute_number_gen_ai$$usage$$output_tokens_exists` Bool DEFAULT if(mapContains(attributes_number, 'gen_ai.usage.output_tokens') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_number_gen_ai$$usage$$cache_read$$input_tokens` Float64 DEFAULT attributes_number['gen_ai.usage.cache_read.input_tokens'] CODEC(ZSTD(1)),
    `attribute_number_gen_ai$$usage$$cache_read$$input_tokens_exists` Bool DEFAULT if(mapContains(attributes_number, 'gen_ai.usage.cache_read.input_tokens') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_number_gen_ai$$usage$$cache_creation$$input_tokens` Float64 DEFAULT attributes_number['gen_ai.usage.cache_creation.input_tokens'] CODEC(ZSTD(1)),
    `attribute_number_gen_ai$$usage$$cache_creation$$input_tokens_exists` Bool DEFAULT if(mapContains(attributes_number, 'gen_ai.usage.cache_creation.input_tokens') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_number_signoz$$gen_ai$$usage$$input_tokens$$cost` Float64 DEFAULT attributes_number['signoz.gen_ai.usage.input_tokens.cost'] CODEC(ZSTD(1)),
    `attribute_number_signoz$$gen_ai$$usage$$input_tokens$$cost_exists` Bool DEFAULT if(mapContains(attributes_number, 'signoz.gen_ai.usage.input_tokens.cost') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_number_signoz$$gen_ai$$usage$$output_tokens$$cost` Float64 DEFAULT attributes_number['signoz.gen_ai.usage.output_tokens.cost'] CODEC(ZSTD(1)),
    `attribute_number_signoz$$gen_ai$$usage$$output_tokens$$cost_exists` Bool DEFAULT if(mapContains(attributes_number, 'signoz.gen_ai.usage.output_tokens.cost') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_number_signoz$$gen_ai$$usage$$cache_read$$input_tokens$$cost` Float64 DEFAULT attributes_number['signoz.gen_ai.usage.cache_read.input_tokens.cost'] CODEC(ZSTD(1)),
    `attribute_number_signoz$$gen_ai$$usage$$cache_read$$input_tokens$$cost_exists` Bool DEFAULT if(mapContains(attributes_number, 'signoz.gen_ai.usage.cache_read.input_tokens.cost') != 0, true, false) CODEC(ZSTD(1)),
    `attribute_number_signoz$$gen_ai$$usage$$cache_write$$input_tokens$$cost` Float64 DEFAULT attributes_number['signoz.gen_ai.usage.cache_write.input_tokens.cost'] CODEC(ZSTD(1)),
    `attribute_number_signoz$$gen_ai$$usage$$cache_write$$input_tokens$$cost_exists` Bool DEFAULT if(mapContains(attributes_number, 'signoz.gen_ai.usage.cache_write.input_tokens.cost') != 0, true, false) CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:traces}', 'signoz_index_v3', cityHash64(trace_id))
;

CREATE TABLE IF NOT EXISTS {db:traces}.distributed_signoz_spans ON CLUSTER {cluster}
(
    `timestamp` DateTime64(9) CODEC(DoubleDelta, LZ4),
    `traceID` FixedString(32) CODEC(ZSTD(1)),
    `model` String CODEC(ZSTD(9))
)
ENGINE = Distributed('{cluster}', '{db:traces}', 'signoz_spans', cityHash64(traceID))
;

CREATE TABLE IF NOT EXISTS {db:traces}.distributed_span_attributes ON CLUSTER {cluster}
(
    `timestamp` DateTime CODEC(DoubleDelta, ZSTD(1)),
    `tagKey` LowCardinality(String) CODEC(ZSTD(1)),
    `tagType` Enum8('tag' = 1, 'resource' = 2) CODEC(ZSTD(1)),
    `dataType` Enum8('string' = 1, 'bool' = 2, 'float64' = 3) CODEC(ZSTD(1)),
    `stringTagValue` String CODEC(ZSTD(1)),
    `float64TagValue` Nullable(Float64) CODEC(ZSTD(1)),
    `isColumn` Bool CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:traces}', 'span_attributes', cityHash64(rand()))
;

CREATE TABLE IF NOT EXISTS {db:traces}.distributed_span_attributes_keys ON CLUSTER {cluster}
(
    `tagKey` LowCardinality(String) CODEC(ZSTD(1)),
    `tagType` Enum8('tag' = 1, 'resource' = 2, 'scope' = 3) CODEC(ZSTD(1)),
    `dataType` Enum8('string' = 1, 'bool' = 2, 'float64' = 3) CODEC(ZSTD(1)),
    `isColumn` Bool CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:traces}', 'span_attributes_keys', cityHash64(rand()))
;

CREATE TABLE IF NOT EXISTS {db:traces}.distributed_tag_attributes_v2 ON CLUSTER {cluster}
(
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `tag_key` String CODEC(ZSTD(1)),
    `tag_type` LowCardinality(String) CODEC(ZSTD(1)),
    `tag_data_type` LowCardinality(String) CODEC(ZSTD(1)),
    `string_value` String CODEC(ZSTD(1)),
    `number_value` Nullable(Float64) CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:traces}', 'tag_attributes_v2', cityHash64(rand()))
;

CREATE TABLE IF NOT EXISTS {db:traces}.distributed_top_level_operations ON CLUSTER {cluster}
(
    `name` LowCardinality(String) CODEC(ZSTD(1)),
    `serviceName` LowCardinality(String) CODEC(ZSTD(1)),
    `time` DateTime DEFAULT now() CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:traces}', 'top_level_operations', cityHash64(rand()))
;

CREATE TABLE IF NOT EXISTS {db:traces}.distributed_trace_summary ON CLUSTER {cluster}
(
    `trace_id` String CODEC(ZSTD(1)),
    `start` SimpleAggregateFunction(min, DateTime64(9)) CODEC(ZSTD(1)),
    `end` SimpleAggregateFunction(max, DateTime64(9)) CODEC(ZSTD(1)),
    `num_spans` SimpleAggregateFunction(sum, UInt64) CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:traces}', 'trace_summary', cityHash64(trace_id))
;

CREATE TABLE IF NOT EXISTS {db:traces}.distributed_traces_v3_resource ON CLUSTER {cluster}
(
    `labels` String CODEC(ZSTD(5)),
    `fingerprint` String CODEC(ZSTD(1)),
    `seen_at_ts_bucket_start` Int64 CODEC(Delta(8), ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:traces}', 'traces_v3_resource', cityHash64(labels, fingerprint))
;

CREATE TABLE IF NOT EXISTS {db:traces}.distributed_usage ON CLUSTER {cluster}
(
    `tenant` String CODEC(ZSTD(1)),
    `collector_id` String CODEC(ZSTD(1)),
    `exporter_id` String CODEC(ZSTD(1)),
    `timestamp` DateTime CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:traces}', 'usage', cityHash64(rand()))
;

CREATE TABLE IF NOT EXISTS {db:traces}.distributed_usage_explorer ON CLUSTER {cluster}
(
    `timestamp` DateTime64(9) CODEC(DoubleDelta, LZ4),
    `service_name` LowCardinality(String) CODEC(ZSTD(1)),
    `count` UInt64 CODEC(T64, ZSTD(1))
)
ENGINE = Distributed('{cluster}', '{db:traces}', 'usage_explorer', cityHash64(rand()))
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:meter}.samples_agg_1d_mv ON CLUSTER {cluster} TO {db:meter}.samples_agg_1d
(
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT 'default',
    `unit` LowCardinality(String) DEFAULT 'default',
    `type` LowCardinality(String) DEFAULT 'default',
    `is_monotonic` Bool DEFAULT false,
    `labels` String CODEC(ZSTD(5)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(DoubleDelta, ZSTD(1)),
    `last` SimpleAggregateFunction(anyLast, Float64) CODEC(ZSTD(1)),
    `min` SimpleAggregateFunction(min, Float64) CODEC(ZSTD(1)),
    `max` SimpleAggregateFunction(max, Float64) CODEC(ZSTD(1)),
    `sum` SimpleAggregateFunction(sum, Float64) CODEC(ZSTD(1)),
    `count` SimpleAggregateFunction(sum, UInt64) CODEC(ZSTD(1))
)
AS SELECT
    temporality,
    metric_name,
    description,
    unit,
    type,
    is_monotonic,
    labels,
    fingerprint,
    intDiv(unix_milli, 86400000) * 86400000 AS unix_milli,
    anyLast(value) AS last,
    min(value) AS min,
    max(value) AS max,
    sum(value) AS sum,
    count(*) AS count
FROM {db:meter}.samples
GROUP BY
    temporality,
    metric_name,
    fingerprint,
    description,
    unit,
    type,
    is_monotonic,
    labels,
    unix_milli
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:metrics}.samples_v4_agg_30m_mv ON CLUSTER {cluster} TO {db:metrics}.samples_v4_agg_30m
(
    `env` LowCardinality(String),
    `temporality` LowCardinality(String),
    `metric_name` LowCardinality(String),
    `fingerprint` UInt64,
    `unix_milli` Int64,
    `last` SimpleAggregateFunction(anyLast, Float64),
    `min` SimpleAggregateFunction(min, Float64),
    `max` SimpleAggregateFunction(max, Float64),
    `sum` Float64,
    `count` UInt64
)
AS SELECT
    env,
    temporality,
    metric_name,
    fingerprint,
    intDiv(unix_milli, 1800000) * 1800000 AS unix_milli,
    anyLast(last) AS last,
    min(min) AS min,
    max(max) AS max,
    sum(sum) AS sum,
    sum(count) AS count
FROM {db:metrics}.samples_v4_agg_5m
GROUP BY
    env,
    temporality,
    metric_name,
    fingerprint,
    unix_milli
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:metrics}.samples_v4_agg_5m_mv ON CLUSTER {cluster} TO {db:metrics}.samples_v4_agg_5m
(
    `env` LowCardinality(String),
    `temporality` LowCardinality(String),
    `metric_name` LowCardinality(String),
    `fingerprint` UInt64,
    `unix_milli` Int64,
    `last` Float64,
    `min` Float64,
    `max` Float64,
    `sum` Float64,
    `count` UInt64
)
AS SELECT
    env,
    temporality,
    metric_name,
    fingerprint,
    intDiv(unix_milli, 300000) * 300000 AS unix_milli,
    anyLast(value) AS last,
    min(value) AS min,
    max(value) AS max,
    sum(value) AS sum,
    count(*) AS count
FROM {db:metrics}.samples_v4
WHERE bitAnd(flags, 1) = 0
GROUP BY
    env,
    temporality,
    metric_name,
    fingerprint,
    unix_milli
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:metrics}.samples_v4_mv ON CLUSTER {cluster} TO {db:metrics}.samples_v4
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(DoubleDelta, ZSTD(1)),
    `value` Float64 CODEC(Gorilla(8), ZSTD(1)),
    `flags` UInt32 DEFAULT 0 CODEC(ZSTD(1)),
    `inserted_at_unix_milli` Int64 CODEC(ZSTD(1))
)
AS SELECT
    env,
    temporality,
    metric_name,
    fingerprint,
    unix_milli,
    value,
    flags,
    inserted_at_unix_milli
FROM {db:metrics}.samples_v4_buffer
WHERE reduced_fingerprint = 0
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:metrics}.samples_v4_reduced_last_30m_mv ON CLUSTER {cluster}
REFRESH EVERY 30 MINUTE RANDOMIZE FOR 2 MINUTE APPEND TO {db:metrics}.samples_v4_reduced_last_30m
(
    `env` LowCardinality(String),
    `temporality` LowCardinality(String),
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64,
    `unix_milli` Int64,
    `last` Float64,
    `min` Float64,
    `max` Float64,
    `sum` Float64,
    `sum_values` Float64,
    `count` UInt64,
    `sum_count_series` UInt64,
    `count_samples` UInt64,
    `computed_at` DateTime
)
DEFINER = default SQL SECURITY DEFINER
AS SELECT
    env,
    temporality,
    metric_name,
    reduced_fingerprint,
    bucket_unix_milli AS unix_milli,
    last,
    min,
    max,
    sum,
    sum_values,
    count,
    sum_count_series,
    count_samples,
    now() AS computed_at
FROM
(
    SELECT
        env,
        temporality,
        metric_name,
        reduced_fingerprint,
        intDiv(unix_milli, 1800000) * 1800000 AS bucket_unix_milli,
        argMax(last, unix_milli) AS last,
        min(min) AS min,
        max(max) AS max,
        sum(sum) AS sum,
        sum(sum_values) AS sum_values,
        sum(count) AS count,
        sum(sum_count_series) AS sum_count_series,
        sum(count_samples) AS count_samples
    FROM {db:metrics}.samples_v4_reduced_last_5m
    FINAL
    WHERE (unix_milli >= (toUnixTimestamp(now() - toIntervalMinute(180)) * 1000)) AND (unix_milli < (toUnixTimestamp(now() - toIntervalMinute(40)) * 1000))
    GROUP BY
        env,
        temporality,
        metric_name,
        reduced_fingerprint,
        bucket_unix_milli
)
WHERE (bucket_unix_milli >= (toUnixTimestamp(now() - toIntervalMinute(150)) * 1000)) AND ((bucket_unix_milli + 1800000) <= (toUnixTimestamp(now() - toIntervalMinute(40)) * 1000))
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:metrics}.samples_v4_reduced_last_5m_mv ON CLUSTER {cluster}
REFRESH EVERY 5 MINUTE RANDOMIZE FOR 30 SECOND APPEND TO {db:metrics}.samples_v4_reduced_last_5m
(
    `env` LowCardinality(String),
    `temporality` LowCardinality(String),
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64,
    `unix_milli` Int64,
    `last` Float64,
    `min` Float64,
    `max` Float64,
    `sum` Float64,
    `sum_values` Float64,
    `count` UInt64,
    `sum_count_series` UInt64,
    `count_samples` UInt64,
    `computed_at` DateTime
)
DEFINER = default SQL SECURITY DEFINER
AS SELECT
    env,
    temporality,
    metric_name,
    reduced_fingerprint,
    bucket_unix_milli AS unix_milli,
    last,
    min,
    max,
    sum,
    sum_values,
    count,
    sum_count_series,
    count_samples,
    now() AS computed_at
FROM
(
    SELECT
        env,
        temporality,
        metric_name,
        reduced_fingerprint,
        intDiv(unix_milli, 300000) * 300000 AS bucket_unix_milli,
        argMax(sum_last, unix_milli) AS last,
        min(min) AS min,
        max(max) AS max,
        sum(sum_last) AS sum,
        sum(sum_values) AS sum_values,
        count() AS count,
        sum(count_series) AS sum_count_series,
        sum(count_samples) AS count_samples
    FROM {db:metrics}.samples_v4_reduced_last_60s
    FINAL
    WHERE (unix_milli >= (toUnixTimestamp(now() - toIntervalMinute(40)) * 1000)) AND (unix_milli < (toUnixTimestamp(now() - toIntervalMinute(12)) * 1000))
    GROUP BY
        env,
        temporality,
        metric_name,
        reduced_fingerprint,
        bucket_unix_milli
)
WHERE (bucket_unix_milli >= (toUnixTimestamp(now() - toIntervalMinute(35)) * 1000)) AND ((bucket_unix_milli + 300000) <= (toUnixTimestamp(now() - toIntervalMinute(12)) * 1000))
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:metrics}.samples_v4_reduced_last_60s_mv ON CLUSTER {cluster}
REFRESH EVERY 1 MINUTE RANDOMIZE FOR 10 SECOND APPEND TO {db:metrics}.samples_v4_reduced_last_60s
(
    `env` LowCardinality(String),
    `temporality` LowCardinality(String),
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64,
    `unix_milli` Int64,
    `sum_last` Float64,
    `min` Float64,
    `max` Float64,
    `sum_values` Float64,
    `count_series` UInt64,
    `count_samples` UInt64,
    `computed_at` DateTime
)
DEFINER = default SQL SECURITY DEFINER
AS SELECT
    env,
    temporality,
    metric_name,
    reduced_fingerprint,
    bucket_unix_milli AS unix_milli,
    sum(last) AS sum_last,
    min(min_value) AS min,
    max(max_value) AS max,
    sum(sum_value) AS sum_values,
    count() AS count_series,
    sum(num_values) AS count_samples,
    now() AS computed_at
FROM
(
    SELECT
        env,
        temporality,
        metric_name,
        reduced_fingerprint,
        fingerprint,
        intDiv(unix_milli, 60000) * 60000 AS bucket_unix_milli,
        argMax(value, unix_milli) AS last,
        min(value) AS min_value,
        max(value) AS max_value,
        sum(value) AS sum_value,
        count(value) AS num_values
    FROM {db:metrics}.samples_v4_buffer
    PREWHERE reduced_fingerprint != 0
    WHERE (bitAnd(flags, 1) = 0) AND ((temporality = 'Unspecified') OR ((temporality = 'Cumulative') AND (is_monotonic = false))) AND (unix_milli < (toUnixTimestamp(now() - toIntervalSecond(120)) * 1000)) AND (unix_milli >= (toUnixTimestamp(now() - toIntervalMinute(11)) * 1000))
    GROUP BY
        env,
        temporality,
        metric_name,
        reduced_fingerprint,
        fingerprint,
        bucket_unix_milli
)
WHERE bucket_unix_milli >= (toUnixTimestamp(now() - toIntervalMinute(10)) * 1000)
GROUP BY
    env,
    temporality,
    metric_name,
    reduced_fingerprint,
    bucket_unix_milli
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:metrics}.samples_v4_reduced_sum_30m_mv ON CLUSTER {cluster}
REFRESH EVERY 30 MINUTE RANDOMIZE FOR 2 MINUTE APPEND TO {db:metrics}.samples_v4_reduced_sum_30m
(
    `env` LowCardinality(String),
    `temporality` LowCardinality(String),
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64,
    `unix_milli` Int64,
    `sum` Float64,
    `count` UInt64,
    `sum_count_series` UInt64,
    `count_samples` UInt64,
    `computed_at` DateTime
)
DEFINER = default SQL SECURITY DEFINER
AS SELECT
    env,
    temporality,
    metric_name,
    reduced_fingerprint,
    bucket_unix_milli AS unix_milli,
    sum,
    count,
    sum_count_series,
    count_samples,
    now() AS computed_at
FROM
(
    SELECT
        env,
        temporality,
        metric_name,
        reduced_fingerprint,
        intDiv(unix_milli, 1800000) * 1800000 AS bucket_unix_milli,
        sum(sum) AS sum,
        sum(count) AS count,
        sum(sum_count_series) AS sum_count_series,
        sum(count_samples) AS count_samples
    FROM {db:metrics}.samples_v4_reduced_sum_5m
    FINAL
    WHERE (unix_milli >= (toUnixTimestamp(now() - toIntervalMinute(180)) * 1000)) AND (unix_milli < (toUnixTimestamp(now() - toIntervalMinute(40)) * 1000))
    GROUP BY
        env,
        temporality,
        metric_name,
        reduced_fingerprint,
        bucket_unix_milli
)
WHERE (bucket_unix_milli >= (toUnixTimestamp(now() - toIntervalMinute(150)) * 1000)) AND ((bucket_unix_milli + 1800000) <= (toUnixTimestamp(now() - toIntervalMinute(40)) * 1000))
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:metrics}.samples_v4_reduced_sum_5m_mv ON CLUSTER {cluster}
REFRESH EVERY 5 MINUTE RANDOMIZE FOR 30 SECOND APPEND TO {db:metrics}.samples_v4_reduced_sum_5m
(
    `env` LowCardinality(String),
    `temporality` LowCardinality(String),
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64,
    `unix_milli` Int64,
    `sum` Float64,
    `count` UInt64,
    `sum_count_series` UInt64,
    `count_samples` UInt64,
    `computed_at` DateTime
)
DEFINER = default SQL SECURITY DEFINER
AS SELECT
    env,
    temporality,
    metric_name,
    reduced_fingerprint,
    bucket_unix_milli AS unix_milli,
    sum,
    count,
    sum_count_series,
    count_samples,
    now() AS computed_at
FROM
(
    SELECT
        env,
        temporality,
        metric_name,
        reduced_fingerprint,
        intDiv(unix_milli, 300000) * 300000 AS bucket_unix_milli,
        sum(sum) AS sum,
        count() AS count,
        sum(count_series) AS sum_count_series,
        sum(count_samples) AS count_samples
    FROM {db:metrics}.samples_v4_reduced_sum_60s
    FINAL
    WHERE (unix_milli >= (toUnixTimestamp(now() - toIntervalMinute(40)) * 1000)) AND (unix_milli < (toUnixTimestamp(now() - toIntervalMinute(12)) * 1000))
    GROUP BY
        env,
        temporality,
        metric_name,
        reduced_fingerprint,
        bucket_unix_milli
)
WHERE (bucket_unix_milli >= (toUnixTimestamp(now() - toIntervalMinute(35)) * 1000)) AND ((bucket_unix_milli + 300000) <= (toUnixTimestamp(now() - toIntervalMinute(12)) * 1000))
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:metrics}.samples_v4_reduced_sum_60s_cumulative_mv ON CLUSTER {cluster}
REFRESH EVERY 1 MINUTE RANDOMIZE FOR 10 SECOND APPEND TO {db:metrics}.samples_v4_reduced_sum_60s
(
    `env` LowCardinality(String),
    `temporality` LowCardinality(String),
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64,
    `unix_milli` Int64,
    `sum` Float64,
    `count_series` UInt64,
    `count_samples` UInt64,
    `computed_at` DateTime
)
DEFINER = default SQL SECURITY DEFINER
AS SELECT
    env,
    'Delta' AS temporality,
    metric_name,
    reduced_fingerprint,
    bucket_unix_milli AS unix_milli,
    sum(series_increment) AS sum,
    count() AS count_series,
    sum(num_values) AS count_samples,
    now() AS computed_at
FROM
(
    SELECT
        env,
        temporality,
        metric_name,
        reduced_fingerprint,
        fingerprint,
        bucket_unix_milli,
        sum(increment) AS series_increment,
        count() AS num_values
    FROM
    (
        SELECT
            env,
            temporality,
            metric_name,
            reduced_fingerprint,
            fingerprint,
            intDiv(unix_milli, 60000) * 60000 AS bucket_unix_milli,
            multiIf(row_number() OVER rate_window = 1, nan, value < lagInFrame(value, 1) OVER rate_window, value, value - lagInFrame(value, 1) OVER rate_window) AS increment
        FROM {db:metrics}.samples_v4_buffer
        PREWHERE reduced_fingerprint != 0
        WHERE (bitAnd(flags, 1) = 0) AND ((temporality = 'Cumulative') AND (is_monotonic = true)) AND (unix_milli < (toUnixTimestamp(now() - toIntervalSecond(120)) * 1000)) AND (unix_milli >= (toUnixTimestamp(now() - toIntervalMinute(11)) * 1000))
        WINDOW rate_window AS (PARTITION BY env, temporality, metric_name, fingerprint ORDER BY unix_milli ASC)
    )
    WHERE (isNaN(increment) = 0) AND (bucket_unix_milli >= (toUnixTimestamp(now() - toIntervalMinute(10)) * 1000))
    GROUP BY
        env,
        temporality,
        metric_name,
        reduced_fingerprint,
        fingerprint,
        bucket_unix_milli
)
GROUP BY
    env,
    temporality,
    metric_name,
    reduced_fingerprint,
    bucket_unix_milli
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:metrics}.samples_v4_reduced_sum_60s_delta_mv ON CLUSTER {cluster}
REFRESH EVERY 1 MINUTE RANDOMIZE FOR 10 SECOND APPEND TO {db:metrics}.samples_v4_reduced_sum_60s
(
    `env` LowCardinality(String),
    `temporality` LowCardinality(String),
    `metric_name` LowCardinality(String),
    `reduced_fingerprint` UInt64,
    `unix_milli` Int64,
    `sum` Float64,
    `count_series` UInt64,
    `count_samples` UInt64,
    `computed_at` DateTime
)
DEFINER = default SQL SECURITY DEFINER
AS SELECT
    env,
    temporality,
    metric_name,
    reduced_fingerprint,
    bucket_unix_milli AS unix_milli,
    sum(series_sum) AS sum,
    count() AS count_series,
    sum(num_values) AS count_samples,
    now() AS computed_at
FROM
(
    SELECT
        env,
        temporality,
        metric_name,
        reduced_fingerprint,
        fingerprint,
        intDiv(unix_milli, 60000) * 60000 AS bucket_unix_milli,
        sum(value) AS series_sum,
        count(value) AS num_values
    FROM {db:metrics}.samples_v4_buffer
    PREWHERE reduced_fingerprint != 0
    WHERE (bitAnd(flags, 1) = 0) AND (temporality = 'Delta') AND (unix_milli < (toUnixTimestamp(now() - toIntervalSecond(120)) * 1000)) AND (unix_milli >= (toUnixTimestamp(now() - toIntervalMinute(11)) * 1000))
    GROUP BY
        env,
        temporality,
        metric_name,
        reduced_fingerprint,
        fingerprint,
        bucket_unix_milli
)
WHERE bucket_unix_milli >= (toUnixTimestamp(now() - toIntervalMinute(10)) * 1000)
GROUP BY
    env,
    temporality,
    metric_name,
    reduced_fingerprint,
    bucket_unix_milli
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:metrics}.time_series_v4_1day_mv ON CLUSTER {cluster} TO {db:metrics}.time_series_v4_1day
(
    `env` LowCardinality(String),
    `temporality` LowCardinality(String),
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String),
    `unit` LowCardinality(String),
    `type` LowCardinality(String),
    `is_monotonic` Bool,
    `fingerprint` UInt64,
    `unix_milli` Float64,
    `labels` String,
    `attrs` Map(LowCardinality(String), String),
    `scope_attrs` Map(LowCardinality(String), String),
    `resource_attrs` Map(LowCardinality(String), String),
    `__normalized` Bool
)
AS SELECT
    env,
    temporality,
    metric_name,
    description,
    unit,
    type,
    is_monotonic,
    fingerprint,
    floor(unix_milli / 86400000) * 86400000 AS unix_milli,
    labels,
    attrs,
    scope_attrs,
    resource_attrs,
    __normalized
FROM {db:metrics}.time_series_v4_6hrs
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:metrics}.time_series_v4_1week_mv ON CLUSTER {cluster} TO {db:metrics}.time_series_v4_1week
(
    `env` LowCardinality(String),
    `temporality` LowCardinality(String),
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String),
    `unit` LowCardinality(String),
    `type` LowCardinality(String),
    `is_monotonic` Bool,
    `fingerprint` UInt64,
    `unix_milli` Float64,
    `labels` String,
    `attrs` Map(LowCardinality(String), String),
    `scope_attrs` Map(LowCardinality(String), String),
    `resource_attrs` Map(LowCardinality(String), String),
    `__normalized` Bool
)
AS SELECT
    env,
    temporality,
    metric_name,
    description,
    unit,
    type,
    is_monotonic,
    fingerprint,
    floor(unix_milli / 604800000) * 604800000 AS unix_milli,
    labels,
    attrs,
    scope_attrs,
    resource_attrs,
    __normalized
FROM {db:metrics}.time_series_v4_1day
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:metrics}.time_series_v4_6hrs_mv ON CLUSTER {cluster} TO {db:metrics}.time_series_v4_6hrs
(
    `env` LowCardinality(String),
    `temporality` LowCardinality(String),
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String),
    `unit` LowCardinality(String),
    `type` LowCardinality(String),
    `is_monotonic` Bool,
    `fingerprint` UInt64,
    `unix_milli` Float64,
    `labels` String,
    `attrs` Map(LowCardinality(String), String),
    `scope_attrs` Map(LowCardinality(String), String),
    `resource_attrs` Map(LowCardinality(String), String),
    `__normalized` Bool
)
AS SELECT
    env,
    temporality,
    metric_name,
    description,
    unit,
    type,
    is_monotonic,
    fingerprint,
    floor(unix_milli / 21600000) * 21600000 AS unix_milli,
    labels,
    attrs,
    scope_attrs,
    resource_attrs,
    __normalized
FROM {db:metrics}.time_series_v4
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:metrics}.time_series_v4_mv ON CLUSTER {cluster} TO {db:metrics}.time_series_v4
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `unit` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `type` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `labels` String CODEC(ZSTD(5)),
    `attrs` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `scope_attrs` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `resource_attrs` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `__normalized` Bool DEFAULT true CODEC(ZSTD(1)),
    `inserted_at_unix_milli` Int64 CODEC(ZSTD(1))
)
AS SELECT
    env,
    temporality,
    metric_name,
    description,
    unit,
    type,
    is_monotonic,
    fingerprint,
    unix_milli,
    labels,
    attrs,
    scope_attrs,
    resource_attrs,
    __normalized,
    inserted_at_unix_milli
FROM {db:metrics}.time_series_v4_buffer
WHERE (is_reduced = false) AND (reduced_fingerprint = 0)
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:metrics}.time_series_v4_reduced_1day_mv ON CLUSTER {cluster} TO {db:metrics}.time_series_v4_reduced_1day
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `unit` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `type` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `labels` String CODEC(ZSTD(5)),
    `attrs` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `scope_attrs` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `resource_attrs` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `__normalized` Bool DEFAULT true CODEC(ZSTD(1)),
    `inserted_at_unix_milli` Int64 CODEC(ZSTD(1))
)
AS SELECT
    env,
    temporality,
    metric_name,
    description,
    unit,
    type,
    is_monotonic,
    fingerprint,
    floor(unix_milli / 86400000) * 86400000 AS unix_milli,
    labels,
    attrs,
    scope_attrs,
    resource_attrs,
    __normalized,
    inserted_at_unix_milli
FROM {db:metrics}.time_series_v4_reduced
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:metrics}.time_series_v4_reduced_mv ON CLUSTER {cluster} TO {db:metrics}.time_series_v4_reduced
(
    `env` LowCardinality(String) DEFAULT 'default',
    `temporality` LowCardinality(String) DEFAULT 'Unspecified',
    `metric_name` LowCardinality(String),
    `description` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `unit` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `type` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `is_monotonic` Bool DEFAULT false CODEC(ZSTD(1)),
    `fingerprint` UInt64 CODEC(Delta(8), ZSTD(1)),
    `unix_milli` Int64 CODEC(Delta(8), ZSTD(1)),
    `labels` String CODEC(ZSTD(5)),
    `attrs` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `scope_attrs` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `resource_attrs` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `__normalized` Bool DEFAULT true CODEC(ZSTD(1)),
    `inserted_at_unix_milli` Int64 CODEC(ZSTD(1))
)
AS SELECT
    env,
    if((temporality = 'Cumulative') AND is_monotonic, 'Delta', temporality) AS temporality,
    metric_name,
    description,
    unit,
    type,
    is_monotonic,
    fingerprint,
    unix_milli,
    labels,
    attrs,
    scope_attrs,
    resource_attrs,
    __normalized,
    inserted_at_unix_milli
FROM {db:metrics}.time_series_v4_buffer
WHERE is_reduced = true
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:traces}.dependency_graph_minutes_db_calls_mv_v2 ON CLUSTER {cluster} TO {db:traces}.dependency_graph_minutes_v2
(
    `src` LowCardinality(String),
    `dest` LowCardinality(String),
    `duration_quantiles_state` AggregateFunction(quantiles(0.5, 0.75, 0.9, 0.95, 0.99), Float64),
    `error_count` UInt64,
    `total_count` UInt64,
    `timestamp` DateTime,
    `deployment_environment` String,
    `k8s_cluster_name` String,
    `k8s_namespace_name` String
)
AS SELECT
    `resource_string_service$$name` AS src,
    `attribute_string_db$$system` AS dest,
    quantilesState(0.5, 0.75, 0.9, 0.95, 0.99)(toFloat64(duration_nano)) AS duration_quantiles_state,
    countIf(status_code = 2) AS error_count,
    count(*) AS total_count,
    toStartOfMinute(timestamp) AS timestamp,
    resources_string['deployment.environment'] AS deployment_environment,
    resources_string['k8s.cluster.name'] AS k8s_cluster_name,
    resources_string['k8s.namespace.name'] AS k8s_namespace_name
FROM {db:traces}.signoz_index_v3
WHERE (dest != '') AND (kind != 2)
GROUP BY
    timestamp,
    src,
    dest,
    deployment_environment,
    k8s_cluster_name,
    k8s_namespace_name
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:traces}.dependency_graph_minutes_messaging_calls_mv_v2 ON CLUSTER {cluster} TO {db:traces}.dependency_graph_minutes_v2
(
    `src` LowCardinality(String),
    `dest` LowCardinality(String),
    `duration_quantiles_state` AggregateFunction(quantiles(0.5, 0.75, 0.9, 0.95, 0.99), Float64),
    `error_count` UInt64,
    `total_count` UInt64,
    `timestamp` DateTime,
    `deployment_environment` String,
    `k8s_cluster_name` String,
    `k8s_namespace_name` String
)
AS SELECT
    `resource_string_service$$name` AS src,
    `attribute_string_messaging$$system` AS dest,
    quantilesState(0.5, 0.75, 0.9, 0.95, 0.99)(toFloat64(duration_nano)) AS duration_quantiles_state,
    countIf(status_code = 2) AS error_count,
    count(*) AS total_count,
    toStartOfMinute(timestamp) AS timestamp,
    resources_string['deployment.environment'] AS deployment_environment,
    resources_string['k8s.cluster.name'] AS k8s_cluster_name,
    resources_string['k8s.namespace.name'] AS k8s_namespace_name
FROM {db:traces}.signoz_index_v3
WHERE (dest != '') AND (kind != 2)
GROUP BY
    timestamp,
    src,
    dest,
    deployment_environment,
    k8s_cluster_name,
    k8s_namespace_name
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:traces}.dependency_graph_minutes_service_calls_mv_v2 ON CLUSTER {cluster} TO {db:traces}.dependency_graph_minutes_v2
(
    `src` LowCardinality(String),
    `dest` LowCardinality(String),
    `duration_quantiles_state` AggregateFunction(quantiles(0.5, 0.75, 0.9, 0.95, 0.99), Float64),
    `error_count` UInt64,
    `total_count` UInt64,
    `timestamp` DateTime,
    `deployment_environment` String,
    `k8s_cluster_name` String,
    `k8s_namespace_name` String
)
AS SELECT
    A.`resource_string_service$$name` AS src,
    B.`resource_string_service$$name` AS dest,
    quantilesState(0.5, 0.75, 0.9, 0.95, 0.99)(toFloat64(B.duration_nano)) AS duration_quantiles_state,
    countIf(B.status_code = 2) AS error_count,
    count(*) AS total_count,
    toStartOfMinute(B.timestamp) AS timestamp,
    B.resources_string['deployment.environment'] AS deployment_environment,
    B.resources_string['k8s.cluster.name'] AS k8s_cluster_name,
    B.resources_string['k8s.namespace.name'] AS k8s_namespace_name
FROM {db:traces}.signoz_index_v3 AS A, {db:traces}.signoz_index_v3 AS B
WHERE (A.`resource_string_service$$name` != B.`resource_string_service$$name`) AND (A.span_id = B.parent_span_id) AND (B.span_id != '') AND (A.span_id != '')
GROUP BY
    timestamp,
    src,
    dest,
    deployment_environment,
    k8s_cluster_name,
    k8s_namespace_name
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:traces}.root_operations ON CLUSTER {cluster} TO {db:traces}.top_level_operations
(
    `name` LowCardinality(String),
    `serviceName` LowCardinality(String)
)
AS SELECT DISTINCT
    name,
    `resource_string_service$$name` AS serviceName
FROM {db:traces}.signoz_index_v3
WHERE parent_span_id = ''
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:traces}.sub_root_operations ON CLUSTER {cluster} TO {db:traces}.top_level_operations
(
    `name` LowCardinality(String),
    `serviceName` LowCardinality(String)
)
AS SELECT DISTINCT
    name,
    `resource_string_service$$name` AS serviceName
FROM {db:traces}.signoz_index_v3 AS A, {db:traces}.signoz_index_v3 AS B
WHERE (A.`resource_string_service$$name` != B.`resource_string_service$$name`) AND (A.parent_span_id = B.span_id) AND (B.span_id != '') AND (A.span_id != '')
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:traces}.trace_summary_mv ON CLUSTER {cluster} TO {db:traces}.trace_summary
(
    `trace_id` FixedString(32),
    `start` DateTime64(9),
    `end` DateTime64(9),
    `num_spans` UInt64
)
AS SELECT
    trace_id,
    min(timestamp) AS start,
    max(timestamp) AS end,
    toUInt64(count()) AS num_spans
FROM {db:traces}.signoz_index_v3
GROUP BY trace_id
;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db:traces}.usage_explorer_mv ON CLUSTER {cluster} TO {db:traces}.usage_explorer
(
    `timestamp` DateTime64(9) CODEC(DoubleDelta, LZ4),
    `service_name` LowCardinality(String) CODEC(ZSTD(1)),
    `count` UInt64 CODEC(T64, ZSTD(1))
)
AS SELECT
    toStartOfHour(timestamp) AS timestamp,
    serviceName AS service_name,
    count() AS count
FROM {db:traces}.signoz_index_v2
GROUP BY
    timestamp,
    serviceName
;
