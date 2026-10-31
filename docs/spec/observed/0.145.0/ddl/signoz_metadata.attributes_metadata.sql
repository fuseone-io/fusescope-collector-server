CREATE TABLE signoz_metadata.attributes_metadata
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

