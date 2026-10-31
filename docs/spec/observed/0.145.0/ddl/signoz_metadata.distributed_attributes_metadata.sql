CREATE TABLE signoz_metadata.distributed_attributes_metadata
(
    `unix_milli` UInt64,
    `data_source` String,
    `resource_fingerprint` UInt64,
    `attrs_fingerprint` UInt64,
    `resource_attributes` Map(LowCardinality(String), String),
    `attributes` Map(LowCardinality(String), String)
)
ENGINE = Distributed('cluster', 'signoz_metadata', 'attributes_metadata', cityHash64(data_source, resource_fingerprint, attrs_fingerprint))
