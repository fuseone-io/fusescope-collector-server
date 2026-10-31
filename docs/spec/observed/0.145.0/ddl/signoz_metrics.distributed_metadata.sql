CREATE TABLE signoz_metrics.distributed_metadata
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
ENGINE = Distributed('cluster', 'signoz_metrics', 'metadata', rand())

