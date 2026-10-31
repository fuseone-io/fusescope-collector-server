CREATE TABLE signoz_meter.samples
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

