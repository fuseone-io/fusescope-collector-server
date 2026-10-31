CREATE TABLE signoz_logs.distributed_logs_v2_resource
(
    `labels` String CODEC(ZSTD(5)),
    `fingerprint` String CODEC(ZSTD(1)),
    `seen_at_ts_bucket_start` Int64 CODEC(Delta(8), ZSTD(1)),
    `_retention_days` UInt16 DEFAULT 15,
    `_retention_days_cold` UInt16 DEFAULT 0
)
ENGINE = Distributed('cluster', 'signoz_logs', 'logs_v2_resource', cityHash64(labels, fingerprint))

