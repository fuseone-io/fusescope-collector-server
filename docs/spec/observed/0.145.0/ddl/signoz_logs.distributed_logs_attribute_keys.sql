CREATE TABLE signoz_logs.distributed_logs_attribute_keys
(
    `name` String,
    `datatype` String
)
ENGINE = Distributed('cluster', 'signoz_logs', 'logs_attribute_keys', cityHash64(datatype))
