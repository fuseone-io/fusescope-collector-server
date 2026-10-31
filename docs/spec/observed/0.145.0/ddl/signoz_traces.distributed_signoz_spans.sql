CREATE TABLE signoz_traces.distributed_signoz_spans
(
    `timestamp` DateTime64(9) CODEC(DoubleDelta, LZ4),
    `traceID` FixedString(32) CODEC(ZSTD(1)),
    `model` String CODEC(ZSTD(9))
)
ENGINE = Distributed('cluster', 'signoz_traces', 'signoz_spans', cityHash64(traceID))

