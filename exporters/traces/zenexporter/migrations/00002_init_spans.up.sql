CREATE TABLE IF NOT EXISTS zen_traces_test.spans ON CLUSTER cluster (
    timestamp DateTime64(9) CODEC(DoubleDelta, LZ4),
    traceID FixedString(32) CODEC(ZSTD(1)),
    spanData String CODEC(ZSTD(9))
) ENGINE MergeTree
PARTITION BY toDate(timestamp)
ORDER BY traceID
TTL toDateTime(timestamp) + INTERVAL 604800 SECOND DELETE
SETTINGS index_granularity=1024;



CREATE TABLE IF NOT EXISTS zen_traces_test.distributed_spans ON CLUSTER cluster AS zen_traces_test.spans
ENGINE = Distributed("cluster", "zen_traces_test", spans, cityHash64(traceID));
