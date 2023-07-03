CREATE TABLE IF NOT EXISTS zen_traces_test.query_keys ON CLUSTER cluster (
    tagKey LowCardinality(String) CODEC(ZSTD(1)),
    tagType Enum('tag', 'resource') CODEC(ZSTD(1)),
    dataType Enum('string', 'bool', 'float64') CODEC(ZSTD(1)),
    serviceName LowCardinality(String) CODEC(ZSTD(1)),
    isColumn bool CODEC(ZSTD(1)),
) ENGINE ReplacingMergeTree
ORDER BY (tagKey, tagType, dataType, serviceName, isColumn);

CREATE TABLE IF NOT EXISTS zen_traces_test.distributed_query_keys ON CLUSTER cluster AS zen_traces_test.query_keys
ENGINE = Distributed("cluster", "zen_traces_test", query_keys, cityHash64(rand()));