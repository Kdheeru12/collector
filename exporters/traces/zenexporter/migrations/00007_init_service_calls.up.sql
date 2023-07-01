CREATE TABLE IF NOT EXISTS zen_traces_test.service_calls ON CLUSTER cluster (
    name LowCardinality(String) CODEC(ZSTD(1)),
    serviceName LowCardinality(String) CODEC(ZSTD(1))
) ENGINE ReplacingMergeTree
ORDER BY (serviceName, name);


CREATE MATERIALIZED VIEW IF NOT EXISTS zen_traces_test.internal_service_calls ON CLUSTER cluster 
TO zen_traces_test.service_calls
AS SELECT DISTINCT
    name,
    serviceName
FROM zen_traces_test.traces AS A, zen_traces_test.traces AS B
WHERE (A.serviceName != B.serviceName) AND (A.parentSpanID = B.spanID);

CREATE MATERIALIZED VIEW IF NOT EXISTS zen_traces_test.external_service_calls ON CLUSTER cluster 
TO zen_traces_test.service_calls
AS SELECT DISTINCT
    name,
    serviceName
FROM zen_traces_test.traces
WHERE parentSpanID = '';

CREATE TABLE IF NOT EXISTS zen_traces_test.distributed_service_calls ON CLUSTER cluster AS zen_traces_test.service_calls
ENGINE = Distributed("cluster", "zen_traces_test", service_calls, cityHash64(rand()));
