CREATE TABLE IF NOT EXISTS zen_traces.service_dependency_graph ON CLUSTER cluster (
    src LowCardinality(String) CODEC(ZSTD(1)),
    dest LowCardinality(String) CODEC(ZSTD(1)),
    duration_quantiles_state AggregateFunction(quantiles(0.5, 0.75, 0.9, 0.95, 0.99), Float64) CODEC(Default),
    error_count SimpleAggregateFunction(sum, UInt64) CODEC(T64, ZSTD(1)),
    total_count SimpleAggregateFunction(sum, UInt64) CODEC(T64, ZSTD(1)),
    timestamp DateTime CODEC(DoubleDelta, LZ4),
    deployment_environment LowCardinality(String) CODEC(ZSTD(1)),
    k8s_cluster_name LowCardinality(String) CODEC(ZSTD(1)),
    k8s_namespace_name LowCardinality(String) CODEC(ZSTD(1))
) ENGINE AggregatingMergeTree
PARTITION BY toDate(timestamp)
ORDER BY (timestamp, src, dest, deployment_environment, k8s_cluster_name, k8s_namespace_name)
TTL toDateTime(timestamp) + INTERVAL 604800 SECOND DELETE;



CREATE MATERIALIZED VIEW IF NOT EXISTS zen_traces.service_dependency_graph_service_calls_mv ON CLUSTER cluster
TO zen_traces.service_dependency_graph AS
SELECT
    A.serviceName as src,
    B.serviceName as dest,
    quantilesState(0.5, 0.75, 0.9, 0.95, 0.99)(toFloat64(B.durationNano)) as duration_quantiles_state,
    countIf(B.statusCode=2) as error_count,
    count(*) as total_count,
    toStartOfMinute(B.timestamp) as timestamp,
    B.resourceTagsMap['deployment.environment'] as deployment_environment,
    B.resourceTagsMap['k8s.cluster.name'] as k8s_cluster_name,
    B.resourceTagsMap['k8s.namespace.name'] as k8s_namespace_name
FROM zen_traces.traces AS A, zen_traces.traces AS B
WHERE (A.serviceName != B.serviceName) AND (A.spanID = B.parentSpanID)
GROUP BY timestamp, src, dest, deployment_environment, k8s_cluster_name, k8s_namespace_name;


CREATE MATERIALIZED VIEW IF NOT EXISTS zen_traces.service_dependency_graph_db_calls_mv ON CLUSTER cluster
TO zen_traces.service_dependency_graph AS
SELECT
    serviceName as src,
    tagMap['db.system'] as dest,
    quantilesState(0.5, 0.75, 0.9, 0.95, 0.99)(toFloat64(durationNano)) as duration_quantiles_state,
    countIf(statusCode=2) as error_count,
    count(*) as total_count,
    toStartOfMinute(timestamp) as timestamp,
    resourceTagsMap['deployment.environment'] as deployment_environment,
    resourceTagsMap['k8s.cluster.name'] as k8s_cluster_name,
    resourceTagsMap['k8s.namespace.name'] as k8s_namespace_name
FROM zen_traces.traces
WHERE dest != '' and kind != 2
GROUP BY timestamp, src, dest, deployment_environment, k8s_cluster_name, k8s_namespace_name;


CREATE MATERIALIZED VIEW IF NOT EXISTS zen_traces.service_dependency_graph_messaging_calls_mv ON CLUSTER cluster
TO zen_traces.service_dependency_graph AS
SELECT
    serviceName as src,
    tagMap['messaging.system'] as dest,
    quantilesState(0.5, 0.75, 0.9, 0.95, 0.99)(toFloat64(durationNano)) as duration_quantiles_state,
    countIf(statusCode=2) as error_count,
    count(*) as total_count,
    toStartOfMinute(timestamp) as timestamp,
    resourceTagsMap['deployment.environment'] as deployment_environment,
    resourceTagsMap['k8s.cluster.name'] as k8s_cluster_name,
    resourceTagsMap['k8s.namespace.name'] as k8s_namespace_name
FROM zen_traces.traces
WHERE dest != '' and kind != 2
GROUP BY timestamp, src, dest, deployment_environment, k8s_cluster_name, k8s_namespace_name;


CREATE TABLE IF NOT EXISTS zen_traces.distributed_service_dependency_graph ON CLUSTER cluster AS zen_traces.service_dependency_graph
ENGINE = Distributed("cluster", "zen_traces", service_dependency_graph, cityHash64(rand()));
