DROP TABLE IF EXISTS zen_traces_test.service_dependency_graph ON CLUSTER cluster;
DROP VIEW IF EXISTS zen_traces_test.service_dependency_graph_service_calls_mv ON CLUSTER cluster;
DROP VIEW IF EXISTS zen_traces_test.service_dependency_graph_db_calls_mv ON CLUSTER cluster;
DROP VIEW IF EXISTS zen_traces_test.service_dependency_graph_messaging_calls_mv ON CLUSTER cluster;

DROP TABLE IF EXISTS zen_traces_test.distributed_service_dependency_graph ON CLUSTER cluster;
