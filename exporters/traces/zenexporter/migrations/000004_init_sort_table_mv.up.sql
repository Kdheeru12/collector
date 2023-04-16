CREATE TABLE IF NOT EXISTS zen_traces.traces_durationSort ON CLUSTER cluster (
  timestamp DateTime64(9) CODEC(DoubleDelta, LZ4),
  traceID FixedString(32) CODEC(ZSTD(1)),
  spanID String CODEC(ZSTD(1)),
  parentSpanID String CODEC(ZSTD(1)),
  serviceName LowCardinality(String) CODEC(ZSTD(1)),
  name LowCardinality(String) CODEC(ZSTD(1)),
  kind Int8 CODEC(T64, ZSTD(1)),
  durationNano UInt64 CODEC(T64, ZSTD(1)),
  statusCode Int16 CODEC(T64, ZSTD(1)),
  component LowCardinality(String) CODEC(ZSTD(1)),
  httpMethod LowCardinality(String) CODEC(ZSTD(1)),
  httpUrl LowCardinality(String) CODEC(ZSTD(1)), 
  httpCode LowCardinality(String) CODEC(ZSTD(1)), 
  httpRoute LowCardinality(String) CODEC(ZSTD(1)), 
  httpHost LowCardinality(String) CODEC(ZSTD(1)), 
  gRPCCode LowCardinality(String) CODEC(ZSTD(1)),
  gRPCMethod LowCardinality(String) CODEC(ZSTD(1)),
  hasError bool CODEC(T64, ZSTD(1)),
  tagMap Map(LowCardinality(String), String) CODEC(ZSTD(1)),
  rpcSystem LowCardinality(String) CODEC(ZSTD(1)),
  rpcService LowCardinality(String) CODEC(ZSTD(1)),
  rpcMethod LowCardinality(String) CODEC(ZSTD(1)),
  responseStatusCode LowCardinality(String) CODEC(ZSTD(1)),
  stringTagMap Map(String, String) CODEC(ZSTD(1)),
  numberTagMap Map(String, Float64) CODEC(ZSTD(1)),
  boolTagMap Map(String, bool) CODEC(ZSTD(1)),

  INDEX idx_service serviceName TYPE bloom_filter GRANULARITY 4,
  INDEX idx_name name TYPE bloom_filter GRANULARITY 4,
  INDEX idx_kind kind TYPE minmax GRANULARITY 4,
  INDEX idx_duration durationNano TYPE minmax GRANULARITY 1,
  INDEX idx_httpCode httpCode TYPE set(0) GRANULARITY 1,
  INDEX idx_hasError hasError TYPE set(2) GRANULARITY 1,
  INDEX idx_tagMapKeys mapKeys(tagMap) TYPE bloom_filter(0.01) GRANULARITY 64,
  INDEX idx_tagMapValues mapValues(tagMap) TYPE bloom_filter(0.01) GRANULARITY 64,
  INDEX idx_httpRoute httpRoute TYPE bloom_filter GRANULARITY 4,
  INDEX idx_httpUrl httpUrl TYPE bloom_filter GRANULARITY 4,
  INDEX idx_httpHost httpHost TYPE bloom_filter GRANULARITY 4,
  INDEX idx_httpMethod httpMethod TYPE bloom_filter GRANULARITY 4,
  INDEX idx_timestamp timestamp TYPE minmax GRANULARITY 1,
  INDEX idx_rpcMethod rpcMethod TYPE bloom_filter GRANULARITY 4,
  INDEX idx_responseStatusCode rpcService TYPE bloom_filter GRANULARITY 4,

) ENGINE MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (durationNano, timestamp)
TTL toDateTime(timestamp) + INTERVAL 604800 SECOND DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS zen_traces.traces_durationSortMV ON CLUSTER cluster
TO zen_traces.traces_durationSort
AS SELECT
  timestamp,
  traceID,
  spanID,
  parentSpanID,
  serviceName,
  name,
  kind,
  durationNano,
  statusCode,
  component,
  httpMethod,
  httpUrl,
  httpCode,
  httpRoute,
  httpHost,
  gRPCMethod,
  gRPCCode,
  hasError,
  tagMap,
  rpcSystem,
  rpcService,
  rpcMethod,
  responseStatusCode,
  stringTagMap,
  numberTagMap,
  boolTagMap
FROM zen_traces.traces
ORDER BY durationNano, timestamp;



CREATE TABLE IF NOT EXISTS zen_traces.distributed_traces_durationSort ON CLUSTER cluster AS zen_traces.traces_durationSort
ENGINE = Distributed("cluster", "zen_traces", traces_durationSort, cityHash64(traceID));