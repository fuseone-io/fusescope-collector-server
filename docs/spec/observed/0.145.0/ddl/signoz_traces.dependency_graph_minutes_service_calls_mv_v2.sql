CREATE MATERIALIZED VIEW signoz_traces.dependency_graph_minutes_service_calls_mv_v2 TO signoz_traces.dependency_graph_minutes_v2
(
    `src` LowCardinality(String),
    `dest` LowCardinality(String),
    `duration_quantiles_state` AggregateFunction(quantiles(0.5, 0.75, 0.9, 0.95, 0.99), Float64),
    `error_count` UInt64,
    `total_count` UInt64,
    `timestamp` DateTime,
    `deployment_environment` String,
    `k8s_cluster_name` String,
    `k8s_namespace_name` String
)
AS SELECT
    A.`resource_string_service$$name` AS src,
    B.`resource_string_service$$name` AS dest,
    quantilesState(0.5, 0.75, 0.9, 0.95, 0.99)(toFloat64(B.duration_nano)) AS duration_quantiles_state,
    countIf(B.status_code = 2) AS error_count,
    count(*) AS total_count,
    toStartOfMinute(B.timestamp) AS timestamp,
    B.resources_string['deployment.environment'] AS deployment_environment,
    B.resources_string['k8s.cluster.name'] AS k8s_cluster_name,
    B.resources_string['k8s.namespace.name'] AS k8s_namespace_name
FROM signoz_traces.signoz_index_v3 AS A, signoz_traces.signoz_index_v3 AS B
WHERE (A.`resource_string_service$$name` != B.`resource_string_service$$name`) AND (A.span_id = B.parent_span_id) AND (B.span_id != '') AND (A.span_id != '')
GROUP BY
    timestamp,
    src,
    dest,
    deployment_environment,
    k8s_cluster_name,
    k8s_namespace_name
