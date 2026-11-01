CREATE MATERIALIZED VIEW signoz_traces.sub_root_operations TO signoz_traces.top_level_operations
(
    `name` LowCardinality(String),
    `serviceName` LowCardinality(String)
)
AS SELECT DISTINCT
    name,
    `resource_string_service$$name` AS serviceName
FROM signoz_traces.signoz_index_v3 AS A, signoz_traces.signoz_index_v3 AS B
WHERE (A.`resource_string_service$$name` != B.`resource_string_service$$name`) AND (A.parent_span_id = B.span_id) AND (B.span_id != '') AND (A.span_id != '')
