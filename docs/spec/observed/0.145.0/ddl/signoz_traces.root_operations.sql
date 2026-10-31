CREATE MATERIALIZED VIEW signoz_traces.root_operations TO signoz_traces.top_level_operations
(
    `name` LowCardinality(String),
    `serviceName` LowCardinality(String)
)
AS SELECT DISTINCT
    name,
    `resource_string_service$$name` AS serviceName
FROM signoz_traces.signoz_index_v3
WHERE parent_span_id = ''

