# ClickHouse traces · write contract

What `fusescope_clickhouse` must write for each OTLP span so the FuseScope backend and the
ClickHouse materialized views see exactly what the existing collector produces.

**Sources** (clean-room, [ADR 0002](../adr/0002-clean-room.md)):

- **[obs]**: black-box observation of `fusescope-otel-collector` 0.145.0 on ClickHouse 25.12.5
  in [`observed/0.145.0/`](observed/0.145.0/). The corpus is in `test/observe/corpus.py`; DDL is
  in `ddl/`, rows in `rows/`, and the `INSERT` statements from `system.query_log` in `inserts.jsonl`.
- **[be]**: FuseScope backend (MIT), `pkg/statementbuilder/resourcefilter`.
- **[otel]**: OpenTelemetry trace data model.

## 1. Tables written

The collector inserts into the `Distributed` tables only; ClickHouse forwards to the local
replicated tables and runs the materialized views [obs `inserts.jsonl`].

| Insert into `signoz_traces.` | Rows | Feeds (via MV) |
|---|---|---|
| `distributed_signoz_index_v3` | one per span | `dependency_graph_minutes_v2`, `top_level_operations`, `trace_summary` |
| `distributed_signoz_error_index_v2` | one per `exception` event | — |
| `distributed_traces_v3_resource` | one per distinct resource per time bucket | — |
| `distributed_tag_attributes_v2` | one per distinct (key, type, value) per hour | — |
| `distributed_span_attributes_keys` | one per distinct (key, type, data type) | — |

The exporter never writes the MV targets. The Distributed sharding key is `cityHash64(trace_id)`
[obs `ddl/signoz_traces.distributed_signoz_index_v3.sql`]. Columns with a `DEFAULT` or `ALIAS`
(`resource_string_*`, `attribute_string_*`, `*_exists`, `created_at`, camelCase aliases) are
computed by ClickHouse and are not sent.

**Chunks (deliberate difference).** Every span copies its resource into `resources_string` and
`resource`, and its scope into `scope_string`, so a large resource over many small spans expands
far beyond the request. When those copies, plus 256 bytes per span, pass 256 MiB, the batch is
inserted in chunks into every table. A chunk holds whole traces, assigned in order of first
appearance: `dependency_graph_minutes_service_calls_mv_v2` joins a parent span and its child
within one inserted block (as does the sub-root operations view), so splitting a trace would
lose its service-call edges. Only a trace larger than a chunk is split, keeping every span; a
warning reports how many, since their derived parent-child rows may be missing. The per-batch
deduplication of §6 and §7 then applies per chunk: a later chunk can repeat a tag or key row,
which the `ReplacingMergeTree` merges. The reference's limits were not observed.

## 2. Shared derivations

| Value | Rule | Source |
|---|---|---|
| `ts_bucket_start` | `floor(start_unix_seconds / 1800) * 1800` | obs: two runs, 21:52:05 and 21:57:35 → `1793482200` |
| hour bucket | `floor(start_unix_ms / 3600000) * 3600000`, from the span start, not ingest time | obs `tag_attributes_v2.unix_milli`, span `three-hours-old` |
| `inserted_at` | wall clock at insert, millisecond precision | obs |
| resource attribute value | every resource value is stringified (int `4242` → `"4242"`) | obs `resources_string`, `labels`, `resource` |

### 2.1 Resource fingerprint

`resource_fingerprint` (index) and `fingerprint` (resource table) are the same string, for
traces and logs alike [obs `traces_v3_resource`, `logs_v2_resource`]:

```
<k1>=<v1>;<k2>=<v2>;…;hash=<decimal uint64>
```

- **hash**: FNV-1a 64 over the resource attributes sorted by key, each written as
  `key 0xFF value 0xFF` with values stringified. Verified on all 6 observed trace resources and
  the observed log resource.
- **prefix**: `key=value;` for each identifying attribute present, in this order (from all 7
  observed resources):
  1. `cloud.region`;
  2. `k8s.cluster.name`;
  3. `service.namespace`, else `k8s.namespace.name`;
  4. `service.name`;
  5. `deployment.environment`;
  6. `k8s.pod.name`, else `host.name`;
  7. `container.name`.

  Not prefixed: `deployment.environment.name`, `k8s.deployment.name`, `k8s.node.name`,
  `service.version`, `process.pid`, `os.type`, `telemetry.sdk.language`.
- The backend selects fingerprints from the resource table and filters on
  `resource_fingerprint IN (…)` [be]. A matching hash is still required so both collectors write
  one fingerprint per resource during shadow and cutover.

### 2.2 Attribute typing

Span attributes [obs span `typed-attributes`]:

| OTLP value | Map column | Map value |
|---|---|---|
| string | `attributes_string` | as is; empty strings kept |
| bytes | `attributes_string` | standard base64 |
| array | `attributes_string` | compact JSON (`[1,2,3]`, `["a","b"]`) |
| int, double | `attributes_number` | Float64 |
| bool | `attributes_bool` | Bool |
| kvlist | flattened | each leaf as `parent.child`, typed by the leaf |

`attributes` (JSON column) holds the same attributes as a nested object, split on `.`, with
original types (arrays as arrays, bytes as base64). When a key is both a leaf and a prefix of
another key (`a` and `a.b`), the leaf is absent from the JSON column in either order; the map
columns keep both [obs spans `strings`, `collision-reversed`]. NaN and infinite double
attributes are dropped everywhere: map columns, JSON column, tags and keys [obs span
`special-floats`]. `attributes_promoted` is always
`{}` [obs].

`resource` is the nested form of `resources_string`. `scope` is
`{"name", "version", "attributes": <nested>}` [obs].

## 3. `signoz_index_v3`

Inserted columns, in order [obs `inserts.jsonl`]: `ts_bucket_start, resource_fingerprint,
timestamp, trace_id, span_id, trace_state, parent_span_id, flags, name, kind, kind_string,
duration_nano, status_code, status_message, status_code_string, attributes_string,
attributes_number, attributes_bool, attributes, attributes_promoted, resources_string, resource,
scope, events, links, response_status_code, external_http_url, http_url, external_http_method,
http_method, http_host, db_name, db_operation, has_error, is_remote, inserted_at`.

| Column | Rule |
|---|---|
| `timestamp` | span start, ns |
| `trace_id`, `span_id`, `parent_span_id` | lowercase hex; `parent_span_id` empty for roots |
| `trace_state` | as received |
| `flags` | raw OTLP `flags` (`0x301` → `769`) |
| `kind` / `kind_string` | 0 `Unspecified`, 1 `Internal`, 2 `Server`, 3 `Client`, 4 `Producer`, 5 `Consumer` |
| `duration_nano` | end − start in unsigned 64-bit arithmetic: it wraps when end < start (`18446744073704551616` for −5 ms), as the reference does [obs span `inverted-times`] |
| `status_code` / `status_code_string` | 0 `Unset`, 1 `Ok`, 2 `Error` |
| `status_message` | as received, for every status code |
| `has_error` | `status_code == 2` |
| `is_remote` | `yes` / `no` from bit `0x200` when bit `0x100` is set, else `unknown` |
| `response_status_code` | from `http.response.status_code` / `http.status_code`, as decimal string |
| `http_method` | from `http.request.method` / `http.method` |
| `http_url` | from `url.full` / `http.url` |
| `http_host` | from `server.address` / `http.host` / `net.peer.name`; with none of them, on client spans only, the hostname of `http.url`, else of `url.full` (port stripped); `net.host.name` ignored [obs `address-urls`: `http.url`'s host while `http_url` is `url.full`; `url-full-only-server`: empty]. The preference for `http.url` was seen with it first in attribute order only |
| `external_http_method`, `external_http_url` | Client spans only, with or without an HTTP method: `http_method` (empty without one), and the hostname of `http_url` (port stripped) when there is one [obs `url-full-only-client`, `http-url-only-client`, `address-urls`] |
| `db_name` | from `db.namespace` / `db.name` |
| `db_operation` | from `db.operation.name` / `db.operation` |

When several keys of one row are present, the one that comes **last in the span's attribute
order** wins, whether current or legacy [obs spans `precedence`, `precedence-reversed`,
`external-host`, `external-url-only`].

**`events`**: one compact JSON string per event, fields in this order [obs span `escaped`]:

```json
{"name":"exception","timeUnixNano":1793483855047000000,"attributeMap":{"exception.escaped":"true","exception.message":"boom","exception.type":"Boom"},"isError":true}
```

- `attributeMap` values are stringified and keys are sorted; the field is omitted when the
  event has no attributes.
- Invalid UTF-8 is stored raw in every String column and map; in JSON text each invalid byte
  becomes `\ufffd`, which ClickHouse also applies when it parses the JSON columns [obs span
  `7574663875746638`, sent as protobuf since OTLP/JSON cannot carry it; the collector accepts it
  with 200].
- Strings use Go `encoding/json` escaping: `<`, `>`, `&` as `<`, `>`, `&`;
  U+2028/U+2029 escaped; other non-ASCII verbatim [obs span `strings`]. The same applies to
  `links` and to `labels` (§5).
- `isError` is present (`true`) only for events named `exception`, with or without
  `exception.type`.

**`links`**: compact JSON array. The parent comes first as
`{"traceId","spanId","refType":"CHILD_OF"}`, with `spanId` omitted for roots. Each OTLP link
follows as `FOLLOWS_FROM`. Link attributes and trace state are dropped [obs spans `GET /cart`,
`typed-attributes`].

## 4. `signoz_error_index_v2`

One row per `exception` event, including events without `exception.type` [obs span `escaped`].

| Column | Rule |
|---|---|
| `timestamp` | event time |
| `errorID` | random UUIDv4 as 32 hex digits, no dashes |
| `groupID` | `md5(serviceName ‖ exceptionType ‖ exceptionMessage)`, hex; verified on all 3 rows |
| `traceID`, `spanID`, `serviceName` | from the span and `service.name` |
| `exceptionType`, `exceptionMessage`, `exceptionStacktrace` | event attributes, `""` when absent |
| `exceptionEscaped` | `exception.escaped` as Bool, `false` when absent |
| `resourceTagsMap` | `resources_string` |

Observed with `low_cardinal_exception_grouping: false`; the `true` grouping is not observed yet.

## 5. `traces_v3_resource`

`labels`: compact JSON of the stringified resource attributes, keys sorted. `fingerprint`
follows §2.1. `seen_at_ts_bucket_start` is the span's `ts_bucket_start`. The table is a
`ReplacingMergeTree`, so repeats within a bucket are harmless; the exporter still sends each
distinct resource once per batch.

## 6. `tag_attributes_v2`

`unix_milli` is the hour bucket. Rows are deduplicated per batch by key, type and value, without
the hour, so the first span carrying a value decides its bucket [obs span `three-hours-old`: only
its new values land in its own hour]:

| `tag_type` | Keys | `string_value` / `number_value` |
|---|---|---|
| `tag` | span attributes (flattened, §2.2) | string types → `string_value`; numbers → `number_value`; bool → both empty/null |
| `resource` | resource attributes | `string_value`, kept when empty [obs: the resource of `empty-service`] |
| `scope` | `scope.name`, `scope.version`, scope attributes | `string_value` |
| `spanfield` | `name`, `kind`, `kind_string`, `status_code`, `status_code_string`, `http_url`, `http_method`, `http_host`, `external_http_url`, `external_http_method`, `db_name`, `db_operation`, `response_status_code`, `is_remote`, `has_error` | as in §3; empty values skipped; `has_error` bool, no value |

`tag_data_type` is `string`, `float64` or `bool`. String values longer than 256 bytes are
skipped, for `tag` and `spanfield` alike, while the key still reaches `span_attributes_keys`
[obs spans `lengths`, `external-host`]. The limit is assumed for `resource` and `scope` too.

## 7. `span_attributes_keys`

One row per distinct `(tagKey, tagType, dataType)` with `tagType` `tag`, `resource` or `scope`
(including `scope.name`, `scope.version`) and `isColumn = false` [obs]. The Distributed table has
no `timestamp`; the local table defaults it to the insert time, and its 15-day TTL expires keys that
stop arriving [obs `ddl/signoz_traces.distributed_span_attributes_keys.sql`].

Across batches the reference does not write a key again that an earlier batch wrote; the
exporter deduplicates per batch only and repeats it. The table is a `ReplacingMergeTree`, so
both converge to the same rows, and the parity suite compares such tables with `FINAL`
[parity: protobuf probes sent as a second batch].

## 8. Still to observe

- `low_cardinal_exception_grouping: true`;
- NaN and infinite doubles;
- `external_http_url` for a client span with an HTTP method but no URL;
- insert settings on the Distributed tables (sync vs async insert).
