# ClickHouse logs · write contract

What `fusescope_clickhouse` must write for each OTLP log record. Same sources and conventions as
[`clickhouse-traces.md`](clickhouse-traces.md) (clean-room, ADR 0002); **[obs]** refers to
[`observed/0.145.0/`](observed/0.145.0/), record names to the `logs` part of `test/observe/corpus.py`.

## 1. Tables written

Distributed tables only; ClickHouse fills the local tables [obs `inserts.jsonl`].

| Insert into `signoz_logs.` | Rows | Columns |
|---|---|---|
| `distributed_logs_v2` | one per record | §3 |
| `distributed_logs_v2_resource` | one per distinct resource per 30-minute bucket | `labels, fingerprint, seen_at_ts_bucket_start` |
| `distributed_tag_attributes_v2` | one per distinct (key, type, value) per batch | §4 |
| `distributed_logs_attribute_keys` | one per distinct (attribute key, datatype) | `name, datatype` |
| `distributed_logs_resource_keys` | one per distinct (resource key, datatype) | `name, datatype` |

`_retention_days`, `_retention_days_cold`, `created_at` and the `$$` columns are defaults computed
by ClickHouse and are not sent. The key tables' local `timestamp` defaults to the insert time.

## 2. Shared with traces

- **Resource fingerprint, `labels`, `resources_string`, `resource`**: exactly as traces §2.1 and §5.
  The same resource gets the same fingerprint in both signals [obs `logs_v2_resource` vs
  `traces_v3_resource`].
- **`ts_bucket_start`**: `floor(timestamp_seconds / 1800) * 1800`, from the record's `timestamp`
  (§3) [obs record `three hours old`].
- **Escaping** of JSON text: Go `encoding/json` (traces §3).

## 3. `logs_v2`

Inserted columns, in order: `ts_bucket_start, resource_fingerprint, timestamp, observed_timestamp,
id, trace_id, span_id, trace_flags, severity_text, severity_number, body, body_v2, body_promoted,
attributes_string, attributes_number, attributes_bool, resources_string, resource, scope_name,
scope_version, scope_string, inserted_at`.

| Column | Rule |
|---|---|
| `timestamp` | OTLP `time_unix_nano`; if 0, `observed_time_unix_nano`; if both are 0, ingest time (ns) [obs records `observed time only`, `no time at all`] |
| `observed_timestamp` | OTLP `observed_time_unix_nano`; if 0, ingest time (ns), also when the record has a time [obs records `no time at all`, `bad … utf8` of `logs-invalid-utf8.pb`]. The reference reads the clock per record; the exporter reads it once per batch |
| `id` | 27-character base62 (`0-9A-Za-z`, zero-padded) of 20 bytes: big-endian uint64 `timestamp − 1.4·10¹⁸` followed by 12 random bytes. This is the KSUID layout with a nanosecond timestamp, so ids sort by time [obs: all rows] |
| `trace_id`, `span_id` | lowercase hex; empty when absent |
| `trace_flags` | OTLP `flags` |
| `severity_text`, `severity_number` | as received; no derivation between them [obs record `text only severity`: `Warning` / 0] |
| `body` | string: as is; int, double, bool: stringified (`42`, `2.5`, `true`); **bytes: the raw bytes**, not base64; map and array: compact JSON with keys sorted [obs `body.kind` records] |
| `body_v2` | `{}` (renders as `{"message":""}`) |
| `body_promoted` | `{}` |
| `attributes_*` | typed as traces §2.2, except that **kvlist values are not flattened**: they become their JSON text in `attributes_string` [obs record `FATAL`] |
| `scope_name`, `scope_version` | as received |
| `scope_string` | scope attributes, all values stringified (int `2` → `"2"`) |
| `inserted_at` | wall clock at insert, ms |

There is no `attributes` JSON column for logs. Unlike traces, NaN and infinite double attributes
are kept, in `attributes_number`, the tags and the key table [obs record `special floats`].

## 4. `tag_attributes_v2`

As traces §6 (per-batch dedup without the hour, 256-byte limit on string values, bool with no
value), with these differences:

- **`unix_milli` is the ingest hour**, not the record's: a five-hour-old record placed first in
  the batch still lands in the current hour [obs records `three hours old`,
  `five hours old, first in batch`];
- tag types are `tag` (record attributes, kvlists as JSON strings), `resource`, `scope` (scope
  attributes, stringified; `scope.name` and `scope.version` are not tags) and `logfield`;
- `logfield` rows: `severity_text` (string, **empty kept**) and `severity_number` (float64).

## 5. Key tables

`logs_attribute_keys` holds record attribute keys with datatype `string`, `float64` or `bool`, by the
same typing as §3; kvlists are `string`. `logs_resource_keys` holds resource keys, always
`string`. Scope attributes appear in neither [obs].

## 6. Still to observe

- severity text or body longer than any limit (no truncation seen up to 5000 bytes);
- logs without a resource `service.name` in the UI (no write difference expected).
