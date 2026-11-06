# Attribute metadata · `signoz_metadata.attributes_metadata` contract

Which attribute sets the collector records so that the backend can suggest related values in
filters (#61). Clean-room (ADR 0002). Sources:

- **[obs]**: [`observed/meter-metadata/`](observed/meter-metadata/), a black-box run of the
  earlier collector (`fusescope-otel-collector:0.145.0`, chart 0.160.1 on kind, 2026-11-05/06).
  Probe names (`obs-…`) are the `service.name` of the input; the scripts that sent them are in
  `scripts/` there, and `attributes_metadata.tsv` holds every row the run produced.
- **[be]**: the FuseScope backend (`fuseone-io/fusescope`, read at `a2bffc1ae`); paths below
  are relative to it.
- **[ddl]**: [`observed/meter-metadata/ddl-attributes_metadata.sql`](observed/meter-metadata/ddl-attributes_metadata.sql),
  created by the backend's schema migrator, not by the collector.

This is `signoz_metadata`, not the metrics metadata `signoz_metrics.distributed_metadata` that
[`clickhouse-metrics.md`](clickhouse-metrics.md) §5 specifies.

## 1. What the backend reads

- **Related values.** For a filter key, the backend selects
  `DISTINCT if(notEmpty(resource_attributes[k]), resource_attributes[k], attributes[k])` from
  `signoz_metadata.distributed_attributes_metadata`, filtered by `data_source` (the signal), an
  optional existing filter expression over both maps, and `unix_milli` between the request's
  start and end [be `pkg/telemetrymetadata/metadata.go:1347-1440`, table name
  `pkg/telemetrymetadata/tables.go:5`]. Callers: the fields API and AI observability
  [be `pkg/modules/fields/implfields/handler.go:77`,
  `pkg/modules/aiobservability/implaiobservability/handler.go:104`].
- **Window rounding.** The start is rounded down to a multiple of 6 h (21 600 000 ms) before the
  query [be `pkg/types/telemetrytypes/field.go:316-321`], so a row stamped with the start of a
  6 h window is visible to every query that overlaps that window.
- Nothing reads `resource_fingerprint` or `attrs_fingerprint`; they are only part of the table's
  sorting key [ddl].

## 2. Rows

One row per distinct (signal, resource attributes, record attributes) per window (§3, §4).

| Column | Value |
|---|---|
| `unix_milli` | start of the record's 6 h UTC window, in ms (§3) |
| `data_source` | `traces`, `logs` or `metrics` |
| `resource_attributes` | the resource attributes, as strings (§2.1) |
| `attributes` | the record's attributes, as strings (§2.1), plus for spans `name` |
| `resource_fingerprint` | fingerprint of `resource_attributes` (§4) |
| `attrs_fingerprint` | fingerprint of `attributes` (§4) |

- **Spans.** The span attributes, plus the span name under the key `name`
  [obs `obs-w21`: `{name: lets-go, peer.service: …, network.peer.address: …}`; `obs-nosvc`:
  `{name: obs-nosvc, marker: obs-nosvc}`]. A span with no attributes still has a row with
  `{name: …}` [obs `obs-old`].
- **Logs.** The log record's attributes; the body is not included [obs `obs-nosvc`: body
  `obs-nosvc`, attributes `{marker: obs-nosvc}`]. A log without attributes has a row with `{}`
  [obs `obs-keyid`].
- **Metrics.** The data point's attributes; the metric name is not included [obs `obs-metrics`:
  `{obs.key: mv}`; `obs-series`: `{}`].
- **Resource.** All resource attributes, with no default added: a resource with none gives `{}`
  [obs `obs-nosvc`], one with only `service.name` gives just that [obs `obs-bare-logs`], and
  `signoz.workspace.key.id` and `host.name` are kept like any other [obs `obs-keyid`].

### 2.1 Values as strings

The columns are `Map(LowCardinality(String), String)` [ddl], so every value is stored as its
string form. An int `1` and the string `"1"` give the same row [obs `obs-fp`: `fp-ab` with
`a="1"` and `fp-int` with `a=1` left one row].

**Deliberate, tested, not observed:** values of the other kinds are converted with the
OpenTelemetry `AnyValue` string form (pdata `Value.AsString`: decimal numbers, `true`/`false`,
JSON for arrays and maps, base64 for bytes). Only int and string were observed; the tests cover
every kind.

A key that is absent and a key present with an empty value are different sets.

## 3. Window

- **Width.** 6 h, aligned to UTC midnight: inputs at 19:03, 20:05 and 21:15 gave
  `2026-11-05 18:00`, one at 02:35 gave `2026-11-06 00:00` [obs: phase 1–3; `obs-w21` at 21:15,
  `obs-fp` at 02:35]. A 3 h window would have given 21:00 for `obs-w21`.
- **Time used.** Spans: the start time. Logs: the record timestamp. Metrics: the arrival time
  [obs `obs-old`: a span, a log and a gauge point all stamped 7 h in the past, sent at 02:56;
  the span and the log got `2026-11-05 18:00`, the metric `2026-11-06 00:00`].
- **Log without a timestamp (deliberate, tested, not observed).** It uses its observed
  timestamp, then the arrival time.
- **Admitted windows (deliberate).** A record's window must be one of the `max_windows` (4,
  that is 24 h) most recent windows by the collector's clock. A record time in a later window
  (a clock ahead, or a bogus timestamp) is clamped to the current window; one in an earlier
  window gives no row and is counted.
- **Backfill loss (deliberate break).** The table keeps 30 days [ddl TTL] and the backend can
  query related values over any range in it (§1). Data sent while it happens is unaffected:
  each window it falls in gets its rows at the time. Data that arrives more than a day after its
  own time (a backfill, a client replaying a buffer) gets no metadata, so related values for
  those windows miss its sets; the data itself is written. The reference recorded those
  windows [obs `obs-old`, 7 h late].

## 4. Deduplication and fingerprints

- **Within a window.** An identical set sent again in the same window gives no new row
  [obs phase 1: the repeated `obs-traces`, `obs-logs` and `obs-metrics` sets left 9 rows raw and
  9 with `FINAL`]; a set with a new attribute value gives a new row [obs: `obs-traces` with
  `obs.key=tv-new`].
- **Key.** The collector deduplicates on (signal, window start, resource fingerprint, attributes
  fingerprint), the table's sorting key [ddl], so at most one row per set and window leaves the
  collector.
- **Order.** The fingerprint does not depend on attribute order [obs `obs-fp`: `a,b` and `b,a`
  left one row]. The collector sorts by key, then hashes each key and string value as in
  [`clickhouse-metrics.md`](clickhouse-metrics.md) §4 (FNV-1a 64, each string followed by
  `0xFF`), over the string form of §2.1, so type does not change it.
- **Values differ from the reference.** The reference's hash function was not identified and is
  not needed: nothing reads the fingerprints (§1). At cutover a set may be written once by each
  collector in the same window; the backend reads `DISTINCT` values, so the result is the same.

## 5. Deliberate differences

- **A new row per window (bug fix).** The reference does not write an identical set again in a
  later window: `obs-w21`, first seen at 21:15 in the 18:00 window, was sent again at 00:01 and
  got no 00:00 row, while a new set at 02:35 did [obs phase 3b, `obs-fp`]. Because the backend
  filters `unix_milli >= start` rounded to 6 h (§1), a stable set then disappears from related
  values for queries that start after its first window. The collector writes each set once per
  window it appears in.
- **After a restart** the deduplication state is empty, so a set may be written a second time in
  the same window. Both rows have the same sorting key; `ReplacingMergeTree` merges them [ddl],
  and `DISTINCT` hides them before that.
- **Limits (admission without eviction).** Each admitted window holds at most
  `max_sets_per_window` (100 000) keys per signal, and at most `max_sets_per_resource` (1 000)
  of them per resource fingerprint. A key is admitted once and kept until its window leaves the
  `max_windows` range (§3); nothing is evicted earlier. When a limit is reached, new sets in
  that window are dropped and counted until the window expires; sets already admitted keep
  deduplicating. So rows written per signal are bounded by `max_sets_per_window` per window
  whatever the attributes. Rotating, high-cardinality attributes such as request ids fill a
  resource's quota and stop there. The reference's limits were not observed.
- **Bytes and rows.** A row's cost is its keys and values, plus `attribute_overhead` (64
  bytes) per map entry, plus a fixed `row_overhead` (256 bytes) for the row and its two maps, so
  neither empty `{}` rows nor rows of many short attributes are cheap. A row with more than
  `max_attributes_per_row` (256) entries across both maps, or costing more than
  `max_row_bytes` (16 KiB), is not written; it is not truncated, since a cut set would be a
  wrong suggestion. The entry count is checked before sorting or hashing, so an oversized row
  costs one pass over its maps. Rows waiting for insert are bounded by both `max_queue_rows`
  (20 000) and `max_queue_bytes` (8 MiB) of cost; at the 256-byte minimum the bytes alone
  would allow 32 768 rows, so with the defaults the row limit binds first for small rows. The
  deduplication state holds fingerprints and the window, a fixed size per key, plus the
  per-resource quota index, capped together by `max_state_bytes` (32 MiB).
- **Memory tests.** Heap tests check that the costs above bound the retained memory: a full
  queue of rows of `max_attributes_per_row` one-byte attributes, a full queue of `{}` rows, and
  a full state whose keys come from distinct resources, so that the quota index is measured
  too. Each stays within its budget plus a stated slack.
- **Admission order.** A key enters the state only when its row is queued: it is checked
  against the state, then `max_row_bytes`, then the window and resource quotas, then
  `max_state_bytes`, then the queue. A row refused at any step is not admitted, so the same set
  is tried again on its next occurrence; a set refused for a quota keeps being refused until
  its window expires because the quota stays full. An insert that fails releases the keys of
  its rows, which are then dropped, so a later occurrence in the same window writes them again.
- **Only metadata is lost.** Every refusal drops only the metadata row and increments
  `fusescope_attributes_metadata_dropped` (unit `{row}`, attributes `signal` and `reason`),
  with `reason` one of `window` (`max_sets_per_window`), `resource`
  (`max_sets_per_resource`), `row_bytes` (`max_row_bytes` or `max_attributes_per_row`),
  `queue` (`max_queue_rows` or `max_queue_bytes`), `state_bytes`, `too_old` (§3) and `insert`; the data itself is always written.
- **Isolation.** Metadata does not wait for ClickHouse and never returns an error to the data
  path; computing fingerprints still costs CPU on it, which the benchmarks bound. Records are
  read after the signal's exporter has accepted them for writing; queued rows are inserted in
  batches. Inserts report the exporter's insert latency, rows and errors under
  `table=distributed_attributes_metadata`, like the other tables.
- **Regression tests.** Many distinct sets with empty maps fill `max_queue_rows`, not memory
  (with the defaults, and with a reduced `max_queue_rows` and a larger `max_queue_bytes` so each
  limit is reached alone);
  a set refused by a full queue is written on its next occurrence once the queue drains; a row
  over `max_row_bytes` is retried, and refused again, on every occurrence without entering the
  state.

## 6. Not observed

- Value kinds other than int and string (§2.1) and logs without a timestamp (§3): decided
  above and covered by tests.
- The reference's behaviour at its limits, and its memory over long runs.
