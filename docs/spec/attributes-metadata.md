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
  window gives no row and is counted. The table keeps 30 days [ddl TTL], so this only drops
  metadata of data older than a day, which the backend's recent-window queries do not reach.

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
  whatever the attributes, and memory by `max_windows × max_sets_per_window` keys (16 bytes of
  fingerprints plus overhead each) per signal. Rotating, high-cardinality attributes such as
  request ids fill a resource's quota and stop there. The reference's limits were not observed.
- **Isolation.** Metadata does not wait for ClickHouse and never returns an error to the data
  path; computing fingerprints still costs CPU on it, which the benchmarks bound. Records are
  read after the signal's exporter has accepted them for writing; rows go to a bounded queue
  and are inserted in batches; a full queue drops rows and counts them. An insert that fails is
  logged and counted, its rows are dropped, and their keys are released so a later occurrence in
  the same window writes them again. Inserts report the exporter's insert latency, rows and
  errors under `table=distributed_attributes_metadata`, like the other tables.

## 6. Not observed

- Value kinds other than int and string (§2.1) and logs without a timestamp (§3): decided
  above and covered by tests.
- The reference's behaviour at its limits, and its memory over long runs.
