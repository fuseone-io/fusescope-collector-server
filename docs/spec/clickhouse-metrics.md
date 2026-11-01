# ClickHouse metrics · write contract

What `fusescope_clickhouse` must write for each OTLP metric data point. Same sources and
conventions as [`clickhouse-traces.md`](clickhouse-traces.md) (clean-room, ADR 0002). **[obs]**
refers to [`observed/0.145.0/`](observed/0.145.0/), metric names to the `metrics` part of
`test/observe/corpus.py`. The observed run also contains the reference's span-metrics output
(`signoz_*` series, phase 4); this document covers only the corpus metrics.

## 1. Tables written

Distributed tables only [obs `inserts.jsonl`]. ClickHouse derives the `_agg_*`, `_reduced_*`,
`_1day`/`_1week`/`_6hrs` tables and the buffers through materialized views.

| Insert into `signoz_metrics.` | Rows | Columns, in order |
|---|---|---|
| `distributed_samples_v4` | one per OTLP data point and derived series (§3) | `env, temporality, metric_name, fingerprint, unix_milli, value, flags, inserted_at_unix_milli` |
| `distributed_time_series_v4` | one per series per hour per batch | `env, temporality, metric_name, description, unit, type, is_monotonic, fingerprint, unix_milli, labels, attrs, scope_attrs, resource_attrs, __normalized, inserted_at_unix_milli` |
| `distributed_metadata` | one per (metric, attribute, value) per batch | `temporality, metric_name, description, unit, type, is_monotonic, attr_name, attr_type, attr_datatype, attr_string_value, first_reported_unix_milli, last_reported_unix_milli` |

## 2. Series

Each OTLP point becomes one or more series, each with a name, type, temporality and labels.

| OTLP | Series | `type` | `temporality` | `is_monotonic` | `unit` | `value` |
|---|---|---|---|---|---|---|
| Gauge | `<name>` | `Gauge` | `Unspecified` | false | metric unit | point value |
| Sum | `<name>` | `Sum` | `Cumulative` / `Delta` | from the metric | metric unit | point value |
| Histogram | `<name>.bucket`, one per bound plus `le="+Inf"` | `Histogram` | the histogram's | true | metric unit | **cumulative** count up to the bound, also for delta histograms |
| | `<name>.count` | `Sum` | the histogram's | true | `1` | count |
| | `<name>.sum` | `Sum` | the histogram's | true | metric unit | sum; sample and series only if the point has a sum |
| | `<name>.min`, `<name>.max` | `Gauge` | `Unspecified` | false | metric unit | sample and series only if the point has them |
| Summary | `<name>.count` | `Sum` | `Cumulative` | true | `1` | count |
| | `<name>.sum` | `Sum` | `Cumulative` | true | metric unit | sum |
| | `<name>.quantile`, one per quantile | `Summary` | `Unspecified` | false | metric unit | quantile value |
| Exponential histogram | none: not written to any table | | | | | |

[obs: corpus metrics, `probe.delta_histogram`, `probe.histogram_no_minmax`, `probe.exp_cumulative`]

- `le` and `quantile` labels: `strconv.FormatFloat(v, 'f', -1, 64)` (`0.00001`, `1000000`, `1`),
  bucket upper bound `+Inf` [obs `probe.extreme_bounds`, `probe.extreme_quantiles`].
- **NaN** values, and a histogram's absent sum, min or max: no sample and no series. Their
  metadata rows are written for the resource and scope attributes only: not for the point
  attributes, nor `__temporality__` [obs `probe.nan`, `probe.nan_attrs`,
  `probe.histogram_no_minmax`, `probe.no_minmax_attrs`].
- **±Infinity** values are written as they are [obs `probe.inf`, `probe.ninf`].
- **No recorded value** (point flag 1): written with value 0 and `flags = 1` [obs `probe.no_value`].
- Int values become Float64; negative values are kept [obs `probe.cumulative_updown`].

## 3. Columns

| Column | Rule |
|---|---|
| `env` | resource `deployment.environment`, else `""`; `deployment.environment.name` is not used [obs `probe.resource_typed`] |
| `metric_name` | series name (§2), as received otherwise (dots, dashes, colons kept) |
| `unix_milli` (samples) | point time, ms |
| `unix_milli` (time series) | point time floored to the hour, ms [obs `probe.three_hours_old`] |
| `flags` | OTLP point flags |
| `description` | metric description |
| `labels` | compact JSON, keys sorted, of: resource attributes, scope attributes, `__scope.name__`, `__scope.schema_url__`, `__scope.version__`, point attributes, `le`/`quantile`, `__temporality__` and `__name__`; all values stringified (traces §2.2 stringification). Of equal keys it keeps the generated value (`le`/`quantile`, `__temporality__`, `__name__`, `__scope.*__`), then the resource's, the scope's and the point's [obs `probe.collide_*`] |
| `attrs` | point attributes, `le`/`quantile` and `__temporality__`; the generated `le`/`quantile` and `__temporality__` replace a point attribute of that key, other keys (`__name__`) are kept [obs `probe.collide_le`, `probe.collide_generated`] |
| `scope_attrs` | `__scope.name__`, `__scope.schema_url__`, `__scope.version__` and the scope attributes; the generated entries replace scope attributes of those keys [obs `probe.collide_*`] |
| `resource_attrs` | resource attributes |
| `__normalized` | false |
| `inserted_at_unix_milli` | wall clock at insert, ms |
| `fingerprint` | §4 |

## 4. Fingerprint (deliberate difference)

The reference's fingerprint is deterministic but its function is not identifiable from
observation: FNV-1a, xxHash, CityHash, MurmurHash3, MD5 and SHA-256 over the labels in every
subset, order and separator tested do not reproduce it [obs: `probe.bare`, 5 labels].

The exporter therefore computes its own, FNV-1a 64 by level. Each string is hashed as
`bytes 0xFF`:

1. `R` = the string `resource`, then each resource attribute sorted by key, as key and value;
   `S` = the same for `scope` over `__scope.name__`, `__scope.schema_url__`,
   `__scope.version__` and the scope attributes sorted by key; `P` = the same for `point` over
   the point attributes sorted by key.
2. The fingerprint hashes `R`, `S` and `P` as 8 little-endian bytes each, then the series name,
   the temporality and, for buckets and quantiles, `le`/`quantile` and its value.

Hashing each level apart keeps two kinds of series distinct, as the reference does:

- series whose `labels` are equal because a point value is shadowed (§3)
  [obs `probe.collide_resource`, `probe.collide_scope`];
- series that differ only in the level of an attribute, which also changes `env` and the
  attribute maps.

`R` and `S` are computed once per resource and scope, so the cost per point does not grow with
the resource.

- **Queries are unaffected**: the backend joins `samples_v4` to `time_series_v4` on the
  fingerprint and groups by labels, so any consistent function works.
- **At cutover**, a series written by both collectors over time gets two fingerprints: two
  `time_series_v4` rows with the same labels for the hours around the switch. Counters are
  per fingerprint, so rates stay correct on each side of the switch.
- The parity suite compares fingerprints as consistent (the same labels always get the same
  fingerprint, and samples reference their series), not as equal values.

## 5. `metadata`

One row per batch for each distinct (temporality, metric name, attribute name, attribute type,
datatype, string value) among the entries of the series' `resource_attrs`, `scope_attrs` and
`attrs` (§3; for series not written, §2), with:

- `attr_type`: `resource`, `scope` (also the three `__scope.*__` entries) or `point` (also
  `__temporality__`, `le`, `quantile`);
- `attr_datatype`: the pdata type name (`Str`, `Int`, `Double`, `Bool`, `Slice`, `Map`,
  `Bytes`); the `__…__` entries are `Str`;
- `attr_string_value`: the stringified value;
- `first_reported_unix_milli` / `last_reported_unix_milli`: minimum and maximum point time in the
  batch [obs `probe.two_points`];
- description, unit, type and `is_monotonic` of the series. For a `.count` series the reference's
  unit is not deterministic: runs show either `1` or the metric's unit (`go.gc.pause.count`: `s`
  in one run, `1` in the next), as rows with the same key collapse on insert and keep an
  arbitrary unit. The exporter writes the metric's unit, or `1` when that is empty; the parity
  suite ignores this one column.

The table is an `AggregatingMergeTree` with min/max states, so rows from different batches
merge.

## 6. Expansion limits (deliberate difference)

Every derived series carries its own copy of the resource, scope and point attributes (§3), so
a request of a few MiB can expand into many GiB: a histogram with 10,000 bounds yields 10,005
series, and 100,000 gauge points with distinct attributes under a 1 MiB resource yield 100,000
copies of it. The sending queue counts data points, not this expansion. The reference's limits
were not observed; the exporter enforces its own, on estimated encoded bytes:

- **Per point, 16 MiB**: derived series × (6 × (attribute bytes + metric name) + 2 × attribute
  bytes + text + 256 + metadata rows × (text + 256)), where attribute bytes are those of the
  resource, scope and point, 6 is the worst case of JSON escaping in `labels`, text is the
  metric name, description and unit that series and metadata rows copy, and metadata rows are
  the resource, scope and point attributes plus two. A point above it is dropped before any row is built;
  the rest of the batch is written and a warning reports `dropped_points`.
- **Per chunk, 256 MiB**: bytes of the samples, series and metadata rows built so far, after
  deduplication, so repeated points of the same series cost one series row. Above it, the rows
  built so far are inserted into the three tables and encoding goes on with an empty chunk,
  also midway through a histogram's buckets or a summary's quantiles. A series row written in
  one chunk is not repeated in a later one. Metadata rows can repeat across chunks; the
  `AggregatingMergeTree` merges them (§5). A failed chunk fails the batch, which is retried
  whole.

## 7. Still to observe

- metric names with characters outside `[A-Za-z0-9._:-]`.
