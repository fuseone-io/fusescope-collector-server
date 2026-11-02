# APM metrics · span-metrics contract

What the collector must derive from spans so that the FuseScope Services page, the per-service
pages and APM alerts work on span metrics (phase 4). Clean-room (ADR 0002). Sources:

- **[obs]**: [`observed/0.145.0/`](observed/0.145.0/). Span names are in the `traces` part of
  `test/observe/corpus.py`, the span-metric probes in `apm_spans`. The reference ran
  `signozspanmetrics/delta` with the configuration in `test/observe/reference-collector.yaml`.
- **[be]**: the FuseScope backend (`fuseone-io/fusescope`, read at `fe371d4`); paths below
  are relative to it.

The metrics are written by `fusescope_clickhouse` as ordinary OTLP metrics
([`clickhouse-metrics.md`](clickhouse-metrics.md)), so this document specifies only the OTLP
the connector emits.

## 1. When the backend uses them

- **Feature flag.** Span metrics are read only with the feature flag `use_span_metrics`, off by
  default, or `USE_SPAN_METRICS=true` [be `pkg/flagger/registry.go:6,20-25`,
  `pkg/signoz/config.go:328-333`]. Otherwise the pages query traces, which phases 1–3 already
  cover.
- **Services list.** It comes from `signoz_traces.top_level_operations`, written by the traces
  exporter, not from metrics [be `pkg/query-service/app/clickhouseReader/reader.go:296-330`].
  Every metric query filters `service.name = X AND operation IN <X's top-level operations>`
  [be `frontend/src/lib/apm/filters.ts:52-62`].
- **Service map.** It reads `dependency_graph_minutes_v2`, filled by materialized views from
  traces [be `reader.go:860-905`]. No metric is involved.

## 2. Metrics

All metrics are delta, emitted once per flush (§5), with double values. Scope name
`signozspanmetricsprocessor`, empty version and schema URL. The resource is empty: every
dimension is a point attribute (§3) [obs: all `signoz_*` series have empty `resource_attrs`].

| Metric | OTLP type | Unit | Point value | Per span |
|---|---|---|---|---|
| `signoz_calls_total` | Sum, monotonic | `""` | span count | every span (§4) |
| `signoz_latency` | Histogram | `ms` | duration in ms, explicit bounds below, no min/max | every span |
| `signoz_db_latency_count` | Sum, monotonic | `1` | span count | database spans (§4) |
| `signoz_db_latency_sum` | Sum, monotonic | `1` | duration in ms | database spans |
| `signoz_external_call_latency_count` | Sum, monotonic | `1` | span count | external calls (§4) |
| `signoz_external_call_latency_sum` | Sum, monotonic | `1` | duration in ms | external calls |

[obs: `type`, `temporality`, `is_monotonic` and `unit` of the `signoz_*` rows in
`time_series_v4`. The histogram is written as `signoz_latency.bucket`, `.count` and `.sum`, as
clickhouse-metrics §2 derives them. There are no `.min` or `.max` rows, so the points carry
none.]

- **Duration.** `(end − start)` in milliseconds as a fraction (`0.05`, `1.15` for 0.05 + 0.1 +
  1), and 0 when end < start [obs `agg`, `inverted-times`].
- **Bounds (ms).** `0.1, 1, 2, 6, 10, 50, 100, 250, 500, 1000, 1400, 2000, 5000, 10000, 20000,
  40000, 60000`, then `+Inf`. A duration equal to a bound counts in that bucket, as OTLP
  explicit bounds do: 0.05 and 0.1 both count at `le=0.1` [obs `agg`, `long`]. These are the
  reference configuration's `latency_histogram_buckets`.
- The backend expects milliseconds, cumulative `le` counts and a final `+Inf`
  [be `frontend/src/lib/apm/spanMetrics.ts:1`, `frontend/src/lib/apm/histogram.ts:1-54`]. The
  exporter turns OTLP bucket counts into cumulative `le` rows (clickhouse-metrics §2).
- No exponential histogram reaches ClickHouse, although the reference sets
  `enable_exp_histogram`: the metrics exporter does not write them (clickhouse-metrics §2).

## 3. Attributes

### 3.1 Calls and latency

`signoz_calls_total` and `signoz_latency` points carry:

| Key | Value |
|---|---|
| `service.name` | resource `service.name` |
| `operation` | span name |
| `span.kind` | `SPAN_KIND_UNSPECIFIED`, `_INTERNAL`, `_SERVER`, `_CLIENT`, `_PRODUCER`, `_CONSUMER` |
| `status.code` | `STATUS_CODE_UNSET`, `STATUS_CODE_OK`, `STATUS_CODE_ERROR` |
| `http.status_code` | `signoz_calls_total` only, not `signoz_latency`: the span's `http.status_code`, else `http.response.status_code`, stringified; absent otherwise |
| dimensions | §3.3 |

[obs: `status-ok`, `status-error`, `strings` (kind 0), `GET /cart` (new key only: `200` on calls, no
`http.status_code` on any `signoz_latency` series),
`GET /legacy` (old key), `precedence` and `precedence-reversed` (both keys, in either order:
`410`, the old one)]

The backend filters errors with `status.code = 'STATUS_CODE_ERROR'` and groups endpoints by
`operation` [be `frontend/src/lib/apm/filters.ts:12-15`, `serviceRequests.ts:81`].

### 3.2 Database and external calls

These points carry `service.name`, `status.code` and the dimensions, but no `operation` and no
`span.kind` [obs: all `signoz_db_latency_*` and `signoz_external_call_latency_*` series], plus:

- database: `db.system`, and `db.name` if the span has it [obs `UPDATE stock`,
  `db-old-system-only`, `db-internal-kind`];
- external: `address` (§4) and `http.status_code` as in §3.1 [obs `GET upstream`: `201`,
  `precedence`: `410`].

The backend groups them by `db.system` and by `address`
[be `frontend/src/container/MetricsApplication/MetricsPageQueries/DBCallQueries.ts:32-38`,
`frontend/src/lib/apm/serviceRequests.ts:173-198`].

### 3.3 Dimensions

The configured dimensions are `service.namespace` and `deployment.environment`, both with
default `default`, and `signoz.collector.id`, `service.version`, `browser.platform`,
`browser.mobile`, `k8s.cluster.name`, `k8s.node.name`, `k8s.namespace.name`, `host.name`,
`host.type` and `container.name`, without defaults [reference configuration]. For each one:

- **Value.** The span attribute if present, else the resource attribute, else the default. A
  dimension with neither value nor default is absent [obs `dims`: `host.name` and
  `deployment.environment` from the span, `k8s.namespace.name` from the resource,
  `k8s.node.name` from the span only; `probe-resource`: `default` for both defaulted keys].
- **`resource_` copy.** When the key is a resource attribute, the point also carries
  `resource_<key>` with the same value as above, even when the span's value won
  [obs `dims`: `resource_host.name` = `span-host`; no `resource_k8s.node.name`]. The backend's
  resource-attribute picker reads these keys from `signoz_calls_total`, and the environment
  filter uses `resource_deployment.environment`
  [be `frontend/src/hooks/useResourceAttribute/utils.ts:147-156`].
- **`signoz.collector.id`.** A per-collector-instance UUID; its `resource_` copy appears as for
  any dimension [obs: one value across all series]. The exporter uses its own instance ID; the
  parity suite ignores the value (§6).

Attributes that are not dimensions do not reach the points [obs `custom.res`, `custom.span`].

## 4. Which spans count

- **Calls and latency.** Every span of a resource that has `service.name`, even an empty one
  [obs: every corpus span except the resource without `service.name`; `empty-service`: series
  with `service.name` `""`].
- **Database.** Any span kind, when the span has `db.system`. The newer `db.system.name` and
  `db.namespace` are not read [obs `db-internal-kind`, `db-old-system-new-namespace` (no
  `db.name`), `db-new-system-old-name` and `SELECT cart` (no series)].
- **External call.** Span kind client, with an address [obs `external-internal-kind` and
  `external-server-kind` (no series), `SELECT stock` (client, no address keys: no series)].
  The address is the first present of:
  1. `http.host` [obs `precedence`, `precedence-reversed`];
  2. `net.peer.name`, then `server.address` [obs `address-server-vs-peer`,
     `address-peer-vs-server`: `peer.example.com` in either order; `UPDATE stock`,
     `external-host`];
  3. the host and port of `http.url`, else of `url.full` [obs `address-urls`:
     `http-url.example.com`; `external-url-only`: `only-url.example.com:8443`].

  A host from 2 is followed by `:` and its paired port, `net.peer.port` or `server.port`, when
  that is present, read as a string: an integer port gives an empty one [obs
  `external-server-port`: `port.example.com:`, `address-string-port`:
  `string-port.example.com:8080`, `external-peer-port`, `SELECT cart`: `db.internal:`]. The
  other key's port does not count [obs `address-peer-with-server-port`: `mixed.example.com`].
  The backend drops addresses matching `^(0[.]0[.]0[.]0)?(:[0-9]*)?$|:$`, so `host:` never shows
  [be `serviceRequests.ts:170`].

## 5. Time

- **Sample time.** The span's start time, floored to the minute [obs: `agg` at t0 and a minute
  earlier give two samples; `long` starts and ends in different minutes and has one sample at
  its start's minute; `three-hours-old`].
- **Aggregation.** Spans of one series in one minute add up into one point [obs `agg`: count 3,
  sum 1.15 ms].
- **Flush.** The reference flushes every 10 s [reference configuration]; each flush emits the
  deltas of the spans it saw, so spans of one series and minute that two flushes see give two
  points [obs: the late batch, 12 s after the corpus: `agg` has samples 3 and 2 at t0's
  minute]. The connector does the same; queries add the deltas.
- **Point order.** The reference's order of a flush's points varies from run to run [obs: `last`
  of `agg` in `samples_v4_agg_5m` was 3, 1 and 3 in three runs]. The connector emits points in
  order of their series' first span; nothing reads the order but `anyLast` in the rollups.

## 6. Deliberate differences

- `signoz.collector.id` holds this collector's `service.instance.id`, or a random UUID when it
  has none. It is set as if it were a resource attribute, so its `resource_` copy appears as in
  the reference.
- **Series limits.** At most `max_series` (100000) series, holding at most `max_series_bytes`
  (64 MiB) of keys, attributes (arrays and maps by their elements) and histogram counters, are
  kept between flushes, across the six metrics; a
  database or external-call series counts twice, as it emits a count and a sum. Spans of
  further series are dropped from the metrics with a warning. The reference's
  `dimensions_cache_size` was not observed when exceeded (§7).
- **Refused flushes.** A flush the metrics pipeline refuses is dropped with an error, retryable
  or not. A retryable error does not tell that nothing was written: with the exporter's queue
  off, an export can store some chunks and then fail, and a resend would be a new export
  request with new deduplication tokens (DESIGN §4), writing those chunks twice. The exporter's
  sending queue, on by default, absorbs ClickHouse outages and retries with the request's
  identity. Shutdown stops scheduling flushes, lets one in progress finish within its
  deadline, then flushes the rest, and reports either failure.

## 7. Not reproduced

- **Flush-time scope metadata.** Every flush of the reference also writes, for each span metric,
  scope metadata rows stamped with the flush time, with no point or sample [obs: rows 19 s,
  29 s and 39 s after t0, three per metric]. They only refresh `last_reported` of the scope
  keys. The connector emits nothing on a flush without spans; the parity suite ignores these
  rows.
- **`dimensions_cache_size`** (100000) when exceeded: observing it needs more series than the
  observation records. The connector's own limits are in §6.

## 8. Connector

`fusescope_spanmetrics` is a traces-to-metrics connector: a traces pipeline exports to it, a
metrics pipeline receives from it and exports to `fusescope_clickhouse`.

| Setting | Default | Meaning |
|---|---|---|
| `dimensions` | the list in §3.3, with its defaults | `name` and optional `default` of each dimension |
| `latency_histogram_buckets` | the bounds in §2 | durations, positive and increasing |
| `metrics_flush_interval` | `10s` | how often the deltas are emitted |
| `max_series` | `100000` | §6 |
| `max_series_bytes` | `67108864` (64 MiB) | §6 |

The defaults reproduce the reference configuration, so the parity suite runs the connector with
none of these set. Dimensions named like a connector key (`service.name`, `operation`,
`span.kind`, `status.code`, `http.status_code`, `db.system`, `db.name`, `address`), or starting
with `resource_`, the prefix of the connector's resource copies, are refused.
