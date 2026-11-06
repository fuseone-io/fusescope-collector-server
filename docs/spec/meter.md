# Meter · usage contract

The usage metrics the collector writes to `signoz_meter` for the backend's Meter Explorer and
Ingestion Settings (#60, fuseone-io/fusescope#228). Clean-room (ADR 0002). Sources:

- **[obs]**: [`observed/meter-metadata/`](observed/meter-metadata/), a black-box run of the
  earlier collector (`fusescope-otel-collector:0.145.0`, chart 0.160.1 on kind, 2026-11-05/06).
  Probe names (`obs-…`) are the `service.name` of the input; the scripts that sent them are in
  `scripts/` there, and `samples.tsv` holds every meter row the run produced.
- **[be]**: the FuseScope backend (`fuseone-io/fusescope`, read at `a2bffc1ae`); paths below
  are relative to it.
- **[ddl]**: [`observed/meter-metadata/ddl-samples.sql`](observed/meter-metadata/ddl-samples.sql),
  created by the backend's schema migrator.

## 1. What the backend reads

- **Tables.** `signoz_meter.distributed_samples`, and `samples_agg_1d` that ClickHouse derives
  from it [be `pkg/telemetryschema/metertelemetryschema/samples.go:11-13`,
  `pkg/telemetrymetadata/metadata.go:92`]. The collector writes `distributed_samples` only.
- **Names.** `signoz.meter.span.size`, `signoz.meter.log.size`,
  `signoz.meter.metric.datapoint.count` and `signoz.meter.platform.active`
  [be `pkg/types/zeustypes/meter_name.go:11-14`]; the dashboards migration also lists
  `log.count` and `span.count` [be `pkg/sqlmigration/104_fill_dashboard_meter_source.go:21-26`].
- **Ingestion Settings** shows `log.size`, `span.size` and `metric.datapoint.count` per ingestion
  key, filtered by `signoz.workspace.key.id='<key id>'`
  [be `frontend/src/container/IngestionSettings/MultiIngestionSettings.tsx:108-124,895`].
- **Meter Explorer** queries each metric with time aggregation `increase`, space aggregation
  `sum` and no group by [be `frontend/src/container/MeterExplorer/Breakdown/graphs.ts:87-144`].
  Values are therefore summed across rows, whatever their labels.

## 2. Metrics

All are OTLP Sum, delta, monotonic, with the scope name `signozmeterconnector` and empty scope
version and schema URL [obs: `labels` of every row; `temporality`, `type`, `is_monotonic`,
`unit` and `description` columns].

| Metric | Unit | Description | Value per flush |
|---|---|---|---|
| `signoz.meter.span.count` | `1` | `The number of spans observed.` | spans |
| `signoz.meter.span.size` | `By` | `The size of spans observed.` | §4 |
| `signoz.meter.log.count` | `1` | `The number of log records observed.` | log records |
| `signoz.meter.log.size` | `By` | `The size of log records observed.` | §4 |
| `signoz.meter.metric.datapoint.count` | `1` | `The number of data points observed.` | data points |
| `signoz.meter.metric.datapoint.size` | `By` | `The size of data points observed.` | always 0 |

- **Spans, not traces.** 10 traces of 2 spans give `span.count` 20 [obs `obs-traces`,
  `obs-control`].
- **Points, not series.** One series of 30 points gives `metric.datapoint.count` 30 [obs
  `obs-series`: 1 fingerprint and 30 samples in `signoz_metrics`].
- **`metric.datapoint.size` is 0** in every row [obs: `obs-metrics`, `obs-series`].
- **`signoz.meter.platform.active` is not emitted**: only the six names above appear in the run
  [obs: `groupUniqArray(metric_name)`]. The backend's name stays unused by the collector.

## 3. Labels

Each row's `labels` hold `__name__`, `__temporality__` (`Delta`), the scope labels
`__scope.name__`, `__scope.version__` and `__scope.schema_url__`, and:

| Label | Value | Source |
|---|---|---|
| `service.name` | the resource's `service.name` | [obs] |
| `deployment.environment` | the resource's `deployment.environment.name`, else its `deployment.environment` (§3.1) | [obs] name; precedence deliberate |
| `host.name` | the resource's `host.name` | [obs `obs-keyid`] |
| `connector_id` | §5 | [obs] |
| `signoz.workspace.key.id` | the ingestion key (§3.2) | deliberate addition |

- **Absent dimension.** When the resource lacks one of them, the label is absent, not empty:
  `obs-bare` (no `deployment.environment`) has `service.name` only; `obs-nosvc` (no resource
  attributes) has neither [obs: raw `labels`].
- **Other resource attributes are not labels**: the resource of `obs-keyid` held `service.name`,
  `host.name` and `signoz.workspace.key.id`, and its rows have only the first two [obs].
- **One row per (metric, labels) per flush.** Records with equal dimensions add up; 20 spans of
  one service sent in two rounds within one window give one `span.count` row of 40 [obs
  `obs-traces` at 20:11:07].

### 3.1 Environment (deliberate)

The reference reads `deployment.environment` [obs]. Current OpenTelemetry semantic conventions
name it `deployment.environment.name`, which `rumreceiver` writes
(`components/receiver/rumreceiver/process.go:31`, [`rum.md`](rum.md) §8.1). The label takes
`deployment.environment.name` when the resource has it (an empty value counts as present),
otherwise `deployment.environment`, and is always emitted as `deployment.environment`, the
name the reference wrote, so one environment never splits into two labels.

### 3.2 Ingestion key (deliberate addition)

Ingestion Settings filters by `signoz.workspace.key.id` (§1), but the reference never writes it
(§3), so per-key usage is empty with the reference. The collector adds it:

- **Authenticated requests.** The value is the key ID that `rumreceiver` or `ingestauth` put in
  the request's auth data, `fusescope.ingest.key_id` (`ingestpolicy.AuthKeyID`), read from
  `client.Info` of the request context, never from the payload
  ([`rum.md`](rum.md) §8.1, [ADR 0006](../adr/0006-single-ingress.md)). Counting therefore
  happens where that context is still attached, before any batching that drops it.
- **Unauthenticated requests.** The resource's `signoz.workspace.key.id`, if present, is copied
  as is. It is unverified accounting: any sender can claim any key. It is never taken when the
  request is authenticated.
- Without either, the label is absent.

## 4. Size (deliberate difference)

`span.size` and `log.size` are the OTLP protobuf encoded size of each record on its own,
without its resource and scope: `ptrace.ProtoMarshaler.SpanSize` and
`plog.ProtoMarshaler.LogRecordSize`, which compute it without copying or serializing.
`metric.datapoint.size` stays 0 as observed (§2).

The reference's sizes could not be reproduced [obs]. The right column is what the collector
writes for the same records, computed with pdata v1.70.0 from the fields the probes sent
(`obs-size-*`: timestamp, severity `Info`/9, the body, `app=server`, as stored in
`signoz_logs`; spans: trace and span ID, name, kind, start and end):

| Probe | Reference, per record | Record-only protobuf size |
|---|---|---|
| log, body of 10 chars (`obs-size-10`) | 56 | 52 |
| log, body of 1010 chars (`obs-size-1010`) | 1058 | 1054 |
| log `obs-nosvc` (body and one attribute, empty resource) | 33 | 49 |
| span, name of 10 chars (`obs-ssize-10`) | 338 | 64 |
| span, name of 1010 chars (`obs-ssize-1010`) | 1342 | 1065 |

- 1000 more characters add 1002 bytes to a log and 1004 to a span in the reference. That fits
  protobuf length prefixes growing by one byte per enclosing message: two (`AnyValue`, `body`)
  for a log, four for a span, as if each span were measured inside a whole export request.
- The absolute values fit neither the record alone nor the whole request: the span's 338 bytes
  are more than its whole request (about 100 bytes), and `obs-nosvc`'s 33 are less than its
  record. The reference measures something not visible from outside.
- **Consequence.** After cutover, sizes in Meter Explorer and Ingestion Settings change for the
  same traffic: small spans by a few hundred bytes less, logs by a few bytes either way
  (`obs-nosvc` would grow). Counts are unchanged. Record-only size is chosen because it is
  stable, cheap and does not depend on how the client batched its requests.

## 5. Flush and identity

- **Interval.** One flush per hour, counted from the process start: a pod started at 20:10:49
  flushed at 21:10:50 and 22:10:50; a pod started at 19:02:22 flushed at 20:03:22 [obs].
- **Graceful shutdown.** It flushes what was counted since the last flush: the markers
  sent before the 20:10 restart (`obs-restart`, `obs-bare`, the 20:05 round) appear at 20:11:07,
  when the old pod stopped, and the control sent after it (`obs-control`) at the next periodic
  flush [obs phase 2]. A crash, OOM kill or node loss loses the counts held in memory; this was
  not observed and follows from in-memory aggregation.
- **Time.** `unix_milli` is the flush instant, for every row of that flush [obs].
- **`connector_id`.** A random UUID per signal (one for traces, one for logs, one for metrics),
  generated at start: the three ids changed across the restart [obs: `84ff8b5c…` →
  `d19a3b03…` for traces].

## 6. Rows

`fusescope_clickhouse` writes each point to `signoz_meter.distributed_samples` [ddl]:

| Column | Value |
|---|---|
| `temporality` | `Delta` |
| `metric_name`, `description`, `unit` | §2 |
| `type` | `Sum` |
| `is_monotonic` | `true` |
| `labels` | JSON object of §3 |
| `fingerprint` | of the labels, as in [`clickhouse-metrics.md`](clickhouse-metrics.md) §4 |
| `unix_milli` | §5 |
| `value` | the count or size, as a float |

The fingerprint's values differ from the reference's, which nothing joins on: Meter Explorer
sums without group by (§1).

## 7. Limits and isolation (deliberate)

- **Label values.** `service.name`, the environment (§3.1), `host.name` and an unverified key
  are cut to `max_label_bytes` (256) bytes, at the last UTF-8 boundary at or before it, before
  they form a label set. Values that differ only after their first 256 bytes therefore add up
  under the same label; the tests cover that collision and a cut inside a multi-byte character.
  Authenticated key IDs come from the ingest policy and are not cut.
- **Label sets.** Per signal, between flushes, at most `max_series` (10 000) label sets holding
  at most `max_series_bytes` (4 MiB) of label keys and values. When a record's set would exceed
  either, it goes to the overflow set of its authenticated key (§3.2), which keeps
  `connector_id` and `signoz.workspace.key.id` and drops `service.name`,
  `deployment.environment`, `host.name` and an unverified key.
- **Keys.** At most `max_authenticated_keys` (1 000) such per-key overflow sets per signal and
  flush, whatever keys rotate during the hour. A record that needs one more, or has no
  authenticated key, goes to a last overflow set with only `connector_id`.
- **Bound.** So a signal holds at most `max_series + max_authenticated_keys + 1` sets and
  `max_series_bytes` plus the overflow sets' keys, between flushes.
- **Loss counter.** `fusescope_meter_overflow_records` (unit `{record}`, attributes `signal`
  and `reason`) counts every record sent to an overflow set, with `reason` one of `series`
  (`max_series`), `bytes` (`max_series_bytes`), `keys` (`max_authenticated_keys`) and
  `no_key` (no authenticated key once an overflow is needed);
  `fusescope_meter_truncated_values` (`{value}`, `signal`) counts cut label values. The loss
  of the split is therefore visible. Totals stay correct in Meter Explorer, and per key unless `max_authenticated_keys`
  is exceeded. The reference's limits were not observed.
- **Isolation.** Counting does not wait for ClickHouse and never returns an error to the data
  path; it reads each batch without copying it, though counting and sizing still cost CPU there,
  which the benchmarks bound. The flush goes through a separate metrics pipeline to
  `fusescope_clickhouse`, whose queue and retries are its own. A flush that pipeline refuses is
  logged and counted and its values are dropped, as a resend could double them (same reasoning
  as [`apm-metrics.md`](apm-metrics.md) §6).

## 8. Not observed

- Behaviour after a crash, OOM kill or node loss (§5).
- The reference's limits, and data points of types other than gauge.

## 9. Connector

`fusescope_meter` is a traces-, logs- and metrics-to-metrics connector. It is an exporter of
each ingestion pipeline, beside `fusescope_clickhouse`, and the receiver of a `metrics/meter`
pipeline that exports to a `fusescope_clickhouse` instance in `mode: meter`:

```yaml
connectors:
  fusescope_meter: {}
exporters:
  fusescope_clickhouse/meter:
    mode: meter            # writes only signoz_meter.distributed_samples
service:
  pipelines:
    traces:       {receivers: [otlp, rum], processors: [...], exporters: [fusescope_clickhouse, fusescope_meter]}
    metrics/meter: {receivers: [fusescope_meter], exporters: [fusescope_clickhouse/meter]}
```

- **Where it counts.** Everything that reaches it, after the pipeline's processors, whether or
  not the sibling `fusescope_clickhouse` accepts the batch: a client that resends a refused batch
  is counted again. This is the reference's point of observation (§2) and is tested.
- **Ingestion only.** Pipelines of derived data (`fusescope_spanmetrics`, the collector's own
  telemetry) do not export to it.
- **Authentication.** The pipeline's context reaches the connector before any queue or batch,
  so `client.Info` holds the key of §3.2. A batch processor ahead of it would drop that context
  and must not be placed there.
- **Flush.** The current window is swapped for an empty one under a lock; the metrics are built
  and sent without it, so a slow or stuck metrics pipeline never holds up the data path. One
  flush runs at a time. Shutdown gives up at its deadline.
- **Mode `meter`** of `fusescope_clickhouse` accepts only metrics pipelines, writes the fixed
  table `distributed_samples` of `meter.database` (`signoz_meter`), and keeps its own queue,
  retries and insert telemetry (`table=distributed_samples`).

| Setting | Default | Meaning |
|---|---|---|
| `flush_interval` | `1h` | §5 |
| `max_series` | `10000` | §7 |
| `max_series_bytes` | `4194304` (4 MiB) | §7 |
| `max_authenticated_keys` | `1000` | §7 |
| `max_label_bytes` | `256` | §7 |
