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
| `deployment.environment` | the resource's `deployment.environment` | [obs] |
| `host.name` | the resource's `host.name` | [obs `obs-keyid`] |
| `connector_id` | §5 | [obs] |
| `signoz.workspace.key.id` | the ingestion key (§3.1) | deliberate addition |

- **Absent dimension.** When the resource lacks one of them, the label is absent, not empty:
  `obs-bare` (no `deployment.environment`) has `service.name` only; `obs-nosvc` (no resource
  attributes) has neither [obs: raw `labels`].
- **Other resource attributes are not labels**: the resource of `obs-keyid` held `service.name`,
  `host.name` and `signoz.workspace.key.id`, and its rows have only the first two [obs].
- **One row per (metric, labels) per flush.** Records with equal dimensions add up; 20 spans of
  one service sent in two rounds within one window give one `span.count` row of 40 [obs
  `obs-traces` at 20:11:07].

### 3.1 Ingestion key (deliberate addition)

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

`span.size` and `log.size` are the OTLP protobuf encoded size of each record on its own (pdata
`Span` and `LogRecord` sizes), without its resource and scope. `metric.datapoint.size` stays 0
as observed (§2).

The reference's sizes could not be reproduced [obs]:

| Probe | Reference value per record | Protobuf size of the record |
|---|---|---|
| log, body of 10 chars (`obs-size-10`) | 56 | — |
| log, body of 1010 chars (`obs-size-1010`) | 1058 | — |
| log `obs-nosvc` (body and one attribute, empty resource) | 33 | 45 |
| span, name of 10 chars (`obs-ssize-10`) | 338 | 60 |
| span, name of 1010 chars (`obs-ssize-1010`) | 1342 | 1061 |

- 1000 more characters add 1002 bytes to a log and 1004 to a span. That fits protobuf length
  prefixes growing by one byte per enclosing message: two (`AnyValue`, `body`) for a log, four
  for a span, as if each span were measured inside a whole export request.
- The absolute values fit neither the record alone nor the whole request: the span's 338 bytes
  are more than its request (about 100 bytes), and the log's 33 are less than its record (45).
  The reference measures something not visible from outside.
- **Consequence.** After cutover, sizes in Meter Explorer and Ingestion Settings drop for the
  same traffic, by a few hundred bytes per span at most for small spans. Counts are unchanged.
  Record-only size is chosen because it is stable, cheap and does not depend on how the client
  batched its requests.

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

- **Series limit.** At most `max_series` (10 000) label sets per signal are held between
  flushes. Records of further sets go to an overflow set that keeps `connector_id` and
  `signoz.workspace.key.id` when it comes from an authenticated request (§3.1), and drops
  `service.name`, `deployment.environment`, `host.name` and an unverified key; a counter reports
  the overflow. Per-key totals of authenticated keys, and Meter Explorer's totals, stay correct;
  only the split by service, environment and host degrades. Authenticated key IDs come from the
  ingest policy, so the overflow sets are bounded by the policy's keys. The reference's limit was
  not observed.
- **Isolation.** Counting does not wait for ClickHouse and never returns an error to the data
  path; it reads each batch without copying it, though counting and sizing still cost CPU there,
  which the benchmarks bound. The flush goes through a separate metrics pipeline to
  `fusescope_clickhouse`, whose queue and retries are its own. A flush that pipeline refuses is
  logged and counted and its values are dropped, as a resend could double them (same reasoning
  as [`apm-metrics.md`](apm-metrics.md) §6).

## 8. Not observed

- Behaviour after a crash, OOM kill or node loss (§5).
- The reference's limits, and data points of types other than gauge.
