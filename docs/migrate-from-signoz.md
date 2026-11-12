# Moving from SigNoz to the FuseScope collector

This guide is for an operator who runs SigNoz in Kubernetes, with agents and SDKs that send OTLP
in plaintext, without a key, to the collector inside the cluster, and who moves to the FuseScope
chart. It covers the collector side:
1. receive the existing clients unchanged;
2. move each one to TLS and a key;
3. turn plaintext off.

The rules come from [ADR 0013](adr/0013-otlp-plaintext-for-migration.md) for plaintext OTLP and
[ADR 0007](adr/0007-clickhouse-plaintext-for-migration.md) for plaintext ClickHouse.

For a **new installation** none of this applies. By default the chart renders no plaintext
receiver, and the collector is deployed only when one of its receiving paths is on (§4). Turn on
the ones you need.

## 1. Before you start

- **Chart.** `fusescope` **0.168.1 or later**.
  - 0.168.0 added `fusescopeCollector.otlp.insecure` and its refusals.
  - 0.168.1 added `otlp.networkPolicy.ingressFrom`, which restricts the plaintext ports even next to
    the agents' listener (§3).
  - Earlier charts served plaintext OTLP by default, with nothing to measure or bound it.
- **Collector self-metrics.** `fusescopeCollector.selfMetrics.enabled: true`, the default. The chart
  refuses `otlp.insecure` without it, because the exit in §5 is read from those metrics.
- **Metrics retention.** The exit in §5 reads the collector's metrics over the whole window.
  - The observed schema keeps raw samples, series and their 5- and 30-minute aggregates for 30 days
    (`TTL … + toIntervalSecond(2592000)`), so a window must be under 30 days.
  - If a job of your inventory runs less often than that, a monthly job for example, raise the
    metrics retention first. Or **run that job** during the window and verify its telemetry at its
    new destination. Reading its configuration does not replace a window longer than its period.
- **Schema.** `fusescope-schema` adopts an existing database only when its schema equals the
  observed one (FuseScope fork 0.145.0). It refuses any other schema and lists each object that is
  missing, extra or different ([spec §4](spec/fusescope-schema.md)). Run `fusescope-schema plan`
  against your ClickHouse first.
- **ClickHouse without TLS.** If your ClickHouse has no TLS, keep the connection plaintext by
  explicit choice: `clickhouse.secure: false`, or `externalClickhouse.secure: false`. The collector's
  exporter then runs with `tls.insecure: true`, and warns at start that the password and telemetry
  travel in clear ([ADR 0007](adr/0007-clickhouse-plaintext-for-migration.md)). Moving ClickHouse to
  TLS is a separate step.

## 2. Inventory every emitter

Make the inventory before switching. The exit in §5 needs it: zero traffic only shows that nothing
sent during the window, not that nothing is left.

Keep one row per emitter, per signal and per endpoint:

| Emitter (namespace/workload) | Signals | Endpoint it uses today | Protocol | Target path (§4) | Moved on | Verified |
|---|---|---|---|---|---|---|
| `shop/checkout` | traces, metrics | `signoz-otel-collector.platform:4317` | gRPC | SDK, external TLS endpoint | | |
| `jobs/nightly-export` (cron, weekly) | logs | `…:4318` | HTTP | stays on the opt-in until `ingestauth` | | |

Where to find the emitters:

- **Configuration.** Look for `OTEL_EXPORTER_OTLP_*ENDPOINT` and the SDK or agent configs that name
  the SigNoz collector's Service or ports 4317 and 4318. Include Helm values, ConfigMaps and
  Secrets.
- **What arrives.** Compare the `service.name` values and the k8s resource attributes stored since
  the switch with your list. A service that appears but is not listed is an emitter you missed.
- **Periodic jobs.** Note their period. A weekly or monthly job sets the minimum window in §5.

## 3. Switch collectors with the plaintext receiver

**Values.** Replace the legacy key with the explicit one:

```yaml
fusescopeCollector:
  otlp:
    insecure: true        # was: enabled: true (the old default)
  selfMetrics:
    enabled: true         # required with insecure
```

The chart then renders:
- the receiver `otlp/insecure` on 4317 (gRPC) and 4318 (HTTP);
- those ports on `RELEASE-fusescope-collector`;
- the compatibility Service `RELEASE-fusescope-otel-collector`, with the earlier collector's name
  and ports, so existing clients keep their endpoint.

`NOTES.txt` warns about all three.

**What the chart refuses** with `otlp.insecure`:
- `NodePort` or `LoadBalancer` on `fusescopeCollector.service` or `otelCollector.service`;
- `otelCollector.ingress.enabled`;
- `selfMetrics.enabled: false`.

It also refuses `otlp.enabled: true` without `insecure`. **The chart creates no external route to
the plaintext receiver.** Routes created outside the chart are yours to avoid.

**Network.** While the receiver is on, any workload that reaches 4317 and 4318 can send, inject or
forge telemetry and spend the collector's and ClickHouse's resources. Admit only the namespaces
or pods of your inventory:

```yaml
fusescopeCollector:
  otlp:
    insecure: true
    networkPolicy:
      ingressFrom:
        - namespaceSelector:
            matchLabels:
              fusescope.io/otlp-plaintext: "allowed"
```

The collector's NetworkPolicy then admits these peers **alone** on 4317 and 4318, and no other rule
of it names those ports.
- **With the agents.** If the agents' listener has its policy (`agentListener.networkPolicy.enabled`,
  the default), this list is required, and the chart refuses `otlp.insecure` without it.
- **Without the agents.** The list renders a policy for those ports alone. Left empty, they accept
  any source in the cluster.

It needs a CNI that enforces NetworkPolicy.

> **On chart 0.168.0.** There, with the agents' listener policy on, the chart's policy admits any
> source on 4317 and 4318. NetworkPolicies add up, so a policy of yours cannot narrow that. Upgrade
> to 0.168.1. If you cannot, set `fusescopeCollector.agentListener.networkPolicy.enabled: false`
> and apply a complete policy of your own for the collector pods: 4317/4318 from your peers, 4322
> from the agent pods, and 13133 (health) and 4319 (RUM) from their sources.

## 4. Move each client to a path with TLS and a key

| Client | Target path | What it takes |
|---|---|---|
| **FuseScope agents** (node logs, host and kubelet metrics) | `fusescopeAgent` with `fusescopeCollector.agentListener` (port 4322) | TLS 1.3, a client certificate from the agents' CA, and the agents' token. The chart deploys and configures both sides. |
| **Browser and mobile RUM** | `fusescopeCollector.rum` (port 4319) | TLS and RUM keys from the ingest policy. The FuseScope RUM SDKs send there. |
| **Generic OpenTelemetry SDKs and agents** | an OTLP endpoint with TLS and a key **that you provide** | The chart does not create it. Announce it in FuseScope's onboarding with `signoz.ingestionUrl`, which only shows the URL. |
| **Clients that cannot move yet** | stay on `otlp/insecure`, temporarily | Until `ingestauth` (ADR 0006) gives per-key authentication. Then use its `permissive` or `warn` mode on a TLS endpoint for clients with TLS but no key yet. |

For each client:
1. Change its endpoint and credentials.
2. Confirm its data still arrives.
3. Fill in "Moved on" and "Verified" in the inventory.

## 5. Measure the exit

**Choose the window.**
- It lasts at least 7 days, and longer than the longest period of any job in the inventory.
- It must fit in the metrics retention (§1).
- Below, `7d` and `604800` stand for it. **Replace both with your window**, in PromQL (`[7d]`) and as
  the ClickHouse parameter `window_seconds`.

**A window holds only if the collector's self-metrics cover it without a break.**
- **What breaks it.** A collector pod that restarts, is replaced or scales loses what it counted
  before. A gap in the self-metrics hides what was received meanwhile.
- **Absence proves nothing.** The receiver's own series may not exist at all, for example after a
  restart with no traffic.
- **How coverage is checked.** It is read from `otelcol_process_uptime`, which every collector pod
  reports for its whole life.
- **The rule.** Any restart, rescale or gap **restarts the window**. The check below finds them.

**No increase, on every counter.** The receiver's counters are cumulative. "Zero during the
window" means **no increase in the window**, not a raw value of zero.
- Count spans, log records and metric points.
- Count the `refused` and `failed` counters too, where a client that keeps retrying shows up.

In FuseScope (PromQL), every expression must give 0. Each one has `or vector(0)`, because a counter
with no series in the window returns no data rather than 0:

```promql
sum(increase(otelcol_receiver_accepted_spans{receiver="otlp/insecure"}[7d])) or vector(0)
sum(increase(otelcol_receiver_accepted_log_records{receiver="otlp/insecure"}[7d])) or vector(0)
sum(increase(otelcol_receiver_accepted_metric_points{receiver="otlp/insecure"}[7d])) or vector(0)
sum(increase(otelcol_receiver_refused_spans{receiver="otlp/insecure"}[7d])) or vector(0)
sum(increase(otelcol_receiver_refused_log_records{receiver="otlp/insecure"}[7d])) or vector(0)
sum(increase(otelcol_receiver_refused_metric_points{receiver="otlp/insecure"}[7d])) or vector(0)
sum(increase(otelcol_receiver_failed_spans{receiver="otlp/insecure"}[7d])) or vector(0)
sum(increase(otelcol_receiver_failed_log_records{receiver="otlp/insecure"}[7d])) or vector(0)
sum(increase(otelcol_receiver_failed_metric_points{receiver="otlp/insecure"}[7d])) or vector(0)
```

**Or directly in ClickHouse, read-only.**
- **Use the `distributed_*` tables with `GLOBAL IN`.** On a ClickHouse with several shards, the local
  tables hold only one shard's rows and give a false zero.
- **Pass the window as a parameter**, for example
  `clickhouse-client --readonly 1 --param_window_seconds=604800 --queries-file increase.sql`.
- **Restarts inside one series** count their new value, as PromQL's `increase` does.

```sql
-- increase.sql: the increase of each otlp/insecure receiver counter in the window. Every row must
-- read 0.
SELECT metric_name, sum(if(value >= previous, value - previous, value)) AS increase
FROM (
  SELECT metric_name, fingerprint, value,
         lagInFrame(value, 1, value) OVER (PARTITION BY metric_name, fingerprint ORDER BY unix_milli
                                           ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS previous
  FROM signoz_metrics.distributed_samples_v4
  WHERE unix_milli >= (toUnixTimestamp(now()) - {window_seconds:UInt32}) * 1000
    AND metric_name IN ('otelcol_receiver_accepted_spans', 'otelcol_receiver_accepted_log_records',
                        'otelcol_receiver_accepted_metric_points', 'otelcol_receiver_refused_spans',
                        'otelcol_receiver_refused_log_records', 'otelcol_receiver_refused_metric_points',
                        'otelcol_receiver_failed_spans', 'otelcol_receiver_failed_log_records',
                        'otelcol_receiver_failed_metric_points')
    AND fingerprint GLOBAL IN (
      SELECT fingerprint FROM signoz_metrics.distributed_time_series_v4
      WHERE JSONExtractString(labels, 'receiver') = 'otlp/insecure'
        -- series rows are floored to the hour: one hour of margin
        AND unix_milli >= (toUnixTimestamp(now()) - {window_seconds:UInt32} - 3600) * 1000)
)
GROUP BY metric_name
ORDER BY metric_name
```

A counter with no row has not been reported in the window. A counter that has never been
incremented may never be exported, so a missing row also reads as 0.

**Check that the window holds.** `window.sql` reads the collector's `otelcol_process_uptime` and
returns one row per collector pod series in the window. `scrape_seconds` is
`fusescopeCollector.selfMetrics.scrapeInterval`, 60 by default. Each row says whether the series:
- `started_late`: began after the window opened, which means a new, restarted or rescaled pod;
- `ended_early`: stopped before now, a pod gone or a gap at the end;
- `has_gap`: has a gap longer than three scrape intervals;
- `uptime_reset`: has an uptime that went down, a restart inside the same series.

**The window holds only if the query returns at least one row and every row reads `ok = 1`. No row
at all is a failure, not a success:** it means no self-metrics for the window.

```sql
-- window.sql: --param_window_seconds=604800 --param_scrape_seconds=60.
-- One row per collector pod series. The window holds only if there is at least one row and every
-- row reads ok = 1. No row is a failure.
SELECT fingerprint,
       fromUnixTimestamp64Milli(min(unix_milli)) AS first_seen,
       fromUnixTimestamp64Milli(max(unix_milli)) AS last_seen,
       min(unix_milli) > (toUnixTimestamp(now()) - {window_seconds:UInt32} + 3 * {scrape_seconds:UInt32}) * 1000 AS started_late,
       max(unix_milli) < (toUnixTimestamp(now()) - 3 * {scrape_seconds:UInt32}) * 1000 AS ended_early,
       max(gap) > 3 * {scrape_seconds:UInt32} * 1000 AS has_gap,
       max(went_down) AS uptime_reset,
       NOT (started_late OR ended_early OR has_gap OR uptime_reset) AS ok
FROM (
  SELECT fingerprint, unix_milli,
         unix_milli - lagInFrame(unix_milli, 1, unix_milli) OVER w AS gap,
         value < lagInFrame(value, 1, value) OVER w AS went_down
  FROM signoz_metrics.distributed_samples_v4
  WHERE unix_milli >= (toUnixTimestamp(now()) - {window_seconds:UInt32}) * 1000
    AND metric_name = 'otelcol_process_uptime'
    AND fingerprint GLOBAL IN (
      SELECT fingerprint FROM signoz_metrics.distributed_time_series_v4
      WHERE metric_name = 'otelcol_process_uptime'
        AND JSONExtractString(labels, 'service.name') = 'fusescope-collector'
        -- series rows are floored to the hour: one hour of margin
        AND unix_milli >= (toUnixTimestamp(now()) - {window_seconds:UInt32} - 3600) * 1000)
  WINDOW w AS (PARTITION BY fingerprint ORDER BY unix_milli ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
)
GROUP BY fingerprint
ORDER BY fingerprint
```

**The exit holds when all of these are true:**
1. `window.sql` returns at least one row, and every row reads `ok = 1`;
2. every counter shows no increase through it;
3. every row of the inventory reads "Moved on" and "Verified".

## 6. Turn the plaintext receiver off

```yaml
fusescopeCollector:
  otlp:
    insecure: false
```

The chart then removes the receiver `otlp/insecure`, ports 4317 and 4318 on the collector and its
Service, and the compatibility Service. FuseScope's onboarding no longer shows or checks that
endpoint; it shows `signoz.ingestionUrl` if you set it. If none of the agents' listener, RUM or the
plaintext receiver is on, the chart deploys no collector, and `NOTES.txt` says so.

Then remove the NetworkPolicy of §3, or narrow it to the ports that remain.

## 7. Rolling back

| Stage | To go back |
|---|---|
| After §3 (switch) | Point the clients back to the SigNoz collector, or roll the release back. A schema `fusescope-schema` already migrated cannot go back to the fork's migrator ([ADR 0010](adr/0010-fusescope-schema.md)). **Rolling the FuseScope chart back below 0.168.0** brings back plaintext OTLP by default, and **removes the exposure protections**. It again allows a `NodePort` or `LoadBalancer` on `fusescopeCollector.service` and `otelCollector.service`, and the OTLP Ingress `otelCollector.ingress`. Before such a rollback, audit those values so plaintext is not exposed outside the cluster. Below 0.168.1, the plaintext ports also lose `otlp.networkPolicy.ingressFrom`. |
| A client after §4 | Point it back to the compatibility Service. It is plaintext again and counts in §5 again, and its inventory row is open again. |
| After §6 (off) | Set `otlp.insecure: true` again. The receiver, its ports and the compatibility Service come back, and the clients that still need them resume. Restart the window of §5. |
