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

- **Chart.** `fusescope` **0.168.0 or later**, which has `fusescopeCollector.otlp.insecure` and its
  refusals. Earlier charts served plaintext OTLP by default, with nothing to measure or bound it.
- **Collector self-metrics.** `fusescopeCollector.selfMetrics.enabled: true`, the default. The chart
  refuses `otlp.insecure` without it, because the exit in §5 is read from those metrics.
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
forge telemetry and spend the collector's and ClickHouse's resources. Limit who reaches those
ports with a NetworkPolicy, for example to the namespaces in your inventory:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: fusescope-collector-otlp-insecure
  namespace: platform
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/component: fusescope-collector
  policyTypes: [Ingress]
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              fusescope.io/otlp-plaintext: "allowed"
      ports:
        - {port: 4317, protocol: TCP}
        - {port: 4318, protocol: TCP}
    # A policy that selects the pods isolates them: list every other port they serve, such as
    # health (13133), RUM (4319) and the agents' listener (4322), with its own sources.
```

> **Limitation.** NetworkPolicies add up.
>
> - `fusescopeCollector.agentListener.networkPolicy.enabled` is on by default with the agents'
>   listener. The chart's own policy then allows 4317 and 4318 from any source, and a policy of
>   yours cannot narrow that.
> - Until the chart can restrict those ports itself, choose one of these while plaintext is on:
>   - run the agents' listener with `agentListener.networkPolicy.enabled: false` and write the
>     policy for all ports yourself;
>   - accept that the OTLP ports are open to the whole cluster.

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

The receiver's counters are cumulative. "Zero during the window" means **no increase in the
window**, not a raw value of zero. Count every signal, and the refused and failed counters too,
because a client that keeps retrying shows there. The window lasts at least 7 days, and longer than
the longest period of any job in the inventory.

In FuseScope (PromQL), every expression must give 0 over the window:

```promql
sum(increase(otelcol_receiver_accepted_spans{receiver="otlp/insecure"}[7d]))
sum(increase(otelcol_receiver_accepted_log_records{receiver="otlp/insecure"}[7d]))
sum(increase(otelcol_receiver_accepted_metric_points{receiver="otlp/insecure"}[7d]))
sum(increase(otelcol_receiver_refused_spans{receiver="otlp/insecure"}[7d]))
sum(increase(otelcol_receiver_refused_log_records{receiver="otlp/insecure"}[7d]))
sum(increase(otelcol_receiver_refused_metric_points{receiver="otlp/insecure"}[7d]))
sum(increase(otelcol_receiver_failed_spans{receiver="otlp/insecure"}[7d]))
sum(increase(otelcol_receiver_failed_log_records{receiver="otlp/insecure"}[7d]))
sum(increase(otelcol_receiver_failed_metric_points{receiver="otlp/insecure"}[7d]))
```

Or directly in ClickHouse, read-only. The same rule applies: a counter that restarts counts its new
value, as PromQL's `increase` does.

```sql
-- Increase of each otlp/insecure receiver counter in the last 7 days. Every row must read 0.
WITH series AS (
  SELECT DISTINCT fingerprint
  FROM signoz_metrics.time_series_v4
  WHERE JSONExtractString(labels, 'receiver') = 'otlp/insecure'
    AND unix_milli >= (toUnixTimestamp(now()) - 7 * 86400) * 1000
),
points AS (
  SELECT metric_name, fingerprint, value,
         lagInFrame(value, 1, value) OVER (PARTITION BY metric_name, fingerprint ORDER BY unix_milli
                                           ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS previous
  FROM signoz_metrics.samples_v4
  WHERE unix_milli >= (toUnixTimestamp(now()) - 7 * 86400) * 1000
    AND fingerprint IN (SELECT fingerprint FROM series)
    AND metric_name IN ('otelcol_receiver_accepted_spans', 'otelcol_receiver_accepted_log_records',
                        'otelcol_receiver_accepted_metric_points', 'otelcol_receiver_refused_spans',
                        'otelcol_receiver_refused_log_records', 'otelcol_receiver_refused_metric_points',
                        'otelcol_receiver_failed_spans', 'otelcol_receiver_failed_log_records',
                        'otelcol_receiver_failed_metric_points')
)
SELECT metric_name, sum(if(value >= previous, value - previous, value)) AS increase
FROM points
GROUP BY metric_name
ORDER BY metric_name
```

A counter with no row has not been reported in the window. A counter that has never been
incremented may never be exported, so a missing row also reads as 0.

**The exit holds when both of these are true:**
1. every counter shows no increase through the window;
2. every row of the inventory reads "Moved on" and "Verified".

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
| After §3 (switch) | Point the clients back to the SigNoz collector, or roll the release back. Rolling back the FuseScope chart below 0.168.0 brings back plaintext OTLP by default. A schema `fusescope-schema` already migrated cannot go back to the fork's migrator ([ADR 0010](adr/0010-fusescope-schema.md)). |
| A client after §4 | Point it back to the compatibility Service. It is plaintext again and counts in §5 again, and its inventory row is open again. |
| After §6 (off) | Set `otlp.insecure: true` again. The receiver, its ports and the compatibility Service come back, and the clients that still need them resume. Restart the window of §5. |
