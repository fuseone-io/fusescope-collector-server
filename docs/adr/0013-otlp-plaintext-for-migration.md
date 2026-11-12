# 0013 · Plaintext OTLP inside the cluster during a SigNoz migration

- Status: accepted (2026-11-12), option B
- Date: 2026-11-12
- Issue: [#9](https://github.com/fuseone-io/fusescope-collector-server/issues/9) (open item from
  the #43 review, 2026-11-03)
- Amends: [ADR 0004](0004-security-baseline.md)
- Related: [ADR 0006](0006-single-ingress.md) (ingest authentication),
  [ADR 0007](0007-clickhouse-plaintext-for-migration.md) (the same kind of opt-in, for ClickHouse)

## Context

**The rule.** ADR 0004 requires TLS 1.3 on every network listener. ADR 0006 puts `ingestauth` on
every public receiver to check ingestion keys. `ingestauth` is not built yet
(fuseone-io/fusescope#232).

**What SigNoz installs do.** In a typical SigNoz install, agents and SDKs send OTLP without TLS
and without a key to the collector inside the cluster, on ports 4317 (gRPC) and 4318 (HTTP). A
gateway that requires TLS and a key breaks them until each one is reconfigured.

**What the chart already does.** FuseScope chart 0.167.0 already serves OTLP this way:
- The `otlp` receiver of `fusescopeCollector` listens on 4317 and 4318 with no `tls` and no
  `auth` (`templates/fusescope-collector/configmap.yaml`), and it is on by default
  (`fusescopeCollector.otlp.enabled: true`).
- Two Services expose it: the collector's own, and one named like the earlier collector's
  (`service-otlp-compat.yaml`), so existing agents keep their endpoint. Both default to
  `ClusterIP`.
- The OTLP Ingress (`ingress-otlp-compat.yaml`, behind `otelCollector.ingress.enabled`, off by
  default) can route outside traffic to this unauthenticated receiver.
- TLS and a token are required only on the FuseScope agents' receiver (`otlp/agents`) and on RUM.

So an exception to ADR 0004 already ships, and no decision records it. The migration guide
(phase 8) needed one of two options:

- **A · No exception:** reconfigure every agent and SDK to TLS and a token before switching
  collectors, and drop the plaintext receiver. The migration then touches every client before
  any traffic moves. Until `ingestauth` exists, the only key is `bearertokenauth`, a shared token.
- **B · An explicit opt-in:** off by default, internal only and measured, as ADR 0007 does for
  ClickHouse.

## Decision

Option B. The existing broad exposure becomes an explicit exception, off by default, measured and
scoped to the migration.

1. **Off by default, one key.** The plaintext receiver exists only with
   `fusescopeCollector.otlp.insecure: true`.
   - **With `insecure: false`, the default,** the chart renders no plaintext receiver, does not
     open ports 4317 and 4318 on the collector or its Service, and does not render the
     compatibility Service `RELEASE-fusescope-otel-collector`.
   - **With `insecure: true`,** it renders the receiver `otlp/insecure` on those ports, the ports
     on the collector's Service, and the compatibility Service.
   - **The old key.** `fusescopeCollector.otlp.enabled` no longer turns plaintext on. The chart
     refuses `otlp.enabled: true` unless `otlp.insecure: true` is also set, so an old values
     file cannot bring plaintext back without the explicit key.
2. **Its own receiver id, `otlp/insecure`.** The collector accepts full ids of the form
   `<type>/<name>` ([receiver configuration][receivers]). The receiver's metrics are
   `otelcol_receiver_accepted_spans`, `otelcol_receiver_accepted_log_records` and
   `otelcol_receiver_accepted_metric_points`, and their `refused` and `failed` counterparts
   ([receiver metrics][receiver-metrics]).
   - **The id label.** The collector's component telemetry labels them with the full id. This was
     observed on `fusescope-collector` 0.6.1 (core v0.164.0) with a receiver `otlp/insecure` and
     one span over HTTP: `otelcol_receiver_accepted_spans{receiver="otlp/insecure",transport="http"} 1`.
   - **What they measure.** These counters measure accepted volume, not clients.
   - **No patch.** The upstream receiver is not patched (ADR 0001).
3. **Measured, or refused.** `otlp.insecure: true` requires
   `fusescopeCollector.selfMetrics.enabled: true`, which writes those metrics to ClickHouse. The
   chart refuses the combination with `selfMetrics.enabled: false`, because the exception would
   then not be measurable.
4. **No external route from the chart.** With `otlp.insecure: true`, the chart refuses three
   things:
   - `fusescopeCollector.service.type` set to `NodePort` or `LoadBalancer`;
   - `otelCollector.service.type`, the compatibility Service, set to `NodePort` or
     `LoadBalancer`;
   - `otelCollector.ingress.enabled: true`.

   The chart creates no external route to the receiver. Services and Ingresses created outside
   the chart remain the operator's responsibility.
5. **Warned.** With `otlp.insecure: true`, `NOTES.txt` names `otlp/insecure` and its ports. It
   says that telemetry arrives there without TLS or a key, and points to the migration guide.
6. **Exit.** The receiver is turned off when two things hold together:
   - the accepted counters of `otlp/insecure` stay at zero for every signal through a window the
     migration guide defines;
   - the operator's inventory of agents and SDKs shows that every client was moved to TLS and a
     key.

   Zero traffic alone does not prove that no client remains, because a client may be idle.
7. **Scope.** The exception covers this receiver only. RUM, `otlp/agents`, OpAMP and every
   exporter keep ADR 0004, ADR 0007 and ADR 0008 unchanged.
8. **When `ingestauth` lands** (ADR 0006), the guide uses its `permissive` and `warn` modes on a
   TLS receiver for clients that have TLS but no key yet. `otlp/insecure` stays only for clients
   that cannot do TLS, and remains off by default.

## Consequences

- A SigNoz cluster can switch collectors before it reconfigures its agents, with the exception
  visible in its values and counted in its own metrics.
- New installs get no plaintext receiver. An installation that relied on the old default turns
  the receiver on explicitly. FuseScope has no installs that depend on that default.
- **Exposure while it is on.** Any workload with network access to the collector can send
  telemetry, inject or forge data under any service or resource attributes, and consume the
  collector's and ClickHouse's resources. There is no key to tell clients apart or to limit
  them. The operator's mitigations are a NetworkPolicy that limits who reaches ports 4317 and
  4318, and an inventory of the emitters.
- **Work in fusescope-charts:**
  - `otlp.insecure`, the receiver id `otlp/insecure`, and the rendering rules of decision 1;
  - the refusals of decisions 1, 3 and 4;
  - the `NOTES.txt` warning.

  Tests cover:
  - the secure default: no receiver, no ports, no compatibility Service;
  - the opt-in;
  - each refusal: `fusescopeCollector.service` and `otelCollector.service` as `NodePort` and as
    `LoadBalancer`, `otelCollector.ingress.enabled`, `selfMetrics.enabled: false`, and
    `otlp.enabled` without `insecure`;
  - `otlp/insecure` in every pipeline that the `otlp` receiver fed;
  - the warning.
- **Work in the migration guide (#9):** turning the receiver on, the exit window and the
  inventory, moving each client to TLS and a key, and turning the receiver off.

[receivers]: https://github.com/open-telemetry/opentelemetry-collector/blob/main/receiver/README.md
[receiver-metrics]: https://github.com/open-telemetry/opentelemetry-collector/blob/main/receiver/receiverhelper/documentation.md
