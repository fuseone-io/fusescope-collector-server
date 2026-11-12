# 0013 · Plaintext OTLP inside the cluster during a SigNoz migration

- Status: proposed
- Date: 2026-11-12
- Issue: [#9](https://github.com/fuseone-io/fusescope-collector-server/issues/9) (open item from
  the #43 review, 2026-11-03)
- Amends, if accepted: [ADR 0004](0004-security-baseline.md)
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
(phase 8) needs one of the two options below.

## Options

### A · No exception: reconfigure every agent first

The guide moves every agent and SDK to TLS and a token before switching collectors, and the chart
drops the plaintext `otlp` receiver.

- **For:** ADR 0004 holds with no exception.
- **Against:**
  - The migration has to touch every agent and SDK before any traffic moves.
  - Until `ingestauth` exists, the only key is `bearertokenauth`, a shared token with no per-key
    identity or limits.
  - It is a breaking change to the chart's default.

### B · Plaintext by explicit opt-in, internal only, measured (recommended)

The plaintext receiver stays possible, as a named and bounded exception, in the way ADR 0007 does
it for ClickHouse.

1. **Off by default.** New installs get no plaintext receiver. The operator turns it on with an
   explicit key, `fusescopeCollector.otlp.insecure: true`, and the migration guide sets it.
2. **Its own receiver id, `otlp/insecure`.** The collector's own receiver metrics then count
   exactly the clients still on plaintext. Examples are `otelcol_receiver_accepted_spans` and
   `otelcol_receiver_accepted_log_records`, labelled `receiver="otlp/insecure"`. The migration is
   done when they stay at zero. The upstream receiver is not patched (ADR 0001).
3. **Inside the cluster only.** The chart refuses to render it on a Service of type
   `LoadBalancer` or `NodePort`, and behind the OTLP Ingress. An unauthenticated ingest endpoint
   is then never reachable from outside the cluster.
4. **Warned.** The chart's `NOTES.txt` says that telemetry arrives without TLS or a key, and names
   the receiver. The migration guide says how to move each client to TLS and a key, and how to
   turn the receiver off afterwards.
5. **Scope.** The exception covers this receiver only. RUM, `otlp/agents`, OpAMP and every
   exporter keep ADR 0004 and ADR 0007 unchanged.
6. **When `ingestauth` lands** (ADR 0006), the guide uses its `permissive` and `warn` modes on the
   TLS receiver for clients that have TLS but no key yet. `otlp/insecure` stays only for clients
   that cannot do TLS, and it remains off by default.

- **For:**
  - A SigNoz cluster can switch collectors before it touches its agents.
  - The exception is explicit, measurable and impossible to expose by accident.
  - New installs are secure by default.
- **Against:**
  - Where it is on, anyone on the cluster network can send telemetry, with no key to tell
    clients apart. Network policies are the operator's mitigation, as in ADR 0007.
  - It changes the chart's default. FuseScope has no installs that depend on the current default.

## Decision

To be taken by the maintainer, between A and B.

## Consequences

To be written with the decision. Under B: the chart PR that adds `otlp.insecure` and the render
refusals, a chart test for each refusal, and the plaintext section of the migration guide (#9).
