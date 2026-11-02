# 0007 · Plaintext to ClickHouse, by explicit opt-in

- Status: accepted
- Date: 2026-11-01
- Amends: [ADR 0004](0004-security-baseline.md), `DESIGN.md` §5.

## Context

ADR 0004 requires TLS 1.3 on every exporter that leaves the host. Existing SigNoz and FuseScope
installations, which this collector replaces as is (`DESIGN.md` §9), usually run ClickHouse on
the native port 9000 in plaintext, inside the cluster network. Requiring TLS would make the
migration depend on reconfiguring ClickHouse first, with certificates on every server, before
the collector can be swapped.

## Decision

1. The `fusescope_clickhouse` exporter defaults to TLS, with TLS 1.3 as the minimum version
   even when `tls.min_version` is unset. Lower versions are rejected.
2. Plaintext is allowed only when the operator sets `tls.insecure: true` on that exporter. The
   exporter then logs a warning at start that credentials and telemetry travel in clear text,
   naming the endpoint.
3. The exception covers this exporter only. Receivers, the agent-to-gateway and gateway-to-engine
   links, and every other exporter keep ADR 0004 unchanged.
4. Shipped example configurations stay on TLS. The migration guide (phase 8) documents
   `tls.insecure: true` for clusters whose ClickHouse has no TLS, and how to move to TLS later.

## Consequences

- A SigNoz or FuseScope cluster can switch collectors without touching ClickHouse.
- Where `tls.insecure: true` is set, the ClickHouse password and the telemetry are readable on
  the cluster network. Network policies limiting who reaches ClickHouse are the operator's
  mitigation.
- A typo cannot downgrade TLS silently: plaintext needs the explicit key, and a TLS connection
  never negotiates below 1.3.
