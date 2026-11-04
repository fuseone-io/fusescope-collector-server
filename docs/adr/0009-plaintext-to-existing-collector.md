# 0009 · Plaintext OTLP to the existing collector, while it writes

- Status: proposed
- Date: 2026-11-04
- Amends: [ADR 0004](0004-security-baseline.md).

## Context

The first step of the single ingress ([ADR 0006](0006-single-ingress.md), `DESIGN.md` §9 phase I,
[#13](https://github.com/fuseone-io/fusescope-collector-server/issues/13)) moves the RUM gateway from
the FuseScope backend into this collector. The backend's gateway forwards what it accepts, in
plaintext, to the existing collector's OTLP/HTTP receiver inside the cluster (FuseScope chart
`charts/fusescope/templates/_helpers.tpl`, `signoz.rumEnv`: `http://<otelCollector>:<otlp-http>`).
There the backend's log pipelines, pushed over OpAMP to that collector, apply to RUM logs, and its
span metrics count RUM spans.

This collector replaces the gateway at the same place in the flow, so RUM data keeps that processing
until phase 5 moves it here. ADR 0004 requires TLS 1.3 on every exporter that leaves the host. The
existing collector's receivers are configured by its own configuration in the chart, which this
project does not read (ADR 0002), so TLS on that receiver is not ours to add.

## Decision

1. An `otlphttp` exporter of an `ingress` collector may send to the existing collector in plaintext,
   only with `tls.insecure: true` set on that exporter, and only to an address inside the cluster
   (a Service name), as the backend's gateway does today.
2. The exception covers that exporter only. The RUM and OTLP listeners, OpAMP (TLS for ingress
   collectors, the token only over TLS), ClickHouse (ADR 0007) and every other exporter keep their
   rules.
3. The chart sets it. The hop stays inside the cluster, as the backend's does today; the existing
   collector still takes other ingest on that port (its ingress, the in-cluster agents), so who
   may reach it is the operator's to restrict, with the chart's optional NetworkPolicy.
4. It ends with phase 6: once this collector writes every signal itself, the existing collector is
   gone and so is this hop. The chart then drops the value; no release ships it by default after.

## Consequences

- RUM keeps the processing it has today; the move changes no data.
- RUM telemetry is readable on the cluster network on that hop, as it is today from the backend.
  It carries no key: the key travels only in the `FuseScope-RUM-Key` request header, which never
  reaches the pipeline (`docs/spec/rum.md` §2, G4); only the key ID may.
- A typo cannot downgrade TLS elsewhere: plaintext needs the explicit key on that exporter.
