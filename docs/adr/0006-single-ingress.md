# 0006 · Single ingress: RUM, ingestion keys and the OpAMP ingest policy

- Status: proposed
- Date: 2026-10-31
- Follows: FuseScope ADR 0009 ([fuseone-io/fusescope#222](https://github.com/fuseone-io/fusescope/pull/222)) and ADR 0010 ([#223](https://github.com/fuseone-io/fusescope/pull/223)), both proposed.
- Amends: [ADR 0004](0004-security-baseline.md), `DESIGN.md` §2, §3.2, §5 and §9.
- Issue: [#13](https://github.com/fuseone-io/fusescope-collector-server/issues/13)

## Context

FuseScope ADR 0010 makes this collector the only internet-facing ingest tier and moves the RUM
gateway out of the backend. ADR 0009 puts every new collector component in this repo and starts
with an ingress tier in front of the existing collector. `DESIGN.md` and ADR 0004 said the
opposite: the RUM gateway stays in the backend, the collector never receives browser traffic,
and nothing related to credentials arrives over OpAMP.

## Decision

1. **Single ingress.** The gateway is the only collector exposed for ingest: OTLP gRPC and
   HTTP, RUM (`/rum/v1/*`), profiles, and artifact uploads. The backend serves no ingest route.
2. **New components:**
   - `ingestauth`: a server authenticator on every public receiver. It validates keys locally,
     stamps `fusescope.ingest.key_id` and enforces per-key limits, in `permissive`, `warn` or
     `enforce` mode.
   - `rumreceiver`: the backend's RUM gateway, moved rather than rewritten. It is FuseOne's MIT
     code and keeps its MIT notice. Before porting, check that no part came from SigNoz, and
     specify its behaviour in `docs/spec/rum.md`.
   - `artifactreceiver`: source maps, R8 mappings and debug files, streamed to the backend's
     internal intake.
3. **ADR 0004 amended, ingest side.** The collector may receive one OpAMP custom message, the
   ingest policy (`io.fuseone.fusescope.ingest.v1`). It carries:
   - SHA-256 verifiers of ingestion keys, never the keys themselves, with their org, signals,
     expiry and limits;
   - the RUM applications' origins, package names, platform and PII policy.

   The message can only **restrict** what is accepted. It cannot add or change receivers,
   exporters, extensions, endpoints or pipelines. It is cached on disk, holding hashes only, so
   ingest survives a backend outage. It never appears in the effective config the collector
   reports.
4. **ADR 0004 kept, outbound side.** The collector's own credentials still come only from
   mounted files: ClickHouse password, engine token, TLS keys. The one exception is the
   per-collector artifact-intake credential of FuseScope ADR 0010 §7: it is delivered over
   OpAMP, valid only against the backend that issued it, and never used by an exporter.
5. **Phasing.** The ingress tier, forwarding OTLP to the existing collector, can ship
   independently of ClickHouse parity (DESIGN phases 1–5). RUM depends only on this tier.
6. **Naming (decided).** The product, binary and image are `fusescope-collector` (as in
   `builder-config.yaml` `dist.name`). Only the repository is `fusescope-collector-server`.
   FuseScope ADR 0009 §1 records the same rule.

## Consequences

- One internet-facing ingest surface with one auth model; the backend can run fully private.
- A compromised backend can at worst refuse traffic or change which keys are accepted. It still
  cannot redirect telemetry or read the collector's outbound credentials.
- Browser traffic reaches this collector, so CORS, body limits and per-IP rate limits become
  part of its attack surface and of its tests.
