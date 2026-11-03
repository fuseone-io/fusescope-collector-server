# 0006 · Single ingress: RUM, ingestion keys and the OpAMP ingest policy

- Status: accepted (2026-11-03), with FuseScope ADRs 0009 and 0010 accepted by
  [fuseone-io/fusescope#303](https://github.com/fuseone-io/fusescope/pull/303), merge commit
  `4bee14144f2f0331e42c1ce0f672efa58aa19338`.
- Date: 2026-10-31
- Follows: FuseScope ADR 0009 ([fuseone-io/fusescope#222](https://github.com/fuseone-io/fusescope/pull/222),
  `docs/adr/0009-fusescope-collector.md`) and ADR 0010
  ([#223](https://github.com/fuseone-io/fusescope/pull/223), `docs/adr/0010-single-ingress.md`),
  both merged 2026-11-01, and ADR 0003 (`docs/adr/0003-real-user-monitoring.md`), which ADR 0010
  amends.
- Amends: [ADR 0004](0004-security-baseline.md), `DESIGN.md` §2, §3.2, §5 and §9.
- Specified in: [`docs/spec/rum.md`](../spec/rum.md).
- Issue: [#13](https://github.com/fuseone-io/fusescope-collector-server/issues/13)

## Context

FuseScope ADR 0010 makes this collector the only internet-facing ingest tier and moves the RUM
gateway out of the backend. ADR 0009 puts every new collector component in this repo and starts
with an ingress tier in front of the existing collector. `DESIGN.md` and ADR 0004 said the
opposite: the RUM gateway stays in the backend, the collector never receives browser traffic,
and nothing related to credentials arrives over OpAMP.

Issue #13 listed three conflicts to settle first. This ADR resolves them:

| Conflict | Resolution |
|---|---|
| `DESIGN.md` §2 kept the RUM gateway in the backend and browsers away from the collector | §2 now makes the gateway the only ingest surface, browsers and mobile apps included, under the security gates below |
| ADR 0004 forbade credentials over OpAMP | ADR 0004 is amended on the ingest side only (decision 3): one custom message with key verifiers, never keys, and never the collector's own credentials (decision 4) |
| ADR 0009 named the repository `fusescope-collector` | naming decided (decision 6); FuseScope ADR 0009 §1 says the same |

## Decision

1. **Single ingress.** The gateway is the only collector exposed for ingest: OTLP gRPC and
   HTTP, RUM (`/rum/v1/*`), profiles, and artifact uploads. The backend serves no ingest route.
2. **New components:**
   - `ingestauth`: a server authenticator on every public receiver. It validates keys locally,
     stamps `fusescope.ingest.key_id` and enforces per-key limits, in `permissive`, `warn` or
     `enforce` mode.
   - `rumreceiver`: the backend's RUM gateway, moved rather than rewritten. It is FuseOne's MIT
     code and keeps its MIT notice. Before porting, check that no part came from SigNoz, and
     specify its behaviour in `docs/spec/rum.md`. It validates RUM keys itself, against the same
     immutable policy store `ingestauth` uses.
   - `artifactreceiver`: source maps, R8 mappings and debug files, streamed to the backend's
     internal intake.
3. **ADR 0004 amended, ingest side.** The collector may receive one OpAMP custom message, the
   ingest policy (`io.fuseone.fusescope.ingest.v1`), a strict, versioned JSON snapshot. It
   carries:
   - SHA-256 verifiers of ingestion keys, never the keys themselves, with their org, signals,
     expiry and limits;
   - the RUM applications' org, name, environment, origins, package names, platform and PII
     policy.

   The policy is the **remote authority for ingest authentication and identity**: it decides
   which keys are accepted, for which signals and origins, which org data is attributed to, and
   the application name and environment stamped on RUM data. It **cannot** change listeners,
   receivers, pipelines, processors, exporters, extensions, destinations or outbound
   credentials. It is cached on disk with the highest revision accepted, holding verifiers
   only, so ingest survives a backend outage. It never appears in the effective config the
   collector reports. The same capability carries the collector's ingest report to the
   backend and the backend's acknowledgement of it, under security gate 5.
4. **ADR 0004 kept, outbound side.** The collector's own credentials still come only from
   mounted files: ClickHouse password, engine token, TLS keys. The one exception is the
   per-collector artifact-intake credential of FuseScope ADR 0010 §7: it is delivered over
   OpAMP, valid only against the backend that issued it, and never used by an exporter.
5. **Phasing.** The ingress tier, forwarding OTLP to the existing collector, can ship
   independently of ClickHouse parity (DESIGN phases 1–5). RUM depends only on this tier. The
   backend publishes the policy first: a collector without a valid policy is not ready, and the
   chart routes `/rum/v1` to it only once it holds one.
6. **Naming (decided).** The product, binary and image are `fusescope-collector` (as in
   `builder-config.yaml` `dist.name`). Only the repository is `fusescope-collector-server`.
   FuseScope ADR 0009 §1 records the same rule.

## Security gates

Decided by the maintainer and binding on `ingestauth`, `rumreceiver` and the ingest policy.
FuseScope ADR 0010 §10 (fuseone-io/fusescope#303, `4bee141`) records the differences from what 0010 first
said; `docs/spec/rum.md` §12 lists each difference from the backend gateway.

1. **Fail closed.** With no valid ingest policy, none received over OpAMP and none cached on
   disk, or a cached one that cannot be read or validated, `rumreceiver` and `ingestauth` reject
   all traffic, in every mode, and the collector reports not ready until a policy arrives. A
   policy is accepted only whole and only if strictly valid: a known `schemaVersion`, no unknown
   field, a revision not lower than the highest accepted (the same revision only with identical
   content), no duplicate IDs or verifiers, no dangling reference, and the same org on a key and
   its application.
2. **`X-Forwarded-For` only from configured proxies.** The client IP is taken from
   `X-Forwarded-For` only when the connection comes from an address in an explicit CIDR list in
   local configuration. The default list is empty: no proxy is trusted and the peer address is
   the client IP.
3. **Body size limited before and after decompression.** Every receiver that accepts a
   compressed body caps the bytes read from the wire and the bytes after decoding.
4. **Keys never leave the request.** RUM public keys and ingestion keys travel only in request
   headers, never in a URL; the RUM query-string key is removed. Keys are never logged, never
   put in the collector's own telemetry (spans, metrics, logs, attributes), never in OpAMP
   reports or status. Only key IDs and SHA-256 verifiers may appear.
5. **Minimised OpAMP reports.** The ingest report holds only an explicit list of fields, each
   bounded in length and count (`docs/spec/rum.md` §10.3): IDs, counts, timestamps, rejection
   reasons, the SDK name and version, and the refused origin or package name for the two
   rejections the setup screen shows. No IP address, user agent, URL path or query, session or
   user identifier, or payload content. Origin, package and SDK strings are client-controlled
   and may still carry identifiers, so they are validated, length-limited, held only for the
   status window and until acknowledged, and the backend keeps them no longer than the status
   window.
6. **Bounded state.** All state indexed by external input is bounded, with a default for every
   bound in `docs/spec/rum.md` §7.1: rate-limit buckets (IPv6 grouped by /64, a global cap with
   eviction), session IDs as fixed-size hashes, strings length-limited before they are stored,
   per-application monitors limited to the policy's applications, unknown keys as one global
   counter, concurrent connections and requests, and finite header-read, body-read and write
   timeouts.

## Consequences

- One internet-facing ingest surface with one auth model; the backend can run fully private.
- A compromised backend can authorise keys, widen origins and signals, and change which org
  data is attributed to and how RUM data is stamped. It cannot change listeners, pipelines or
  destinations, nor read the collector's outbound credentials, so it cannot redirect telemetry
  to another endpoint.
- Browser traffic reaches this collector, so CORS, body limits, per-IP rate limits and the
  bounds of gate 6 become part of its attack surface and of its tests.
- Fail closed means a new ingress collector ingests nothing until the backend has published a
  policy to it: the backend side (FuseScope ADR 0010 phase 1) ships before `ingestauth` and
  `rumreceiver` are deployed, and an upgrade that adds them to a running install needs a backend
  that already publishes the policy.
- Behind a load balancer or ingress controller, operators must list its addresses in the
  trusted-proxy CIDRs, or every client shares the proxy's per-IP rate limit.
