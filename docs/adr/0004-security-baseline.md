# 0004 · Security baseline

- Status: accepted, amended by [ADR 0006](0006-single-ingress.md) (ingest authentication, the
  ingest policy over OpAMP, security gates),
  [ADR 0007](0007-clickhouse-plaintext-for-migration.md) (plaintext to ClickHouse by opt-in),
  [ADR 0008](0008-opamp-remote-config.md) (remote configuration, OpAMP transport) and
  [ADR 0009](0009-rum-tls-compatibility.md) (TLS 1.2 opt-in on RUM ingress)
- Date: 2026-10-31

## Decision

- TLS 1.3 minimum on every network listener and exporter that leaves the host; the RUM
  listener may opt in to TLS 1.2 with ECDHE AEAD suites only
  ([ADR 0009](0009-rum-tls-compatibility.md)). mTLS between agents and gateways and from
  gateways to engine ingesters.
- Ingest authentication on the gateway's public receivers by `ingestauth` and `rumreceiver`,
  against SHA-256 verifiers of ingestion and RUM keys from the OpAMP ingest policy
  ([ADR 0006](0006-single-ingress.md), decision 3 and security gates). Engine exporters present
  the tenant's `fst_` ingest token (engine DESIGN §10.3). The collector's own credentials come
  only from mounted files; the one exception is the artifact-intake credential (ADR 0006,
  decision 4).
- Remote configuration over OpAMP accepts only allow-listed processor types; receivers,
  exporters, extensions, endpoints and credentials are local configuration only. The ingest
  policy custom message (ADR 0006) is not configuration: it is the remote authority for ingest
  authentication and identity (which keys, signals, origins and org), holds verifiers and never
  keys, and is cached on disk. It cannot change listeners, pipelines, destinations or outbound
  credentials.
- Debug and introspection endpoints (`pprof`, `zpages`, the health config view) are off or
  bound to `localhost` by default.
- Distroless, non-root, read-only root filesystem images.
- CI blocks on unpinned modules, licenses outside the allow-list and known vulnerabilities.
  A vulnerability with a fix is fixed by bumping the module (`replaces` in
  `builder-config.yaml`); one without a fix may be accepted only in `.govulncheck-allow`
  with the reason, what removes it and a revisit date.

## Consequences

Plaintext or token-less example configurations are not shipped; local development uses the
same configs with self-signed certificates.
