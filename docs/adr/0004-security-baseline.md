# 0004 · Security baseline

- Status: accepted, amended by [ADR 0006](0006-single-ingress.md) (ingest policy over OpAMP) and
  [ADR 0007](0007-clickhouse-plaintext-for-migration.md) (plaintext to ClickHouse by opt-in)
- Date: 2026-10-31

## Decision

- TLS 1.3 minimum on every network listener and exporter that leaves the host; mTLS between
  agents and gateways and from gateways to engine ingesters.
- Bearer-token authentication on gateway OTLP ingest; engine exporters present the tenant's
  `fst_` ingest token (engine DESIGN §10.3). Tokens come only from mounted files.
- Remote configuration over OpAMP accepts only allow-listed processor types; receivers,
  exporters, extensions, endpoints and credentials are local configuration only.
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
