# 0009 · TLS 1.2 opt-in on RUM ingress

- Status: accepted (2026-11-06)
- Issue: [#62](https://github.com/fuseone-io/fusescope-collector-server/issues/62); Android delivery: [FuseScope #337](https://github.com/fuseone-io/fusescope/issues/337), [#189](https://github.com/fuseone-io/fusescope/issues/189).
- Amends: [ADR 0004](0004-security-baseline.md), transport floor for the RUM listener only.
- Deciders: FuseOne maintainers.

## Context

The Android SDK supports API 23+. Stock Android 6–9 (API 23–28) cannot negotiate TLS 1.3, which
the RUM listener requires. Bundling another TLS implementation in every consuming application
would add binary size, ABI packaging and maintenance obligations, so compatibility is provided
at collector ingress instead.

## Decision

- **TLS 1.3 stays the default** of the `rumreceiver` listener. TLS 1.2 is an explicit opt-in
  with `tls.min_version: "1.2"`, for deployments that serve older Android clients; capable
  clients still negotiate TLS 1.3.
- **With TLS 1.2 enabled, only ECDHE with AEAD is accepted:** AES-GCM and ChaCha20-Poly1305
  (`TLS_ECDHE_{ECDSA,RSA}_WITH_AES_{128_GCM_SHA256,256_GCM_SHA384}`,
  `TLS_ECDHE_{ECDSA,RSA}_WITH_CHACHA20_POLY1305_SHA256`). This is the receiver's suite list when
  `tls.cipher_suites` is unset, and validation refuses any configured suite outside it: CBC,
  SHA-1, static RSA key exchange and all others. Go's default TLS 1.2 list is not enough on its
  own: it also accepts ECDHE with AES-CBC-SHA. TLS 1.3 uses Go's own suites.
- TLS 1.0, TLS 1.1 and plaintext remain refused. Certificate and key files, authentication,
  origin and package checks, tenant isolation, limits and fail-closed policy behaviour keep their
  contracts.
- The exception applies only to public RUM client ingress. OpAMP, ClickHouse, agent and gateway
  transports, exporters and other receivers keep TLS 1.3. No remote policy message can change the
  local TLS configuration.
- A terminating proxy in front of the collector must accept TLS 1.2 with the same AEAD suites on
  the RUM public endpoint for old clients. The proxy-to-collector hop may stay TLS 1.3, with
  certificate verification and preferably mTLS. A shared non-RUM public listener is not lowered
  implicitly: split the endpoints or configure an explicitly reviewed RUM ingress.

## Consequences

Deployments that do not opt in are unchanged. The SDK gains no TLS provider dependency. Serving
Android 6–9 takes three steps: a collector with this change, `min_version: "1.2"` in its RUM
configuration (the chart's `fusescopeCollector.rum.tlsMinVersion`), and a public proxy that
accepts TLS 1.2. Receiver tests cover traces and logs with TLS 1.2 clients, refusal of a
CBC-only client, the AEAD suites, TLS 1.3 negotiation, refusal of 1.0 and 1.1, and the strict
default. Device acceptance on API 23 and 28 remains a release gate in FuseScope #337.
