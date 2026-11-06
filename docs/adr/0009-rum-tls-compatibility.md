# 0009 · TLS 1.2 compatibility on RUM ingress

- Status: proposed (2026-11-06), implementation submitted for review with this amendment.
- Issue: [#62](https://github.com/fuseone-io/fusescope-collector-server/issues/62); Android delivery: [FuseScope #337](https://github.com/fuseone-io/fusescope/issues/337), [#189](https://github.com/fuseone-io/fusescope/issues/189).
- Amends: [ADR 0004](0004-security-baseline.md), transport floor for the RUM listener only.
- Deciders: FuseOne maintainers.

## Context

The Android SDK retains API 23+ support. Stock Android 6–9 cannot negotiate the collector's TLS-1.3-only RUM listener. Bundling another native TLS implementation in every consuming application would add binary size, ABI packaging and maintenance obligations. The user chose compatibility at collector ingress instead of that SDK dependency.

## Decision

The `rumreceiver` listener accepts TLS 1.2 and TLS 1.3 by default. A capable client negotiates TLS 1.3. Operators can retain a TLS-1.3-only listener by setting `tls.min_version: "1.3"`; such deployments do not provide direct compatibility with stock API 23–28.

TLS 1.0, TLS 1.1 and plaintext remain refused. Certificate/key files, certificate validation, authentication, origin/package checks, tenant isolation, ingestion limits and fail-closed policy behavior retain their contracts. Do not enable obsolete ciphers, static RSA key exchange or certificate-verification bypasses to obtain compatibility; use the pinned Go runtime's supported default cipher suites.

This exception applies only to public RUM client ingress. OpAMP, ClickHouse, agent/gateway transports, exporters and unrelated receivers retain their existing transport policy. No remote policy message can change the local TLS configuration.

A terminating proxy must also accept TLS 1.2 and TLS 1.3 on the RUM public endpoint for old clients. Keep certificate verification and preferably mTLS on the proxy-to-collector hop; that hop may remain TLS 1.3. Do not lower a shared non-RUM public listener implicitly: split endpoints or configure an explicitly reviewed RUM ingress. The deployment's public handshake, not just the collector binary, defines compatibility.

## Consequences

The SDK gains no TLS provider dependency or global provider changes. A collector update alone does not change existing explicit minimums or proxy policies: chart configuration, deployment and actual Android acceptance must be reconciled before declaring the compatibility gate complete.

Receiver tests cover traces and logs with a TLS-1.2-only client, modern TLS 1.3 negotiation, older-version rejection and the strict 1.3 override. Real API 23/28 and modern Android validation, including consent cycles, remains a release gate in #337. No claim of support for Android-native crash APIs unavailable on older Android versions is implied.
