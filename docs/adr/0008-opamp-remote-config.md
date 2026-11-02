# 0008 · Remote configuration over OpAMP

- Status: accepted
- Date: 2026-11-02
- Amends: [ADR 0004](0004-security-baseline.md) (transport to the backend), `DESIGN.md` §3.2, §5.
- Issue: [#6](https://github.com/fuseone-io/fusescope-collector-server/issues/6)

## Context

The FuseScope backend manages log pipelines, span attribute mapping and LLM pricing through
OpAMP ([`docs/spec/opamp.md`](../spec/opamp.md)). It does not send fragments: it takes the
configuration the collector reports as effective, edits it and sends back a whole
`collector.yaml`. ADR 0004 allows only processor types on an allow-list to change remotely,
never receivers, exporters, extensions, endpoints or credentials; with a whole configuration
coming back, the collector itself has to enforce that.

The backend's OpAMP server listens on port 4320 without TLS or authentication, as a port
reachable only inside the cluster. ADR 0004 asks for mTLS to the backend.

The upstream collector has no component that applies remote configuration within limits. The
contrib OpAMP supervisor restarts the collector with whatever the server sends.

## Decision

1. **A confmap provider, `fusescope_opamp`,** is the collector's configuration source. Run as
   `--config fusescope_opamp:/etc/fusescope-collector/config.yaml`, it:
   - reads the local file, the base configuration, and removes its top-level
     `fusescope_opamp:` section, which holds the provider's own settings (endpoint, TLS,
     storage);
   - speaks OpAMP to the backend as an agent, reporting the configuration it returns as the
     effective one;
   - accepts from a pushed configuration only what the allow-list (point 2) permits, and
     returns the merged result; on a change it asks the collector to reload in process.
2. **Allow-list, enforced by comparison with the base:**
   - processor blocks of type `signozlogspipeline`, `signozspanmapper` and `signozllmpricing`
     come from the pushed configuration; allow-listed blocks it no longer has are removed;
   - a pipeline's processor list comes from the pushed configuration only if it equals the
     base list once allow-listed processors are left out of both, in order;
   - anything else that differs from the base (a receiver, exporter, extension, connector,
     telemetry setting, another processor's block, a pipeline's receivers or exporters, a
     new pipeline) rejects the whole pushed configuration. The collector keeps running what
     it ran, and reports `FAILED` with what differed.
3. **Transport.** TLS by default, with a client certificate when one is configured. Plaintext
   only with an explicit `tls.insecure: true`, logged as a warning at start, for the
   in-cluster port the backend serves today, as ADR 0007 allows for ClickHouse. A backend
   issue asks for TLS on its OpAMP server ([fuseone-io/fusescope#261](https://github.com/fuseone-io/fusescope/issues/261)).
4. **Restart and outage.** The last accepted pushed configuration is cached on disk, and used
   at start until the backend answers, so a backend outage does not revert the managed
   processing. The instance UID is stored there too, so the backend sees one agent across
   restarts.
5. **Library.** `github.com/open-telemetry/opamp-go` (Apache-2.0, OpenTelemetry) is the OpAMP
   client, the same library the backend's server uses.

## Consequences

- One binary; an accepted configuration is validated by the collector's own reload, and a
  rejected one never runs.
- A compromised or buggy backend can at most change log pipelines, span mapping and LLM
  pricing; it cannot redirect telemetry or change what is received.
- Until the backend adds TLS, the OpAMP link inside the cluster is plaintext and
  unauthenticated, by the operator's explicit choice.
- Tail sampling and the metrics filter, which the backend still has code for but never
  pushes, are out of the allow-list until it does.
