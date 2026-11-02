# 0008 · Remote configuration over OpAMP

- Status: proposed (accepted once the reload spike of §Open settles point 6)
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
     returns the merged result; on a change it asks the collector to reload in process, under
     the lifecycle of point 6.
2. **Allow-list, enforced by comparison with the base:**
   - **Log pipelines.** `signozlogspipeline/pipeline_*` blocks come from the pushed
     configuration, and blocks it no longer has are removed. They may be added, removed or
     moved only within `service.pipelines.logs`: that list is taken from the push only if it
     equals the base list once `signozlogspipeline/pipeline_*` entries are left out of both,
     in order. No other pipeline may list them.
   - **Span mapping and LLM pricing.** Only the `signozspanmapper` and `signozllmpricing`
     blocks may change, and only if the base declares them; they keep exactly the position
     the base gives them in every pipeline. A push without one keeps the base's block.
   - **Everything else** that differs from the base (a receiver, exporter, extension,
     connector, telemetry setting, any other processor's block, any other change to a
     pipeline's processors, receivers or exporters, a new pipeline) rejects the whole push.
   - **Failure report.** A rejected push is reported `FAILED` with the configuration paths
     that differed (for example `exporters.otlp/engine.endpoint`), never their values, which
     may hold endpoints or other configuration the backend should not see echoed.
3. **Transport.** TLS by default, with a client certificate when one is configured. Plaintext
   only with an explicit `tls.insecure: true`, logged as a warning at start, for the
   in-cluster port the backend serves today, as ADR 0007 allows for ClickHouse. A backend
   issue asks for TLS on its OpAMP server ([fuseone-io/fusescope#261](https://github.com/fuseone-io/fusescope/issues/261)).
4. **Restart and outage.** Only the last push that was effectively applied (point 6) is cached
   on disk, written atomically (a temporary file renamed over the old one). At start it is
   never applied as it is: it is compared and merged again with the current base under point 2,
   so a newer local configuration (after an upgrade, say) is never reverted by an old push. A
   cached push the current base rejects is discarded with a warning, and the base runs until
   the backend pushes again. The instance UID is stored next to it, so the backend sees one
   agent across restarts.
5. **Library.** `github.com/open-telemetry/opamp-go` (Apache-2.0, OpenTelemetry) is the OpAMP
   client, the same library the backend's server uses.
6. **Reload lifecycle (open, see below).** A push is reported `APPLIED`, cached and reported as
   the effective configuration only once the collector runs it; until then it is pending.

## Open: reload lifecycle

The upstream collector does not reload atomically. On a configuration change that is not
receivers-only, `otelcol` shuts the running service down and then builds the new graph; if
that fails, `Run` returns the error and the process exits (`go.opentelemetry.io/collector/otelcol`
v0.164.0, `Collector.reloadConfiguration`). A confmap provider is not told when, or whether, the
reload finished. As it stands, the provider could neither keep the old configuration running
after a bad push nor report `APPLIED` only after a real application.

A spike, before the provider, has to settle:

- **Preflight:** validate the merged configuration before triggering the reload, with the
  same factories and validation the collector uses (unmarshal, `Validate`, and building the
  allow-listed processors), so a push that would fail is rejected while the old graph runs.
- **Confirmation:** how the provider learns that the new graph started, for example an
  extension in the same binary that sees the pipelines come up and signals the provider.
- **Rollback:** what happens when the new graph fails to start despite the preflight; at
  least, the next start must not reuse that push, since only applied pushes are cached.

This ADR stays `proposed` until the spike records its answers here.

## Consequences

- One binary; an accepted configuration is validated by the collector's own reload, and a
  rejected one never runs.
- A compromised or buggy backend can at most change log pipelines, span mapping and LLM
  pricing; it cannot redirect telemetry or change what is received.
- Until the backend adds TLS, the OpAMP link inside the cluster is plaintext and
  unauthenticated, by the operator's explicit choice.
- Tail sampling and the metrics filter, which the backend still has code for but never
  pushes, are out of the allow-list until it does.
