# OpAMP · contract with the FuseScope backend

What the collector must do so that the FuseScope backend manages it unchanged (phase 5,
[ADR 0008](../adr/0008-opamp-remote-config.md)). Clean-room (ADR 0002): every rule comes from
the FuseScope backend (`fuseone-io/fusescope`, read at `fe371d4`; paths relative to it) or the
OpAMP specification.

## 1. Server

- `github.com/open-telemetry/opamp-go` v0.22.0 server, WebSocket, on `0.0.0.0:4320`, at the
  library's default path `/v1/opamp` (`go.mod:39`, `pkg/query-service/app/opamp/opamp_server.go:51-70`,
  `pkg/query-service/constants/constants.go:13`).
- No TLS, no authentication: every connection is accepted (`opamp_server.go:56-66`). The port
  is meant to be reachable only by collectors (FuseScope `docs/adr/0010-single-ingress.md:25,39`).
- Server capabilities: `AcceptsEffectiveConfig`, `OffersRemoteConfig`, `AcceptsStatus`
  (`opamp_server.go:32-34`).

## 2. Agent identity

- The agent is its `InstanceUid`, a UUID; an empty one is rejected
  (`opamp_server.go:107`, `model/agents.go:77-79`).
- The organization is the backend's single one; with none or several it answers
  `ErrorResponse{Unavailable, RetryInfo 5s}` (`opamp_server.go:112-136`).
- The only description attribute read is the non-identifying `capabilities.lbexporter`
  (`model/agent.go:40-42,93-114`), and nothing uses it. The collector does not send it.

## 3. First message and pushes

- The backend computes a configuration only when the agent description changes, so on the
  first message, and only if that message carries `EffectiveConfig.ConfigMap`; otherwise it
  asks for `ReportFullState` (`model/agent.go:126-138,327-347`). The first message therefore
  carries the description and the effective configuration together.
- The effective configuration is one file; the backend takes "the" config of the map
  (`model/agent.go:269-281`). The collector reports one entry, `collector.yaml`, with the
  configuration it runs, as YAML.
- A push is a `RemoteConfig` with one file under `collector.yaml`, content type
  `application/x-yaml`: the reported configuration with the backend's edits
  (`model/constants.go:3-4`, `model/agent.go:367-382`, `agentConf/manager.go:102-161`).
- `ConfigHash` is not a content hash: it is `<feature>:<version>` per feature, joined by
  commas, and SHA-256 only when no feature is registered (`manager.go:129-158`). The collector
  echoes it byte for byte in `RemoteConfigStatus.last_remote_config_hash`.
- `$` in log pipeline values is escaped as `$$` (`logparsingpipeline/collector_config.go:214-233`);
  the collector's confmap resolves it back.

## 4. Status

- `RemoteConfigStatus` with the echoed hash, `APPLIED` or `FAILED`, and an error message on
  failure; the backend shows `deployed` or `failed` with `<agentId>: <message>` in the log
  pipelines history (`manager.go:164-182`, `model/coordinator.go:20-47`).
- The configuration reported as effective after an `APPLIED` is the merged one, so the
  backend's next edit starts from what runs.

## 5. What the backend edits

Applied in this order: log pipelines, span attribute mapping, LLM pricing
(`pkg/query-service/app/server.go:91-99`).

- **Log pipelines.** Processor blocks `signozlogspipeline/pipeline_<alias>` (alias with
  characters outside `[a-zA-Z0-9-]` as `-`, `-<n>` on collision), inserted into
  `service.pipelines.logs` only: after `memory_limiter*`, before the processors that precede
  the first `batch*`; with a `signozlogsnormalizer*` in the pipeline, just before it, or after
  it when its config has `opamp.position: before`. Stale `signozlogspipeline/pipeline_*`
  entries are removed (`collector_config.go:40-189`, `pipelineBuilder.go:26-86`). A
  configuration without `service.pipelines.logs` fails the backend's edit.
- **Span attribute mapping.** One `signozspanmapper` block; not inserted into any pipeline
  (`pkg/types/spantypes/spanmapperprocessor.go:14,73-76`). The base configuration wires it into
  the traces pipeline.
- **LLM pricing.** One `signozllmpricing` block; not inserted into any pipeline
  (`pkg/types/llmpricingruletypes/processorconfig.go:11,138-139`). The base configuration
  wires it.
- **Not pushed.** `signoz_tail_sampling` and the metrics `filter` have code but no caller in
  the backend (`agentConf/manager.go:241-363`); `signozlogsnormalizer` is only read
  (`collector_config.go:32-34,164-189`).

The processors' own behaviour is specified per processor in `docs/spec/`.

## 6. Collector rules (ADR 0008)

- The allow-list and the comparison with the base configuration of ADR 0008 §2 decide what
  of a push is applied; a push is applied whole or not at all.
- Since the base configuration wires `signozspanmapper` and `signozllmpricing`, it declares
  them too; a push replaces their blocks.

## 7. Backend behaviour the collector does not depend on

Recorded so it is not mistaken for collector bugs (FuseScope backend issue to be filed):

- the settings broadcast stops at the first agent whose configuration is unchanged
  (`model/agents.go:121-166`);
- an agent that reconnects with a non-`UNSET` status gets no push until the next settings
  change (`model/agent.go:187-189`);
- the span mapping and LLM pricing features have no element type, so their part of the hash
  is `:<version>` and their status rows are likely never updated
  (`pkg/types/opamptypes/agent.go:126-138`, `manager.go:112-133`).
