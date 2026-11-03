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

- A push is applied whole or not at all, under ADR 0008 §2: `signozlogspipeline/pipeline_*`
  may be added, removed or moved only within `service.pipelines.logs`; `signozspanmapper` and
  `signozllmpricing` may change only their blocks, in the position the base configuration
  gives them; anything else rejects the push.
- The base configuration wires and declares `signozspanmapper` and `signozllmpricing`, since
  the backend never places them (§5).
- `FAILED` lists the configuration paths that differed, never values.
- A pushed allow-listed block may not hold a confmap reference (`${...}`, not escaped as
  `$${`) that the base does not hold at the same path: the collector would resolve it, so a
  push could read a local credential file or environment variable into an exported value
  (ADR 0004). A reference the base already holds there, echoed back by the backend, is kept.
  The backend escapes `$` as `$$` in log pipeline values (§3) and writes no reference
  elsewhere.
- For the same reason a log pipeline expression may not call `env()`: the processor refuses
  the configuration, so the push fails preflight and is reported `FAILED` naming the operator,
  never the expression ([signozlogspipeline](signozlogspipeline.md) §4.1).
- A push is preflighted; `APPLIED` is reported, and the push cached, only once the new graph
  is confirmed running; a push that stops the collector is reported `FAILED` at the next start
  (ADR 0008, reload lifecycle), and that failure is kept on disk until the backend answers, so
  restarts while it is unreachable do not lose it.

## 7. Backend behaviour the collector does not depend on

Recorded so it is not mistaken for collector bugs ([fuseone-io/fusescope#261](https://github.com/fuseone-io/fusescope/issues/261)):

- the settings broadcast stops at the first agent whose configuration is unchanged
  (`model/agents.go:121-166`);
- an agent that reconnects with a non-`UNSET` status gets no push until the next settings
  change (`model/agent.go:187-189`);
- the span mapping and LLM pricing features have no element type, so their part of the hash
  is `:<version>` and their status rows are likely never updated
  (`pkg/types/opamptypes/agent.go:126-138`, `manager.go:112-133`).

## 8. Running it

The collector runs with `--config fusescopeopamp:<file>`. The file is the base configuration
plus a top-level section that never reaches the collector:

```yaml
fusescope_opamp:
  endpoint: wss://backend:4320/v1/opamp   # ws:// only with tls.insecure: true
  tls: {ca_file: /etc/fusescope/ca.pem}   # configtls client settings
  storage: /var/lib/fusescope-collector/opamp
  role: writer                            # or ingress (rum.md §11.1); default writer
  auth: {token_file: /etc/fusescope/ingest-token}   # ingress only
extensions:
  fusescope_opamp: {}                     # confirms pushes; required
service:
  extensions: [fusescope_opamp]
  pipelines:
    logs: {receivers: [otlp], processors: [memory_limiter, batch], exporters: [fusescope_clickhouse]}
    traces: {receivers: [otlp], processors: [memory_limiter, signozspanmapper, batch, signozllmpricing], exporters: [fusescope_clickhouse]}
```

`role` is reported as the non-identifying description attribute `fusescope.collector.role`
(FuseScope `docs/adr/0009-fusescope-collector.md` line 79), next to `host.name` and `os.type`. With `ingress` the agent
also declares the custom capability `io.fuseone.fusescope.ingest.v1` and keeps the ingest policy
and the report sequence under `<storage>/ingest/` ([rum.md](rum.md) §§10.4, 11). Any other
value is refused at start. The instance UID persists across restarts (ADR 0008 point 4) with
one exception: an `ingress` collector without a valid report sequence state for its UID takes
a new UID before connecting (rum.md §10.4), so the backend sees a new agent.

The backend sends the ingest policy only to a connection that presented, at the handshake, one
of the tokens in its `rum.ingest_policy.token_file`, as `Authorization: Bearer` (FuseScope
`pkg/query-service/app/opamp/opamp_server.go`, fuseone-io/fusescope#310). An `ingress`
collector reads its token from `auth.token_file`: one token of 32 to 256 characters, the
backend's bounds too, read once when the client starts, never logged and never in the effective
configuration. The client follows no redirect, with or without a token: one would resend the
token and could move the connection to another host or from `wss` to `ws`. A connection without it still takes remote configuration. With `tls.insecure`
the token travels unencrypted, as the plaintext warning says.

The section never reaches the collector, so the provider resolves `${env:NAME}` and `${NAME}`
in it itself (`$$` for a literal `$`); other substitution schemes are not available there. A change
of these settings takes a restart: the reload after it fails with an error saying so.

The provider refuses a base configuration without the `fusescope_opamp` extension. The OpAMP
client starts, and the start is recorded on disk, only when that extension starts, so `validate`
neither connects to the backend nor changes the persisted state. A change of the local file is a
new revision too: once its graph is `Ready`, the new effective configuration is reported, and a
push being preflighted meanwhile is merged again on the new base.

Limits, each rejecting a push with `FAILED`: 4 MiB of pushed YAML, 256 allow-listed processors,
10 s for each processor's preflight start. A processor type this build lacks fails the
preflight, so pushes with it are rejected until the build has it.

