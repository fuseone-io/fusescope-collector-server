# FuseScope Collector · design

> Status: proposal, 2026-10-31. Decisions are recorded in [docs/adr](docs/adr) and are
> binding until superseded by a new ADR.

## 1. What it is

The FuseScope Collector is a distribution of the upstream
[OpenTelemetry Collector](https://github.com/open-telemetry/opentelemetry-collector) (Apache-2.0),
built with the OpenTelemetry Collector Builder (`ocb`) from [`builder-config.yaml`](builder-config.yaml).
Upstream components are Go module dependencies at pinned versions; FuseScope adds a small set
of its own components under [`components/`](components/).

It replaces `fusescope-otel-collector` (AGPL-3.0, derived from SigNoz). Nothing here is derived
from that code ([ADR 0002](docs/adr/0002-clean-room.md)).

It writes to two storage backends ([ADR 0003](docs/adr/0003-clickhouse-and-engine.md)):

- **ClickHouse**, in the schema SigNoz and today's FuseScope query, so an existing SigNoz or
  FuseScope installation can switch collectors without migrating data;
- **fusescope-engine**, over standard OTLP (later OTel-Arrow), where the engine owns the schema;
- **both at once** (dual-write) during a migration from ClickHouse to the engine.

## 2. Deployment modes

| Mode | Runs as | Receives from | Sends to |
|---|---|---|---|
| Agent | DaemonSet / host service | local SDKs on `localhost`, node files, kubelet, host | a gateway, over mTLS |
| Gateway | Deployment behind a load balancer | agents, SDKs, the RUM gateway in the FuseScope backend | ClickHouse, the engine, or both |

One binary, two configurations ([`config/`](config/)). The FuseScope backend's RUM gateway
stays in the backend (FuseScope ADR 0003) and forwards scrubbed OTLP to a gateway; the
collector never exposes OTLP to browsers or mobile apps.

## 3. Components

### 3.1 Upstream (pinned in `builder-config.yaml`)

Receivers `otlp`, `otelarrow`, `file_log`, `host_metrics`, `prometheus`, `kubelet_stats`,
`k8s_cluster`; processors `memory_limiter`, `batch`, `k8s_attributes`, `resource_detection`,
`resource`, `attributes`, `filter`, `transform`, `tail_sampling`; exporters `otlp_grpc`,
`otlp_http`, `otelarrow`, `debug`; connector `span_metrics`; extensions `healthcheckv2`,
`bearertokenauth`, `oidc`, `file_storage`, `pprof`, `zpages`, `memory_limiter`; config providers
`env`, `file`, `yaml`. The `http(s)` config providers are left out on purpose: remote
configuration goes through OpAMP with an allow-list (§5), never an arbitrary URL.

### 3.2 FuseScope (planned, in `components/`)

| Component | Kind | Purpose |
|---|---|---|
| `fusescope_clickhouse` | exporter | Traces, logs and metrics into the SigNoz-compatible ClickHouse schema |
| `fusescope-schema` | command | Creates and migrates that ClickHouse schema for new installations |
| `signozlogspipeline` | processor | Log pipelines configured in the FuseScope UI |
| `signozspanmapper`, `signozllmpricing`, `signoz_tail_sampling` | processors | Other UI-managed processing the backend pushes today |
| APM span metrics | connector | Service RED metrics under the names the Services page queries |
| `fusescope_opamp` | extension | Applies backend-managed config fragments within an allow-list |

The processors keep the **type names** the FuseScope backend already sends over OpAMP, so the
backend works with either collector unchanged. Type names are interface identifiers; their
behavior is specified clean-room (§6).

Log pipelines are built on upstream `pkg/stanza` (Apache-2.0) as a library module that the
FuseScope backend can import for its pipeline preview, so preview and production run the same
code (FuseScope ADR 0001, option 2).

## 4. Performance

- **Memory first:** `memory_limiter` is the first processor of every pipeline; containers set
  `GOMEMLIMIT` to ~90% of the limit. Refusals propagate to senders as retryable errors instead
  of out-of-memory restarts.
- **Batch in the exporter:** batching lives in each exporter's `sending_queue.batch`, sized to
  the destination (engine ingest batches, ClickHouse inserts), not in a global `batch` processor.
- **Wire efficiency:** `zstd` on OTLP to the engine; OTel-Arrow (`otelarrow`) once the engine's
  Arrow ingest lands (engine ADR 0006).
- **ClickHouse exporter:** native protocol through `ch-go`, columnar blocks built straight from
  `pdata` without per-row structs or maps, one `INSERT` per table per batch, LZ4 on the wire,
  bounded concurrency per table, and no `async_insert` dependency.
- **Backpressure, not loss:** persistent queues (`file_storage`) at the gateway; when the
  engine answers `RESOURCE_EXHAUSTED` or ClickHouse is slow, data waits in the queue up to its
  byte bound and then refuses upstream, which agents and SDKs retry.
- **Budgets measured in CI:** a load benchmark (`telemetrygen`) per release records spans/s and
  bytes per span per core; a regression beyond 10% fails the release.

## 5. Security

The engine's ingest decisions (engine `DESIGN.md` §10.3) apply end to end. The collector side
([ADR 0004](docs/adr/0004-security-baseline.md)):

- **Transport:** TLS 1.3 minimum on every listener and exporter that leaves the host; mTLS
  between agents and gateways and from gateways to engine ingesters.
- **Authentication:** gateways require a bearer token on OTLP ingest; engine-bound exporters
  present the tenant's `fst_` ingest token. Tokens are read from mounted files
  (`bearertokenauth.filename`), never from inline config or OpAMP, and are rotated by
  replacing the file (the engine's rotation overlap keeps both valid meanwhile).
- **Tenancy:** one engine token per tenant; a shared gateway serving several tenants routes by
  the authenticated identity, never by an attribute in the payload.
- **Remote configuration (OpAMP):** mTLS to the FuseScope backend; accepted fragments are
  limited to an allow-list of processor types (§3.2). Receivers, exporters, extensions,
  endpoints and credentials can never be changed remotely, so a compromised backend cannot
  redirect telemetry.
- **Limits:** gRPC message size, concurrent streams, and request timeouts on receivers; the
  engine enforces attribute and cardinality limits per tenant.
- **Surface:** health on `localhost` by default with the config endpoint off; `pprof` and
  `zpages` compiled in but disabled unless configured on `localhost`.
- **Runtime:** distroless, non-root, read-only root filesystem; only the queue and checkpoint
  directories are writable.
- **Supply chain:** pinned module versions, license allow-list and `govulncheck` in CI, SBOM
  and signed images on release.

## 6. Clean-room

Behavior that must match the AGPL collector (ClickHouse schema and row layout, processor
semantics, metric names) is specified from:

1. the FuseScope backend (MIT): the configs it pushes over OpAMP and the queries it runs;
2. observed data: `SHOW CREATE TABLE` from a running installation, rows written for a known
   OTLP input, and the outputs of the existing collector run as a black box;
3. the OpenTelemetry specification and semantic conventions;
4. fusescope-engine's `fs-schema` mapping (`MAPPING.md`).

Every rule is written down with its source in `docs/spec/` before it is implemented. Nobody
working here opens, copies, translates or summarises `fusescope-otel-collector` or SigNoz
collector source, including from git history.

## 7. Compatibility testing

A parity suite sends the same OTLP corpus through the existing collector (as a black box) and
through this one into two ClickHouse instances, then compares rows table by table. It gates
every change to `fusescope_clickhouse` and to the processors in §3.2.

## 8. Upgrading upstream

Upstream releases every two weeks. An upgrade bumps every module in `builder-config.yaml`
together (core `v0.x`, contrib `v0.x`, stable modules `v1.x` from the same release), runs CI
and the parity suite, and reads the upstream changelog for renamed component types
(`otlp` → `otlp_grpc`, `filelog` → `file_log`, …). See [docs/upgrading.md](docs/upgrading.md).

## 9. Phases

| Phase | Deliverable | Exit criterion |
|---|---|---|
| 0 · Skeleton | distribution from upstream components, CI, configs, ADRs | binary builds, configs validate, licenses and vulnerabilities checked |
| 1 · Engine path | gateway → engine with mTLS and tokens, load benchmark | engine phase 1 dual-write runs on this collector |
| 2 · ClickHouse | `fusescope_clickhouse`, `fusescope-schema`, parity suite | parity suite green on traces, logs and metrics |
| 3 · Managed processing | log pipelines, span mapper, LLM pricing, APM metrics, `fusescope_opamp` | FuseScope UI features work unchanged on this collector |
| 4 · Replacement | migration guide, Helm chart, deprecation of the AGPL collector | an existing installation switches collectors without data changes |
