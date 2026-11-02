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
| Gateway | Deployment behind a load balancer | agents, SDKs, browsers and mobile apps (RUM), CI artifact uploads | ClickHouse, the engine, both, or the existing collector during migration |

One binary, two configurations ([`config/`](config/)). The gateway is the **only** ingest
surface exposed to the internet ([ADR 0006](docs/adr/0006-single-ingress.md), FuseScope ADR
0010). It carries OTLP, RUM, profiles and artifact uploads, and the backend serves no ingest
route. Until this collector writes storage itself, the gateway is an ingress tier in front of
the existing collector and forwards OTLP to it (FuseScope ADR 0009, phase 1).

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
| `fusescope_spanmetrics` | connector | Service RED metrics under the names the Services page queries ([apm-metrics](docs/spec/apm-metrics.md)) |
| `fusescope_opamp` | extension | Applies backend-managed config fragments within an allow-list, and receives the ingest policy (§5) |
| `ingestauth` | extension | Validates ingestion keys locally against the ingest policy, stamps `fusescope.ingest.key_id`, per-key rate limits ([ADR 0006](docs/adr/0006-single-ingress.md)) |
| `rumreceiver` | receiver | RUM ingest (`/rum/v1/*`): public keys, per-application CORS, PII scrubbing, moved from the backend gateway |
| `artifactreceiver` | receiver | Source maps, R8 mappings and debug files, streamed to the backend's internal intake |
| profiling (`pyroscopereceiver`, `clickhouseprofilesexporter`) | receiver, exporter | Continuous profiling (FuseScope ADR 0008) |

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
- **Idempotent retries:** every insert carries an `insert_deduplication_token` made of a
  random identity of the export request, the table and, for a metrics
  batch inserted in chunks, the chunk index (chunks are cut deterministically). Deduplication is
  on for dependent materialized views, so a retry after a timeout or a partial failure does not
  duplicate rows (ClickHouse `Replicated*` tables). Every attempt also reuses the request's
  ingest time and a random-ID stream seeded by its identity, so a retry rebuilds the same rows
  and each sharded row goes to the same shard. Equal telemetry sent twice is two requests and is
  written twice; the parity suite checks that. A request restored from a persistent queue after
  a restart gets a new identity. The exporter applies `timeout` and `retry_on_failure` itself,
  not the exporter helper, so all attempts of a request run in one call and share its identity
  with nothing kept between requests. Tables sharded by `rand()` (tag attributes, span keys, metric
  metadata) get no token, as a retry routes their rows to other shards; they are Replacing or
  Aggregating merge trees, so the repeats merge away.
- **Backpressure, not loss:** persistent queues (`file_storage`) at the gateway; when the
  engine answers `RESOURCE_EXHAUSTED` or ClickHouse is slow, data waits in the queue up to its
  byte bound and then refuses upstream, which agents and SDKs retry.
- **Budgets measured in CI:** a load benchmark (`telemetrygen`) per release records spans/s and
  bytes per span per core; a regression beyond 10% fails the release.

## 5. Security

The engine's ingest decisions (engine `DESIGN.md` §10.3) apply end to end. The collector side
([ADR 0004](docs/adr/0004-security-baseline.md)):

- **Transport:** TLS 1.3 minimum on every listener and exporter that leaves the host; mTLS
  between agents and gateways and from gateways to engine ingesters. The one exception is the
  ClickHouse exporter: `tls.insecure: true` allows plaintext, with a warning, for clusters
  whose ClickHouse has no TLS ([ADR 0007](docs/adr/0007-clickhouse-plaintext-for-migration.md)).
- **Ingest authentication:** `ingestauth` validates the ingestion key every client presents, for
  all signals, against SHA-256 verifiers in the ingest policy. Keys are managed in the backend;
  the policy arrives over OpAMP and is cached on disk ([ADR 0006](docs/adr/0006-single-ingress.md)).
- **Outbound credentials:** the collector's own credentials (ClickHouse password, the tenant's
  `fst_` engine token, TLS keys, agent-to-gateway bearer tokens) are read from mounted files,
  never from inline config or OpAMP, and are rotated by replacing the file:
  `fusescope_clickhouse` reconnects when its password file changes, and the engine's rotation
  overlap keeps both tokens valid meanwhile. The only exception is the artifact-intake
  credential (ADR 0006).
- **Tenancy:** one engine token per tenant; a shared gateway serving several tenants routes by
  the authenticated identity, never by an attribute in the payload.
- **Remote configuration (OpAMP):** mTLS to the FuseScope backend. Accepted config fragments are
  limited to an allow-list of processor types (§3.2). The ingest policy custom message can only
  restrict what is accepted. Receivers, exporters, extensions, endpoints and outbound
  credentials can never be changed remotely, so a compromised backend cannot redirect telemetry.
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

Parity with the existing collector comes first: FuseScope installations run on ClickHouse
today. Phases 1–5 are each gated by the parity suite (§7) in CI: the same corpus through both
collectors must produce identical rows. Real traffic comes once, in **shadow mode** before the
cutover: the new collector receives a copy of production traffic and writes to a separate
ClickHouse, never to production tables, and the parity suite compares both table by table.

| Phase | Deliverable | Exit criterion |
|---|---|---|
| 0 · Skeleton | distribution from upstream components, CI, configs, ADRs | done: binary builds, configs validate, licenses and vulnerabilities checked |
| I · Ingress tier | gateway in front of the existing collector, forwarding OTLP; `ingestauth`, `rumreceiver`, `artifactreceiver`, limits ([ADR 0006](docs/adr/0006-single-ingress.md), [#13](https://github.com/fuseone-io/fusescope-collector-server/issues/13)) | independent of phases 1–5; the demo ingests only through this collector |
| 1 · Traces in ClickHouse | `fusescope_clickhouse` traces into the existing schema; parity suite | parity green on traces |
| 2 · Logs in ClickHouse | logs in the same exporter | parity green on logs |
| 3 · Metrics in ClickHouse | metrics in the same exporter | parity green on metrics |
| 4 · APM metrics | service RED metrics under the names the Services page queries | parity green; Services, service map and APM alerts match |
| 5 · Managed processing | `fusescope_opamp`; log pipelines, span mapper, LLM pricing, tail sampling | pipelines and mappings configured in the UI apply identically in parity |
| 6 · Shadow and cutover | shadow deployment on production traffic, then the new collector takes it signal by signal; old one retired | parity green over a week of shadow traffic; 30 days in production without the old collector |
| 7 · Engine | gateway → engine with mTLS and tokens, dual-write, load benchmark | engine dual-write runs on this collector |
| 8 · New installations | `fusescope-schema`, migration guide, Helm chart | a fresh installation and a SigNoz migration both run without the AGPL collector |

`fusescope-schema` waits until phase 8: existing clusters already have the schema, which the
exporter writes into as it is.
