<p align="center">
  <img alt="FuseScope" src=".github/brand/fusescope-logo-universal.svg" width="440">
</p>

<h1 align="center">FuseScope Collector</h1>

<p align="center">
  <b>FuseScope distribution of the OpenTelemetry Collector, built from upstream components.</b>
</p>

<p align="center">
  <a href="DESIGN.md">Design</a> ·
  <a href="docs/adr">ADRs</a> ·
  <a href="LICENSE"><img alt="License: Apache-2.0" src="https://img.shields.io/badge/license-Apache--2.0-0f5fb8"></a>
  <a href="https://opentelemetry.io/docs/collector/"><img alt="OpenTelemetry Collector" src="https://img.shields.io/badge/OpenTelemetry-Collector-950fb8"></a>
  <a href="https://github.com/fuseone-io/fusescope-collector-server/issues"><img alt="GitHub issues" src="https://img.shields.io/github/issues/fuseone-io/fusescope-collector-server?color=950fb8"></a>
</p>

FuseScope distribution of the [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/),
built from upstream components with the OpenTelemetry Collector Builder. It writes to
ClickHouse in the SigNoz-compatible schema, to [fusescope-engine](https://github.com/fuseone-io/fusescope-engine),
or to both during a migration.

Read [DESIGN.md](DESIGN.md) first. Decisions are in [docs/adr](docs/adr).

## Status

Phase 0 done: the distribution builds from upstream components and the example configurations
validate. Next is parity with the existing collector on ClickHouse, step by step in shadow mode; see
DESIGN.md §9.

## Build

Requires Go 1.26.

```sh
make build      # installs ocb and builds _build/fusescope-collector
make validate   # validates every file in config/
make licenses   # checks dependency licenses against the allow-list
make vuln       # govulncheck
docker build -t fusescope-collector .
```

## Configurations

| File | Mode |
|---|---|
| [`config/gateway-engine.yaml`](config/gateway-engine.yaml) | gateway: authenticated OTLP in, fusescope-engine out |
| [`config/agent-kubernetes.yaml`](config/agent-kubernetes.yaml) | agent: node, pod and log collection to a gateway |

Both expect TLS material and tokens as mounted files; see the `${env:…}` references in each.

## Upgrading upstream

See [docs/upgrading.md](docs/upgrading.md).

## License

Apache-2.0. See [NOTICE](NOTICE) and [ADR 0005](docs/adr/0005-license-policy.md). Nothing here
is derived from `fusescope-otel-collector` (AGPL-3.0); see [ADR 0002](docs/adr/0002-clean-room.md).
