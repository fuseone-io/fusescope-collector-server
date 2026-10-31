# 0001 · Upstream OpenTelemetry Collector as a library

- Status: accepted
- Date: 2026-10-31

## Context

The collector must keep pace with upstream (a release every two weeks, frequent component
renames) without carrying a fork.

## Decision

Build the distribution with the OpenTelemetry Collector Builder (`ocb`) from
`builder-config.yaml`. Upstream receivers, processors, exporters, extensions and config
providers are Go module dependencies at pinned versions. FuseScope components are separate Go
modules under `components/`, referenced with `path:`. The generated `main` is not committed.

## Consequences

An upgrade is a version bump in one file plus CI. FuseScope code depends only on public
collector APIs. Patching an upstream component means contributing upstream or wrapping it in a
FuseScope component, never vendoring a modified copy.

## Rejected

Forking `opentelemetry-collector-contrib`: every upgrade becomes a merge.
