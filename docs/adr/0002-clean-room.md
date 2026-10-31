# 0002 · Clean-room from the AGPL collector

- Status: accepted
- Date: 2026-10-31

## Context

`fusescope-otel-collector` is AGPL-3.0 and derived from SigNoz. This repository is Apache-2.0
and must stay so, yet it has to match that collector's ClickHouse schema and processing.

## Decision

Behavior is specified from the FuseScope backend (MIT), observed data, the OpenTelemetry
specification and fusescope-engine's mapping, and written down in `docs/spec/` with its source
before it is implemented. The existing collector may be run as a black box to observe outputs.
Its source is never opened, copied, translated or summarised, including from git history.
Component type names the backend already sends (for example `signozlogspipeline`) are kept as
interface identifiers.

## Consequences

Slower than porting, and every rule needs a cited source. Commits touching specified behavior
say "clean-room" in the message.
