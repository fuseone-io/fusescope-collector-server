# 0003 · ClickHouse and fusescope-engine, separately or both

- Status: accepted
- Date: 2026-10-31

## Context

SigNoz and current FuseScope installations store telemetry in ClickHouse under a schema owned
by the AGPL migrator. fusescope-engine replaces ClickHouse and owns its schema (engine ADR
0006). Users moving from SigNoz to FuseScope must keep their data and queries working.

## Decision

Support three outputs: ClickHouse in the SigNoz-compatible schema through a FuseScope exporter
and schema command, the engine over standard OTLP, and both together for migrations
(dual-write). The ClickHouse path is a supported product feature, not a temporary bridge.

## Consequences

The ClickHouse schema becomes a contract this repository owns clean-room, with its own
migrations for new installations and compatibility with existing ones. A parity suite against
the existing collector gates every change to it.
