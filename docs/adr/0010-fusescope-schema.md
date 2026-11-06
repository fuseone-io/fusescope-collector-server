# 0010 · fusescope-schema, our own ClickHouse migrator

- Status: accepted (2026-11-06, after the protocol spike, spec §11)
- Date: 2026-11-06
- Issue: [#9](https://github.com/fuseone-io/fusescope-collector-server/issues/9); unblocks
  [fuseone-io/fusescope#229](https://github.com/fuseone-io/fusescope/issues/229)
- Spec: [`docs/spec/fusescope-schema.md`](../spec/fusescope-schema.md)

## Context

ADR 0003 makes the SigNoz-compatible ClickHouse schema a contract this repository owns. Our
collector writes it (`fusescope_clickhouse`, `mode: meter`, `attributes_metadata`), but the
schema is still created and migrated by the AGPL fork's migrator
(`fusescope-otel-collector migrate …`). Every published chart (0.147.0 to 0.161.1) runs the fork
0.145.0 image as `telemetrystore-migrator`, and so do the devenv, the demo kit and the backend's
integration suites. Until that changes, the fork is still shipped in every installation, and
the fork cannot be retired ([fusescope#229](https://github.com/fuseone-io/fusescope/issues/229)).

## Decision

1. **A clean-room migrator, `fusescope-schema`.** It is specified only from:
   - the observed schema of databases the fork created (`SHOW CREATE`, already recorded in
     `docs/spec/observed/0.145.0/ddl`);
   - the backend's reads;
   - ClickHouse's documentation.

   The fork's migration source, its migration list and its history contents are never opened,
   queried or copied (ADR 0002). The migrations are written from the target DDL, not translated
   from the fork's steps.
2. **Its own module and binary.** A Go module in `components/` (so `make forbid-fork` and the
   licence check cover it), built as a second binary of the `fusescope-collector` image. The
   chart, the devenv and the suites run the image they already pin and sign, with
   `fusescope-schema` as the entrypoint.
3. **Its own history, never the fork's.** `fusescope-schema` records every migration in its own
   database, with the migration's ID, the SHA-256 of its statements and its state (started or
   done). The fork's `schema_migrations_v2` tables are never queried or written: only their
   observed structure takes part in recognising a cluster the fork migrated, like any other
   table. They stay as they are.
4. **Supported starting points:**
   - an empty cluster (a fresh install);
   - a cluster whose schema is exactly what the fork 0.145.0 created (every FuseScope chart and
     kit so far);
   - a cluster `fusescope-schema` already manages.

   Anything else (an older or newer fork, a SigNoz version whose schema differs, a hand-edited
   table, a schema migrated with `telemetryStoreMigrator.enableReplication: false`) is refused,
   with the differences listed. The SigNoz migration guide (phase 8) documents how to reach a
   supported starting point first.

   Supported deployments (spec §2.1) are the chart's own ClickHouse and a self-managed cluster with
   Keeper and `keeper_map_path_prefix`. Replication off, ClickHouse Cloud and managed services that
   do not expose Keeper are not supported in this phase and are refused before anything is
   written.
5. **Fail closed.** It refuses to run when:
   - the history holds a migration this binary does not know;
   - a recorded checksum differs from the binary's;
   - a migration was started and not finished;
   - the live schema differs from what the history says.

   A migration that is `started` while its migrator still holds a valid lease is in progress, not
   interrupted: `check` keeps waiting. It does not repair automatically; `repair` is an explicit,
   locked command whose result is verified (spec §1, §10). DDL is not assumed reversible: there
   is no automatic rollback; a correction is a new forward migration.
6. **Serialized and idempotent.**
   - One migrator at a time per cluster, through a lease in a strict `KeeperMap` table, taken and
     renewed by conditional updates with a generation token, and taken only from a free or expired
     lease. A migrator that loses its lease stops before its next statement (spec §6). Safety rests
     on these conditional writes, not on reads, which KeeperMap makes only sequentially
     consistent.
   - The starting point is decided only after the lease is held, so concurrent first runs
     cannot act on a stale classification (spec §4).
   - Every statement is safe to repeat (`IF NOT EXISTS`, `IF EXISTS`, or a post-condition check
     before it runs).
   - A migration is marked done only after its post-conditions are verified on every replica.
7. **Same connection rules as the exporter.**
   - TLS 1.3 by default, credentials from files only, plaintext only by explicit
     `tls.insecure` (ADR 0004, 0007).
   - The in-chart and external ClickHouse, with or without a password.
   - A cluster with replicas and shards, through `ON CLUSTER` DDL.
8. **The chart keeps its shape.** `telemetryStoreMigrator` runs `fusescope-schema migrate` as
   the Job. The collector's init container runs `fusescope-schema check`, which waits and does not
   write. The fork image leaves the chart, devenv and suites only after the tests of §9 of the
   spec pass.

## Consequences

- No AGPL artefact is needed to install, upgrade or run FuseScope on ClickHouse. ADR 0001 is
  amended when the fork leaves the chart (fusescope#229).
- The schema changes the backend makes itself (promoted columns and their indexes, retention
  defaults, TTLs) are an explicit, tested allow-list (spec §8); anything similar outside it is
  drift.
- Going back to the fork's migrator is possible only before the first migration after `0001`:
  later migrations change the schema the fork knows.
- From then on, schema changes are ours to write, review and test. Parity with the fork stops at
  0.145.0. Later FuseScope schema changes are migrations in this repository, under the
  schema-contract rule ("ask before changing the ClickHouse schema contract").
- Installations on a schema other than the fork 0.145.0 need a manual step; the refusal names
  it.
- The tests need a real ClickHouse cluster with Keeper (spec §9), in CI.

## Spike

A spike on a 2×2 cluster ran the spec §6 protocol (spec §11), with Keeper and with the chart's
ZooKeeper. It is reproducible with `docs/spec/observed/fusescope-schema-spike/run.sh`, and 20 of
20 assertions pass with each service. Every protocol case behaved as specified. It also led to
three changes in the spec:

- **The read-back is mandatory:** a losing update usually reports no error.
- **`keeper_map_path_prefix`** is not visible in `system` tables, so its absence is detected by the
  bootstrap's first `KeeperMap` create.
- **A replica configured with another prefix or ensemble can hold a lease of its own.** A random
  cluster identity, written once and checked on every replica before the lease and after each
  renewal, refuses it (spec §3.2).

The limits are recorded in the spec: single-node coordination, one ClickHouse version, and clock
skew. The CI matrix covers them before any other version, ensemble or deployment is supported.

## Review decisions

1. KeeperMap in strict mode for the lease and the history, with a corrected protocol (spec §6).
   No Keeper client dependency for now.
2. The fork's history tables stay untouched and read-only (never queried, spec §3).
3. `fusescope-schema` ships as a second binary in the signed `fusescope-collector` image.
