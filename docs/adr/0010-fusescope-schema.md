# 0010 · fusescope-schema, our own ClickHouse migrator

- Status: proposed
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

   The fork's migration source, its migration list and its history contents are never opened or
   copied (ADR 0002). The migrations are written from the target DDL, not translated from the
   fork's steps.
2. **Its own module and binary.** A Go module in `components/` (so `make forbid-fork` and the
   licence check cover it), built as a second binary of the `fusescope-collector` image. The
   chart, the devenv and the suites run the image they already pin and sign, with
   `fusescope-schema` as the entrypoint.
3. **Its own history, never the fork's.** `fusescope-schema` records every migration in its own
   database, with the migration's ID, the SHA-256 of its statements and its state (started or
   done). It reads the fork's `schema_migrations_v2` tables only to recognise a database the
   fork created, and never writes them.
4. **Supported starting points:**
   - an empty cluster (a fresh install);
   - a cluster whose schema is exactly what the fork 0.145.0 created (every FuseScope chart and
     kit so far);
   - a cluster `fusescope-schema` already manages.

   Anything else (an older or newer fork, a SigNoz version whose schema differs, a hand-edited
   table) is refused, with the differences listed. The SigNoz migration guide
   (phase 8) documents how to reach a supported starting point first.
5. **Fail closed.** It refuses to run when:
   - the history holds a migration this binary does not know;
   - a recorded checksum differs from the binary's;
   - a migration was started and not finished;
   - the live schema differs from what the history says.

   It does not repair automatically. A runbook covers each case. DDL is not assumed reversible:
   there is no automatic rollback; a correction is a new forward migration.
6. **Serialized and idempotent.**
   - One migrator at a time per cluster, through a lock held in ClickHouse Keeper (§6 of the
     spec).
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
- From then on, schema changes are ours to write, review and test. Parity with the fork stops at
  0.145.0. Later FuseScope schema changes are migrations in this repository, under the
  schema-contract rule ("ask before changing the ClickHouse schema contract").
- Installations on a schema other than the fork 0.145.0 need a manual step; the refusal names
  it.
- The tests need a real ClickHouse cluster with Keeper (spec §9), in CI.

## Open points for review

1. Keeper for the lock. The proposed lock is a `KeeperMap` table in strict mode, which needs
   `keeper_map_path_prefix` in the ClickHouse server configuration. The chart's ClickHouse can
   set it; an external ClickHouse may not. The alternative is a Keeper client in Go, which is a
   new dependency (`github.com/go-zookeeper/zk`, BSD-3-Clause, needs approval).
2. Whether the fork's history tables (`schema_migrations_v2`) are dropped once a cluster is
   adopted, or left as they are (proposed: left, read-only, so a rollback to the fork stays
   possible during the transition).
3. The second binary in the collector image (proposed) or a separate signed image.
