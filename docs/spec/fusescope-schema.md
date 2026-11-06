# fusescope-schema · ClickHouse migrator contract

How `fusescope-schema` creates and migrates the SigNoz-compatible ClickHouse schema ([ADR
0010](../adr/0010-fusescope-schema.md), #9). Clean-room (ADR 0002). Sources:

- **[obs]**: [`observed/0.145.0/ddl/`](observed/0.145.0/ddl/). This is the schema of every
  object in a cluster that the fork 0.145.0 migrated with replication on (135 objects in
  `signoz_traces`, `signoz_logs`, `signoz_metrics`, `signoz_meter`, `signoz_metadata` and
  `signoz_analytics`), from a black-box run (`observed/0.145.0/run.yaml`).
- **[be]**: the FuseScope backend's DDL and reads, cited where used.
- **[ch]**: ClickHouse documentation: distributed DDL, KeeperMap, the `system` tables.

The fork's migration source and list are never read, and its history tables are never queried:
only their observed structure is part of [obs]. The target of every migration is the schema in
[obs] (or a later schema change this repository decides), written as DDL from that target.

## 1. Commands

| Command | Lease | Writes | Exit codes |
|---|---|---|---|
| `migrate` | takes it | the schema and the history | 0 up to date; 1 refused (§7); 2 connection, ClickHouse or lease error, safe to retry |
| `check [--wait D]` | never | nothing | 0 up to date; 3 pending or in progress; 1 refused. With `--wait`, polls until 0 or 1, or until D passes (then 3) |
| `plan` | never | nothing | prints the starting point and the pending migrations, marked advisory: `migrate` decides again under the lease |
| `status` | never | nothing | prints the history, the lease and any difference from the expected schema |
| `repair --id N --mark done\|absent` | takes it | one history row | 0 repaired; 1 refused, because the post-conditions do not match (§10.1); 2 connection or lease error |

The chart's Job runs `migrate`, and the collector's init container runs `check --wait 10m`.

## 2. Connection

The settings are the ones `fusescope_clickhouse` takes, from flags, environment variables or a
file:

- the endpoint (native protocol);
- the username and `password_file`;
- `tls` (`min_version` 1.3, `ca_file`, `insecure_skip_verify` only with `verify: false`,
  `insecure` only by explicit opt-in, ADR 0007);
- `dial_timeout`;
- `cluster`, the name in `remote_servers`;
- the databases, with the chart's names as defaults, each settable as the chart allows today.

Inline passwords are refused (ADR 0004).

### 2.1 Supported deployments

| Deployment | Supported | Why |
|---|---|---|
| The chart's ClickHouse (operator, ZooKeeper, `enableReplication: true`, the default) | yes | Keeper and the macros are there, and the chart sets `keeper_map_path_prefix` |
| Self-managed cluster with Keeper or ZooKeeper, the `{shard}` and `{replica}` macros, a cluster in `remote_servers` and `keeper_map_path_prefix` | yes | the same requirements, met by the operator |
| `telemetryStoreMigrator.enableReplication: false` | **no**: chart validation fails with `fusescope-schema`, and the precheck refuses | [obs] is the replicated schema; a non-replicated one was never observed |
| ClickHouse Cloud | **no**, refused | no server configuration for `keeper_map_path_prefix`, and engines are rewritten |
| Managed services that do not expose Keeper or server configuration | **no**, refused | no Keeper, so neither the lease nor `ON CLUSTER` |

A precheck runs before anything is written. It verifies:
- the server version;
- `system.zookeeper` is readable;
- the macros and the cluster exist;
- a probe `KeeperMap` table can be created and dropped.

A failed precheck is a refusal (§7.6) that names the missing item.

## 3. History and coordination

`fusescope-schema` keeps its state in database `fusescope_schema`, created `ON CLUSTER`, in
`KeeperMap` tables, so every replica and shard reads the same rows, linearizably [ch]. Every write
runs with `keeper_map_strict_mode = 1`. Under it, an `INSERT` of an existing key fails, and an
`ALTER … UPDATE` or `DELETE` fails when the row changed since it was read [ch].

| Table | Key | Columns |
|---|---|---|
| `meta` | `name` | `name String`, `value String`. The row `coordination = 1` is the version of these tables' own layout |
| `lease` | `name` | `name String` (only `migrate`), `owner String` (`host/pid/random`), `token UInt64` (generation), `expires_at DateTime64(3)` |
| `migrations` | `id` | `id UInt32`, `name String`, `checksum FixedString(64)` (SHA-256 of the statements after substitution, §5), `state Enum('started', 'done')`, `adopted Bool`, `token UInt64` (the lease generation that wrote it), `started_at DateTime64(3)`, `finished_at Nullable(DateTime64(3))`, `tool_version String`, `host String` |

### 3.1 Bootstrap

1. `CREATE DATABASE IF NOT EXISTS fusescope_schema ON CLUSTER`, then the three tables with
   `CREATE TABLE IF NOT EXISTS … ON CLUSTER`. Concurrent bootstraps converge, because each table
   maps to the same Keeper path.
2. Insert `meta(coordination, 1)` and `lease(migrate, '', 0, 0)`. A "key exists" failure means
   another process got there first, and is ignored.
3. Read `meta.coordination`. A value this binary does not know is a refusal (§7.1).

Bootstrap creates only `fusescope_schema`. It never touches the telemetry databases, and it
decides nothing about them.

## 4. Starting points

The starting point is decided by `migrate` **after the lease is held** (§6), and again on every
run. `plan` shows its own reading, marked advisory.

| Starting point | How it is recognised | What `migrate` does |
|---|---|---|
| **Empty** | none of the six databases holds a table, and `migrations` is empty | runs every migration from `0001` |
| **Fork 0.145.0** | `migrations` is empty, and the live schema equals [obs] (§4.1), allowing for §8 | records `0001` as `done` with `adopted = true`, without running it, then runs the later migrations |
| **Managed** | `migrations` has rows | checks §7, then runs the pending migrations |
| **Anything else** | none of the above | refuses (exit 1), listing each object missing, extra or different |

### 4.1 Structural comparison

Schemas are compared as structures read from `system` tables on every replica
(`clusterAllReplicas`), not as `SHOW CREATE` text:

| Object | Compared |
|---|---|
| Table | `engine`; `sorting_key`, `primary_key`, `partition_key`, `sampling_key`; the TTL expression (subject to §8); the settings; the Keeper path with `{uuid}`, `{shard}` and `{replica}` in place |
| Column (`system.columns`) | name, position, type, `default_kind`, `default_expression`, `compression_codec`, `comment` |
| Data-skipping index (`system.data_skipping_indices`) | name, type and its arguments, expression, granularity |
| Projection | name and query |
| Materialized view | target table, and the query from `system.tables.as_select` |
| Distributed table | cluster, database, table, sharding key |
| Dictionary | source, layout, structure, lifetime |

- Every expression is canonicalised by the server with `formatQuery` before it is compared, so
  formatting differences between ClickHouse versions do not count.
- Database names are replaced by their role, so custom names compare.
- The cluster name is replaced by `{cluster}`.

A difference on a single replica counts.

## 5. Migrations

Migrations are compiled into the binary, ordered by `id`, and never edited once released. A
correction is a new migration. Each has:

- `id` and `name`;
- `statements`: DDL with the placeholders `{cluster}` and the database roles, each statement
  idempotent (`CREATE … IF NOT EXISTS`, `DROP … IF EXISTS`, `ALTER … ADD COLUMN IF NOT EXISTS`,
  or guarded by a precondition the runner checks first);
- `postconditions`: the structures (§4.1) it leaves;
- `absent`: the structures it adds, which `repair --mark absent` requires gone (§10.1).

`0001 base` creates the six databases and every object of [obs], in dependency order: local
tables, then distributed tables, then materialized views after their targets.

### 5.1 Running one migration

1. Under the lease, insert `migrations(id, …, state = started, token = <lease token>)`. Strict
   mode fails if the `id` exists.
2. Run each statement `ON CLUSTER {cluster}` with `distributed_ddl_output_mode = throw` and
   `distributed_ddl_task_timeout`, so every host has applied it or the statement fails.
   - Before each statement, the runner checks that its lease is still valid (§6). If it is not,
     it stops.
3. Check the post-conditions on every replica.
4. `ALTER TABLE migrations UPDATE state = 'done', finished_at = now64(3) WHERE id = N AND
   token = <lease token>`. Strict mode makes it fail if another lease generation wrote the row.

## 6. Lease

The lease is the single row `lease(migrate)`. Every change is a conditional update in strict
mode.

1. **Acquire.** Read the row (`owner`, `token`, `expires_at`). It is free when `owner` is empty,
   or when `expires_at` has passed and no `migrations` row is `started` with that `token`. If it
   is free, run:

   ```
   ALTER TABLE lease UPDATE owner = <me>, token = <token + 1>, expires_at = now64(3) + 5 min
   WHERE name = 'migrate' AND token = <token read>
   ```

   Then read the row back. The lease is held only if it shows `<me>` and `<token + 1>`. A failed
   update or a different read-back means another process won; retry until `--lock-timeout`
   (default 10 minutes), then exit 2.
2. **Renew.** A background task renews every 60 seconds with the same conditional update on
   `owner = <me> AND token = <mine>`, moving `expires_at` 5 minutes ahead.
   - When a renewal fails, or 4 minutes pass without a successful one, the lease is lost.
   - From then on the runner issues no new statement. The current one, idempotent, may finish.
   - The migration in progress stays `started`, and `migrate` exits 2.
3. **Release.** `UPDATE owner = '' WHERE owner = <me> AND token = <mine>`.

A later migrator sees an expired lease over a `started` row with the old token. That row was
interrupted, so §7.3 refuses: the next generation never resumes the previous one's work silently.

## 7. Refusals (fail closed)

`migrate` exits 1 without writing to the telemetry databases when:

1. `meta.coordination` is newer than this binary, or a history row has an `id` this binary does
   not know;
2. a recorded checksum differs from the binary's for the same `id`;
3. a row is `started` and its `token` is not the current lease's, or the lease has expired: the
   migration was interrupted;
4. the live schema differs from the post-conditions of the `done` migrations, beyond §8;
5. the starting point is "anything else" (§4);
6. the precheck fails (§2.1);
7. the TLS settings break ADR 0004.

`check` reports these the same way (exit 1), with one difference. A `started` row whose token is
the current lease's, with an unexpired lease, is **in progress**: `check` returns 3, and with
`--wait` it keeps waiting.

Each refusal names its runbook section (§10).

## 8. Schema changes the backend makes

The backend changes the schema at runtime [be]. These changes are allowed by pattern, both in the
comparison and in the post-conditions, and each pattern has a test. Anything else, even if it
looks similar, is drift.

| Pattern | Tables | Source |
|---|---|---|
| Columns `<prefix>_<datatype>_<key>` added with `DEFAULT <map>['<key>'] CODEC(ZSTD(1))`, where `<map>` is `attributes_<type>` or `resources_<type>` | `logs_v2` and `distributed_logs_v2`; `signoz_index_v3` and `distributed_signoz_index_v3` | promoted fields: `clickhouseReader/reader.go` (logs about line 2629, traces about line 2770) |
| Their companion `<column>_exists Bool DEFAULT if(mapContains(<map>, '<key>') != 0, true, false) CODEC(ZSTD(1))` | the same | the same |
| Indexes `<column>_idx` over a promoted column (the backend's index types and granularity), and `<column>_minmax_idx … TYPE minmax GRANULARITY 1` | the local tables above | `reader.go` about lines 2667, 2808 and 2823 |
| RUM promoted columns, `_exists` columns and `_idx` indexes, of the same shapes | `signoz_index_v3`/`distributed_signoz_index_v3` and `logs_v2`/`distributed_logs_v2` (`promoter.go` about lines 38–39) | `implrumapplication/promoter.go` about lines 118–124 |
| The `DEFAULT` value of `_retention_days` and `_retention_days_cold` (type `UInt16` unchanged) | the logs and logs-resource tables | `reader.go` about lines 1309–1340 |
| The table TTL expression | the tables the backend's retention settings target | `clickhouseReader/ttl_statements.go` |

A promoted column must match its pattern exactly: the name, the type, the `DEFAULT` built from the
same `<key>`, and the codec. Anything else is drift, for example:
- a column with another codec;
- an index on a column that is not promoted;
- a change to `_retention_days`' type.

## 9. Tests

All against a real cluster in CI: ClickHouse at the version the chart pins, 2 shards × 2
replicas, Keeper, and `keeper_map_path_prefix`.

| Case | Expected |
|---|---|
| Fresh install | the live schema equals [obs]; history `0001` done |
| Adopting a cluster the fork 0.145.0 migrated (the fork image run as a black box) | `0001` adopted; nothing re-created; data still readable |
| The same, after the backend promoted fields, added RUM columns and changed retention | adopted (§8); a promoted column with a changed codec is refused |
| Running `migrate` twice | the second run is a no-op |
| 8 concurrent `migrate` processes against an empty cluster | one holds the lease and classifies; the others wait, re-classify as Managed and exit 0; each statement runs once |
| A lease taken over while a migrator pauses in a statement (renewals blocked) | the old migrator issues no further statement and exits 2; the new one refuses on the `started` row (§7.3) |
| `SIGKILL` between two statements, then a rerun | refused with `started`; after `repair` (§10.1) it completes |
| `SIGKILL` while holding only the lease | after expiry the next run takes it over |
| `check --wait` during a running migration | keeps waiting, then 0 |
| `check` on an interrupted migration | 1 |
| An unknown history row, a changed checksum, an extra table, a replica missing a table | each refused with its reason |
| `repair --mark done` with the post-conditions unmet, and `--mark absent` with objects present | refused |
| TLS 1.3 with a password file; plaintext opt-in; `verify: false`; TLS below 1.3 | connects as configured; TLS below 1.3 refused |
| Custom database names | migrated and compared under the custom names |
| Precheck failures: no Keeper, no macros, no `keeper_map_path_prefix` | each refused with what is missing |

**Functional validation**
- The backend's integration suites run with `fusescope-schema` instead of the fork migrator
  (a fixture switch, as `--otlp-collector`).
- The collector writes traces, logs, metrics, the meter and attribute metadata, and the backend
  reads them.
- A chart upgrade smoke from 0.161.x (fork-migrated) to a chart that runs `fusescope-schema`.

## 10. Runbook

| Situation | Steps |
|---|---|
| Interrupted migration (§7.3) | `status` shows which post-conditions hold. Finish the remaining statements by hand, or undo the applied ones by hand, then run `repair` (§10.1) |
| Unknown or changed history (§7.1, §7.2) | Run the newer binary that wrote it. Never edit history by hand |
| Schema drift (§7.4, §7.5) | `status` lists the differences. Restore the expected objects, or drop the extra ones, by hand, then rerun |
| Precheck failures (§7.6) | Add what is named, for example `keeper_map_path_prefix` in the server configuration, and restart ClickHouse |
| A bad migration | There is no automatic rollback. Restore from backup, or write a forward migration that corrects it |
| Going back to the fork's migrator | Possible only while the history holds `0001` alone: the fork's own history and the [obs] schema are untouched. After any later migration it is not supported |

### 10.1 `repair`

`repair` takes the lease and refuses unless:

- **`--mark done`**: every post-condition of migration `N` holds on every replica (§4.1, §8).
  The row becomes `done` with the current token.
- **`--mark absent`**: none of the structures in `N`'s `absent` list exists on any replica, and
  no migration after `N` is recorded. The row is deleted.

Either way, the result is verified by reading the history back before exit 0.
