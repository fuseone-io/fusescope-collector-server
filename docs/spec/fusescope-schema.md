# fusescope-schema · ClickHouse migrator contract

How `fusescope-schema` creates and migrates the SigNoz-compatible ClickHouse schema ([ADR
0010](../adr/0010-fusescope-schema.md), #9). Clean-room (ADR 0002). Sources:

- **[obs]**: [`observed/0.145.0/ddl/`](observed/0.145.0/ddl/). This is `SHOW CREATE` of every
  object in a cluster that the fork 0.145.0 migrated (135 objects in `signoz_traces`,
  `signoz_logs`, `signoz_metrics`, `signoz_meter`, `signoz_metadata` and `signoz_analytics`),
  from a black-box run (`observed/0.145.0/run.yaml`).
- **[be]**: the FuseScope backend's reads, as cited in the other specs.
- **[ch]**: ClickHouse documentation: distributed DDL, KeeperMap, `system.*` tables.

The fork's migration source, its list of migrations and the contents of its history are never
read. The target of every migration is the schema in [obs] (or a later schema change this
repository decides), written as DDL from that target.

## 1. Commands

| Command | Writes | Exit codes |
|---|---|---|
| `fusescope-schema migrate` | the schema and the history | 0 up to date; 1 refused (§7); 2 connection or ClickHouse error, safe to retry |
| `fusescope-schema check [--wait D]` | nothing | 0 up to date; 3 migrations pending; 1 refused. With `--wait`, polls until 0 or 1, or until D passes |
| `fusescope-schema plan` | nothing | prints the starting point (§4) and the migrations `migrate` would run |
| `fusescope-schema status` | nothing | prints the history and any difference between the live schema and the expected one |

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
- the databases, with the chart's names as defaults (`signoz_traces`, `signoz_logs`,
  `signoz_metrics`, `signoz_meter`, `signoz_metadata`, `signoz_analytics`), each settable as the
  chart allows today.

Inline passwords are refused (ADR 0004).

The cluster must have Keeper (or ZooKeeper) and the `{shard}` and `{replica}` macros, as the
[obs] DDL already requires (`ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}',
'{replica}')`). A cluster without them is refused before anything is written.

## 3. History

`fusescope-schema` keeps its own history in database `fusescope_schema`, created `ON CLUSTER`:

| Table | Engine | Content |
|---|---|---|
| `migrations` | `KeeperMap` (strict), key `id` | `id UInt32`, `name String`, `checksum FixedString(64)` (SHA-256 of the statements after placeholder substitution, §5), `state Enum('started', 'done')`, `adopted Bool`, `started_at DateTime64(3)`, `finished_at Nullable(DateTime64(3))`, `tool_version String`, `host String` |
| `lock` | `KeeperMap` (strict), key `name` | `name String`, `owner String`, `acquired_at`, `expires_at` (§6) |

KeeperMap keeps both tables linearizable across replicas and shards [ch]. It needs
`keeper_map_path_prefix` in the server configuration; the chart's ClickHouse sets it, and an
external ClickHouse without it is refused with that instruction (ADR 0010, open point 1).

The fork's `schema_migrations_v2` tables are read only in §4 and never written.

## 4. Starting points

`migrate` and `plan` classify the cluster before anything else:

| Starting point | How it is recognised | What `migrate` does |
|---|---|---|
| **Empty** | none of the six databases holds a table, and no `fusescope_schema` | runs every migration from `0001` |
| **Fork 0.145.0** | no `fusescope_schema`, and the live schema equals [obs] after normalisation (§4.1) | records `0001` as `done` with `adopted = true`, without running it, then runs the later migrations |
| **Managed** | `fusescope_schema.migrations` exists | checks §7, then runs the pending migrations |
| **Anything else** | none of the above | refuses (exit 1), listing every object that is missing, extra or different from [obs] |

### 4.1 Normalisation

Two schemas are compared object by object, by database and name. For each object:

- `SHOW CREATE` is normalised by:
  - collapsing whitespace;
  - replacing the table UUID inside Keeper paths by `{uuid}`;
  - replacing the cluster name by `{cluster}`;
  - replacing each database name by its role (`traces`, `logs`, and so on), so custom names
    compare.
- The engine, columns (with their types, codecs, defaults and TTLs), indexes, keys,
  projections and settings must be equal.
- The order of columns is part of the comparison, since inserts that name no columns depend on
  it.

The same comparison runs per replica (`clusterAllReplicas`), so one replica missing a table is
a difference.

## 5. Migrations

Migrations are compiled into the binary, ordered by `id`, and never edited once released. A
correction is a new migration. Each has:

- `id` and `name`;
- `statements`: DDL with the placeholders `{cluster}` and the database roles, each statement
  idempotent (`CREATE … IF NOT EXISTS`, `DROP … IF EXISTS`, `ALTER … ADD COLUMN IF NOT EXISTS`,
  or guarded by a precondition the runner checks first);
- `postconditions`: the normalised objects (§4.1) it leaves, checked on every replica.

`0001 base` creates the six databases and every object of [obs], in dependency order: local
tables, then distributed tables, then materialized views and their targets.

### 5.1 Running one migration

1. Insert `state = started` with the checksum (strict mode: fails if the `id` exists).
2. Run each statement `ON CLUSTER {cluster}` with `distributed_ddl_output_mode = throw` and
   `distributed_ddl_task_timeout`, so every host has applied it or the statement fails.
3. Check the postconditions on every replica.
4. Set `state = done` and `finished_at`.

The lock (§6) is renewed between statements. A failure leaves the row `started`, and §7 then
refuses to go on until an operator follows the runbook (§10).

## 6. Lock

- `migrate` takes the lock by inserting the row `name = migrate` into `fusescope_schema.lock`.
  Strict KeeperMap refuses the insert if the row exists, so only one migrator holds it.
- The holder renews `expires_at` (lease 5 minutes, renewed every minute) and deletes the row
  when done.
- A waiting migrator retries until `--lock-timeout` (default 10 minutes), then exits 2.
- An expired lease is taken over only when no migration is `started`. Otherwise the cluster is
  in an unknown state and §7 refuses.
- `check`, `plan` and `status` never take the lock.

## 7. Refusals (fail closed)

`migrate` and `check` exit 1 without writing when:

1. a history row has an `id` this binary does not know (a newer tool ran);
2. a recorded checksum differs from the binary's for the same `id`;
3. a row is `started` (a migration was interrupted);
4. the live schema differs from the postconditions of the `done` migrations;
5. the starting point is "anything else" (§4);
6. Keeper, the macros or `keeper_map_path_prefix` are missing (§2, §3);
7. the TLS settings break ADR 0004.

Each refusal names its runbook section (§10).

## 8. Interaction with the rest

- **The backend** reads the six databases and does not read any history [be]. It keeps
  applying its own TTL changes through its settings API. Those are table settings, ignored by
  the comparison of §4.1 for `TTL` clauses the backend owns. The ignored set is listed
  explicitly in the code and tested.
- **The collector** writes only to tables of [obs].
- **The fork** may still run during the transition, until fusescope#229. If an installation
  goes back to the fork's migrator, `fusescope_schema` stays untouched, and the fork's history,
  which `fusescope-schema` never changed, still describes the schema.

## 9. Tests

All against a real cluster in CI: ClickHouse at the version the chart pins, 2 shards × 2
replicas, and Keeper.

| Case | Expected |
|---|---|
| Fresh install | live schema equals [obs]; history `0001` done |
| Adopting a cluster the fork 0.145.0 migrated (the fork image run as a black box) | `0001` adopted; nothing re-created; data still readable |
| Running `migrate` twice | the second run is a no-op |
| 8 concurrent `migrate` processes | one runs, the others wait and exit 0, each statement applied once |
| Killed (`SIGKILL`) between two statements, then rerun | refused with `started`; after the runbook step, completes |
| Killed while holding only the lock | the lease expires and the next run takes over |
| Unknown history row, changed checksum, extra table, replica missing a table | each refused with its reason |
| TLS 1.3 with a password file; plaintext opt-in; `verify: false` | connects as configured; TLS below 1.3 refused |
| Custom database names | migrated and compared under the custom names |
| `check --wait` before and after `migrate` | 3, then 0 |

**Functional validation**
- The backend's integration suites run with `fusescope-schema` instead of the fork migrator
  (a fixture switch, as `--otlp-collector`).
- The collector writes traces, logs, metrics, the meter and attribute metadata, and the backend
  reads them.
- A chart upgrade smoke from 0.161.x (fork-migrated) to a chart that runs `fusescope-schema`.

## 10. Runbook

| Situation | Steps |
|---|---|
| `started` row (§7.3) | Read `status`, which shows which postconditions hold. Finish the migration's remaining statements by hand, or undo the applied ones by hand. Then mark the row `done` (or delete it) with `fusescope-schema repair --id N --mark done\|absent`, which re-checks the postconditions first and refuses if they do not match |
| Unknown or changed history (§7.1, §7.2) | Run the newer binary that wrote it. Never edit history by hand |
| Schema drift (§7.4, §7.5) | `status` lists the differences. Restore the expected objects (or drop the extra ones) by hand, then rerun |
| Missing Keeper settings (§7.6) | Add `keeper_map_path_prefix` to the server configuration and restart ClickHouse |
| Going back after a bad migration | There is no automatic rollback. Restore from backup, or write a forward migration that corrects it |
