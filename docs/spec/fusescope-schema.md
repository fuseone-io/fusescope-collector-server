# fusescope-schema · ClickHouse migrator contract

How `fusescope-schema` creates and migrates the SigNoz-compatible ClickHouse schema ([ADR
0010](../adr/0010-fusescope-schema.md), #9). Clean-room (ADR 0002). Sources:

- **[obs]**: [`observed/0.145.0/ddl/`](observed/0.145.0/ddl/). This is the schema of every
  object in a cluster that the fork 0.145.0 migrated with replication on (135 objects in
  `signoz_traces`, `signoz_logs`, `signoz_metrics`, `signoz_meter`, `signoz_metadata` and
  `signoz_analytics`), from a black-box run (`observed/0.145.0/run.yaml`).
- **[obs rows]**: the rows of `signoz_metadata.column_evolution_metadata` the same run left,
  [`observed/0.145.0/rows/signoz_metadata.column_evolution_metadata.jsonl`](observed/0.145.0/rows/signoz_metadata.column_evolution_metadata.jsonl).
  The capture records inserts from every user, so it does not tell whether the fork's migrator or
  its collector wrote them. They are there on every fork install either way, and neither
  `fusescope-schema`'s `0001` nor `fusescope-collector` writes them (§5.3).
- **[be]**: the FuseScope backend's DDL and reads, cited where used.
- **[ch]**: ClickHouse documentation: distributed DDL, KeeperMap, the `system` tables, and `DEFINER = CURRENT_USER` for views (SQL security, release 24.2).

The fork's migration source and list are never read, and its history tables are never queried:
only their observed structure is part of [obs]. The target of every migration is the schema in
[obs] (or a later schema change this repository decides), written as DDL from that target.

## 1. Commands

| Command | Lease | Writes | Exit codes |
|---|---|---|---|
| `migrate` | takes it | the schema and the history | 0 up to date; 1 refused (§7); 2 connection, ClickHouse or lease error **before** any migration was marked `started`: safe to retry; 4 the same error **after** a migration was marked `started`: the next run refuses (§7.3) until `status` and `repair` (§10) |
| `check [--wait D]` | never | nothing | 0 up to date; 3 pending or in progress; 1 refused. With `--wait`, polls until 0 or 1, or until D passes (then 3) |
| `plan` | never | nothing | prints the starting point and the pending migrations, marked advisory: `migrate` decides again under the lease |
| `status` | never | nothing | prints the history, the lease and any difference from the expected schema |
| `repair --id N --mark done\|absent` | takes it | one history row | 0 repaired; 1 refused: the post-conditions or pre-conditions do not match, or `absent` on an irreversible migration (§10.1); 2 connection or lease error, nothing changed |

The chart's Job runs `migrate`, and the collector's init container runs `check --wait 10m`.

- Exit codes come from typed errors only, never from a message's text. For `migrate`, a migration
  marked `started` decides first: any error after the insert of its row was sent exits 4, unless
  the store proved that no row of this run exists. Then a refusal exits 1, and anything else 2.
- `check`, `plan` and `status` run the read-only precheck, read, and write nothing, not even the
  bootstrap of `fusescope_schema`.
  - They first check `fusescope_schema` on every replica as the bootstrap would, without
    writing:
    - a table this binary does not know, or a changed one, is refused;
    - once `meta` is on every replica, another coordination version or another cluster
      identity is refused, whatever else is missing;
    - none of it, this layout only partly, or the `meta` or lease rows missing, leaves `migrate`
      pending;
    - complete, every replica must show the same `meta`, lease and `migrations` rows; rows that
      still differ after the re-read window of §3.2 are a divergence.
  - Only then do they read the history and the lease, on the connected server.
  - Their codes:
  - `check`: 2 is a connection error. With `--wait` it polls through connection errors, since
    ClickHouse may come up later. Every attempt is bounded by the wait's deadline, so a server
    that accepts and never replies cannot hold it longer, and none starts after it. The exit code
    is the last answer before the deadline: 3 if still pending, 2 if the last attempt failed. An
    attempt the deadline cuts short gives no answer, so it ends 2 only if nothing answered before
    it.
  - `plan`: 0, 1 when refused, 2 on a connection error.
  - `status`: 0 whenever it could read, printing a refusal as part of the report; 2 on a connection
    error.
- A usage or configuration error exits 1 before connecting.
- A replica that cannot be read, including the listing of the replicas, is retryable: exit 2, or
  4 once a migration is started. It still loses a held lease (§6). A replica that answers with
  another identity, layout or lease is a divergence and exits 1.
- The first `SIGINT` or `SIGTERM` stops a run before its next statement. The current statement may
  finish, the lease is released, and the exit code follows the table: 4 once a migration was
  started. A second signal ends the process.
- Releasing the lease uses a context of its own, so a signal still releases it. A failed release is
  a warning: the run's outcome and exit code stand, and the lease expires on its own.
- `repair` takes the lease even when it expired over the interrupted migration it repairs; `migrate`
  refuses that lease (§6).
- Settings come from defaults, then a JSON file (`--config`, or `FUSESCOPE_SCHEMA_CONFIG`), then
  `FUSESCOPE_SCHEMA_<FLAG>` variables, then flags. An unknown file key, a `password` key and
  `FUSESCOPE_SCHEMA_PASSWORD` are refused (ADR 0004). No output shows the password.

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
| The chart's ClickHouse (operator, ZooKeeper, `enableReplication: true`, the default) | yes | Keeper and the macros are there; the chart will set `keeper_map_path_prefix` (today it does not), in the same change that switches it to `fusescope-schema` |
| Self-managed cluster with Keeper or ZooKeeper, the `{shard}` and `{replica}` macros, a cluster in `remote_servers` and the same `keeper_map_path_prefix` and coordination ensemble on every replica | yes | the same requirements, met by the operator; a divergent replica is refused by §3.2 |
| `telemetryStoreMigrator.enableReplication: false` | **no**: chart validation fails with `fusescope-schema`, and the precheck refuses | [obs] is the replicated schema; a non-replicated one was never observed |
| ClickHouse Cloud | **no**, refused | no server configuration for `keeper_map_path_prefix`, and engines are rewritten |
| Managed services that do not expose Keeper or server configuration | **no**, refused | no Keeper, so neither the lease nor `ON CLUSTER` |

A read-only precheck runs first, on every replica. It verifies:
- the server version is in the CI matrix of §9 (initially the chart's pinned 25.12; other
  versions are refused until they are added to the matrix and pass it);
- `system.zookeeper` is readable;
- the macros and the cluster exist;
- the clocks of the replicas, read with `now64(3)` on each, differ by less than 30 seconds, since
  the lease's expiry is evaluated on the server that runs each update (§6, §11).

`keeper_map_path_prefix` is not visible in any `system` table (spike, §11), so it cannot be part of
the read-only precheck. The bootstrap's first `CREATE TABLE … ENGINE = KeeperMap` reports its
absence (`Code: 36, BAD_ARGUMENTS: KeeperMap is disabled because 'keeper_map_path_prefix' config is
not defined`), before any telemetry write. That error is retryable (exit 2), not a refusal. The
message names the setting, and nothing has been written to the telemetry databases; part of
`fusescope_schema`'s layout may exist, on replicas that already have the setting during a rolling
restart, and the next bootstrap completes it (§3.1). The setting may be on its way: on an upgrade, the chart's ClickHouse restarts with it while the
migrator's Job already runs (observed on kind, 2026-11-07: the pod was replaced 30 to 75 s after the
upgrade). A server that never gets it exhausts the Job's retries with the same message.

A failed precheck is a refusal (§7.6) that names the missing item. There is no separate probe
table: the bootstrap (§3.1) and the lease's conditional acquire and read-back (§6) are the test that
`KeeperMap` works in strict mode. They write only to `fusescope_schema`, before anything is written
to the telemetry databases. If they fail, the run exits 2 and the telemetry databases are untouched.

## 3. History and coordination

`fusescope-schema` keeps its state in database `fusescope_schema`, created `ON CLUSTER`, in
`KeeperMap` tables, shared by every replica and shard. Writes are linearizable; reads are only
sequentially consistent [ch], so no decision rests on a read alone. Every write runs with
`keeper_map_strict_mode = 1`. Under it, an `INSERT` of an existing key fails, and an
`ALTER … UPDATE` or `DELETE` fails when the row changed since the mutation read it [ch]. Safety
rests on these conditional writes and the lease token (§6). A read-back after one's own write
confirms the result: it is made on the same server, which sees its own Keeper session's writes.
The spike before acceptance verifies this (ADR 0010).

| Table | Key | Columns |
|---|---|---|
| `meta` | `name` | `name String`, `value String`. The row `coordination = 1` is the version of these tables' own layout. The row `cluster_id` is a random identity of the cluster, written once (§3.2) |
| `lease` | `name` | `name String` (only `migrate`), `owner String` (`host/pid/random`), `token UInt64` (generation), `expires_at DateTime64(3)` |
| `migrations` | `id` | `id UInt32`, `name String`, `checksum FixedString(64)` (SHA-256 of the statements after substitution, §5), `state Enum('started', 'done')`, `adopted Bool`, `token UInt64` (the lease generation that wrote it), `started_at DateTime64(3)`, `finished_at Nullable(DateTime64(3))`, `tool_version String`, `host String` |

### 3.1 Bootstrap

The bootstrap reads before it writes.

1. If `fusescope_schema` exists on any replica:
   - read `meta.coordination`;
   - compare, on every replica (`clusterAllReplicas` over `system.tables` and `system.columns`),
     the structure of `meta`, `lease` and `migrations` with the layout of that version: the
     columns in order, `primary_key`, and `engine_full`, which names the KeeperMap path, as in
     `KeeperMap('/fusescope_schema/lease') PRIMARY KEY name` (observed on 25.12, §11).

   It goes on only if every table present on every replica matches this binary's layout and every
   replica's `coordination`, read on that replica, is this binary's.

   ClickHouse resolves a table function's structure on the server it is sent to, and fails on a
   replica without the table (observed on 25.12 for `clusterAllReplicas` over the table and over
   `merge()`). So the version can be read on every replica only once each has `meta`: when `meta`
   is on some replicas only, the bootstrap first creates it where it is missing, `IF NOT EXISTS`
   and without rows, then reads the version on every replica. That `CREATE` is the only write that
   can precede a refusal; when `meta` is on every replica, a refusal writes nothing. Otherwise it refuses (§7.1) without writing. That covers a newer layout, an
   unknown table in `fusescope_schema` and a changed column.
2. If `fusescope_schema` is absent, or holds this binary's layout only partly (some tables
   missing, or `meta` and `lease` without their rows), it completes what is missing:
   - `CREATE DATABASE IF NOT EXISTS` and `CREATE TABLE IF NOT EXISTS … ON CLUSTER`. Concurrent
     runs converge, because each table maps to the same Keeper path.
   - Insert `meta(coordination, 1)`, `meta(cluster_id, <random UUID>)` and
     `lease(migrate, '', 0, 0)`, each in strict mode, so the first insert wins. A "key exists"
     failure means another run got there first, and is ignored.
3. It reads the structures on every replica again, and refuses unless each replica shows all three
   tables exactly. It reads back `meta.coordination`, `meta.cluster_id` and the lease table, which
   must hold the single `migrate` row with fields the protocol of §6 writes: token 0 with no owner
   and a zero expiry, or a positive token with a positive expiry, owned or released.

The bootstrap creates only `fusescope_schema`. It never touches the telemetry databases, and it
decides nothing about them.

### 3.2 Cluster identity

`keeper_map_path_prefix` cannot be read (§2.1). The spike (§11) showed two ways a replica can be
out of step:
- **It restarts with another prefix or ensemble.** Its existing `KeeperMap` tables then fail to
  activate (`Code: 668, INVALID_STATE`), so it cannot answer.
- **Its coordination tables are created fresh under another prefix or ensemble,** for example a
  new replica configured differently. Then it sees another `KeeperMap`, with its own lease that it
  can acquire and confirm as owner: a split brain.

Two checks guard against this. Both read every replica of the cluster (`clusterAllReplicas`), and
a replica that does not answer fails them.

1. **Identity, before acquiring the lease.** Every replica must hold the same non-empty
   `meta.cluster_id`. This catches a replica bootstrapped on its own. It cannot catch a replica
   whose coordination state was copied from the original, since the copy carries the same
   identity.
2. **The lease itself, after acquiring, after every renewal, and before every statement**
   (§5.1). Every replica must show exactly the holder's `owner`, `token` and the `expires_at` of the
   last confirmed acquire or renewal. Reads are only sequentially consistent, so a replica behind a
   lagging member of an ensemble may briefly show an older value. The runner therefore re-reads a
   differing replica every 500 ms for up to 10 seconds. If the replica still differs, that is a
   divergence; an older `expires_at` is never accepted.

   This leaves no fencing margin to an out-of-step store. A store that showed an older expiry could
   let another migrator acquire it up to a renewal period earlier than the real lease expires.
   Requiring the exact value closes that gap.

   A copied, independent store fails this check as soon as one side changes its lease. So does a
   replica on another ensemble, however it was set up.

Either failure is refusal §7.6, naming the replicas that differ. After acquiring, it stops the
run before the first statement. After a renewal, it stops the run before the next statement. Then
the run releases the lease, if it can, and exits 2, or 4 if a migration is `started` (§1). `check`,
`plan` and `status` run the identity comparison, and report the lease comparison without acting on
it.

## 4. Starting points

The starting point is decided by `migrate` **after the lease is held** (§6), and again on every
run. `plan` shows its own reading, marked advisory.

| Starting point | How it is recognised | What `migrate` does |
|---|---|---|
| **Empty** | none of the six databases holds an object on any replica, and `migrations` is empty | runs every migration from `0001` |
| **Fork 0.145.0** | `migrations` is empty, and the live schema equals [obs] (§4.1), allowing for §8 | records `0001` as `done` with `adopted = true`, without running it, then runs the later migrations |
| **Managed** | `migrations` has rows | checks §7, then runs the pending migrations |
| **Anything else** | none of the above | refuses (exit 1), listing each object missing, extra or different |

### 4.1 Structural comparison

Schemas are compared as structures read from `system` tables on every replica
(`clusterAllReplicas`), not as `SHOW CREATE` text:

| Object | Compared |
|---|---|
| Table | the engine and every engine argument, parsed from `engine_full`: the Keeper path (with `{uuid}`, `{shard}` and `{replica}` in place) and the replica name, then each parameter, such as Replacing's version (and is-deleted) column, Summing's columns, Collapsing's sign, VersionedCollapsing's version, Graphite's rule set and Distributed's sharding key, canonicalised with `formatQuery`; `sorting_key`, `primary_key`, `partition_key`, `sampling_key`; the TTL expression (subject to §8); the settings |
| Column (`system.columns`) | name, position, type, `default_kind`, `default_expression`, `compression_codec`, `comment` |
| Data-skipping index (`system.data_skipping_indices`) | name, type and its arguments, expression, granularity |
| Projection | name and query |
| Materialized view | target table and, for a refreshable one, the `REFRESH` clause, both from `create_table_query`; the query from `system.tables.as_select`; for the seven views of §5.4, the definer (`system.tables.definer`) and `SQL SECURITY` (from `create_table_query` without its query) |
| Distributed table | cluster, database, table, sharding key |
| Dictionary | presence only, for now: no migration creates a dictionary, so one found is an extra object. Its source, layout, structure and lifetime are compared from the first migration that creates one |

- Every expression is canonicalised by the server with `formatQuerySingleLine` before it is
  compared, so formatting differences between ClickHouse versions do not count. Each text is
  formatted inside a statement that parses it, and the wrapper is cut off again: expressions and
  keys in `SELECT …`, queries as they are, the TTL in `ALTER TABLE t MODIFY TTL …` and the settings
  in `ALTER TABLE t MODIFY SETTING …`. The connected server formats every replica's texts. The
  observed schema needs this: in a TTL the server prints `(a + b) + c` where a `SELECT` prints
  `a + b + c`, and it drops a rule's `DELETE` (observed on 25.12).
- Database names are replaced by their role, as `{db:<role>}` (`{db:logs}`, `{db:traces}`, …), so
  custom names compare: a qualifier in an expression or query, a materialized view's target, and
  a Distributed table's database argument.
- The cluster name in a Distributed table's arguments is replaced by `{cluster}`.
- A view's definer is replaced by `{definer}` when it is the connected user or `default` (§5.4).
- Columns keep their position. A column §8 allows must come after every expected column, since
  the backend adds them with `ADD COLUMN` and no `FIRST` or `AFTER`, and an `_exists` column after
  its promoted column.

A difference on a single replica counts.

## 5. Migrations

Migrations are compiled into the binary, ordered by `id`, and never edited once released. A
correction is a new migration. Each has:

- `id` and `name`;
- `statements`: DDL with the placeholders `{cluster}` and the database roles, each statement
  idempotent (`CREATE … IF NOT EXISTS`, `DROP … IF EXISTS`, `ALTER … ADD COLUMN IF NOT EXISTS`,
  or guarded by a precondition the runner checks first);
- `postconditions`: the complete structure (§4.1) of the whole schema after it, every object of
  the six databases and not only the ones it changes. Each migration's post-conditions are
  therefore cumulative, and the live schema is always compared whole;
- `preconditions`: the previous migration's post-conditions (none for `0001`), the whole schema as
  it must be before the migration runs. The runner checks them before step 1 of §5.1, as the
  post-conditions of the last `done` migration. The objects a migration creates, alters or drops
  are those that differ between its pre- and post-conditions, which `repair --mark absent` (§10.1)
  restores;
- `reversible`: whether its effect can be undone by hand to exactly `preconditions`. A migration
  that drops or rewrites data, such as dropping a column, is `false`.

`0001 base` creates the six databases and every object of [obs], in dependency order: local
tables, then distributed tables, then materialized views after their targets.

- Its statements, `internal/migrations/sql/0001_base.sql`, were generated once from the [obs] DDL
  (`TestGenerateBase`): each object `IF NOT EXISTS` and `ON CLUSTER`, with database and cluster
  names replaced by placeholders. They are frozen from then on.
- Its post-conditions, `post/0001_base.json`, are a separate artifact. They are read from a cluster
  where the [obs] DDL was applied as it is, not through `0001`'s statements, and a test requires
  that read to equal the artifact on every replica. Another test runs `0001` on an empty cluster
  and compares the result with the artifact, so neither is checked against itself.
- The checksum is the SHA-256 of the id, the name, `reversible`, the statements and the
  post-conditions, and of the data statements and data conditions when a migration has any
  (§5.2). A test pins it. The preconditions are the previous migration's
  post-conditions, covered by that migration's checksum. A history row is accepted only with this
  binary's id, name and checksum.
- The placeholders render as quoted identifiers (`{db:<role>}`, `{cluster}`) or as string
  literals (`'{db:<role>}'`, `'{cluster}'`, in a Distributed table's arguments).
- [obs] includes the fork's history tables (`schema_migrations_v2`). `0001` creates them empty, and
  the comparison reads their structure from the system tables. No query ever reads their rows.

### 5.1 Running one migration

1. Under the lease, insert `migrations(id, …, state = started, token = <lease token>)`. Strict
   mode fails if the `id` exists.
2. Run each DDL statement `ON CLUSTER {cluster}` with `distributed_ddl_output_mode = throw` and
   `distributed_ddl_task_timeout`, so every host has applied it or the statement fails.
   - Before each statement, the runner checks that its lease is still valid (§6) and that every
     replica shows it (§3.2). If either fails, it stops.
   A data statement (§5.2) runs once, through the connected server, as §5.2 says.
3. Check the post-conditions on every replica: the structure (§4.1) and the data conditions
   (§5.2).
4. `ALTER TABLE migrations UPDATE state = 'done', finished_at = now64(3) WHERE id = N AND
   token = <lease token>`. Strict mode makes it fail if another lease generation wrote the row.

### 5.2 Data conditions

A migration may also own rows. It then has, besides its structure:

- **data statements**: `INSERT … SELECT` statements, each marked as data in the migration file.
  They are not run `ON CLUSTER`. Each runs once, through the connected server, into the
  Distributed table with `distributed_foreground_insert = 1`. Every row then lands on the shard its
  sharding key picks, as the backend's inserts do, and the statement returns only after that
  shard has written it. Every data statement must be idempotent by key: it inserts only the keys
  that no shard holds yet, so running it again inserts nothing;
- **data conditions**, cumulative like the post-conditions: each migration's set is the whole set
  that must hold after it, and not only the rows it adds. A later migration copies the previous
  set and changes it, for example by widening a condition's rows. Each condition names:
  - a replicated local table by role, such as `{db:metadata}.column_evolution_metadata`;
  - its **key**: the table's whole sorting key, which must equal the `sorting_key` in the
    structural post-conditions;
  - its **scope**: a predicate on the key columns, selecting the rows the condition owns. Rows
    outside the scope are not the condition's business;
  - its **rows**: the exact set of keys in the scope, and for each a constraint on every other
    column: a value, or "greater than zero".

  The checksum covers the data statements, each with its kind, and the data conditions. A
  migration with neither hashes exactly as before, so `0001`'s pinned checksum does not change.

A migration with data statements, or whose data conditions differ from the previous migration's,
is **not reversible**: `repair --mark absent` compares only structure (§10.1), so it could not
prove its rows gone. Loading the migrations fails otherwise, and a test covers that.

**Reading.** On every replica of every shard, the local table is read in the scope, grouped by
the key. Every other column is read as the engine merges it: for
`SimpleAggregateFunction(f, T)`, `f(column)`. Unmerged parts therefore read as the merged row.
The replicas of one shard must show the same rows. A replica that differs is re-read every
500 ms for up to 10 seconds, as in §3.2, since replication may lag behind an insert. If it still
differs, that is a divergence (§7.4). A replica that cannot be read, or a run whose context ends
during the re-reads, is not a divergence: it is the retryable error of §1, exit 2, or 4 once a
migration is `started`. Then the shards are united. A key held by more than one
shard is a contradiction, since each key must be one row in the cluster.

A data condition is checked in two ways:

- **It holds** when the keys in the scope across the cluster are exactly its rows, each on one
  shard, and every constraint holds.
- **It admits** the live rows when every key in the scope is one of its rows, on one shard, with
  its constraints met. Missing keys are what the migration inserts. A partial state is admitted,
  and only the missing rows are inserted.

Only one set is ever checked against the live rows, as for the structure: that of the last
`done` migration, or the next migration's while it runs.

1. On every run, `migrate`, `check` and `status` require the set of the last `done` migration to
   hold (§7.4). Sets of earlier migrations are not checked: a later set replaced them.
2. Before the next migration's `started` row is written, its set must admit the live rows. If it
   does not, that is a contradiction (§7.8), and nothing is written. When several migrations are
   pending, only the next one's set is checked against the live rows, since each later set starts
   from the rows the one before it leaves. `check` and `plan` report the same refusal (exit 1).
   `status` evaluates the same admission and prints it: the rows the next migration would insert,
   or each key in its scope that is extra, on more than one shard or with a broken constraint. It
   still exits 0 whenever it could read (§1).
3. After its statements and before `done`, the migration's own set must hold. From then on it is
   the set of step 1. `repair --mark done` checks the same (§10.1).

### 5.3 `0002 column_evolution_baseline`

A fork install leaves five rows in `signoz_metadata.column_evolution_metadata` [obs rows]. The
backend reads them, through the Distributed table, ordered by `release_time` (`pkg/telemetrymetadata/metadata.go`,
about line 2447 [be]), to decide which column holds a field for a time range. With no rows at
all, it falls back to every candidate column (`SelectEvolutionsForColumns`,
`pkg/types/querybuildertypes/querybuildertypesv5/evolution.go`, about line 23 [be]): the legacy
maps. An install by `0001` alone has no rows, so the backend would read it unlike a fork install.
The rows are the schema's bootstrap state, not telemetry, so `0002` writes them.

- **Statements:** no DDL. One data statement inserts, through
  `{db:metadata}.distributed_column_evolution_metadata`, the rows below whose key no shard holds.
  Every v1 row it inserts takes one instant, `toUnixTimestamp64Nano(now64(9))`, evaluated once for
  the statement. That is nanoseconds, the unit the backend reads (`release_time` is
  `SimpleAggregateFunction(min, Float64)` holding nanoseconds since the epoch [obs]); `now()` is in
  seconds and would be read as a time in 1970. The v0 rows take `0`.
- **Structure:** `0002`'s post-conditions equal `0001`'s.
- **Data condition:** the table's key is
  `(signal, column_name, column_type, field_context, field_name, version)` [obs]. The scope is
  `field_name = '__all__'`, the base rows. Rows for promoted paths, whose `field_name` is the path
  and which the backend writes itself (`PromotePaths`, `pkg/telemetrymetadata/body_json_metadata.go`,
  about line 466 [be]), are outside the scope and allowed. The rows, from [obs rows]:

  | `signal` | `column_name` | `column_type` | `field_context` | `version` | `release_time` |
  |---|---|---|---|---|---|
  | `logs` | `resource` | `JSON()` | `resource` | 1 | > 0 |
  | `logs` | `resources_string` | `Map(LowCardinality(String), Float64)` | `resource` | 0 | 0 |
  | `traces` | `attributes` | `JSON()` | `attribute` | 1 | > 0 |
  | `traces` | `resource` | `JSON()` | `resource` | 1 | > 0 |
  | `traces` | `resources_string` | `Map(LowCardinality(String), Float64)` | `resource` | 0 | 0 |

  `field_name` is `__all__` in each. The type text of `resources_string` is as observed, and it
  is compared byte for byte.
- **Existing rows are kept:** adoption writes nothing when all five are there, and their
  `release_time` stays as the fork wrote it. A partial state receives only the missing rows.
  Another `__all__` row, such as a base row with another type or version, is a contradiction:
  `0002` refuses before `started` (§7.8). So does a v0 row whose `release_time` is not 0, or a
  v1 row whose `release_time` is not greater than 0.
- **Not reversible.** After `0002`, going back to the fork's migrator is not supported (§10).

### 5.4 View definers

Seven refreshable views of [obs], `signoz_metrics.samples_v4_reduced_{last_30m, last_5m,
last_60s, sum_30m, sum_5m, sum_60s_cumulative, sum_60s_delta}_mv`, are created with
`DEFINER = default SQL SECURITY DEFINER`, and `0001`'s statements hold that text. A refresh runs
as the definer. On a ClickHouse without a user `default`, creating them fails (`UNKNOWN_USER`,
observed on 25.12 with `fusescope-schema` 0.4.0, which then exits 4 with `0001` started).

- **Rendering:** each statement's exact text `DEFINER = default SQL SECURITY DEFINER` renders as
  `DEFINER = CURRENT_USER SQL SECURITY DEFINER`, which binds the view to the user that creates it
  [ch]. A statement holds it at most once, and only in a materialized view's own statement:
  loading the migrations fails otherwise. `0001`'s text and checksum stay as released.
- **Comparison:** for these seven views, in every migration's post-conditions, `SQL SECURITY` is
  `DEFINER` and the definer is the connected user or `default`. `default` is what the fork and
  `fusescope-schema` 0.4.0 leave, so their installs are accepted as they are and nothing is
  re-created. Any other definer, no definer, or another `SQL SECURITY` (`NONE`; ClickHouse refuses
  `INVOKER` for a materialized view, observed on 25.12) is drift (§7.4) on that replica. The post
  files store neither field: they are set from the statements when the migrations load.
- **Changing the migrator's user** after an install leaves the views bound to the previous one.
  When that one is not `default`, it is then another definer: drift (§10). When it is `default`
  (a fork or 0.4.0 install), the views stay accepted under any later user, since the live schema
  does not tell a legacy `default` from a previous migrator's user; they keep refreshing as
  `default`.

## 6. Lease

The lease is the single row `lease(migrate)`. Every change is a conditional update in strict
mode.

1. **Acquire.** Read the row (`owner`, `token`, `expires_at`). It is free when `owner` is empty,
   or when `expires_at` has passed and no `migrations` row is `started` with that `token`. If it
   is free, run:

   ```
   ALTER TABLE lease UPDATE owner = <me>, token = <token + 1>, expires_at = now64(3) + 5 min
   WHERE name = 'migrate' AND token = <token read>
     AND (owner = '' OR expires_at <= now64(3))
   ```

   Then read the row back. The lease is held only if it shows `<me>` and `<token + 1>`. A failed
   update or a different read-back means another process won; retry until `--lock-timeout`
   (default 10 minutes), then exit 2.
2. **Renew.** A background task renews every 60 seconds with
   `UPDATE expires_at = now64(3) + 5 min WHERE name = 'migrate' AND owner = <me> AND token = <mine>
   AND expires_at > now64(3)`. A lease that has already expired is never revived, even by its
   owner. Each renewal is confirmed by reading it back.
   - When a renewal fails, or 4 minutes pass without a successful one, the lease is lost.
   - From then on the runner issues no new statement. The current one, idempotent, may finish.
   - The migration in progress stays `started`, and `migrate` exits 4 (if no migration was
     `started` yet, it exits 2).
3. **Release.** `UPDATE owner = '' WHERE name = 'migrate' AND owner = <me> AND token = <mine>`.

The timings must keep the holder's own deadline inside the lease on every server: the runner
refuses a configuration unless the owner is set, every duration is positive, the renewal period
is shorter than the loss deadline, and loss deadline + re-read window (§3.2) + allowed clock
skew (§2.1) ≤ lease duration; the defaults give 4 min + 10 s + 30 s ≤ 5 min. A statement check
that starts just before the loss deadline then ends before the lease can expire anywhere.

Acquire, renewal, the statement check and release run one at a time in a runner, so a comparison
never meets a renewal it did not read. Every store call is bounded by the deadline of the step
that makes it. The comparison's own deadline is exact: its last re-read waits only for the time
left, and a read showing the lease counts only if it ends within the re-read window.

Replicas are compared by `hostName()` and `serverUUID()`. The listing must hold as many entries as
`system.clusters` lists for the cluster, with no key twice, since a key twice means one server
answering for two replicas. A failure to list them is a divergence, never a fallback to the
replicas that answered.

Every condition is evaluated by the server inside the conditional update, so an acquire cannot
take a lease renewed after it was read: the renewal leaves `owner` set and `expires_at` in the
future.

A later migrator sees an expired lease over a `started` row with the old token. That row was
interrupted, so §7.3 refuses: the next generation never resumes the previous one's work silently.

## 7. Refusals (fail closed)

`migrate` exits 1 without writing to the telemetry databases when:

1. `meta.coordination` is newer than this binary, or a history row has an `id` this binary does
   not know;
2. a recorded checksum differs from the binary's for the same `id`;
3. a row is `started` and its `token` is not the current lease's, or the lease has expired: the
   migration was interrupted;
4. the live schema differs from the post-conditions of the last `done` migration, beyond §8, or
   that migration's data conditions do not hold (§5.2);
5. the starting point is "anything else" (§4);
6. the precheck fails (§2.1);
7. the TLS settings break ADR 0004;
8. a pending migration's data condition does not admit the live rows (§5.2): a key in its scope
   that is not one of its rows, a key on more than one shard, or a constraint broken. This is
   checked before the `started` row is written.

`check` reports these the same way (exit 1), with one difference. A `started` row whose token is
the current lease's, with an unexpired lease, is **in progress**: `check` returns 3, and with
`--wait` it keeps waiting. `check` reads the lease before the history, so a `started` row the
lease it read does not cover may have been written after that read: the lease is read once more,
after the row, before the row counts as interrupted.

Each refusal names its runbook section (§10).

## 8. Schema changes the backend makes

The backend changes the schema at runtime [be]. These changes are allowed by pattern, both in the
comparison and in the post-conditions, and each pattern has a test. Anything else, even if it
looks similar, is drift.

| Pattern | Tables | Source |
|---|---|---|
| Columns `<prefix>_<datatype>_<key>` added with `DEFAULT <map>['<key>'] CODEC(ZSTD(1))`, where `<map>` is `attributes_<type>` or `resources_<type>` | `logs_v2` and `distributed_logs_v2`; `signoz_index_v3` and `distributed_signoz_index_v3` | promoted fields: `clickhouseReader/reader.go` (logs about line 2629, traces about line 2770) |
| Their companion `<column>_exists Bool DEFAULT if(mapContains(<map>, '<key>') != 0, true, false) CODEC(ZSTD(1))` | the same | the same |
| Indexes `<column>_idx` over a promoted string or number column, of a type the backend accepts (`minmax`, `set(n)`, `bloom_filter[(rate)]`, `tokenbf_v1(…)`, `ngrambf_v1(…)`, as `indexTypeRe` bounds them) and any granularity; and `<column>_minmax_idx … TYPE minmax GRANULARITY 1` over a promoted number column. The expression is the column alone | the local tables above | `reader.go` about lines 2667, 2808 and 2823; the types in `pkg/types/promotetypes/types.go` about line 25 |
| RUM promoted columns, `_exists` columns and `_idx` indexes, of the same shapes | `signoz_index_v3`/`distributed_signoz_index_v3` and `logs_v2`/`distributed_logs_v2` (`promoter.go` about lines 38–39) | `implrumapplication/promoter.go` about lines 118–124 |
| Skip indexes on JSON subcolumns, named `<column>.<path>_<jsontype>_<indextype>`, where `<indextype>` is the base of the index type (one of the types above) and `<column>` is the target's base or promoted JSON column, with any granularity. For logs, `<jsontype>` is `string`, `int64` or `float64` and the expression `assumeNotNull(dynamicElement(<column>.<path>, '<Type>'))`, wrapped in `lower(…)` for `String`, a path segment holding `-`, `:` or `@` backquoted. For traces, `<jsontype>` is `string` or `float64` (Int64 is folded into Float64), the path is one backquoted identifier, and the expression is `` <column>.`<path>`::String ``, or `` toFloat64OrNull(<column>.`<path>`::String) `` for `float64` | `logs_v2` (`body_v2`, `body_promoted`); `signoz_index_v3` (`attributes`, `attributes_promoted`) | `pkg/modules/promote/implpromote/module.go` about line 255; names and logs expressions in `pkg/telemetryschema/chschema/index.go` about lines 55–103; traces expressions in `pkg/types/promotetypes/target.go` about line 32 and `tracestelemetryschema/storage.go` about line 343; the indexable types in `pkg/types/telemetrytypes/json_datatype.go` |
| The `DEFAULT` of `_retention_days` and `_retention_days_cold`, the type `UInt16` and everything else unchanged. `_retention_days_cold` takes a number. `_retention_days` takes a number or the custom retention's `multiIf(<cond>, <days>, …, <default days>)`, each `<cond>` one or more `<key> IN ('<value>', …)` joined by `AND`, where `<key>` is `resources_string['<k>']` on the logs tables and `JSONExtractString(labels, '<k>')` on the resource tables, and every value a string literal and every days value a number. The rules are read from the live text and the backend's text rebuilt from them must canonicalise to it | `logs_v2`, `distributed_logs_v2`, `logs_v2_resource`, `distributed_logs_v2_resource` | `reader.go` about lines 1273–1349 (`buildMultiIfExpression` about line 1408) |
| The table TTL, as the retention settings write it: `<time> + <delete> DELETE`, optionally followed by `, <time> + <move> TO VOLUME '<volume>'`, with any numbers and volume. `<time>` is `toDateTime(timestamp / 1000000000)` for `logs_v2`, `toDateTime(seen_at_ts_bucket_start) + toIntervalSecond(1800)` for the logs and traces resource tables, `timestamp` for the logs keys tables, `toDateTime(end)` for `trace_summary`, `toDateTime(timestamp)` for the other traces tables and `toDateTime(toUInt32(unix_milli / 1000), 'UTC')` for the metrics tables. `<delete>` and `<move>` are `INTERVAL n SECOND`; for `logs_v2` and `logs_v2_resource` also `toIntervalDay(_retention_days)` and `toIntervalDay(if(_retention_days_cold = 0, n, _retention_days_cold))`, and for the keys tables `toIntervalDay(n)`. The keys tables and `span_attributes_keys` take no move rule | logs: `logs_v2`, `logs_v2_resource`, `logs_attribute_keys`, `logs_resource_keys`; traces: `signoz_index_v3`, `traces_v3_resource`, `signoz_error_index_v2`, `usage_explorer`, `dependency_graph_minutes_v2`, `trace_summary`, `span_attributes_keys`; metrics: `samples_v4`, `samples_v4_agg_5m`, `samples_v4_agg_30m`, `exp_hist`, `time_series_v4`, `time_series_v4_6hrs`, `time_series_v4_1day`, `time_series_v4_1week` | `clickhouseReader/ttl_statements.go`; `pkg/types/retentiontypes/ttlclause.go`; the tables in `reader.go` about lines 942, 1059 and 1734 |

Each pattern builds the text the backend writes, from the names it parses, and the server's
canonical form of that text must equal the live one. For a TTL, the template is rendered with
sentinel numbers and volume, canonicalised, and matched as an anchored pattern where only the
sentinels vary.
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
| Fresh install | the live schema equals [obs] and the five base rows of §5.3 are there, the v1 rows with one `release_time` in nanoseconds within the run and the v0 rows with 0; history `0001` and `0002` done |
| Adopting a fork install (the fork image run as a black box) | `0001` adopted; `0002` inserts nothing; every row's bytes and `release_time` unchanged |
| A cluster made by chart 0.162.0 (`fusescope-schema` 0.4.0): `0001` done, the table empty | `0002` inserts the five rows |
| Some of the five rows present (each subset tested: the v0 rows only, one v1 row only, four of five) | only the missing rows inserted; the present ones unchanged |
| A contradictory base row (`traces`/`attributes`/`__all__` with another `column_type` or `version`), a v0 row with `release_time` ≠ 0, a v1 row with `release_time` 0, the same key on two shards | refused (§7.8) before `started`; history and table unchanged |
| Promoted-path rows (`field_name` ≠ `__all__`) before and after `0002` | allowed; untouched |
| A base row deleted after `0002` is `done` | `check`, `status` and `migrate` refuse (§7.4), naming the row |
| The response to `0002`'s insert lost (the connection cut after the statement is sent) | exit 4; `status` shows whether the data condition holds; `repair --mark done` succeeds when it holds and is refused when it does not; `--mark absent` refused (not reversible) |
| A replica of the row's shard lagging behind the insert | the post-condition waits for it within the re-read window, then holds; a replica that never catches up is a divergence |
| `status` with `0001` done and the next migration's set contradicted (an extra `__all__` row), or partly met (two of the five rows) | exit 0 in both; it names the contradictory row, or lists the three rows `0002` would insert; `check` exits 1 and 3 respectively |
| A replica of a data condition's table unreachable, before `started` and after it | exit 2 before, exit 4 after; never a divergence (§7.4) |
| The run's context cancelled during the re-reads of a lagging replica, before the 10 s pass | exit 2 before `started`, 4 after; never a divergence |
| A later migration widening `0002`'s rows (a test-only `0003` adding a sixth base row) | before it, `0002`'s set must hold and `0003`'s must admit; after it, only `0003`'s set is checked, and the sixth row is not refused |
| A migration with data statements or changed data conditions marked reversible | loading the migrations fails |
| Fresh install connected as a user that is not `default` (§5.4) | on each of the four replicas, the seven views exist, are bound to that user with `SQL SECURITY DEFINER`, and refresh; `0001`'s checksum is the released one; a second run is a no-op |
| Adopting the fork's views, bound to `default`, connected as another user | `0001` adopted; no view touched; the views still bound to `default` and refreshing |
| One view on one replica bound to a third user, another with `SQL SECURITY NONE` | drift on that replica, naming the definer and the `SQL SECURITY`; nothing written |
| Each `0002` case above, with Keeper and with ZooKeeper, on the 2 × 2 cluster | the same results |
| Adopting a cluster the fork 0.145.0 migrated (the fork image run as a black box) | `0001` adopted; nothing re-created; data still readable |
| The same, after the backend promoted fields, added RUM columns and changed retention | adopted (§8); a promoted column with a changed codec is refused |
| Running `migrate` twice | the second run is a no-op |
| 8 concurrent `migrate` processes against an empty cluster | one holds the lease and classifies; the others wait, re-classify as Managed and exit 0; each statement runs once |
| A lease taken over while a migrator pauses in a statement (renewals blocked) | the old migrator issues no further statement and exits 4; the new one refuses on the `started` row (§7.3) |
| `SIGKILL` between two statements, then a rerun | refused with `started`; after `repair` (§10.1) it completes |
| `SIGKILL` while holding only the lease | after expiry the next run takes it over |
| `check --wait` during a running migration | keeps waiting, then 0 |
| `check` on an interrupted migration | 1 |
| An unknown history row, a changed checksum, an extra table, a replica missing a table | each refused with its reason |
| `repair --mark done` with the post-conditions unmet; `--mark absent` with the live state not exactly the pre-conditions; `--mark absent` on an irreversible migration | refused |
| Acquire racing a renewal (the waiter read the row before the owner renewed) | the acquire changes nothing, its read-back shows the other owner, and it keeps waiting |
| Renewing after expiry | refused; the old owner stops |
| A newer `meta.coordination`, an unknown table in `fusescope_schema`, a partial bootstrap of this layout | refused without writes; refused without writes; completed |
| Promoted JSON subcolumn indexes (`promote` API) on logs and traces, then adoption | adopted; the same index with another expression is refused |
| A Replacing engine whose version column differs from [obs] | refused |
| A failure before any `started` row, and one after | exit 2, then a clean rerun; exit 4, then the rerun refuses |
| TLS 1.3 with a password file; plaintext opt-in; `verify: false`; TLS below 1.3 | connects as configured; TLS below 1.3 refused |
| Custom database names | migrated and compared under the custom names |
| Precheck failures: no Keeper, no macros, replicas' clocks 60 s apart | each refused with what is missing |
| No `keeper_map_path_prefix` | exit 2, naming the setting; nothing written to the telemetry databases; a later run completes `fusescope_schema` once the setting is in place |
| Each concurrency and lease case above, with each coordination service: Keeper (one node and a 3-node ensemble) and the chart's ZooKeeper (`signoz/zookeeper:3.7.1`, one node as the chart deploys it by default, and a 3-node ensemble) | the same results with every service |
| The coordination service stopped during a migration, then started again: a single node, or the member of a 3-node ensemble the migrator's server holds its session on | **Single node: exit 4.** No statement is sent after the lease is lost, `0001` stays `started` and the history unchanged, the rerun and `check` are refused, and only §7 (finish by hand, `repair`) recovers it. **Ensemble: exit 0 or 4.** With the member down, the two others stay up, and a key written through one is read through the other, so they hold a quorum and one state. Exit 0 means everything is done and `check` is 0. Exit 4 is as for a single node. **Either way, never 1, 2 or a hang.** Which of 0 and 4 an ensemble gives depends on how the client meets the interruption. The results are observed, not an availability guarantee: on 2026-11-07 both ensembles exited 0 in some runs and ZooKeeper's 4 in another, and single nodes always exited 4 |
| A replica with another `keeper_map_path_prefix`, and a replica on another coordination ensemble | refused by §3.2 before the lease, naming the replica; nothing written to the telemetry databases |
| A replica on another ensemble whose coordination state was copied from the original, so the same `cluster_id` | the identity check passes; the lease comparison after acquiring fails, and nothing is written |
| A replica briefly behind on `expires_at`, catching up within 10 s (a lagging ensemble member) | accepted after the re-reads |
| A replica that stays one renewal behind, or two, on `expires_at`, or shows another `owner`/`token` | refused after 10 s of re-reads |
| Killed during the bootstrap, at each step of §3.1 | the rerun completes the bootstrap and nothing is left in `fusescope_schema` outside the layout |

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
| A chart upgrade replaced a Job still migrating | See §10.2 |
| Unknown or changed history (§7.1, §7.2) | Run the newer binary that wrote it. Never edit history by hand |
| Schema drift (§7.4, §7.5) | `status` lists the differences. Restore the expected objects, or drop the extra ones, by hand, then rerun |
| Precheck failures (§7.6) | Add what is named, for example the `shard` and `replica` macros, and restart ClickHouse |
| No `keeper_map_path_prefix` (exit 2, §2.1) | Set it in the server configuration, the same on every replica, and restart ClickHouse; the next run retries. The chart's ClickHouse sets it, and its Job retries while the pods restart |
| A bad migration | There is no automatic rollback. Restore from backup, or write a forward migration that corrects it |
| A data condition broken (§7.4, §7.8) | `status` names each row missing, extra or different. Restore the base row as §5.3 lists it, or delete the contradictory one, by hand, then rerun. Rows outside the scope are never the cause |
| `0001` started after `UNKNOWN_USER` (0.4.0 on a ClickHouse without `default`, §5.4) | `status` lists the objects still missing. Create them by hand from `0001_base.sql`, connected as the migrator's user, with the placeholders replaced and `DEFINER = default` as `DEFINER = CURRENT_USER`; then `repair --id 1 --mark done` (§10.1) and run 0.4.1 or later |
| The migrator's user changed after an install whose views are bound to a user other than `default` (§5.4) | On every replica, `ALTER TABLE signoz_metrics.<view> MODIFY SQL SECURITY DEFINER DEFINER = <new user>` for each of the seven views, then rerun. Views bound to `default` need nothing: they stay accepted |
| Going back to the fork's migrator | Possible only while the history holds `0001` alone: the fork's own history and the [obs] schema are untouched. After any later migration, `0002` included, it is not supported |

### 10.1 `repair`

`repair` takes the lease, checks on every replica, and refuses unless:

- **`--mark done`**: every post-condition of migration `N` holds (§4.1, §5.2, §8). The row becomes
  `done` with the current token.
- **`--mark absent`**: `N` is reversible (so it owns no rows, §5.2), no migration after `N` is recorded, and the live
  structure of every object `N` touches equals `N`'s pre-conditions exactly: created objects
  absent, altered and dropped objects as they were. Any third state is refused. The row is
  deleted.

Either way, the result is verified by reading the history back before exit 0.

### 10.2 A chart upgrade that replaced a running Job

The chart names the migrator's Job by a hash of its spec and `telemetryStoreMigrator.runId`. An
upgrade that changes neither leaves a running Job alone. An upgrade that changes either replaces
the Job: Helm, or Argo CD, deletes the old one. If the old Job was still migrating, its pod gets
`SIGTERM`, stops before its next statement, and exits 4 (§1).

- **Symptom.** If the stopped run released its lease, or once an unreleased lease expires, the new
  Job and the collectors' `check --wait` fail with exit 1 while the interrupted row remains (§7.3),
  and the collectors' pods keep failing in their init container. Before then, `check` reports the
  migration in progress (3) and the new Job waits for the lease.
- **Diagnosis.** `fusescope-schema status` shows:
  - the `started` row;
  - the current lease state;
  - any differences from that migration's post-conditions.

  `fusescope-schema check` exits 1 once the lease is free or expired.
- **Do not just run `migrate` again**, by hand or with another `runId`. It refuses the started
  migration (§7.3), and it would not finish it in any case. Never edit the history by hand
  (§10).
- **Recovery,** by §10.1, from the state `status` shows. The lease may need to expire before
  `repair` can acquire it.
  - **Zero differences** (the run stopped after its last statement, before marking it done): run
    `repair --id N --mark done` directly.
  - **The migration's statements ran in part.**
    1. Finish the remaining statements by hand. Each is idempotent (§5).
    2. Check that `status` shows no difference from its post-conditions.
    3. Run `repair --id N --mark done`.
  - **The migration is reversible** and you would rather undo it.
    1. Undo the applied statements by hand, until every object it touches is exactly as its
       preconditions.
    2. Run `repair --id N --mark absent`.

    `0001 base` is not reversible, so for it only `--mark done` applies.

  Then set a new `runId`, so the chart runs a new Job, which finds the schema up to date.
- **Prevention.** Change neither the Job's spec (its image, its connection, its resources and
  scheduling) nor `runId` while a migration runs. `check` exiting 0, or the Job complete, says the
  run is over.

## 11. Spike results (2026-11-06)

The protocol of §3 and §6 was run with plain SQL on a 2×2 cluster, ClickHouse 25.12.5.44 (the
chart's pin), against each coordination service:
- one Keeper node;
- the chart's ZooKeeper (`signoz/zookeeper:3.7.1`, one node, as the chart deploys it by default).

[`observed/fusescope-schema-spike/run.sh`](observed/fusescope-schema-spike/run.sh) reproduces it:
`./run.sh keeper` or `./run.sh zookeeper`. It pins every image by digest, generates the four
nodes' configuration, runs each case as an assertion, and prints PASS or FAIL. Its output is in
`results-keeper.txt` and `results-zookeeper.txt`: 26 of 26 passed with each service. The script aborts (exit 2) if `docker compose` fails or any node does not reach its coordination service, so a run never passes on an unready cluster.

| Case | Result, with Keeper and with ZooKeeper |
|---|---|
| 8 concurrent bootstraps (database, `meta`, `lease`, the identity and lease rows) from the 4 hosts | the `CREATE … IF NOT EXISTS` all succeed; one insert of each row wins and seven get `Code: 999`; the same `cluster_id` on every replica, read directly and through `clusterAllReplicas` |
| Acquire on a free lease, then read back on the same server | the read-back shows the new owner and token |
| Acquire with a token read before the owner renewed | no change |
| Renewal after expiry | no change: not revived |
| Acquire of the expired lease by another process | succeeds; token + 1 |
| Release with a stale token | no change |
| 8 concurrent acquires with the same read token, each process reading back on its own node, 10 rounds | exactly one process read itself as owner and the token rose by 1, every round |
| A replica restarted with another `keeper_map_path_prefix` | its `KeeperMap` tables do not activate (`Code: 668`); the identity check fails with that error |
| A replica whose coordination tables were created under another prefix | it acquires its own lease while another process holds the real one (the split brain); the identity check finds differing `cluster_id` values |
| The same with a replica on another coordination ensemble | the identity check finds differing values |
| A replica on another ensemble with a copy of the original identity (the same `cluster_id`) | the identity check passes; the lease comparison fails, because that replica does not show the holder's `owner` and `token` |
| That replica's copied lease set to the holder's `owner`, `token` and current `expires_at`; then to the previous renewal's `expires_at`; then to the one before | the full comparison passes only with the current value; it fails with the previous one and with the one before |
| The coordination service stopped during a conditional update, then started | the update fails with an error; afterwards the lease is unchanged |
| `KeeperMap` without `keeper_map_path_prefix` | `Code: 36 … KeeperMap is disabled because 'keeper_map_path_prefix' config is not defined` |
| `keeper_map_path_prefix` in `system.server_settings` | absent (see §2.1) |

What this establishes, and its limits:

- **Mutual exclusion comes from the condition in the update and the token, not from strict mode
  alone.** A losing conditional update usually succeeds without changing anything: ClickHouse
  reports no affected rows. So the read-back of §6 is required, not a check on top. `migrate`
  treats a held lease as acquired only when its read-back shows its own owner and token.
- **A replica out of step on the coordination is refused by §3.2**, whether its tables fail to
  activate, it carries another identity, or it carries a copied identity but another lease. The
  identity alone is not enough. The lease comparison on every replica is what proves the storage is
  still shared, so it runs after acquiring, after every renewal and before every statement.
- **Read-back on the same server saw the server's own writes** in every round, with both
  services. That matches the per-session ordering of Keeper and ZooKeeper, but it is an
  observation on one ClickHouse version and topology. The CI matrix of §9 repeats these cases on
  every version and service it accepts.
- **Only single-node coordination was tested here.** 3-node Keeper and ZooKeeper ensembles, and an
  ensemble member lost during a migration, are in §9 and must pass before an ensemble is declared
  supported.
- **Time:** the conditions use the server's `now64(3)`, so the replicas' clocks bound how
  precisely a lease expires. The 5-minute lease with 1-minute renewal tolerates seconds of skew,
  not minutes. The precheck of §2.1 refuses replicas more than 30 seconds apart.
