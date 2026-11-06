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
| `migrate` | takes it | the schema and the history | 0 up to date; 1 refused (§7); 2 connection, ClickHouse or lease error **before** any migration was marked `started`: safe to retry; 4 the same error **after** a migration was marked `started`: the next run refuses (§7.3) until `status` and `repair` (§10) |
| `check [--wait D]` | never | nothing | 0 up to date; 3 pending or in progress; 1 refused. With `--wait`, polls until 0 or 1, or until D passes (then 3) |
| `plan` | never | nothing | prints the starting point and the pending migrations, marked advisory: `migrate` decides again under the lease |
| `status` | never | nothing | prints the history, the lease and any difference from the expected schema |
| `repair --id N --mark done\|absent` | takes it | one history row | 0 repaired; 1 refused: the post-conditions or pre-conditions do not match, or `absent` on an irreversible migration (§10.1); 2 connection or lease error, nothing changed |

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
not defined`), and that error is turned into the refusal §7.6, before any telemetry write.

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

   It goes on only if every table present on every replica matches this binary's layout and no
   `coordination` value is newer. Otherwise it refuses (§7.1) without writing. That covers a newer layout, an
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
   must hold the single `migrate` row with fields the protocol of §6 can write (token 0 only with
   no owner and a zero expiry).

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
| **Empty** | none of the six databases holds a table, and `migrations` is empty | runs every migration from `0001` |
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
- `preconditions`: the complete structure (§4.1) of every object the migration creates, alters or
  drops, as it must be before the migration runs (an object it creates must not exist). The runner
  checks them before step 1 of §5.1;
- `postconditions`: the complete structure of the same objects after it;
- `reversible`: whether its effect can be undone by hand to exactly `preconditions`. A migration
  that drops or rewrites data, such as dropping a column, is `false`.

`0001 base` creates the six databases and every object of [obs], in dependency order: local
tables, then distributed tables, then materialized views after their targets.

### 5.1 Running one migration

1. Under the lease, insert `migrations(id, …, state = started, token = <lease token>)`. Strict
   mode fails if the `id` exists.
2. Run each statement `ON CLUSTER {cluster}` with `distributed_ddl_output_mode = throw` and
   `distributed_ddl_task_timeout`, so every host has applied it or the statement fails.
   - Before each statement, the runner checks that its lease is still valid (§6) and that every
     replica shows it (§3.2). If either fails, it stops.
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
that makes it.

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
| Skip indexes on JSON subcolumns, named `` `<column>.<path>_<jsontype>_<indextype>` ``, with the expression `assumeNotNull(dynamicElement(<column>.<path>, '<Type>'))` (wrapped in `lower(…)` for `String`), of type `tokenbf_v1`, `ngrambf_v1` or `minmax`, with any granularity. `<column>` is the target's base or promoted JSON column | `logs_v2` (`body_v2`, `body_promoted`); `signoz_index_v3` (`attributes`, `attributes_promoted`) | `pkg/modules/promote/implpromote/module.go` about line 199; names and expressions in `pkg/telemetryschema/chschema/index.go` about lines 55–80; targets in `pkg/types/promotetypes/target.go` |
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
| Precheck failures: no Keeper, no macros, no `keeper_map_path_prefix`, replicas' clocks 60 s apart | each refused with what is missing |
| Each concurrency and lease case above, with each coordination service: Keeper (one node and a 3-node ensemble) and the chart's ZooKeeper (`signoz/zookeeper:3.7.1`, one node as the chart deploys it by default, and a 3-node ensemble) | the same results with every service |
| The coordination service (one node, and one member of an ensemble) stopped during a migration, then started again | the update in flight fails; the run exits 2 or 4 by §1; the rerun follows §7; the lease and the history are unchanged by the failed update |
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
| Unknown or changed history (§7.1, §7.2) | Run the newer binary that wrote it. Never edit history by hand |
| Schema drift (§7.4, §7.5) | `status` lists the differences. Restore the expected objects, or drop the extra ones, by hand, then rerun |
| Precheck failures (§7.6) | Add what is named, for example `keeper_map_path_prefix` in the server configuration, and restart ClickHouse |
| A bad migration | There is no automatic rollback. Restore from backup, or write a forward migration that corrects it |
| Going back to the fork's migrator | Possible only while the history holds `0001` alone: the fork's own history and the [obs] schema are untouched. After any later migration it is not supported |

### 10.1 `repair`

`repair` takes the lease, checks on every replica, and refuses unless:

- **`--mark done`**: every post-condition of migration `N` holds (§4.1, §8). The row becomes
  `done` with the current token.
- **`--mark absent`**: `N` is reversible, no migration after `N` is recorded, and the live
  structure of every object `N` touches equals `N`'s pre-conditions exactly: created objects
  absent, altered and dropped objects as they were. Any third state is refused. The row is
  deleted.

Either way, the result is verified by reading the history back before exit 0.

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
