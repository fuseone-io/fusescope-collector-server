# Cutover runbook: the FuseScope demo

How the FuseScope demo finishes moving from the earlier collector (`fusescope-otel-collector`, AGPL) to this one
([#7](https://github.com/fuseone-io/fusescope-collector-server/issues/7)), and removes it completely. **There is no
rollback** ([decision on #7, 2026-11-08](https://github.com/fuseone-io/fusescope-collector-server/issues/7#issuecomment-6726848745)): the earlier collector is not an option to go back to at any step, and
a problem is fixed in this collector or its configuration. Backup, `repair` and restore exist only for a failed
schema migration (§8). Every command here is run by the operator on the demo host, in `/opt/fusescope`, from this
directory copied there as in [README](README.md), "Install". The generic guide for installs before chart 0.161.0 is
#9's, not this one.

The sections follow one structure, to reuse for another environment: its [facts](#1-facts), the
[preconditions](#2-preconditions), [monitoring the comparison](#3-monitoring-the-comparison), the
[health thresholds](#4-health-thresholds), [acceptance per signal](#5-acceptance-per-signal), the
[performance comparison](#6-performance-comparison), [removing the ghost](#7-remove-the-ghost), the
[schema migrator](#8-the-schema-migrator) and the [exit](#9-exit-30-days).

## 1. Facts

| | The demo, 2026-11-08 |
|---|---|
| Senders | `hotrod`, the `telemetrygen` services (OTLP), browsers and apps (RUM through Caddy) |
| This collector | `fusescope-collector` 0.2.0 (`FUSESCOPE_COLLECTOR_IMAGE`), the demo kit's collector, writing `fusescope-demo-clickhouse-1` |
| Earlier collector | the ghost: `otel-collector`, `fusescope-otel-collector` 0.145.0, on the shadow network only, fed a copy, writing `clickhouse-shadow` ([README](README.md), "Swap") |
| Schema migrator | `telemetrystore-migrator`, the earlier collector's image (`COLLECTOR_VERSION`), on the demo's ClickHouse |
| Comparison | `compare.py`, hourly from cron, into `/var/log/fusescope-shadow.log` |
| Rollback | none, [by decision](https://github.com/fuseone-io/fusescope-collector-server/issues/7#issuecomment-6726848745) (#7, 2026-11-08): problems are fixed forward |

The swap happened on 2026-11-05 at 03:00 UTC. Since then this collector has served every signal of the demo. Parity
is accepted (#7, 2026-11-08): the 91 hourly runs from the swap to 2026-11-08 16:05 UTC were `ok` on every check of
user data, and their one difference, this collector's own telemetry, is what #80 leaves out and counts. What is left
is to compare performance, take the earlier collector off the path, move the schema migrator, and run 30 days
without any of the earlier collector's images.

Shorthand used below:

```sh
C="docker compose -f compose.yaml -f shadow/compose.shadow.yaml -f shadow/compose.swap.yaml"   # with the ghost
B="docker compose -f compose.yaml"                                                             # the kit alone
Q="docker exec -i fusescope-demo-clickhouse-1 clickhouse-client --readonly 1"
QS="docker exec -i fusescope-demo-clickhouse-shadow-1 clickhouse-client --readonly 1"
```

## 2. Preconditions

1. **The internal series filter is deployed** (#80): the metric series check leaves out `fusescope-collector`'s own
   telemetry and prints how many series it left out. Copy the reviewed `compare.py` to `/opt/fusescope/shadow/`.
2. **The cron marks each run** with its start time and exit code, for the monitoring of §3. Replace the line with
   this one (`%` is escaped for cron):

   ```
   5 * * * * cd /opt/fusescope && { echo "=== run $(date -u +\%FT\%TZ)"; docker compose -f compose.yaml -f shadow/compose.shadow.yaml -f shadow/compose.swap.yaml run --rm shadow-compare; echo "=== exit $?"; } >> /var/log/fusescope-shadow.log 2>&1
   ```

3. **The first run with both** shows `=== run`, a `metric series (… ignored N internal series of …)` line and
   `=== exit 0`.
4. **The performance comparison with the ghost** ([§6](#6-performance-comparison)) is noted on #7.
5. **Just before §7**: `runs.py` exits 0 (the latest run is green and recent), and every failure or gap it lists, and
   every `DIFF` in user data since the first marked run, has an explanation noted on #7. None of this starts a new
   waiting period.

## 3. Monitoring the comparison

The hourly comparison keeps running until §7, as a regression signal, not a window to wait out: no number of green
runs is required, and nothing restarts a count. `runs.py` reads the log and lists every run that exited non-zero and
every gap of more than 90 minutes between runs (a missed hour). It exits 0 when the latest finished run exited 0 and
is at most 90 minutes old:

```sh
docker run --rm -v /var/log/fusescope-shadow.log:/log:ro -v "$PWD/shadow/runs.py:/runs.py:ro" \
  python:3.12-alpine python3 -I /runs.py /log
```

Once a day until §7:

```sh
docker run --rm -v /var/log/fusescope-shadow.log:/log:ro -v "$PWD/shadow/runs.py:/runs.py:ro" python:3.12-alpine python3 -I /runs.py /log
$Q < shadow/health.sql
$Q < shadow/queues.sql
grep -c '^DIFF' /var/log/fusescope-shadow.log
```

A `DIFF` in user data, as opposed to an error, is investigated before anything else: a new case for `compare.py` or a
fix to this collector, noted on #7.

## 4. Health thresholds

From this collector's own telemetry, which it writes to the demo's ClickHouse under `service.name`
`fusescope-collector` (`health.sql` and `queues.sql`, last 10 minutes). The steady state on 2026-11-08 was 0 for every
counter, queues at most 1.2% used, and about 160 MiB resident.

| Signal of trouble | Metric | Act when |
|---|---|---|
| Refused by the memory limiter | `otelcol_receiver_refused_{spans,log_records,metric_points}` | it increases in two 10-minute checks in a row |
| Failed receiving | `otelcol_receiver_failed_{spans,log_records,metric_points}` | it increases in two checks in a row |
| Dropped on export | `otelcol_exporter_send_failed_*`, `otelcol_exporter_enqueue_failed_*` (reported only once non-zero) | it appears for `fusescope_clickhouse` |
| ClickHouse insert errors | `otelcol_exporter_fusescope_clickhouse_insert_errors` | it increases in two checks in a row |
| Queue backing up | `otelcol_exporter_queue_size` / `_capacity` for `fusescope_clickhouse` | above 50% in two checks in a row |
| Memory | `otelcol_process_memory_rss` | above 600 MiB (80% of the 768 MiB limit) in two checks in a row |
| RUM refused | `otelcol_receiver_rum_requests_refused` | it rises above its usual rate, by key or client limit, without a load test running |
| No telemetry | no row for `fusescope-collector` in `health.sql` | for 10 minutes: the collector or its self-scrape is down |

The `otlp/ghost` queues do not count: a full ghost queue drops copies and never fails the demo. Acting means fixing
forward: stop the next cutover step, find the cause in this collector or its configuration, and ship the fix (a new
image or configuration through the demo kit), noted on #7. There is no going back to the earlier collector.

## 5. Acceptance per signal

The comparison covers every signal at once. Before §7, each is also checked by hand, once, and noted on #7:

| Signal | Check |
|---|---|
| Traces | `compare.py`'s spans, error spans and resource keys `ok`; the Traces explorer and a trace's waterfall show hotrod's spans |
| APM | APM calls per service `ok`; the Services page and its RED charts match between `compare_ui.py`'s two backends (README, "UI") |
| Logs | logs per service and minute and log attribute keys `ok`; the Logs explorer shows the telemetrygen services |
| Metrics | metric series `ok`, with the internal series counted; host metrics (`system.*`) present in a dashboard |
| RUM | a session from `rum-driver` shows on the RUM pages; `otelcol_receiver_rum_requests_refused` flat |

## 6. Performance comparison

Both collectors get the same traffic while the ghost runs, and each writes its own telemetry to its own ClickHouse,
so `perf.sql` reads them side by side over the same window: CPU, resident memory, heap, allocation rate and items
received per hour. Read-only:

```sh
F="2026-11-06 00:00:00"; U="2026-11-08 20:00:00"   # whole hours [F, U) both collectors cover in full
$Q  --param_svc=fusescope-collector       --param_since="$F" --param_until="$U" < shadow/perf.sql
$QS --param_svc=/fusescope-otel-collector --param_since="$F" --param_until="$U" < shadow/perf.sql
```

`perf.sql` sums counters sample to sample across hour boundaries and divides by the whole hours of the window;
`hours_with_samples` below `hours` means a gap in the collector's own telemetry. The first comparison,
[2026-11-06 00:00, 2026-11-08 20:00) UTC, 68 hours, all with samples on both sides, about 13,570 spans and 2,880
logs per hour on each:

| | this collector | the earlier collector |
|---|---|---|
| CPU, average (p95 hour) | 0.90% (0.98%) | 0.53% (0.58%) |
| Resident memory, average (max) | 158 MiB (167) | 240 MiB (243) |
| Heap, average | 34 MiB | 51 MiB |
| Allocation | 436 MiB/h | 1,058 MiB/h |

The CPU is not like for like while the ghost runs: this collector also exports every item a second time, to the ghost
over OTLP/gRPC, receives RUM and derives the meter. So the comparison closes after §7: this collector alone, on the
same synthetic traffic, over 24 hours, against the earlier collector's figures above:

```sh
$Q --param_svc=fusescope-collector --param_since="<first whole hour after §7>" --param_until="<24 hours later>" < shadow/perf.sql
```

Both results are noted on #7. A regression, here or later, is fixed forward (§4).

## 7. Remove the ghost

When the preconditions of §2 hold (the performance comparison noted, `runs.py` exiting 0, every finding explained),
§5 is noted on #7, and the removal is approved there:

```sh
set -e
cp -p /var/log/fusescope-shadow.log /var/log/fusescope-shadow.log.cutover  # the evidence, kept
crontab -l | grep -v 'shadow-compare' | crontab -                           # the hourly comparison stops
$C rm -sf otel-collector                                                    # the ghost stops
$B up -d --no-deps --force-recreate fusescope-collector                     # the kit's collector, without the ghost copy
```

From here this collector is the only one on the demo's path, and no `fusescope-otel-collector` process handles
telemetry. Update the demo with `$B` (the kit's `update.sh`) from now on. Then run the 24-hour measurement of §6.
`clickhouse-shadow` holds the earlier collector's figures for §6; once they are noted on #7, remove it:

```sh
$C rm -sf migrator-shadow clickhouse-shadow
docker volume rm fusescope-demo_clickhouse-shadow-data   # drops the shadow's data
```

## 8. The schema migrator

The demo's `telemetrystore-migrator` still runs the earlier collector's image. Moving it to `fusescope-schema`, which
adopts the existing schema and then applies `0002`, is a separate reviewed change to the demo kit (`deploy/demo` in
the fusescope repo, fuseone-io/fusescope#348, with the `keeper_map_path_prefix` it needs). It is irreversible: once
`0002` has run, the earlier collector's migrator refuses the schema. It waits for all of these:

1. the 24-hour measurement of §6, noted on #7;
2. the demo collector's version settled: `fusescope-collector` 0.2.0 checked on a throwaway copy of the schema
   migrated by `fusescope-schema` 0.4.1 (done on 2026-11-08, below), or the demo collector moved to the version that
   will stay before §7, while the ghost still runs, with parity and performance repeated on it;
3. a backup taken with the procedure below just before the switch.

### Backup and restore

For a failed or interrupted migration only: never to go back to the earlier collector. The backup is cold: the
writers stopped, then ClickHouse and ZooKeeper stopped together, and both volumes archived. They are one state:
the replicated tables' metadata lives in ZooKeeper, and `fusescope-schema`'s history in KeeperMap tables there too.
Telemetry sent while the collector is stopped is lost once the senders stop retrying.

Backup, from `/opt/fusescope`. The archives hold the demo's telemetry, so they are readable by root only. Everything
the backup needs is pulled before any service stops, and a failure while archiving starts the stopped services again:

```sh
set -euo pipefail
K="docker compose --profile generators -f compose.yaml"      # the kit
A=alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc
docker pull -q "$A"                                           # before anything stops
umask 077
BK=/var/backups/fusescope-demo/$(date -u +%Y%m%dT%H%MZ)-pre-schema
install -d -m 0700 /var/backups/fusescope-demo "$BK"
restart() { $K start zookeeper clickhouse fusescope-collector fusescope; }
trap 'echo "backup failed: starting the stopped services again" >&2; restart' ERR
$K stop fusescope-collector fusescope                         # the writers
$K stop clickhouse zookeeper                                  # one state, stopped together
for v in clickhouse-data zookeeper-data; do
  docker run --rm -v fusescope-demo_$v:/v:ro -v "$BK":/b "$A" \
    sh -c "umask 077 && tar -C /v -czf /b/$v.tgz . && chmod 0600 /b/$v.tgz"
done
(cd "$BK" && sha256sum ./*.tgz > SHA256SUMS \
  && for v in clickhouse-data zookeeper-data; do tar -tzf $v.tgz | grep -vc '^\./$' > $v.entries; done)
trap - ERR
stat -c '%a %U %n' "$BK" "$BK"/*                              # 700 root, then 600 root for each file
```

If the backup fails, the trap starts ZooKeeper, ClickHouse, the collector and the backend again, in that order, and
nothing else has changed: `$K ps` shows them up. A successful backup leaves them stopped for the switch, which
follows at once; if the switch has to wait, `restart` brings them back.

Then the switch: the kit with fusescope#348, through its `update.sh`, which starts ClickHouse with
`keeper_map_path_prefix` before the migrator (`ensure_keeper_map`).

If `fusescope-schema migrate` exits 1 or 4, or `check` does not end `up to date`, read `status` first. A migration
`started` whose post-conditions hold can be finished with `repair --mark done` (collector-server spec §10.1); `0002` is
irreversible, so `--mark absent` is refused. Otherwise, restore, into new, empty volumes, from the archives alone:

```sh
set -euo pipefail
BK=${BK:?the backup directory, /var/backups/fusescope-demo/<time>-pre-schema}
K="docker compose --profile generators -f compose.yaml"      # the kit
A=alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc
docker pull -q "$A"                                           # before anything stops
(cd "$BK" && sha256sum -c SHA256SUMS)
$K stop fusescope-collector fusescope
$K rm -sf telemetrystore-migrator clickhouse zookeeper
docker volume rm fusescope-demo_clickhouse-data fusescope-demo_zookeeper-data
for v in clickhouse-data zookeeper-data; do
  # Created empty and filled before any of the kit's containers mounts it: Docker copies
  # nothing from an image into a volume that is not empty.
  docker volume create --label com.docker.compose.project=fusescope-demo --label com.docker.compose.volume=$v fusescope-demo_$v
  [ "$(docker run --rm -v fusescope-demo_$v:/v "$A" sh -c 'find /v -mindepth 1 | wc -l')" = 0 ]
  docker run --rm -v fusescope-demo_$v:/v -v "$BK":/b:ro "$A" tar -C /v -xzf /b/$v.tgz
  [ "$(docker run --rm -v fusescope-demo_$v:/v "$A" sh -c 'find /v -mindepth 1 | wc -l')" = "$(cat "$BK"/$v.entries)" ]
done
```

The schema is then as before the switch, with no `fusescope_schema` database. Fix the cause, and run the switch
again: forward, with `fusescope-schema`. Nothing restarts the earlier collector.

**Rehearsed on 2026-11-08**, on a throwaway copy of the demo's storage, with the demo's images:
the kit's ClickHouse configuration plus `keeper_map_path_prefix`, and:

- ClickHouse `clickhouse/clickhouse-server:25.12.5@sha256:cacf32d6884291dc2ff5e0156a97f46fc53ff7c929a7906d114e268a929dfd3a`
- ZooKeeper `signoz/zookeeper:3.7.1@sha256:fcc4a3288154ccaa3bdb5ae6dc10180c084d29a8a6a26b62ac8e30a8940dc2e6`
- the earlier migrator `ghcr.io/fuseone-io/fusescope-otel-collector:0.145.0@sha256:e03a01636cea5abd9eceec0d4d81769d7c5e9813487886afa415ada2f0adfc4a`
- `fusescope-schema` from `ghcr.io/fuseone-io/fusescope-collector:0.4.1@sha256:9951f2479ac6e5150b116cd856569aaae6ed921e9b951f0fd9c93160e7311077`
- the collector `ghcr.io/fuseone-io/fusescope-collector:0.2.0@sha256:b9f8b298703d65452a9314ce9007473a6164adfc3fa56e65d97cf136f90322f3`

Results:


- The earlier migrator created the schema and collector 0.2.0 wrote to it. `check` reported
  `pending: record 0001 base as adopted, 0002 column_evolution_baseline`.
- **Backup:** 14 s with the restart, on 5.6 MB of data. The demo holds about 1.4 GB in ClickHouse and 350 MB in
  ZooKeeper, so time it there.
- **Interruption:** the migrator was killed (exit 137) once `0002 column_evolution_baseline` was `started`, read back
  from `fusescope_schema.migrations`.
  - `check` then reported `in progress` while the lease held.
  - Once the lease had expired, `migrate` refused: `a migration was interrupted: its lease expired while it was started`.
- **Restore into new volumes:** both had 0 entries before the extract, and exactly the archive's after it.
- **The restored state was identical to the state before the backup**, compared on:
  - the databases and every table definition;
  - the six earlier migrators' histories;
  - `fusescope_schema` absent;
  - count and hash of `signoz_index_v3`, `logs_v2`, `samples_v4` and `time_series_v4`;
  - the ZooKeeper nodes.

  `time_series_v4` is a `ReplacingMergeTree`, compared `FINAL`. Its raw row count changes when parts merge: 136
  rows before the backup's restart, 45 after, the same 45 `FINAL` before and after.
- **Then `migrate`:** `starting point: fork 0.145.0`, `applied 0002 column_evolution_baseline`, `up to date`, and
  `check` exited 0, with `0001` adopted.
- **Collector 0.2.0 wrote again:** +902 spans, +901 logs, +1,209 samples, each signal found by its service name, with
  no error line.

## 9. Exit: 30 days

The 30 days start when both §7 and §8 are done: from then on no image of the earlier collector runs on the demo.
During them:

- weekly, `health.sql` and `queues.sql`, against the thresholds of §4;
- any problem is fixed forward, noted on #7, and does not restart the count.

At 30 days, #7's exit is met: note the dates of §7, §8 and day 30 on #7.
