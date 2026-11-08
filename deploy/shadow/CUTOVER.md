# Cutover runbook: the FuseScope demo

How the FuseScope demo finishes moving from the earlier collector (`fusescope-otel-collector`, AGPL) to this one, and
how it goes back, one signal at a time, while it still can ([#7](https://github.com/fuseone-io/fusescope-collector-server/issues/7)).
Every command here is run by the operator on the demo host, in `/opt/fusescope`, from this directory copied there as in
[README](README.md), "Install". The generic guide for installs before chart 0.161.0 is #9's, not this one.

The sections follow one structure, to reuse for another environment: its [facts](#1-facts), the [preconditions](#2-preconditions),
the [formal window](#3-the-formal-window) and its [abort thresholds](#4-abort-thresholds), [acceptance per signal](#5-acceptance-per-signal),
[rollback per signal](#6-roll-back-a-signal), [removing the earlier collector](#7-remove-the-ghost), the
[point of no return](#8-point-of-no-return-the-schema-migrator) and the [exit](#9-exit-30-days).

## 1. Facts

| | The demo, 2026-11-08 |
|---|---|
| Senders | `hotrod`, the `telemetrygen` services (OTLP), browsers and apps (RUM through Caddy) |
| This collector | `fusescope-collector` 0.2.0 (`FUSESCOPE_COLLECTOR_IMAGE`), the demo kit's collector, writing `fusescope-demo-clickhouse-1` |
| Earlier collector | the ghost: `otel-collector`, `fusescope-otel-collector` 0.145.0, on the shadow network only, fed a copy, writing `clickhouse-shadow` ([README](README.md), "Swap") |
| Schema migrator | `telemetrystore-migrator`, the earlier collector's image (`COLLECTOR_VERSION`), on the demo's ClickHouse |
| Comparison | `compare.py`, hourly from cron, into `/var/log/fusescope-shadow.log` |
| Rollback | per signal, to the earlier collector as a legacy writer of the demo's ClickHouse (`compose.legacy.yaml`), until the point of no return |
| Point of no return | the demo's schema migrator moves to `fusescope-schema` ([§8](#8-point-of-no-return-the-schema-migrator)) |

The swap happened on 2026-11-05 at 03:00 UTC. Since then this collector has served every signal of the demo. What is
left is to prove it over the formal window, take the earlier collector off the path, and run 30 days without it.

Shorthand used below:

```sh
C="docker compose -f compose.yaml -f shadow/compose.shadow.yaml -f shadow/compose.swap.yaml"   # with the ghost
B="docker compose -f compose.yaml"                                                             # the kit alone
L="$B -f shadow/compose.legacy.yaml"                                                           # a rollback
Q="docker exec -i fusescope-demo-clickhouse-1 clickhouse-client --readonly 1"
```

## 2. Preconditions

1. **The internal series filter is deployed** (#80): the metric series check leaves out `fusescope-collector`'s own
   telemetry and prints how many series it left out. Copy the reviewed `compare.py` to `/opt/fusescope/shadow/`.
2. **The cron marks each run** with its start time and exit code, so the window can be read back. Replace the line
   with this one (`%` is escaped for cron):

   ```
   5 * * * * cd /opt/fusescope && { echo "=== run $(date -u +\%FT\%TZ)"; docker compose -f compose.yaml -f shadow/compose.shadow.yaml -f shadow/compose.swap.yaml run --rm shadow-compare; echo "=== exit $?"; } >> /var/log/fusescope-shadow.log 2>&1
   ```

3. **The first run with both** shows `=== run`, a `metric series (… ignored N internal series of fusescope-collector …)`
   line and `=== exit 0`. That run opens the formal window.
4. **Nothing else changes during the window**: the collector's image and configuration, the ghost, the senders. Any of
   these restarts the window at the next run.

## 3. The formal window

**168 hourly comparisons in a row exit 0**, from the first run with the filter: seven days. The window restarts at
the next run after:

- a run that exits non-zero, for any reason (a difference, an error, a ClickHouse unreachable);
- a run without the filter's line;
- a gap of more than 90 minutes since the run before (a missed hour).

`window.py` reads the log and says where the window stands, with each restart and why; it exits 0 once complete:

```sh
docker run --rm -v /var/log/fusescope-shadow.log:/log:ro -v "$PWD/shadow/window.py:/window.py:ro" \
  python:3.12-alpine python3 -I /window.py /log
```

**Prior evidence, not counted.** The 91 hourly runs from the swap (2026-11-05 03:00 UTC) to 2026-11-08 16:05 UTC each
failed only on `metric series: 20 only in fusescope, 0 only in fork`: `otelcol.resourcedetection.*`, this collector's
own telemetry. Spans per service and minute, error spans, span resource keys, logs per service and minute, log
attribute keys, APM calls and duplicates were `ok` in every one. They support the window but do not count toward it.

During the window, once a day:

```sh
docker run --rm -v /var/log/fusescope-shadow.log:/log:ro -v "$PWD/shadow/window.py:/window.py:ro" python:3.12-alpine python3 -I /window.py /log
$Q < shadow/health.sql
$Q < shadow/queues.sql
grep -c '^DIFF' /var/log/fusescope-shadow.log
```

A `DIFF` in user data, as opposed to an error, is investigated before anything else: a new case for `compare.py` or a
fix to this collector, then the window starts over.

## 4. Abort thresholds

From this collector's own telemetry, which it writes to the demo's ClickHouse under `service.name`
`fusescope-collector` (`health.sql` and `queues.sql`, last 10 minutes). The steady state on 2026-11-08 was 0 for every
counter, queues at most 1.2% used, and about 160 MiB resident.

| Signal of trouble | Metric | Abort when |
|---|---|---|
| Refused by the memory limiter | `otelcol_receiver_refused_{spans,log_records,metric_points}` | it increases in two 10-minute checks in a row |
| Failed receiving | `otelcol_receiver_failed_{spans,log_records,metric_points}` | it increases in two checks in a row |
| Dropped on export | `otelcol_exporter_send_failed_*`, `otelcol_exporter_enqueue_failed_*` (reported only once non-zero) | it appears for `fusescope_clickhouse` |
| ClickHouse insert errors | `otelcol_exporter_fusescope_clickhouse_insert_errors` | it increases in two checks in a row |
| Queue backing up | `otelcol_exporter_queue_size` / `_capacity` for `fusescope_clickhouse` | above 50% in two checks in a row |
| Memory | `otelcol_process_memory_rss` | above 600 MiB (80% of the 768 MiB limit) in two checks in a row |
| RUM refused | `otelcol_receiver_rum_requests_refused` | it rises above its usual rate, by key or client limit, without a load test running |
| No telemetry | no row for `fusescope-collector` in `health.sql` | for 10 minutes: the collector or its self-scrape is down |

The `otlp/ghost` queues do not count: a full ghost queue drops copies and never fails the demo. Aborting means rolling
back the affected signal ([§6](#6-roll-back-a-signal)), or every signal when the cause is not signal-specific (memory,
no telemetry), and restarting the window once fixed.

## 5. Acceptance per signal

The window covers every signal at once. Before §7, each is also checked by hand, once, and noted on #7:

| Signal | Check |
|---|---|
| Traces | `compare.py`'s spans, error spans and resource keys `ok`; the Traces explorer and a trace's waterfall show hotrod's spans |
| APM | APM calls per service `ok`; the Services page and its RED charts match between `compare_ui.py`'s two backends (README, "UI") |
| Logs | logs per service and minute and log attribute keys `ok`; the Logs explorer shows the telemetrygen services |
| Metrics | metric series `ok`, with the internal series counted; host metrics (`system.*`) present in a dashboard |
| RUM | a session from `rum-driver` shows on the RUM pages; `otelcol_receiver_rum_requests_refused` flat |

## 6. Roll back a signal

Possible until the point of no return ([§8](#8-point-of-no-return-the-schema-migrator)), and also after the ghost is
gone. The earlier collector runs a second time as the **legacy writer**, on the demo's network, where `clickhouse` is
the demo's ClickHouse, with the kit's configuration for it, which is not read (ADR 0002). This collector keeps
receiving everything and forwards the rolled-back signals to it instead of writing them (`rollback/`):

| Variable | Overlay | This collector then |
|---|---|---|
| `ROLLBACK_TRACES=traces` | `rollback/traces.yaml` | sends spans (RUM's included) to the legacy writer, which also derives the APM metrics; stops deriving them itself |
| `ROLLBACK_LOGS=logs` | `rollback/logs.yaml` | sends log records (RUM's included) to the legacy writer |
| `ROLLBACK_METRICS=metrics` | `rollback/metrics.yaml` | sends OTLP metrics to the legacy writer; discards its own host metrics and telemetry, which the legacy writer scrapes itself |

To roll back traces, for example (set several variables for several signals):

```sh
set -e
export ROLLBACK_TRACES=traces
res() { docker run --rm --network "$1" python:3.12-alpine python3 -c "import socket,sys; print(sorted({a[4][0] for a in socket.getaddrinfo(sys.argv[1], None, socket.AF_INET)}))" "$2"; }
res fusescope-demo clickhouse                                   # the demo's ClickHouse only: what the legacy writer will write
res fusescope-demo fusescope-demo-clickhouse-1                  # the same address
$L up -d --no-deps otel-collector-legacy                        # migrate sync check, then the legacy writer starts
docker logs --tail 20 fusescope-demo-otel-collector-legacy-1    # running, no exporter errors
$L up -d --no-deps --force-recreate fusescope-collector         # forwards traces; "Everything is ready"
```

Then, every minute for the first few, on the demo's ClickHouse: rows and unique spans per minute must stay equal (more
rows than spans is a duplicate writer), and spans must keep arriving:

```sh
$Q -q "SELECT toStartOfMinute(timestamp) m, count(), uniqExact(traceID, spanID) FROM signoz_traces.signoz_index_v3 WHERE timestamp > now() - INTERVAL 5 MINUTE GROUP BY m ORDER BY m"
```

For logs, the same with `signoz_logs.logs_v2` and `count()`. For metrics, `system.cpu.time` must have one series per
CPU and state, not two.

Known costs:

- **Any rollback writes the host metrics twice** unless metrics are rolled back too: the legacy writer scrapes the
  host whenever it runs. Roll back metrics with traces or logs where that matters.
- This collector sends the ghost no copy while rolled back (`compose.legacy.yaml` leaves `ghost-copy.yaml` out and the
  shadow network), so the hourly comparisons fail and the formal window restarts at the undo.
- `fusescope_meter` usage is not counted for a rolled-back signal.

**Undo the rollback**, once the cause is fixed. This collector goes first, so nothing is sent to a stopped writer.
While the ghost exists (before §7), with its copy back:

```sh
set -e
$C up -d --no-deps --force-recreate fusescope-collector         # writes every signal again, copies to the ghost
$L rm -sf otel-collector-legacy
```

After §7, the kit alone: `$B up -d --no-deps --force-recreate fusescope-collector`, then `$L rm -sf otel-collector-legacy`.

## 7. Remove the ghost

When the window is complete (`window.py` exits 0), §5 is noted on #7, and the removal is approved there:

```sh
set -e
cp -p /var/log/fusescope-shadow.log /var/log/fusescope-shadow.log.window   # the evidence, kept
crontab -l | grep -v 'shadow-compare' | crontab -                           # the hourly comparison stops
$C rm -sf otel-collector                                                    # the ghost stops
$B up -d --no-deps --force-recreate fusescope-collector                     # the kit's collector, without the ghost copy
```

From here this collector is the only one on the demo's path, and no `fusescope-otel-collector` process handles
telemetry. Update the demo with `$B` (the kit's `update.sh`) from now on. `clickhouse-shadow` and `migrator-shadow` can
stay until the exit, for a look back, then go as in [README](README.md), "Rollback" (last two commands). Rolling back
a signal ([§6](#6-roll-back-a-signal)) still works: `compose.legacy.yaml` needs only the kit's compose file.

## 8. Point of no return: the schema migrator

The demo's `telemetrystore-migrator` still runs the earlier collector's image. Moving it to `fusescope-schema`, which
adopts the existing schema and then applies `0002`, is a separate reviewed change to the demo kit (`deploy/demo` in the
fusescope repo). After it:

- the earlier collector refuses the schema (`migrate sync check` fails), so §6 no longer works;
- going back means restoring a ClickHouse backup taken before the switch.

So it goes last: after §7, once the 30 days below show no reason to roll back, or earlier only by an explicit decision
on #7. Take a backup of the demo's ClickHouse just before it.

## 9. Exit: 30 days

The 30 days start at §7, the first moment no `fusescope-otel-collector` process handles the demo's telemetry. During
them:

- weekly, `health.sql` and `queues.sql`, against the thresholds of §4;
- any rollback (§6) stops the count; it starts again from zero at the undo.

At 30 days without a rollback, the earlier collector is gone from the demo's path. With §8 done as well, no image of it
runs on the demo at all, and #7's exit is met: note the dates of §7, §8 and day 30 on #7.
