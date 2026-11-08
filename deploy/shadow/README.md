# Shadow on the FuseScope demo

Runs this collector next to the demo's collector on the same traffic, writing to its own ClickHouse,
and compares the two (DESIGN.md §9, phase 6, [#7](https://github.com/fuseone-io/fusescope-collector-server/issues/7)).
The demo's path is unchanged: the demo collector sends a copy of what its `otlp` receiver accepts to the shadow
collector, through an exporter added to its own configuration.

```
hotrod, telemetrygen, RUM (via fusescope) ──► otel-collector ──► clickhouse          (demo, unchanged)
                                                   └─ otlp/shadow ──► fusescope-collector-shadow ──► clickhouse-shadow
                                       shadow-compare: both, read-only, per settled window
```

Two attempts went wrong before this one, both on 2026-11-02:
- A tee in front of the demo collector kept a dead gRPC connection to the shadow collector after it was recreated;
  its shadow side backed up, senders retried, and the demo collector, already close to its memory limit, refused and
  was overloaded: about 7 minutes of demo data lost (22:29–22:36 UTC) and duplicated spans at 22:28. Hence no hop in
  front of the demo, and an exporter that detects a dead connection.
- The first exporter block deployed pointed at a local test name that resolved to the demo collector itself, which
  then exported to itself in a loop: duplicated demo data for about 2 minutes (23:10–23:12 UTC). Hence the block as a
  reviewed file, [`demo-collector-shadow.yaml`](demo-collector-shadow.yaml), and an endpoint check before it is used.

| File | What |
|---|---|
| `compose.shadow.yaml` | override for the demo's `compose.yaml`: adds the shadow services, gives the demo collector 768 MiB and 0.5 CPU |
| `collector.yaml` | this collector's configuration in shadow |
| `demo-collector-shadow.yaml` | the block merged into the demo collector's configuration to feed the shadow |
| `clickhouse/cluster.xml` | single-node `cluster` with an embedded keeper, named `clickhouse-shadow` |
| `clickhouse/limits.xml`, `users-limits.xml` | memory, caches, system logs and per-query limits bounded as the demo's own ClickHouse |
| `compare_ui.py` | the APM pages from both backends (below) |
| `compare.py`, `compare_test.py` | the comparison, exits 1 on a difference; its unit test (`python3 -m unittest deploy/shadow/compare_test.py`) |
| [`CUTOVER.md`](CUTOVER.md) | the runbook from the swap to 30 days without the earlier collector: monitoring, abort thresholds, rollback per signal |
| `runs.py`, `runs_test.py` | monitoring of the hourly comparison from its log: failed runs, gaps, whether the latest is green; its unit test (`python3 -m unittest deploy/shadow/runs_test.py`) |
| `health.sql`, `queues.sql` | this collector's own telemetry against the abort thresholds, read-only |
| `compose.legacy.yaml`, `rollback/` | a rollback of one or more signals to the earlier collector, as a legacy writer of the demo's ClickHouse |

## Derived from the demo's data

Clean-room (ADR 0002): the demo collector's configuration is not read. What the shadow collector does comes from the
demo's `compose.yaml` and from what the demo's ClickHouse holds (read-only queries, 2026-11-02):

- **Sources.** `hotrod` sends OTLP/HTTP to `otel-collector:4318`, the `telemetrygen` services OTLP/gRPC to
  `otel-collector:4317`, and the backend forwards RUM to `SIGNOZ_RUM_COLLECTOR__ENDPOINT`.
- **Spans and logs** carry only what the senders set: `host.name` and `os.type` appear only on hotrod's spans, which
  its SDK sets, and on no `telemetrygen` or RUM span or log. So no processor touches them.
- **Host metrics** are `system.cpu.time`, `system.cpu.load_average.{1m,5m,15m}`, `system.disk.*`,
  `system.filesystem.{usage,inodes.usage}` and `system.memory.usage`, one point a minute, with resource
  `host.name` = the host's name and `os.type`; the demo mounts the host at `/hostfs` and shares its UTS namespace.
  Hence the `cpu`, `load`, `disk`, `filesystem` and `memory` scrapers at 60 s and the `system` detector. The demo's
  `system.cpu.time` has a `cpu` label (opt-in in the receiver), it writes no `system.cpu.logical.count` (on by
  default) and no disk series for the optical drive `sr0`; the configuration matches each.
- **APM metrics** (`signoz_calls_total`, `signoz_latency`, `signoz_external_call_latency_*`) come from spans: the
  `fusescope_spanmetrics` connector.
- **Not reproduced:** each collector's own telemetry. The demo's ClickHouses hold it under one `service.name` per
  collector, as a resource attribute and a label: `fusescope-collector`, and `/fusescope-otel-collector` for the
  earlier collector (both observed on 2026-11-08 across their retention, with no `job` label). The metric series
  check leaves out the series of the services in `INTERNAL_SERVICES` (default those two) and prints how many it
  left out on each side. It filters by service, not by metric name: series of any other service are compared,
  `otelcol_*`, `exporter_*`, `scrape_*`, `promhttp_*`, `up` and `otelcol.*` from a user's own collector included.
  The collection is unchanged, so those series stay in ClickHouse for monitoring. `compare.py` also ignores the
  scope version, which is each collector's build version. The demo collector runs without OpAMP (its command line is `--config` only), so the
  shadow does not connect to the backend either.

## Install

Copy this directory to the demo host, from this repository at the commit to shadow:

```sh
tar czf - -C deploy shadow | ssh root@<demo-host> 'tar xzf - --no-same-owner -C /opt/fusescope'
```

The shadow collector runs `SHADOW_COLLECTOR_IMAGE`, for example the published
`ghcr.io/fuseone-io/fusescope-collector:sha-<commit>` (set it in the demo's `.env`); a local build works too:
`docker build -t fusescope-collector:shadow . && docker save fusescope-collector:shadow | ssh root@<demo-host> docker load`.

On the demo host, in `/opt/fusescope`, start the shadow side; nothing in the demo changes yet:

```sh
# Waits for clickhouse-shadow to be healthy and for the migration to succeed;
# none of these depends on a demo service.
docker compose -f compose.yaml -f shadow/compose.shadow.yaml up -d --wait fusescope-collector-shadow
```

Then the demo collector's configuration gets an exporter and one pipeline per signal fed by its existing `otlp`
receiver (its own telemetry names it `otlp`), leaving its existing pipelines as they are. The block is
[`demo-collector-shadow.yaml`](demo-collector-shadow.yaml):

```yaml
exporters:
  otlp/shadow:
    endpoint: dns:///fusescope-collector-shadow:4317
    tls:
      insecure: true
    timeout: 5s
    keepalive:
      time: 10s
      timeout: 5s
      permit_without_stream: true
    sending_queue:
      enabled: true
      sizer: bytes
      queue_size: 33554432   # 32 MiB per signal, held in the demo collector's memory
    retry_on_failure:
      enabled: true
      max_elapsed_time: 30s

# The shadow pipelines start with the demo pipelines' own memory_limiter (that
# is its ID in the demo collector; the install checks it exists). Its state is
# shared, but each pipeline asks it separately, so admission is not atomic
# across the two branches (README, Risks). The batch processor accepts the copy
# and exports it on its own, so a full or failing shadow queue drops shadow
# copies and never fails a demo request.
processors:
  batch/shadow: {}

service:
  pipelines:
    traces/shadow:
      receivers: [otlp]
      processors: [memory_limiter, batch/shadow]
      exporters: [otlp/shadow]
    logs/shadow:
      receivers: [otlp]
      processors: [memory_limiter, batch/shadow]
      exporters: [otlp/shadow]
    metrics/shadow:
      receivers: [otlp]
      processors: [memory_limiter, batch/shadow]
      exporters: [otlp/shadow]
```

The demo collector's command takes a single `--config`, so the block is merged into its file. With the demo owner's
go-ahead (an exception to ADR 0002 for this deployment file, which is merged without being read), on the demo host:

```sh
set -e
cd /opt/fusescope/otel-collector
# The first run keeps the configuration as it was before the shadow, for the
# rollback's memory limiter values. Every run starts from the current file.
[ -e config.yaml.bak-pre-shadow ] || cp -p config.yaml config.yaml.bak-pre-shadow
yq() { docker run --rm --user "$(id -u):$(id -g)" -v "$PWD":/w -v /opt/fusescope/shadow:/s:ro \
  mikefarah/yq:4.44.3@sha256:5b3d851bf28b04ef902fee86305f4dd7e063919c1cafedab2042adb16221c025 "$@"; }
# Only the keys the shadow manages are removed; anything else in the file stays.
strip='del(.exporters."otlp/shadow") | del(.processors."batch/shadow") | del(.service.pipelines."traces/shadow") | del(.service.pipelines."logs/shadow") | del(.service.pipelines."metrics/shadow")'
# Headroom: its memory limiter refused at 256 MiB (limit_mib 320, spike 64, from its own logs).
yq "$strip | (.processors[] | select(has(\"limit_mib\"))) |= (.limit_mib = 640 | .spike_limit_mib = 128)" /w/config.yaml > base.yaml
yq eval-all '. as $i ireduce ({}; . * $i)' /w/base.yaml /s/demo-collector-shadow.yaml > merged.yaml
[ "$(yq '.exporters."otlp/shadow".endpoint' /w/merged.yaml)" = dns:///fusescope-collector-shadow:4317 ]
[ "$(yq '.processors | has("memory_limiter")' /w/merged.yaml)" = true ]   # the demo pipelines' limiter the block reuses
mv merged.yaml config.yaml && rm base.yaml
# Recreated, as compose does not see changes inside a mounted file; 768 MiB, 0.5 CPU.
cd .. && docker compose -f compose.yaml -f shadow/compose.shadow.yaml up -d --no-deps --force-recreate otel-collector
```

Watch the first minutes and restore `config.yaml.bak-pre-shadow` at once if the demo collector logs `otlp/shadow`
errors or refusals, or if the demo receives clearly more spans than the shadow (duplicates). The shadow pipelines
carry exactly what the OTLP receiver accepted, before any of the demo collector's processing, so neither its host
metrics nor its span metrics reach the shadow.

Check: the demo's UI keeps showing new data, and `docker logs fusescope-demo-otel-collector-1` shows no
`otlp/shadow` export errors.

Upgrading the shadow collector (new image or configuration) needs nothing on the demo side: the demo collector's
exporter reconnects. Resources: about 4.8 GiB and 1.3 CPU more (`clickhouse-shadow` 4 GiB, as much as the demo's own ClickHouse needs for the
same data; collector 384 MiB; the demo collector's extra 384 MiB).

## Compare

```sh
c="docker compose -f compose.yaml -f shadow/compose.shadow.yaml"
$c run --rm shadow-compare                                  # last settled hour
$c run --rm -e WINDOW_MINUTES=1440 shadow-compare           # last day
```

Counts of spans and logs per service and minute, error spans and APM calls per service, resource and attribute
keys, and series per metric must be equal. Spans and logs are counted once each (a log by its timestamp, body,
trace, span, attributes and resource, since each collector makes its own log IDs); items stored twice are reported
apart, failing the run in the shadow and only noted in the demo. The demo stored 40 spans twice around midnight
of 2026-11-04 while its APM counts matched the shadow's, so the copies came from its own write, not from a sender.

The comparison runs hourly from cron. Parity is accepted (#7, 2026-11-08), so it is a regression signal, not a
window to wait out: the cron line with its run markers, and how its log is read, are in
[CUTOVER.md](CUTOVER.md) §2 and §3.

## APM pages

`compare_ui.py` compares what the FuseScope backend serves for the Services page (`/api/v2/services`), each
service's top and entry-point operations, and the service map (`/api/v1/dependency_graph`), from the demo's
backend and from `fusescope-shadow-ui`, a second backend of the same version reading `clickhouse-shadow` (profile
`ui`). All must match exactly but the service map, whose call counts depend on each collector's insert batches
(traces spec §1) and must agree within 5%:

```sh
umask 077; [ -e shadow/.ui.env ] || printf 'SHADOW_UI_JWT_SECRET=%s\nSHADOW_UI_PASSWORD=%s\n' "$(openssl rand -hex 32)" "Sh-$(openssl rand -hex 12)" > shadow/.ui.env
set -a; . shadow/.ui.env; DEMO_PASSWORD=$(sed -n 's/^DEMO_PASSWORD=//p' credentials.txt); set +a
c="docker compose -f compose.yaml -f shadow/compose.shadow.yaml --profile ui"
$c up -d --no-deps fusescope-shadow-ui
$c run --rm --no-deps shadow-compare-ui            # last settled hour
```

It logs in to the demo backend as the demo viewer.

## Swap

Once the shadow has run green, the roles swap: the senders point at this collector, which writes the demo's
ClickHouse, and the existing collector becomes the ghost, fed a copy by this collector and writing
`clickhouse-shadow`. The demo's UI then shows this collector's data, and the comparison runs the other way.

Since FuseScope 0.159.0 the demo kit itself runs `fusescope-collector` as its collector and RUM ingress
(`deploy/demo` in the fusescope repo), with no `otel-collector`: the swap is the demo's steady state, and
`compose.swap.yaml` defines the ghost instead of moving the kit's collector. On the demo host, update with all
three files, `docker compose -f compose.yaml -f shadow/compose.shadow.yaml -f shadow/compose.swap.yaml up -d`: the
kit's `update.sh` alone runs `--remove-orphans`, which stops the ghost. What follows the swap, up to 30 days without
the earlier collector, and the rollback per signal, are in [`CUTOVER.md`](CUTOVER.md); the "Swap back" below predates
the kit's own collector.

```
hotrod, telemetrygen ──► fusescope-collector (otlp) ──► clickhouse                (demo)
RUM (via Caddy) ───────► fusescope-collector (rum)  ─┘
                              └─ otlp/ghost ──► otel-collector ──► clickhouse-shadow   (ghost)
```

| File | What |
|---|---|
| `ghost-copy.yaml` | merged over the kit's `collector/config.yaml`: the copy of what the `otlp` and `rum` receivers accept, to the ghost |
| `compose.swap.yaml` | override after `compose.shadow.yaml`: merges `ghost-copy.yaml` into `fusescope-collector` and puts it on both networks, turns `fusescope-collector-shadow` off, and defines the ghost on the shadow network only, where `clickhouse` is an alias of `clickhouse-shadow`, with its configuration from the kit (`otel-collector/config.yaml`) |

- **The ghost's path never fails the demo.** The ghost's pipelines start from the same `otlp` and `rum` receivers with a
  `batch/ghost` processor and no memory limiter: they never refuse, and a full or failing 32 MiB queue drops copies.
  Only the demo's pipelines have the memory limiter; a request it refuses, retried by the sender, may store a copy
  twice in the ghost, which `compare.py` notes there.
- **The ghost's destination** is moved by its network, since its configuration is not read (ADR 0002): it is on the
  shadow network only, where `clickhouse` is an alias of `clickhouse-shadow`, so whatever its configuration names
  `clickhouse` is the shadow's. This collector, on both networks, reaches the demo's ClickHouse by its container name,
  `fusescope-demo-clickhouse-1`, which only the demo's network has, and so does `compare.py`. Setting the ghost's
  `SIGNOZ_OTEL_COLLECTOR_CLICKHOUSE_DSN` alone does not move its exporters: the first attempt, on 2026-11-04, left it
  writing the demo's ClickHouse, which `compare.py`'s duplicates check is there to catch.
- **No OpAMP**, as the existing collector on the demo runs without it (above).

### Switch

On the demo host, in `/opt/fusescope`, with this directory copied as in "Install" and the shadow green, one step at a
time, each checked before the next:

```sh
set -e
C="docker compose -f compose.yaml -f shadow/compose.shadow.yaml -f shadow/compose.swap.yaml"
# 0. clickhouse-shadow takes the `clickhouse` alias on the shadow network; the demo's ClickHouse is not touched.
$C up -d --no-deps --force-recreate --wait clickhouse-shadow
res() { docker run --rm --network "$1" python:3.12-alpine python3 -c "import socket,sys; print(sorted({a[4][0] for a in socket.getaddrinfo(sys.argv[1], None, socket.AF_INET)}))" "$2"; }
res fusescope-shadow clickhouse                     # clickhouse-shadow's address on fusescope-shadow, nothing else
res fusescope-demo fusescope-demo-clickhouse-1      # the demo's ClickHouse
# 1. The existing collector stops sending its copy to the shadow collector: the shadow's keys are removed from its
#    configuration, without reading it (the "Install" block, reversed).
cd otel-collector
cp -p config.yaml config.yaml.bak-pre-swap
yq() { docker run --rm --user "$(id -u):$(id -g)" -v "$PWD":/w \
  mikefarah/yq:4.44.3@sha256:5b3d851bf28b04ef902fee86305f4dd7e063919c1cafedab2042adb16221c025 "$@"; }
yq 'del(.exporters."otlp/shadow") | del(.processors."batch/shadow") | del(.service.pipelines."traces/shadow") | del(.service.pipelines."logs/shadow") | del(.service.pipelines."metrics/shadow")' /w/config.yaml > stripped.yaml
[ "$(yq '.exporters | has("otlp/shadow")' /w/stripped.yaml)" = false ]
mv stripped.yaml config.yaml
cd ..
# 2. This collector starts as the demo's ("Everything is ready" in its log); the shadow collector stops.
$C up -d --no-deps fusescope-collector
$C rm -sf fusescope-collector-shadow
# 3. The existing collector restarts as the ghost, on the shadow network only. This collector's copy reconnects
#    to it within a minute (its warnings about the ghost's old address stop).
$C up -d --no-deps --force-recreate otel-collector
# 4. The senders and the backend's RUM forward move to this collector.
$C --profile generators up -d --no-deps hotrod logs-checkout logs-payment-error traces-payment-errors
$C up -d --no-deps fusescope
```

Between steps 3 and 4 the senders reach no collector, since the ghost has left their network, and that time is lost;
run them back to back. It is preferred to the duplicates the other order would write.

Check every minute for the first few: rows and unique spans per minute must be equal on each side, and the two sides
equal to each other:

```sh
for c in fusescope-demo-clickhouse-1 fusescope-demo-clickhouse-shadow-1; do
  docker exec $c clickhouse-client --readonly 1 -q "SELECT toStartOfMinute(timestamp) m, count(), uniqExact(traceID, spanID) FROM signoz_traces.signoz_index_v3 WHERE timestamp > now() - INTERVAL 5 MINUTE GROUP BY m ORDER BY m"
done
```

More rows than unique spans in the demo's ClickHouse mean the ghost still writes there: swap back at once.

The hourly comparison takes the swap's override: [CUTOVER.md](CUTOVER.md) §2 has the cron line, with its run markers.

### Swap back

This collector goes first: while it runs it sends the ghost copies, which the existing collector would write into
the demo's ClickHouse once restored, as duplicates. The shadow collector starts before the existing collector, so its
copy has a destination from its first request: after the first attempt's swap back, the existing collector, started
first, stored the same spans hundreds of times until it ran out of memory. The senders retry for the seconds in between.

```sh
set -e
S="docker compose -f compose.yaml -f shadow/compose.shadow.yaml"
docker rm -f fusescope-demo-fusescope-collector-1
cp -p otel-collector/config.yaml.bak-pre-swap otel-collector/config.yaml
$S up -d --no-deps --force-recreate --wait clickhouse-shadow   # without the alias
$S up -d --no-deps fusescope-collector-shadow
$S up -d --no-deps --force-recreate otel-collector             # the demo's collector again, on the demo's network
$S --profile generators up -d --no-deps hotrod logs-checkout logs-payment-error traces-payment-errors
$S up -d --no-deps fusescope
```

Then the cron line without `compose.swap.yaml`.

## Upgrading from the tee version

The first version (#32) pointed the sources at an `otel-tee`. Before merging the exporter block, put the sources back
on the demo collector and remove the tee, or the shadow gets every item twice:

```sh
docker compose -f compose.yaml --profile generators up -d --no-deps hotrod logs-checkout logs-payment-error traces-payment-errors fusescope
docker rm -f fusescope-demo-otel-tee-1
```

## Rollback

Remove the shadow's keys from the current demo collector configuration, put its memory limiter back to the values
it had before the shadow, recreate it with the demo's compose alone (384 MiB), then stop the shadow:

```sh
set -e
cd /opt/fusescope/otel-collector
yq() { docker run --rm --user "$(id -u):$(id -g)" -v "$PWD":/w \
  mikefarah/yq:4.44.3@sha256:5b3d851bf28b04ef902fee86305f4dd7e063919c1cafedab2042adb16221c025 "$@"; }
strip='del(.exporters."otlp/shadow") | del(.processors."batch/shadow") | del(.service.pipelines."traces/shadow") | del(.service.pipelines."logs/shadow") | del(.service.pipelines."metrics/shadow")'
lim=$(yq '[.processors[] | select(has("limit_mib")) | .limit_mib][0]' /w/config.yaml.bak-pre-shadow)
spike=$(yq '[.processors[] | select(has("limit_mib")) | .spike_limit_mib][0]' /w/config.yaml.bak-pre-shadow)
yq "$strip | (.processors[] | select(has(\"limit_mib\"))) |= (.limit_mib = $lim | .spike_limit_mib = $spike)" /w/config.yaml > restored.yaml
mv restored.yaml config.yaml
cd .. && docker compose -f compose.yaml up -d --no-deps --force-recreate otel-collector
docker compose -f compose.yaml -f shadow/compose.shadow.yaml rm -sf fusescope-collector-shadow migrator-shadow clickhouse-shadow
docker volume rm fusescope-demo_clickhouse-shadow-data   # drops the shadow's data
```

## Risks

- The shadow pipelines sit behind a `batch/shadow` processor, which accepts the copy and exports it on its own: a
  full or failing `otlp/shadow` queue (32 MiB per signal) drops shadow copies and never fails a demo request. The
  keepalive, the 5 s timeout and the 30 s retry limit keep that queue draining while the shadow collector is away.
- Admission is not atomic across the receiver's two branches: if memory crosses the shared limiter's threshold
  between its calls to the demo and the shadow pipelines (microseconds apart), one side accepts and the other refuses,
  and the sender's retry duplicates on one side. At the demo's volume the collector stays far below the limit; a
  duplicate would show in `compare.py`. Limiting once before the branch would mean rewriting the demo collector's
  existing pipelines.
- The demo collector holds the queued copies in its own memory (768 MiB, limiter at 640 MiB); at the demo's volume
  that is a few MiB at most.
- The shadow collector shares the host's CPU and memory with the demo; the limits above bound it.
