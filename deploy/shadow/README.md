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
| `compare.py`, `compare_test.py` | the comparison, exits 1 on a difference; its unit test (`python3 -m unittest deploy/shadow/compare_test.py`) |

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
- **Not reproduced:** the demo collector scrapes its own telemetry (`otelcol_*`, `exporter_*`, `scrape_*`, `up`);
  `compare.py` leaves those out, and ignores the scope version, which is each collector's build version. The demo collector runs without OpAMP (its command line is `--config` only), so the
  shadow does not connect to the backend either.

## Install

On a workstation, from this repository at the commit to shadow:

```sh
docker build -t fusescope-collector:shadow .
docker save fusescope-collector:shadow | ssh root@<demo-host> docker load
tar czf - -C deploy shadow | ssh root@<demo-host> 'tar xzf - --no-same-owner -C /opt/fusescope'
```

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

# The shadow pipelines start with the demo pipelines' own memory_limiter
# (that is its ID in the demo collector; the install checks it exists). The
# processor is one shared instance per ID, so a request is refused or accepted
# for both sides at once, and a sender's retry does not reach the shadow twice.
# The batch processor accepts the copy and exports it on its own, so a full or
# failing shadow queue drops shadow copies and never fails a demo request.
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
# The backup is the configuration before any shadow change; every run starts from it.
[ -e config.yaml.bak-pre-shadow ] || cp -p config.yaml config.yaml.bak-pre-shadow
yq() { docker run --rm --user "$(id -u):$(id -g)" -v "$PWD":/w -v /opt/fusescope/shadow:/s:ro mikefarah/yq:4.44.3 "$@"; }
# Headroom: its memory limiter refused at 256 MiB (limit_mib 320, spike 64, from its own logs).
yq '(.processors[] | select(has("limit_mib"))) |= (.limit_mib = 640 | .spike_limit_mib = 128)' /w/config.yaml.bak-pre-shadow > limited.yaml
yq eval-all '. as $i ireduce ({}; . * $i)' /w/limited.yaml /s/demo-collector-shadow.yaml > merged.yaml
[ "$(yq '.exporters."otlp/shadow".endpoint' /w/merged.yaml)" = dns:///fusescope-collector-shadow:4317 ]
[ "$(yq '.processors | has("memory_limiter")' /w/merged.yaml)" = true ]   # the demo pipelines' limiter the block reuses
mv merged.yaml config.yaml && rm limited.yaml
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
keys, and series per metric must be equal. Phase 6 asks for a week with every hourly run green, e.g. from cron:

```
5 * * * * cd /opt/fusescope && docker compose -f compose.yaml -f shadow/compose.shadow.yaml run --rm shadow-compare >> /var/log/fusescope-shadow.log 2>&1
```

## Upgrading from the tee version

The first version (#32) pointed the sources at an `otel-tee`. Before merging the exporter block, put the sources back
on the demo collector and remove the tee, or the shadow gets every item twice:

```sh
docker compose -f compose.yaml --profile generators up -d --no-deps hotrod logs-checkout logs-payment-error traces-payment-errors fusescope
docker rm -f fusescope-demo-otel-tee-1
```

## Rollback

Restore `otel-collector/config.yaml.bak-pre-shadow`, restart the demo collector with the demo's compose alone, then stop
the shadow:

```sh
cp -p otel-collector/config.yaml.bak-pre-shadow otel-collector/config.yaml
docker compose -f compose.yaml up -d --no-deps otel-collector
docker compose -f compose.yaml -f shadow/compose.shadow.yaml rm -sf fusescope-collector-shadow migrator-shadow clickhouse-shadow
docker volume rm fusescope-demo_clickhouse-shadow-data   # drops the shadow's data
```

## Risks

- The shadow pipelines sit behind a `batch/shadow` processor, which accepts the copy and exports it on its own: a
  full or failing `otlp/shadow` queue (32 MiB per signal) drops shadow copies and never fails a demo request. The
  keepalive, the 5 s timeout and the 30 s retry limit keep that queue draining while the shadow collector is away.
- The demo collector holds the queued copies in its own memory (768 MiB, limiter at 640 MiB); at the demo's volume
  that is a few MiB at most.
- The shadow collector shares the host's CPU and memory with the demo; the limits above bound it.
