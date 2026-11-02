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

A first version put a tee in front of the demo collector. After the shadow collector was recreated, the tee was left
with a dead gRPC connection to it, its shadow side backed up, the senders retried, and the demo collector was
overloaded: the demo lost about 7 minutes of data (2026-11-02, 22:29–22:36 UTC) and got duplicated spans at 22:28.
Hence no hop in front of the demo, and an exporter that detects a dead connection (below).

| File | What |
|---|---|
| `compose.shadow.yaml` | override for the demo's `compose.yaml`: adds the shadow services |
| `collector.yaml` | this collector's configuration in shadow |
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
c="docker compose -f compose.yaml -f shadow/compose.shadow.yaml"
$c up -d --no-deps clickhouse-shadow
$c up --no-deps migrator-shadow
$c up -d --no-deps fusescope-collector-shadow
```

Then the demo's owner adds to `otel-collector/config.yaml` (this repository does not read that file, ADR 0002) an
exporter and one pipeline per signal fed by the existing OTLP receiver, leaving the existing pipelines as they are;
use the receiver's actual name if it is not `otlp`:

```yaml
exporters:
  otlp/shadow:
    endpoint: dns:///fusescope-collector-shadow:4317   # re-resolved on reconnect
    tls:
      insecure: true
    timeout: 5s
    keepalive:            # a dead connection is found in about 15 s, not left hanging
      time: 10s
      timeout: 5s
      permit_without_stream: true
    sending_queue:
      enabled: true
      queue_size: 5000
    retry_on_failure:     # the queue drains instead of filling while the shadow is away
      enabled: true
      max_elapsed_time: 30s

service:
  pipelines:
    traces/shadow:
      receivers: [otlp]
      exporters: [otlp/shadow]
    logs/shadow:
      receivers: [otlp]
      exporters: [otlp/shadow]
    metrics/shadow:
      receivers: [otlp]
      exporters: [otlp/shadow]
```

and restarts it with `docker restart fusescope-demo-otel-collector-1`. The shadow pipelines carry exactly what the
OTLP receiver accepted, before any of the demo collector's processing, so neither its host metrics nor its span
metrics reach the shadow.

Check: the demo's UI keeps showing new data, and `docker logs fusescope-demo-otel-collector-1` shows no
`otlp/shadow` export errors.

Upgrading the shadow collector (new image or configuration) needs nothing on the demo side: the demo collector's
exporter reconnects. Resources: about 3 GiB and 1.1 CPU more (`clickhouse-shadow` 2.5 GiB, collector 384 MiB).

## Compare

```sh
$c run --rm shadow-compare                                  # last settled hour
$c run --rm -e WINDOW_MINUTES=1440 shadow-compare           # last day
```

Counts of spans and logs per service and minute, error spans and APM calls per service, resource and attribute
keys, and series per metric must be equal. Phase 6 asks for a week with every hourly run green, e.g. from cron:

```
5 * * * * cd /opt/fusescope && docker compose -f compose.yaml -f shadow/compose.shadow.yaml run --rm shadow-compare >> /var/log/fusescope-shadow.log 2>&1
```

## Rollback

Remove the `otlp/shadow` exporter and the `*/shadow` pipelines from `otel-collector/config.yaml`, restart
`otel-collector`, then stop the shadow:

```sh
docker compose -f compose.yaml -f shadow/compose.shadow.yaml rm -sf fusescope-collector-shadow migrator-shadow clickhouse-shadow
docker volume rm fusescope-demo_clickhouse-shadow-data   # drops the shadow's data
```

## Risks

- A full `otlp/shadow` queue fails the request at the demo's OTLP receiver, as any exporter of a pipeline does, and
  senders retry. The keepalive, the 5 s timeout and the 30 s retry limit keep the queue (5 000 requests) from filling
  at the demo's few requests a second, even while the shadow collector is down; if it stays unreachable, roll back.
- The demo collector holds the queued copies in its own memory (384 MiB limit); at the demo's volume that is a few
  MiB at most.
- The shadow collector shares the host's CPU and memory with the demo; the limits above bound it.
