# Shadow on the FuseScope demo

Runs this collector next to the demo's collector on the same traffic, writing to its own ClickHouse,
and compares the two (DESIGN.md §9, phase 6, [#7](https://github.com/fuseone-io/fusescope-collector-server/issues/7)).
The demo's collector, ClickHouse and UI are unchanged: only the sources point at a tee.

```
hotrod, telemetrygen, RUM (via fusescope) ──► otel-tee ──► otel-collector ──► clickhouse          (demo, unchanged)
                                                  └──────► fusescope-collector-shadow ──► clickhouse-shadow
                                       shadow-compare: both, read-only, per settled window
```

| File | What |
|---|---|
| `compose.shadow.yaml` | override for the demo's `compose.yaml`: points the sources at `otel-tee`, adds the shadow services |
| `tee.yaml` | upstream core collector: OTLP in, OTLP to the demo collector and, through a shadow-only pipeline, to the shadow collector |
| `collector.yaml` | this collector's configuration in shadow |
| `clickhouse/cluster.xml` | single-node `cluster` with an embedded keeper, named `clickhouse-shadow` |
| `compare.py` | the comparison; exits 1 on a difference |

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
  Hence the `cpu`, `load`, `disk`, `filesystem` and `memory` scrapers at 60 s and the `system` detector.
- **APM metrics** (`signoz_calls_total`, `signoz_latency`, `signoz_external_call_latency_*`) come from spans: the
  `fusescope_spanmetrics` connector.
- **Not reproduced:** the demo collector scrapes its own telemetry (`otelcol_*`, `exporter_*`, `scrape_*`, `up`);
  `compare.py` leaves those out. The demo collector runs without OpAMP (its command line is `--config` only), so the
  shadow does not connect to the backend either.

## Install

On a workstation, from this repository at the commit to shadow:

```sh
docker build -t fusescope-collector:shadow .
docker save fusescope-collector:shadow | ssh root@<demo-host> docker load
scp -r deploy/shadow root@<demo-host>:/opt/fusescope/   # lands in /opt/fusescope/shadow, also on re-runs
```

On the demo host, in `/opt/fusescope`:

```sh
c="docker compose -f compose.yaml -f shadow/compose.shadow.yaml --profile generators"
$c up -d clickhouse-shadow migrator-shadow fusescope-collector-shadow otel-tee
$c up -d hotrod logs-checkout logs-payment-error traces-payment-errors fusescope   # sources to the tee
```

Recreating `fusescope` restarts the backend (a few seconds of UI and RUM unavailability).

Check: `docker logs fusescope-demo-otel-tee-1` and `fusescope-demo-fusescope-collector-shadow-1` show no export
errors, and the demo's UI keeps showing new data.

Resources: about 2.3 GiB and 1.3 CPU more (`clickhouse-shadow` 1.5 GiB, collector 384 MiB, tee 384 MiB).

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

Point the sources back at the demo collector, then stop the shadow:

```sh
docker compose -f compose.yaml --profile generators up -d hotrod logs-checkout logs-payment-error traces-payment-errors fusescope
docker compose -f compose.yaml -f shadow/compose.shadow.yaml rm -sf otel-tee fusescope-collector-shadow migrator-shadow clickhouse-shadow
docker volume rm fusescope-demo_clickhouse-shadow-data   # drops the shadow's data
```

## Risks

- The tee is a new hop on the demo's path. If it stops, the demo gets no data until rollback.
- The shadow side never fails a request to the demo: it sits behind a forward connector and a batch processor, which
  accept the data and export it on their own. A slow, full (32 MiB per signal) or failing shadow path drops shadow
  data, seen as differences in `compare.py`. Checked with the demo destination healthy and the shadow queue full:
  the sender saw no error and the demo received everything. Only the tee's memory limiter (300 MiB) can refuse, under
  a load the demo never sends.
- The shadow collector shares the host's CPU and memory with the demo; the limits above bound it.
