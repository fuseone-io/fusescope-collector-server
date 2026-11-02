# Observation harness

Runs the existing collector (`fusescope-otel-collector`) as a black box against ClickHouse,
sends a deterministic OTLP corpus and records what it wrote: the DDL of every table and the
rows of every table that received data. The output under `docs/spec/observed/<version>/` is
the observed-data source for `docs/spec/` (ADR 0002). Nothing here reads the collector's
source.

```sh
test/observe/observe.sh docs/spec/observed/0.145.0   # needs Docker, curl, python3
```

Beside a stack that holds ports 8123, 4318 or 13133, move them and the compose project:

```sh
COMPOSE_PROJECT_NAME=observe-local OBSERVE_CLICKHOUSE_PORT=38123 OBSERVE_OTLP_PORT=34318 \
  OBSERVE_HEALTH_PORT=33133 test/observe/observe.sh docs/spec/observed/0.145.0
```

`corpus.py` fixes every ID, name and value; only timestamps move, as offsets from `t0`, so
the same corpus fed to two collectors must produce the same rows. The parity suite will
reuse it.

The CI workflow `observe` runs this on demand and commits the result.
