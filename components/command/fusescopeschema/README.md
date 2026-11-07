# fusescope-schema

Our own ClickHouse migrator for the SigNoz-compatible schema ([ADR
0010](../../../docs/adr/0010-fusescope-schema.md), [spec](../../../docs/spec/fusescope-schema.md)).
Clean-room: nothing here comes from the fork's migrator.

- `cmd/fusescope-schema`: the binary, with the commands `migrate`, `check [--wait D]`, `plan`,
  `status` and `repair --id N --mark done|absent` (spec §1), in `internal/cli`.
- `internal/migrate`: the starting point and the runner under the lease. It also holds the
  read-only inspection behind `check`, `plan` and `status`, and `repair`.
- `internal/migrations`: the migrations compiled in, `0001 base` and `0002
  column_evolution_baseline`, with their post-conditions and data conditions (spec §5).
- `internal/schema`: the structural comparison, the backend's allow-list and the data conditions
  (spec §4.1, §5.2, §8).
- `internal/coord`: the coordination protocol, independent of ClickHouse: the bootstrap of
  `fusescope_schema`, the cluster identity and the lease (spec §3, §6).
- `internal/chstore`: everything that runs on ClickHouse: the connection (TLS 1.3, password from a
  file), the precheck, the coordination and history tables, the structure read from the system
  tables, and the rows a data condition owns.

Integration tests need a real 2x2 cluster:

```sh
make schema-integration COORDINATION=keeper     # or zookeeper
```

The binary ships in the collector's image as `/fusescope-schema`, and the chart runs it (chart
0.162.0).
