# fusescope-schema

Our own ClickHouse migrator for the SigNoz-compatible schema ([ADR
0010](../../../docs/adr/0010-fusescope-schema.md), [spec](../../../docs/spec/fusescope-schema.md)).
Clean-room: nothing here comes from the fork's migrator.

Built so far:

- `internal/coord`: the coordination protocol, independent of ClickHouse. It covers the
  bootstrap of `fusescope_schema` (read first, then complete), the cluster identity, and the
  lease. The lease is taken and renewed by conditional writes confirmed by reading back, and
  compared exactly on every replica (spec §3, §6). Unit tests run against a fake that models the
  KeeperMap behaviour the spike observed.
- `internal/chstore`: the connection (TLS 1.3, password from a file) and the coordination store
  on KeeperMap tables. Its integration tests need a real cluster:

  ```sh
  make schema-integration COORDINATION=keeper     # or zookeeper
  ```

Still to come: the structural comparison and the backend's allow-list, migration `0001`
and adoption, and the commands (`migrate`, `check`, `plan`, `status`, `repair`).
