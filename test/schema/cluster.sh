#!/usr/bin/env bash
# A 2x2 ClickHouse cluster with Keeper or ZooKeeper for fusescope-schema's
# integration tests (docs/spec/fusescope-schema.md §9). The four servers'
# native ports are 127.0.0.1:19001..19004. The same servers also form the
# cluster fusescope-custom, for a custom cluster name.
#   test/schema/cluster.sh up [keeper|zookeeper]   prints the cluster's directory,
#                                                  whose addrs file holds FUSESCOPE_SCHEMA_TEST_ADDRS
#   test/schema/cluster.sh down DIR
set -euo pipefail
MARKER=.fusescope-schema-cluster
CH=clickhouse/clickhouse-server:25.12.5@sha256:cacf32d6884291dc2ff5e0156a97f46fc53ff7c929a7906d114e268a929dfd3a
KEEPER=clickhouse/clickhouse-keeper:25.12.5@sha256:525b8b0f93ccb371131c46f22a066ea43d33d74aba314203e8044609945d8524
ZK=signoz/zookeeper:3.7.1@sha256:fcc4a3288154ccaa3bdb5ae6dc10180c084d29a8a6a26b62ac8e30a8940dc2e6
NODES="ch-1-1 ch-1-2 ch-2-1 ch-2-2"

# down removes only a directory up created: a mktemp directory holding the marker.
down() {
  local dir=$1
  case "$dir" in
    /*/fusescope-schema-cluster.??????) ;;
    *) echo "refusing to remove $dir: not a directory made by $0 up" >&2; return 2 ;;
  esac
  if [ -L "$dir" ] || [ ! -d "$dir" ] || [ ! -f "$dir/$MARKER" ]; then
    echo "refusing to remove $dir: not a directory made by $0 up" >&2
    return 2
  fi
  docker compose -f "$dir/compose.yaml" down -v >/dev/null 2>&1 || true
  rm -rf -- "$dir"
}

if [ "${1:-}" = down ]; then
  [ -n "${2:-}" ] || { echo "usage: $0 down DIR" >&2; exit 2; }
  down "$2"
  exit
fi
[ "${1:-}" = up ] || { echo "usage: $0 up [keeper|zookeeper] | down DIR" >&2; exit 2; }
COORD=${2:-keeper}
case "$COORD" in keeper | zookeeper) ;; *) echo "unknown coordination service $COORD" >&2; exit 2 ;; esac
DIR=$(mktemp -d "${TMPDIR:-/tmp}/fusescope-schema-cluster.XXXXXX")
touch "$DIR/$MARKER"
trap 'down "$DIR"' EXIT # until the cluster is up
cd "$DIR"
if [ "$COORD" = keeper ]; then PORT=9181; else PORT=2181; fi
{
  echo "services:"
  if [ "$COORD" = keeper ]; then
    cat > coord.xml <<X
<clickhouse><logger><level>warning</level><console>1</console></logger><listen_host>0.0.0.0</listen_host>
<keeper_server><tcp_port>9181</tcp_port><server_id>1</server_id>
<log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path><snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
<raft_configuration><server><id>1</id><hostname>coord</hostname><port>9234</port></server></raft_configuration></keeper_server></clickhouse>
X
    printf '  coord:\n    image: %s\n    volumes: [./coord.xml:/etc/clickhouse-keeper/keeper_config.d/k.xml:ro]\n' "$KEEPER"
  else
    printf '  coord:\n    image: %s\n    environment: {ALLOW_ANONYMOUS_LOGIN: "yes"}\n' "$ZK"
  fi
  i=0
  for n in $NODES; do
    i=$((i + 1))
    cat > "$n.xml" <<X
<clickhouse><logger><level>warning</level><console>1</console></logger><listen_host>0.0.0.0</listen_host>
<zookeeper><node><host>coord</host><port>$PORT</port></node></zookeeper>
<keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>
<macros><shard>${n:3:1}</shard><replica>$n</replica></macros>
<remote_servers><cluster>
<shard><internal_replication>true</internal_replication><replica><host>ch-1-1</host><port>9000</port></replica><replica><host>ch-1-2</host><port>9000</port></replica></shard>
<shard><internal_replication>true</internal_replication><replica><host>ch-2-1</host><port>9000</port></replica><replica><host>ch-2-2</host><port>9000</port></replica></shard>
</cluster>
<fusescope-custom>
<shard><internal_replication>true</internal_replication><replica><host>ch-1-1</host><port>9000</port></replica><replica><host>ch-1-2</host><port>9000</port></replica></shard>
<shard><internal_replication>true</internal_replication><replica><host>ch-2-1</host><port>9000</port></replica><replica><host>ch-2-2</host><port>9000</port></replica></shard>
</fusescope-custom></remote_servers></clickhouse>
X
    printf '  %s:\n    image: %s\n    hostname: %s\n    environment: {CLICKHOUSE_SKIP_USER_SETUP: "1"}\n    ports: ["127.0.0.1:1900%d:9000"]\n    volumes: [./%s.xml:/etc/clickhouse-server/config.d/test.xml:ro]\n    depends_on: [coord]\n' "$n" "$CH" "$n" "$i" "$n"
  done
} > compose.yaml
docker compose up -d >compose.log 2>&1 || { cat compose.log >&2; exit 2; }
for n in $NODES; do
  ok=
  for _ in $(seq 90); do
    if docker compose exec -T "$n" clickhouse-client -q "SELECT count() > 0 FROM system.zookeeper WHERE path = '/'" </dev/null 2>/dev/null | grep -qx 1; then ok=1; break; fi
    sleep 2
  done
  [ -n "$ok" ] || { echo "$n did not reach its coordination service" >&2; exit 2; }
done
echo 127.0.0.1:19001,127.0.0.1:19002,127.0.0.1:19003,127.0.0.1:19004 > addrs
trap - EXIT
echo "$DIR"
