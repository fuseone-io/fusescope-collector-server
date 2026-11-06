#!/usr/bin/env bash
# Reproduces the fusescope-schema spike (docs/spec/fusescope-schema.md §11): the
# KeeperMap lease and bootstrap protocol on a 2x2 ClickHouse cluster, with Keeper
# or ZooKeeper as the coordination service.
#   ./run.sh keeper|zookeeper > results-<coord>.txt
# Needs docker compose. Plain SQL only; prints PASS/FAIL per assertion.
set -u
COORD=${1:?keeper or zookeeper}
CH=clickhouse/clickhouse-server:25.12.5@sha256:cacf32d6884291dc2ff5e0156a97f46fc53ff7c929a7906d114e268a929dfd3a
KEEPER=clickhouse/clickhouse-keeper:25.12.5@sha256:525b8b0f93ccb371131c46f22a066ea43d33d74aba314203e8044609945d8524
# The chart's ZooKeeper (fusescope-charts, charts/clickhouse, zookeeper.image).
ZK=signoz/zookeeper:3.7.1@sha256:fcc4a3288154ccaa3bdb5ae6dc10180c084d29a8a6a26b62ac8e30a8940dc2e6
W=$(mktemp -d)
trap 'cd "$W" && docker compose down -v >/dev/null 2>&1; rm -rf "$W"' EXIT
cd "$W" || exit 1
NODES="ch-1-1 ch-1-2 ch-2-1 ch-2-2"
if [ "$COORD" = keeper ]; then PORT=9181; else PORT=2181; fi

coord_service() { # name
  if [ "$COORD" = keeper ]; then
    cat > "$1.xml" <<X
<clickhouse><logger><level>warning</level><console>1</console></logger><listen_host>0.0.0.0</listen_host>
<keeper_server><tcp_port>9181</tcp_port><server_id>1</server_id>
<log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path><snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
<raft_configuration><server><id>1</id><hostname>$1</hostname><port>9234</port></server></raft_configuration></keeper_server></clickhouse>
X
    printf '  %s:\n    image: %s\n    volumes: [./%s.xml:/etc/clickhouse-keeper/keeper_config.d/k.xml:ro]\n' "$1" "$KEEPER" "$1"
  else
    printf '  %s:\n    image: %s\n    environment: {ALLOW_ANONYMOUS_LOGIN: "yes"}\n' "$1" "$ZK"
  fi
}
node_config() { # node shard coord-host prefix (empty: no prefix)
  {
    echo "<clickhouse><logger><level>warning</level><console>1</console></logger><listen_host>0.0.0.0</listen_host>"
    echo "<zookeeper><node><host>$3</host><port>$PORT</port></node></zookeeper>"
    [ -n "$4" ] && echo "<keeper_map_path_prefix>$4</keeper_map_path_prefix>"
    echo "<macros><shard>$2</shard><replica>$1</replica></macros>"
    echo "<remote_servers><cluster>"
    echo "<shard><internal_replication>true</internal_replication><replica><host>ch-1-1</host><port>9000</port></replica><replica><host>ch-1-2</host><port>9000</port></replica></shard>"
    echo "<shard><internal_replication>true</internal_replication><replica><host>ch-2-1</host><port>9000</port></replica><replica><host>ch-2-2</host><port>9000</port></replica></shard>"
    echo "</cluster></remote_servers></clickhouse>"
  } > "$1.xml"
}
{
  echo "services:"
  coord_service coord
  coord_service coord-b
  for n in $NODES; do
    printf '  %s:\n    image: %s\n    environment: {CLICKHOUSE_SKIP_USER_SETUP: "1"}\n    volumes: [./%s.xml:/etc/clickhouse-server/config.d/spike.xml:ro]\n    depends_on: [coord]\n' "$n" "$CH" "$n"
  done
} > compose.yaml
for n in $NODES; do node_config "$n" "${n:3:1}" coord /keeper_map_tables; done
docker compose up -d >compose.log 2>&1 || { echo "ABORT: docker compose up failed"; cat compose.log; exit 2; }

Q() { docker compose exec -T "$1" clickhouse-client --keeper_map_strict_mode=1 --multiquery -q "$2" </dev/null 2>&1; }
# Ready once the server answers and reaches its coordination service.
up() {
  for _ in $(seq 90); do
    Q "$1" "SELECT count() > 0 FROM system.zookeeper WHERE path = '/'" | grep -qx 1 && return 0
    sleep 2
  done
  echo "ABORT: $1 did not reach its coordination service"
  exit 2
}
restart() { docker compose restart "$1" >/dev/null 2>&1 || { echo "ABORT: restart $1 failed"; exit 2; }; up "$1"; }
for n in $NODES; do up "$n"; done
pass=0
fail=0
check() { if [ "$2" = "$3" ]; then echo "PASS $1"; pass=$((pass + 1)); else echo "FAIL $1: got [$2] want [$3]"; fail=$((fail + 1)); fi; }
code() { grep -oE 'Code: [0-9]+' <<<"$1" | head -1; }
node() { echo $NODES | cut -d' ' -f$((($1 - 1) % 4 + 1)); }
echo "coordination: $COORD; ClickHouse $(Q ch-1-1 'SELECT version()')"

# 1. Concurrent bootstrap of the layout from all hosts, with a cluster identity.
for i in $(seq 8); do
  (
    n=$(node "$i")
    Q "$n" "CREATE DATABASE IF NOT EXISTS fusescope_schema ON CLUSTER cluster FORMAT Null" >/dev/null
    Q "$n" "CREATE TABLE IF NOT EXISTS fusescope_schema.meta ON CLUSTER cluster (name String, value String) ENGINE = KeeperMap('/fusescope_schema/meta') PRIMARY KEY name FORMAT Null" >/dev/null
    Q "$n" "CREATE TABLE IF NOT EXISTS fusescope_schema.lease ON CLUSTER cluster (name String, owner String, token UInt64, expires_at DateTime64(3)) ENGINE = KeeperMap('/fusescope_schema/lease') PRIMARY KEY name FORMAT Null" >/dev/null
    r1=$(Q "$n" "INSERT INTO fusescope_schema.meta VALUES ('cluster_id', 'id-$i')")
    r2=$(Q "$n" "INSERT INTO fusescope_schema.lease VALUES ('migrate', '', 0, 0)")
    echo "B$i meta:$(code "$r1") lease:$(code "$r2")"
  ) &
done > boot.txt
wait
check "bootstrap: one cluster_id insert wins, 7 get Code 999" "$(grep -c 'meta:Code: 999' boot.txt)" 7
check "bootstrap: one lease insert wins, 7 get Code 999" "$(grep -c 'lease:Code: 999' boot.txt)" 7
ids=$(for n in $NODES; do Q "$n" "SELECT value FROM fusescope_schema.meta WHERE name = 'cluster_id' FORMAT TSV"; done | sort -u | wc -l)
check "cluster identity: the same cluster_id read on each of the 4 replicas" "$ids" 1
IDCHECK="SELECT uniqExact(value) = 1 AND count() = 4 FROM clusterAllReplicas('cluster', fusescope_schema.meta) WHERE name = 'cluster_id' FORMAT TSV"
check "cluster identity through clusterAllReplicas: one value, 4 replicas" "$(Q ch-1-1 "$IDCHECK")" 1

ACQ() { Q "$1" "ALTER TABLE fusescope_schema.lease UPDATE owner = '$2', token = token + 1, expires_at = now64(3) + INTERVAL ${4:-300} SECOND WHERE name = 'migrate' AND token = $3 AND (owner = '' OR expires_at <= now64(3))"; }
REN() { Q "$1" "ALTER TABLE fusescope_schema.lease UPDATE expires_at = now64(3) + INTERVAL ${4:-300} SECOND WHERE name = 'migrate' AND owner = '$2' AND token = $3 AND expires_at > now64(3)"; }
R() { Q "$1" "SELECT owner, token FROM fusescope_schema.lease FORMAT TSV" | tr '\t' ' '; }
TOKEN() { Q ch-1-1 "SELECT token FROM fusescope_schema.lease FORMAT TSV"; }
FREE() { Q ch-1-1 "ALTER TABLE fusescope_schema.lease UPDATE owner = '' WHERE name = 'migrate'" >/dev/null; }

# 2. The single-process protocol.
ACQ ch-1-1 A 0 >/dev/null
check "acquire a free lease, read back on the same server" "$(R ch-1-1)" "A 1"
REN ch-1-1 A 1 >/dev/null
ACQ ch-2-1 B 1 >/dev/null
check "acquire with a token read before a renewal changes nothing" "$(R ch-2-1)" "A 1"
REN ch-1-1 A 1 2 >/dev/null
sleep 3
REN ch-1-1 A 1 >/dev/null
check "a renewal after expiry does not revive the lease" "$(Q ch-1-2 "SELECT expires_at > now64(3) FROM fusescope_schema.lease FORMAT TSV")" 0
ACQ ch-2-1 B 1 >/dev/null
check "acquire an expired lease" "$(R ch-2-1)" "B 2"
Q ch-1-1 "ALTER TABLE fusescope_schema.lease UPDATE owner = '' WHERE name = 'migrate' AND owner = 'A' AND token = 1" >/dev/null
check "a release with a stale token changes nothing" "$(R ch-2-2)" "B 2"

# 3. Eight concurrent acquires with read-back, ten rounds.
good=0
for _ in $(seq 10); do
  FREE
  T=$(TOKEN)
  for i in $(seq 8); do (n=$(node "$i"); ACQ "$n" "P$i" "$T" >/dev/null; echo "P$i $(R "$n")") & done > rb.txt
  wait
  self=$(awk -v t=$((T + 1)) '$1 == $2 && $3 == t' rb.txt | wc -l)
  [ "$self" = 1 ] && [ "$(TOKEN)" = $((T + 1)) ] && good=$((good + 1))
done
check "8 concurrent acquires: exactly one self-confirmed owner and token + 1, every round" "$good/10" "10/10"

# 4. A replica restarted with another keeper_map_path_prefix: its existing KeeperMap tables do not
#    activate, so it cannot answer, and the identity check fails with an error, which is a refusal.
idcheck() { out=$(Q ch-1-1 "$IDCHECK"); case "$out" in 1) echo pass ;; 0) echo mismatch ;; *) echo "error $(code "$out")" ;; esac; }
node_config ch-2-2 2 coord /other_prefix
restart ch-2-2
check "restart with another prefix: the replica's KeeperMap tables do not activate (Code 668)" "$(code "$(Q ch-2-2 "SELECT count() FROM fusescope_schema.lease")")" "Code: 668"
check "restart with another prefix: the identity check fails, with an error" "$(idcheck)" "error Code: 668"

# 5. A replica bootstrapped under another prefix (its own, fresh KeeperMap tables): a real split
#    brain on the lease, caught by the differing cluster identity.
boot_local() { # node id
  Q "$1" "DROP TABLE IF EXISTS fusescope_schema.meta SYNC; DROP TABLE IF EXISTS fusescope_schema.lease SYNC" >/dev/null
  Q "$1" "CREATE TABLE fusescope_schema.meta (name String, value String) ENGINE = KeeperMap('/fusescope_schema/meta') PRIMARY KEY name; CREATE TABLE fusescope_schema.lease (name String, owner String, token UInt64, expires_at DateTime64(3)) ENGINE = KeeperMap('/fusescope_schema/lease') PRIMARY KEY name" >/dev/null
  Q "$1" "INSERT INTO fusescope_schema.meta VALUES ('cluster_id', '$2'); INSERT INTO fusescope_schema.lease VALUES ('migrate', '', 0, 0)" >/dev/null
}
boot_local ch-2-2 id-other-prefix
FREE
ACQ ch-1-1 A2 "$(TOKEN)" >/dev/null
ACQ ch-2-2 Z 0 >/dev/null
check "bootstrapped under another prefix: that replica holds its own lease while ch-1-1 holds the real one" "$(R ch-2-2 | cut -d' ' -f1)/$(R ch-1-1 | cut -d' ' -f1)" "Z/A2"
check "bootstrapped under another prefix: the identity check finds differing values" "$(idcheck)" mismatch
Q ch-2-2 "DROP TABLE IF EXISTS fusescope_schema.meta SYNC; DROP TABLE IF EXISTS fusescope_schema.lease SYNC" >/dev/null

# 5b. The same with a replica on another coordination ensemble.
node_config ch-2-2 2 coord-b /keeper_map_tables
restart ch-2-2
Q ch-2-2 "CREATE DATABASE IF NOT EXISTS fusescope_schema" >/dev/null
boot_local ch-2-2 id-other-ensemble
check "bootstrapped on another ensemble: the identity check finds differing values" "$(idcheck)" mismatch
Q ch-2-2 "DROP TABLE IF EXISTS fusescope_schema.meta SYNC; DROP TABLE IF EXISTS fusescope_schema.lease SYNC" >/dev/null

# 5c. Another ensemble, but with a copy of the original identity: the identity check passes, and
#     the lease comparison on every replica is what refuses.
REAL_ID=$(Q ch-1-1 "SELECT value FROM fusescope_schema.meta WHERE name = 'cluster_id' FORMAT TSV")
boot_local ch-2-2 "$REAL_ID"
check "copied identity on another ensemble: the identity check passes" "$(idcheck)" pass
FREE
ACQ ch-1-1 A3 "$(TOKEN)" >/dev/null
LEASECHECK="SELECT uniqExact(owner, token) = 1 AND count() = 4 FROM clusterAllReplicas('cluster', fusescope_schema.lease) FORMAT TSV"
check "copied identity on another ensemble: the lease comparison across replicas fails" "$(Q ch-1-1 "$LEASECHECK")" 0
Q ch-2-2 "DROP TABLE IF EXISTS fusescope_schema.meta SYNC; DROP TABLE IF EXISTS fusescope_schema.lease SYNC" >/dev/null

# Back on the shared coordination: re-attach the replica's tables to the shared maps.
node_config ch-2-2 2 coord /keeper_map_tables
restart ch-2-2
Q ch-2-2 "CREATE TABLE IF NOT EXISTS fusescope_schema.meta (name String, value String) ENGINE = KeeperMap('/fusescope_schema/meta') PRIMARY KEY name; CREATE TABLE IF NOT EXISTS fusescope_schema.lease (name String, owner String, token UInt64, expires_at DateTime64(3)) ENGINE = KeeperMap('/fusescope_schema/lease') PRIMARY KEY name" >/dev/null
check "back on the shared coordination: the identity check passes" "$(idcheck)" pass
check "back on the shared coordination: every replica shows the same lease" "$(Q ch-1-1 "$LEASECHECK")" 1

# 6. Coordination lost during an update, then back.
FREE
T=$(TOKEN)
docker compose stop coord >/dev/null 2>&1
out=$(ACQ ch-1-1 K "$T")
check "coordination down: the conditional update fails with an error" "$([ -n "$(code "$out")" ] && echo error)" error
docker compose start coord >/dev/null 2>&1
for _ in $(seq 60); do [ "$(TOKEN)" = "$T" ] && break; sleep 2; done
check "coordination back: the lease is unchanged" "$(R ch-1-1)" " $T"

# 7. No keeper_map_path_prefix, and no way to read it.
node_config ch-2-2 2 coord ""
restart ch-2-2
check "no prefix: KeeperMap is refused with Code 36" "$(code "$(Q ch-2-2 "CREATE TABLE default.probe (k String) ENGINE = KeeperMap('/p') PRIMARY KEY k")")" "Code: 36"
check "the prefix is not exposed in system.server_settings" "$(Q ch-1-1 "SELECT count() FROM system.server_settings WHERE name = 'keeper_map_path_prefix' FORMAT TSV")" 0

echo "summary: $pass passed, $fail failed"
[ "$fail" = 0 ]
