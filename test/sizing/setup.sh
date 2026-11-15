#!/bin/bash
# Builds the benchmark environment: a kind cluster, throwaway TLS material and tokens, the
# fusescope chart 0.170.1 with the agents' listener and an OTLP-only agent, the telemetrygen
# image, the batching relay, and port-forwards to both collectors' loopback :8888
# (agent on 18001, gateway on 18002). Certificates and tokens stay in a temp dir, never here.
set -euo pipefail
cd "$(dirname "$0")"
CHART_VERSION=${CHART_VERSION:-0.170.1}
TG=ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:v0.135.0
k() { kubectl --context kind-bench "$@"; }

kind create cluster --name bench --config kind.yaml
k wait --for=condition=Ready node --all --timeout=180s
k create namespace platform
k create namespace apps

T=$(mktemp -d)
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout "$T/ca.key" -out "$T/ca.crt" -days 2 -subj /CN=bench-ca
mk() { # name subjectAltName extendedKeyUsage
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -keyout "$T/$1.key" -out "$T/$1.csr" -subj "/CN=$1"
  printf "subjectAltName=%s\nextendedKeyUsage=%s\n" "$2" "$3" > "$T/$1.ext"
  openssl x509 -req -in "$T/$1.csr" -CA "$T/ca.crt" -CAkey "$T/ca.key" -CAcreateserial \
    -out "$T/$1.crt" -days 2 -extfile "$T/$1.ext"
}
mk listener "DNS:fusescope-collector,DNS:fusescope-collector.platform,DNS:fusescope-collector.platform.svc,DNS:fusescope-collector.platform.svc.cluster.local" serverAuth
mk agent "DNS:fusescope-agent" clientAuth
mk sdkserver "DNS:fusescope-agent-otlp,DNS:fusescope-agent-otlp.platform,DNS:fusescope-agent-otlp.platform.svc,DNS:fusescope-agent-otlp.platform.svc.cluster.local" serverAuth

k -n platform create secret generic agents-tls --type=kubernetes.io/tls \
  --from-file=tls.crt="$T/listener.crt" --from-file=tls.key="$T/listener.key" --from-file=ca.crt="$T/ca.crt"
k -n platform create secret generic agent-client --type=kubernetes.io/tls \
  --from-file=tls.crt="$T/agent.crt" --from-file=tls.key="$T/agent.key" --from-file=ca.crt="$T/ca.crt"
k -n platform create secret tls agent-otlp-server --cert="$T/sdkserver.crt" --key="$T/sdkserver.key"
k -n platform create secret generic agent-token --from-literal=token="$(openssl rand -hex 32)"
SDK=$(openssl rand -hex 32)
k -n platform create secret generic fusescope-agent-otlp-token --from-literal=token="$SDK"
k -n apps create secret generic fusescope-agent-otlp-token --from-literal=token="$SDK"
k -n apps create secret generic fusescope-agent-ca --from-file=ca.crt="$T/ca.crt"
rm -rf "$T"

helm repo add fusescope https://fuseone-io.github.io/fusescope-charts >/dev/null 2>&1 || true
helm repo update fusescope
helm --kube-context kind-bench install fusescope fusescope/fusescope --version "$CHART_VERSION" \
  -n platform -f values.yaml --wait --timeout 15m

docker pull -q "$TG"
kind load docker-image --name bench "$TG"
k apply -f relay.yaml

nohup ./pf.sh app.kubernetes.io/component=fusescope-agent 18001 >/dev/null 2>&1 &
nohup ./pf.sh app.kubernetes.io/component=fusescope-collector 18002 >/dev/null 2>&1 &
sleep 3
curl -sf -o /dev/null localhost:18001/metrics && curl -sf -o /dev/null localhost:18002/metrics
echo "ready"
