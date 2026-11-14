# Sizing benchmark

The measurements behind the FuseScope docs page "Sizing and resources"
(`fusescope-docs`, `src/content/docs/collector/sizing.mdx`). It drives the production path
end to end and reads the collectors' own metrics:

```text
telemetrygen ─► agent otlp/sdks (TLS 1.3 + bearer token) ─► gateway otlp/agents (mTLS + token) ─► ClickHouse
```

It was run on 2026-11-14 with chart `fusescope-0.170.0`. The image was overridden to collector
0.6.4, `sha256:65e3c13edb375a795ea1b3f6e3deba4c37e4326a51e3c0917d308c352800704d`, which changed
only documentation since 0.6.3. The machine was a kind cluster (one control plane, one worker)
on an AMD Ryzen 9 5950X.

## Files

| File | What |
|---|---|
| `kind.yaml` | The cluster. |
| `values.yaml` | Chart values: the agents' listener, an OTLP-only agent, and the image override. The Secrets it names hold no data here; create them as below. |
| `bench.py` | One step: starts a `telemetrygen` Job, waits 40 s, samples both collectors' `:8888` every 5 s for 90 s, and prints one JSON line. |
| `relay.yaml` | For logs and metrics: a collector in the generator pod that sends batches of 512 records to the agent, as SDKs do. `telemetrygen` alone sends one log or point per request. |
| `run.sh` | Runs each `SIGNAL TOTAL_PER_S` line of `steps.txt`, one worker per 250 records/s, and records gateway restarts per step. |
| `pf.sh` | Keeps a port-forward to a pod's loopback `:8888` alive across restarts. |
| `results/valid.jsonl` | The rows the docs page uses. |
| `results/raw-*.jsonl` | Earlier runs, kept for the record. They are not valid measurements, see below. |

## Run it

```bash
kind create cluster --name bench --config kind.yaml
kubectl --context kind-bench create ns platform
kubectl --context kind-bench create ns apps

# A throwaway CA, the listener's, the agents' and the agent OTLP server certificates.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -keyout ca.key -out ca.crt -days 2 -subj /CN=bench-ca
mk() { openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -keyout $1.key -out $1.csr -subj /CN=$1
       printf "subjectAltName=%s\nextendedKeyUsage=%s\n" "$2" "$3" > $1.ext
       openssl x509 -req -in $1.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out $1.crt -days 2 -extfile $1.ext; }
mk listener  "DNS:fusescope-collector.platform.svc" serverAuth
mk agent     "DNS:fusescope-agent" clientAuth
mk sdkserver "DNS:fusescope-agent-otlp,DNS:fusescope-agent-otlp.platform.svc" serverAuth

k() { kubectl --context kind-bench "$@"; }
k -n platform create secret generic agents-tls --type=kubernetes.io/tls --from-file=tls.crt=listener.crt --from-file=tls.key=listener.key --from-file=ca.crt=ca.crt
k -n platform create secret generic agent-client --type=kubernetes.io/tls --from-file=tls.crt=agent.crt --from-file=tls.key=agent.key --from-file=ca.crt=ca.crt
k -n platform create secret tls agent-otlp-server --cert=sdkserver.crt --key=sdkserver.key
k -n platform create secret generic agent-token --from-literal=token=$(openssl rand -hex 32)
SDK=$(openssl rand -hex 32)
k -n platform create secret generic fusescope-agent-otlp-token --from-literal=token=$SDK
k -n apps create secret generic fusescope-agent-otlp-token --from-literal=token=$SDK
k -n apps create secret generic fusescope-agent-ca --from-file=ca.crt=ca.crt

helm --kube-context kind-bench install fusescope fusescope/fusescope --version 0.170.0 -n platform -f values.yaml
# For rows past the default memory limit:
#   helm upgrade ... --set fusescopeCollector.resources.limits.memory=4Gi

kind load docker-image --name bench ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:v0.135.0
k apply -f relay.yaml

./pf.sh pod/<agent-pod> 18001 &
./pf.sh deploy/fusescope-collector 18002 &

printf '%s\n' "traces 10000" "logs 10000" "metrics 10000" > steps.txt
./run.sh            # appends to results.jsonl, restarts.jsonl and errors.log
```

Start one runner at a time. Two runners share the Job names and corrupt each other's steps.

## Results

`results/valid.jsonl`, one row per step. CPU is the rate of `otelcol_process_cpu_seconds` over
the window; memory is the peak of `otelcol_process_memory_rss`.

- **Traces, 2k to 10k spans/s:** default gateway limit, 1Gi.
- **Traces, 20k to 60k spans/s:** gateway limit 4Gi. At 60k offered, 48k were accepted and the
  rest refused at the agent.
- **Logs and metrics:** gateway limit 4Gi, through the relay.

## Failures observed

- **The gateway was OOMKilled at 1Gi and 20k spans/s.** It restarted 4 times, the first at
  04:56:29Z. After each restart it was killed again within about 11 s, while the agent resent
  its backlog. `memory_limiter` (80%, check every 1 s) logged no refusal before the kill.
- **The agent was OOMKilled at its 512Mi default** at 60k log records/s and at 100k metric
  points/s.
- **The agent's sending queue (counted in requests) filled** when ClickHouse inserts slowed, and
  the agent then refused requests.

## Invalid runs, kept as raw

- **`raw-run1`:** 4 `telemetrygen` workers could not offer more than about 7k spans/s, so the
  generator limited the throughput. Fixed with one worker per 250/s.
- **`raw-run2-1gi`:** its trace rows are in `valid.jsonl`. The rest failed when the gateway
  OOM broke the port-forward.
- **`raw-run3-4gi-traces`:** its trace rows are in `valid.jsonl`. Its logs and metrics rows are
  invalid: `telemetrygen` sent one record per request, and its logs and metrics generators exit
  on the first refused export.
