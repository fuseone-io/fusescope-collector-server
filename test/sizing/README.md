# Sizing benchmark

The measurements behind the FuseScope docs page "Sizing and resources"
(`fusescope-docs`, `src/content/docs/collector/sizing.mdx`). It drives the production path end
to end and reads the collectors' own metrics:

```text
telemetrygen ─► agent otlp/sdks (TLS 1.3 + bearer token) ─► gateway otlp/agents (mTLS + token) ─► ClickHouse
```

The runs below used chart `fusescope-0.170.1`, which pins collector 0.6.4
(`sha256:65e3c13edb375a795ea1b3f6e3deba4c37e4326a51e3c0917d308c352800704d`), with no image
override. They ran on a kind cluster (one control plane, one worker) on one AMD Ryzen 9 5950X
host, on 2026-11-14 and 2026-11-15.

## Files

| File | What |
|---|---|
| `setup.sh` | Builds the environment: the kind cluster, throwaway TLS material and tokens in a temp dir, chart 0.170.1, the telemetrygen image, the relay, and port-forwards to both collectors' loopback `:8888` (agent on 18001, gateway on 18002). |
| `all.sh` | Runs the three runs in order, setting the gateway's memory limit to 1Gi, 2Gi and 4Gi with `helm upgrade`. |
| `run.sh RUN` | Runs each line of `runs/RUN/steps.txt`. Before every step it deletes any leftover Job, restarts the gateway and the agent, and waits until they are ready and the agent's queues are empty, so every step starts from fresh pods. It records restarts and, for any pod that restarted, its status and previous logs. |
| `bench.py` | One step. It starts a telemetrygen Job (one worker per 250 records/s), waits until the generator is ready, warms up 40 s, samples both collectors every 5 s for 90 s, prints one JSON line, and always deletes its Job. |
| `relay.yaml` | For logs and metrics, a collector in the generator pod that sends batches of 512 records to the agent, as SDKs do. telemetrygen alone sends one log or point per request. |
| `pf.sh` | Keeps a port-forward to the current pod of a label selector, across restarts. |
| `values.yaml` | Chart values: the agents' listener and an OTLP-only agent. The Secrets it names are created by `setup.sh`. |
| `kind.yaml` | The cluster. |
| `build_valid.py` | Builds `valid.jsonl` from the raw runs; nothing is transcribed by hand. |
| `runs/*/steps.txt` | The steps of each run, in order. |
| `runs/*/results.jsonl` | Raw: one line per step that completed. |
| `runs/*/restarts.jsonl` | Raw: gateway and agent pod, restart count and last termination reason, before and after each step. |
| `runs/*/errors.log` | Raw: errors, including the steps that failed. |
| `runs/*/evidence/` | For each pod that restarted during a step: its container status and the logs before the restart. |
| `valid.jsonl` | The rows the docs use, each with `source` (raw file and line) and `run`. |

## Run it

```bash
./setup.sh
./all.sh
python3 build_valid.py
```

Start one runner at a time. Runners share the Job names.

## What each row records

- **Step:** `step_index`, the step's order in its run; `signal`; `offered_per_s`; `workers`.
- **The limits in force:** `gateway_memory_limit` and `agent_memory_limit`, read from the pods. Also the image digest both pods ran.
- **The pods:** for the gateway and the agent, at both ends of the window, the pod name, UID, restart count and container start time. A row is valid only when they did not change during the window.
- **Memory at the window's start:** `gateway_rss_mib_window_start`, `gateway_heap_mib_window_start` and `agent_rss_mib_window_start`. Pods are restarted before each step, so the steps are independent.
- **Throughput:** accepted and refused at the agent and at the gateway, and `gateway_inserted_rows_per_s`. That last one is the rows of confirmed ClickHouse inserts, the proof of writes; it is not one row per accepted record.
- **Cost:** CPU (the rate of `otelcol_process_cpu_seconds`) and peak RSS and heap during the window.

## Results

Every row is in `valid.jsonl`.

| Gateway limit | Held, with no refusals | Limit reached |
|---|---|---|
| 1Gi (the default) | Traces up to 15,000 spans/s, logs up to 10,000 records/s, metrics up to 50,000 points/s | **20,000 spans/s: OOMKilled** (twice, `runs/1-gateway-1gi/evidence/`). 20,000 log records/s: the gateway's memory limiter refused 454 records/s. |
| 2Gi | Traces up to 30,000 spans/s, logs up to 30,000 records/s | Not reached in this run |
| 4Gi | Traces up to 60,000 spans/s, logs up to 60,000 records/s, metrics up to 100,000 points/s | Not reached in this run |

The agent kept its default 512Mi limit in every run and was not restarted. Its peak RSS was 127 to 434 MiB.

## Superseded runs

Earlier runs on 2026-11-14 measured without restarting the pods between steps, and they let a failed step's generator keep running. Memory carried over between steps, and a leftover log generator kept the agent in an out-of-memory loop. Those runs are not kept here and their numbers are not used. In particular, the agent OOMs they showed at 40,000 and 60,000 log records/s did not happen with fresh pods.
