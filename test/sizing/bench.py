#!/usr/bin/env python3
"""One benchmark step: telemetrygen -> agent (TLS, token) -> gateway (mTLS) -> ClickHouse.

Samples the agent's and the gateway's own metrics (port-forwarded :8888) over a steady window
and prints one JSON line. Usage: bench.py SIGNAL RATE_PER_WORKER WORKERS [WARMUP] [WINDOW]
"""
import json, re, subprocess, sys, time, urllib.request

CTX = ["kubectl", "--context", "kind-bench"]
AGENT, GATEWAY = "http://127.0.0.1:18001/metrics", "http://127.0.0.1:18002/metrics"
ATTRS = ["deployment.environment=\"bench\"", "http.method=\"GET\"", "http.route=\"/api/v1/orders/{id}\"",
         "http.status_code=\"200\"", "net.peer.name=\"orders.apps.svc\"", "user.tier=\"gold\"",
         "region=\"us-east-1\"", "build.version=\"1.42.0\""]
BODY = ("GET /api/v1/orders/8812 200 12ms user=4411 tenant=acme trace_id=4bf92f3577b34da6a3ce929d0e0e4736 "
        "span_id=00f067aa0ba902b7 msg=\"order fetched from primary, cache miss, 3 items, total=129.90 BRL\"")

def fetch(url):
    for attempt in range(6):
        try:
            return urllib.request.urlopen(url, timeout=5).read().decode()
        except Exception:
            # A stuck port-forward: kill it by exact PID; pf.sh starts a new one.
            port = url.split(":")[2].split("/")[0]
            pids = subprocess.run(["pgrep", "-f", f"^kubectl --context kind-bench -n platform port-forward .* {port}:8888$"],
                                  capture_output=True, text=True).stdout.split()
            for pid in pids:
                subprocess.run(["kill", pid])
            time.sleep(4)
    raise RuntimeError(f"cannot scrape {url}")

def scrape(url):
    out = {}
    for line in fetch(url).splitlines():
        if not line or line[0] == "#":
            continue
        m = re.match(r'^([a-zA-Z_:][\w:]*)(\{[^}]*\})?\s+(\S+)', line)
        if m:
            out.setdefault(m.group(1), []).append((m.group(2) or "", float(m.group(3))))
    return out

def total(s, name, contains=""):
    return sum(v for labels, v in s.get(name, []) if contains in labels)

def job(signal, rate, workers, seconds):
    name = f"bench-{signal}"
    flag = {"traces": ["--child-spans=4"], "logs": [f"--body={BODY}"], "metrics": []}[signal]
    args = [signal, "--otlp-endpoint=fusescope-agent-otlp.platform.svc:4317", "--ca-cert=/ca/ca.crt",
            '--otlp-header=Authorization="Bearer $(TOKEN)"', f"--service=bench-{signal}",
            f"--workers={workers}", f"--rate={rate}", f"--duration={seconds}s"] + flag + \
           [f"--telemetry-attributes={a}" for a in ATTRS]
    relay = signal != "traces"
    if relay:
        # telemetrygen exports each log or point as its own request; a relay batches them as
        # SDKs do (512 per request) before the agent, which is what is measured.
        args = [a.replace("--otlp-endpoint=fusescope-agent-otlp.platform.svc:4317", "--otlp-endpoint=127.0.0.1:4317")
                 .replace("--ca-cert=/ca/ca.crt", "--otlp-insecure") for a in args
                if not a.startswith("--otlp-header")]
    gen = {"name": "gen",
           "image": "ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:v0.135.0",
           "env": [{"name": "TOKEN", "valueFrom": {"secretKeyRef": {"name": "fusescope-agent-otlp-token", "key": "token"}}}],
           "args": args, "volumeMounts": [{"name": "ca", "mountPath": "/ca", "readOnly": True}]}
    containers = [gen]
    volumes = [{"name": "ca", "secret": {"secretName": "fusescope-agent-ca"}}]
    if relay:
        containers.append({"name": "relay",
            "image": "ghcr.io/fuseone-io/fusescope-collector@sha256:65e3c13edb375a795ea1b3f6e3deba4c37e4326a51e3c0917d308c352800704d",
            "args": ["--config=/relay/config.yaml"],
            "volumeMounts": [{"name": "ca", "mountPath": "/ca", "readOnly": True},
                             {"name": "relay", "mountPath": "/relay", "readOnly": True},
                             {"name": "token", "mountPath": "/token", "readOnly": True}]})
        volumes += [{"name": "relay", "configMap": {"name": "bench-relay"}},
                    {"name": "token", "secret": {"secretName": "fusescope-agent-otlp-token"}}]
    spec = {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": name, "namespace": "apps"},
            "spec": {"backoffLimit": 1000, "template": {"spec": {
                "restartPolicy": "OnFailure", "containers": containers, "volumes": volumes}}}}
    subprocess.run(CTX + ["-n", "apps", "delete", "job", name, "--ignore-not-found", "--wait"], check=True, capture_output=True)
    subprocess.run(CTX + ["apply", "-f", "-"], input=json.dumps(spec).encode(), check=True, capture_output=True)
    return name

UNIT = {"traces": "spans", "logs": "log_records", "metrics": "metric_points"}

def snap(signal):
    a, g = scrape(AGENT), scrape(GATEWAY)
    u = UNIT[signal]
    return {
        "t": time.time(),
        "a_cpu": total(a, "otelcol_process_cpu_seconds"), "g_cpu": total(g, "otelcol_process_cpu_seconds"),
        "a_rss": total(a, "otelcol_process_memory_rss"), "g_rss": total(g, "otelcol_process_memory_rss"),
        "a_heap": total(a, "otelcol_process_runtime_heap_alloc_bytes"), "g_heap": total(g, "otelcol_process_runtime_heap_alloc_bytes"),
        "a_acc": total(a, f"otelcol_receiver_accepted_{u}", 'receiver="otlp/sdks"'),
        "a_ref": total(a, f"otelcol_receiver_refused_{u}", 'receiver="otlp/sdks"'),
        "a_fail": total(a, f"otelcol_exporter_send_failed_{u}"),
        "g_acc": total(g, f"otelcol_receiver_accepted_{u}", 'receiver="otlp/agents"'),
        "g_ref": total(g, f"otelcol_receiver_refused_{u}", 'receiver="otlp/agents"'),
        "g_fail": total(g, f"otelcol_exporter_send_failed_{u}", 'exporter="fusescope_clickhouse"'),
        "g_queue": total(g, "otelcol_exporter_queue_size", 'exporter="fusescope_clickhouse"'),
        "g_rows": total(g, "otelcol_exporter_fusescope_clickhouse_inserted_rows", 'exporter="fusescope_clickhouse"'),
        "g_ins_err": total(g, "otelcol_exporter_fusescope_clickhouse_insert_errors", 'exporter="fusescope_clickhouse"'),
        "g_dur_sum": total(g, "otelcol_exporter_fusescope_clickhouse_insert_duration_sum", 'exporter="fusescope_clickhouse"'),
        "g_dur_cnt": total(g, "otelcol_exporter_fusescope_clickhouse_insert_duration_count", 'exporter="fusescope_clickhouse"'),
    }

def wait_for_generator(name, timeout=180):
    """Waits until the Job's pod has every container ready, so the warmup starts with load."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        out = subprocess.run(CTX + ["-n", "apps", "get", "pods", "-l", f"job-name={name}", "-o",
                                    "jsonpath={range .items[*].status.containerStatuses[*]}{.ready}{' '}{end}"],
                             capture_output=True, text=True).stdout.split()
        if out and all(x == "true" for x in out):
            return time.time()
        time.sleep(2)
    raise RuntimeError(f"generator {name} not ready after {timeout}s")

def pod_state(selector):
    """Name, UID, restart count and start time of the current pod, to tell measurements apart."""
    out = subprocess.run(CTX + ["-n", "platform", "get", "pods", "-l", f"app.kubernetes.io/component={selector}", "-o",
                                "jsonpath={.items[0].metadata.name}|{.items[0].metadata.uid}|"
                                "{.items[0].status.containerStatuses[0].restartCount}|"
                                "{.items[0].status.containerStatuses[0].state.running.startedAt}"],
                         capture_output=True, text=True).stdout.split("|")
    out += [""] * (4 - len(out))
    return {"pod": out[0], "uid": out[1], "restarts": int(out[2] or 0), "container_started_at": out[3]}

def limits():
    """The memory limits in force for this step, as the pods run them."""
    def one(selector):
        return subprocess.run(CTX + ["-n", "platform", "get", "pods", "-l", f"app.kubernetes.io/component={selector}", "-o",
                                     "jsonpath={.items[0].spec.containers[0].resources.limits.memory}"],
                              capture_output=True, text=True).stdout.strip()
    def image(selector):
        return subprocess.run(CTX + ["-n", "platform", "get", "pods", "-l", f"app.kubernetes.io/component={selector}", "-o",
                                     "jsonpath={.items[0].status.containerStatuses[0].imageID}"],
                              capture_output=True, text=True).stdout.strip()
    return {"gateway_memory_limit": one("fusescope-collector"), "agent_memory_limit": one("fusescope-agent"),
            "gateway_image": image("fusescope-collector"), "agent_image": image("fusescope-agent")}

def main():
    signal, rate, workers = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    warmup = int(sys.argv[4]) if len(sys.argv) > 4 else 40
    window = int(sys.argv[5]) if len(sys.argv) > 5 else 90
    step_index = int(sys.argv[6]) if len(sys.argv) > 6 else 0
    name = job(signal, rate, workers, 180 + warmup + window + 20)
    try:
        measure(signal, rate, workers, warmup, window, step_index, name)
    finally:
        # Always: a generator left running would load the next step.
        subprocess.run(CTX + ["-n", "apps", "delete", "job", name, "--ignore-not-found", "--wait"], capture_output=True)

def measure(signal, rate, workers, warmup, window, step_index, name):
    started = wait_for_generator(name)
    time.sleep(warmup)
    gw0, ag0 = pod_state("fusescope-collector"), pod_state("fusescope-agent")
    s0, samples = snap(signal), []
    while time.time() - s0["t"] < window:
        time.sleep(5)
        samples.append(snap(signal))
    s1 = samples[-1]
    dt = s1["t"] - s0["t"]
    d = lambda k: (s1[k] - s0[k]) / dt
    res = {
        "step_index": step_index,
        "signal": signal, "offered_per_s": rate * workers, "workers": workers,
        **limits(),
        "gateway_pod_at_window_start": gw0, "agent_pod_at_window_start": ag0,
        "gateway_pod_at_window_end": pod_state("fusescope-collector"),
        "agent_pod_at_window_end": pod_state("fusescope-agent"),
        "gateway_rss_mib_window_start": round(s0["g_rss"] / 2**20),
        "gateway_heap_mib_window_start": round(s0["g_heap"] / 2**20),
        "agent_rss_mib_window_start": round(s0["a_rss"] / 2**20),
        "agent_accepted_per_s": round(d("a_acc")), "agent_refused_per_s": round(d("a_ref")),
        "agent_send_failed_per_s": round(d("a_fail")),
        "gateway_accepted_per_s": round(d("g_acc")), "gateway_refused_per_s": round(d("g_ref")),
        "gateway_send_failed_per_s": round(d("g_fail")),
        # Proof of writes to ClickHouse: rows of confirmed inserts across every table (spans,
        # resources, tags, samples, series), so not one row per accepted record.
        "gateway_inserted_rows_per_s": round(d("g_rows")),
        "gateway_insert_errors": s1["g_ins_err"] - s0["g_ins_err"],
        "agent_cores": round(d("a_cpu"), 3), "gateway_cores": round(d("g_cpu"), 3),
        "agent_rss_mib_max": round(max(x["a_rss"] for x in samples) / 2**20),
        "gateway_rss_mib_max": round(max(x["g_rss"] for x in samples) / 2**20),
        "gateway_rss_mib_avg": round(sum(x["g_rss"] for x in samples) / len(samples) / 2**20),
        "gateway_heap_mib_max": round(max(x["g_heap"] for x in samples) / 2**20),
        "gateway_queue_max": max(x["g_queue"] for x in samples),
        "gateway_insert_s_avg": round((s1["g_dur_sum"] - s0["g_dur_sum"]) / max(1, s1["g_dur_cnt"] - s0["g_dur_cnt"]), 3),
        "generator_ready_to_window_s": round(s0["t"] - started),
        "window_s": round(dt),
    }
    print(json.dumps(res), flush=True)

main()
