"""Runs the reference collector as a black box on every case of cases.json.

Usage: python3 -I observe.py <cases.json> <out_dir>

Writes <out_dir>/startup.json (whether the reference starts with each startup
check's processors, and the first error it logs) and <out_dir>/outputs.json
(per case, the OTLP/JSON documents the reference's file exporter wrote). Needs
Docker; uses compose project observe-logspipeline on 127.0.0.1:34318/34133.
"""

import copy
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
CASE_ATTRIBUTE = "observe.case"
HEALTH = "http://127.0.0.1:34133/"
OTLP = "http://127.0.0.1:34318/v1/logs"


def collector_config(cases):
    """One logs pipeline per case: drop other cases' records, strip the marker, run the case's processors."""
    processors = {"resource/observe": {"attributes": [{"key": CASE_ATTRIBUTE, "action": "delete"}]}}
    exporters, pipelines = {}, {}
    for index, case in enumerate(cases):
        cid = f"c{index}"
        processors[f"filter/{cid}"] = {"error_mode": "ignore",
                                       "logs": {"log_record": [f'resource.attributes["{CASE_ATTRIBUTE}"] != "{cid}"']}}
        names = []
        for name in case["processorNames"]:
            kind, _, suffix = name.partition("/")
            renamed = f"{kind}/{cid}_{suffix}"
            processors[renamed] = case["processors"][name]
            names.append(renamed)
        exporters[f"file/{cid}"] = {"path": f"/contract/{cid}.json", "flush_interval": "100ms"}
        pipelines[f"logs/{cid}"] = {"receivers": ["otlp"], "processors": [f"filter/{cid}", "resource/observe", *names],
                                    "exporters": [f"file/{cid}"]}
    return {
        "receivers": {"otlp": {"protocols": {"http": {"endpoint": "0.0.0.0:4318"}}}},
        "extensions": {"health_check": {"endpoint": "0.0.0.0:13133"}},
        "processors": processors,
        "exporters": exporters,
        "service": {"extensions": ["health_check"], "pipelines": pipelines},
    }


def compose(env, *args, check=True):
    return subprocess.run(["docker", "compose", "-f", os.path.join(HERE, "compose.yaml"), *args],
                          env={**os.environ, **env}, check=check, capture_output=True, text=True)


def healthy():
    try:
        with urllib.request.urlopen(HEALTH, timeout=2) as response:
            return response.status == 200
    except (urllib.error.URLError, OSError):
        return False


def start(workdir, config):
    """Starts the reference with config; returns (started, logs)."""
    config_path = os.path.join(workdir, "config.yaml")
    out_dir = os.path.join(workdir, "out")
    os.makedirs(out_dir, exist_ok=True)
    os.chmod(out_dir, 0o777)
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f)
    env = {"OBSERVE_CONFIG": config_path, "OBSERVE_OUT": out_dir}
    compose(env, "down", "-v", check=False)
    compose(env, "up", "-d", "reference-collector")
    for _ in range(90):
        if healthy():
            return True, env
        state = compose(env, "ps", "-a", "--format", "{{.State}}", "reference-collector").stdout.strip()
        if state in ("exited", "dead"):
            return False, env
        time.sleep(1)
    raise TimeoutError("reference neither became healthy nor exited")


def stop(env):
    logs = compose(env, "logs", "--no-color", "reference-collector", check=False).stdout
    compose(env, "down", "-v", check=False)
    return logs


def first_error(logs):
    """The error field of the reference's fatal log line, or its last line.

    Docker splits log lines over 16 KiB, so the field is decoded where it
    starts rather than by parsing the whole line."""
    fatal = logs.find('"level":"fatal"')
    start = logs.find('"error":', fatal) if fatal >= 0 else -1
    if start >= 0:
        try:
            value, _ = json.JSONDecoder().raw_decode(logs, start + len('"error":'))
            return value
        except json.JSONDecodeError:
            pass
    lines = [line.split("|", 1)[-1].strip() for line in logs.splitlines() if line.strip()]
    return lines[-1] if lines else ""


def check_startup(cases, workdir):
    results = []
    for case in cases:
        sub = tempfile.mkdtemp(dir=workdir)
        started, env = start(sub, collector_config([case]))
        logs = stop(env)
        results.append({"name": case["name"], "origin": case["origin"], "starts": started,
                        "error": "" if started else first_error(logs)})
        print(f"startup {case['name']}: {'starts' if started else 'refused'}", flush=True)
    return results


def send(inputs, cases):
    for index, case in enumerate(cases):
        for document in inputs[case["logSet"]]:
            marked = copy.deepcopy(document)
            for rl in marked["resourceLogs"]:
                rl.setdefault("resource", {}).setdefault("attributes", []).append(
                    {"key": CASE_ATTRIBUTE, "value": {"stringValue": f"c{index}"}})
            request = urllib.request.Request(OTLP, data=json.dumps(marked).encode(), method="POST",
                                             headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=10) as response:
                if response.status != 200:
                    raise RuntimeError(f"case {case['name']}: POST {response.status}")


COLLECTOR_TIME = "set-by-collector"


def input_observed_times(inputs):
    return {r["observedTimeUnixNano"] for docs in inputs.values() for d in docs for rl in d["resourceLogs"]
            for sl in rl["scopeLogs"] for r in sl["logRecords"] if "observedTimeUnixNano" in r}


def read_outputs(out_dir, cases, known_times):
    """Reads each case's documents; observed timestamps the input did not carry are the
    reference's clock and are replaced by COLLECTOR_TIME so runs compare."""
    outputs = {}
    for index, case in enumerate(cases):
        path = os.path.join(out_dir, f"c{index}.json")
        docs = []
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                docs = [json.loads(line) for line in f if line.strip()]
        for d in docs:
            for rl in d.get("resourceLogs", []):
                for sl in rl.get("scopeLogs", []):
                    for r in sl.get("logRecords", []):
                        if r.get("observedTimeUnixNano") not in known_times:
                            r["observedTimeUnixNano"] = COLLECTOR_TIME
        outputs[case["name"]] = docs
    return outputs


def settle(out_dir, quiet=4, timeout=60):
    """Waits until the exporter files stop changing."""
    deadline, last, stable_since = time.monotonic() + timeout, None, time.monotonic()
    while time.monotonic() < deadline:
        snapshot = sorted((n, os.path.getsize(os.path.join(out_dir, n))) for n in os.listdir(out_dir))
        if snapshot != last:
            last, stable_since = snapshot, time.monotonic()
        elif time.monotonic() - stable_since >= quiet:
            return
        time.sleep(0.5)


def main():
    cases_path, out = sys.argv[1], os.path.abspath(sys.argv[2])
    with open(cases_path, encoding="utf-8") as f:
        spec = json.load(f)
    os.makedirs(out, exist_ok=True)
    workdir = tempfile.mkdtemp(prefix="observe-logspipeline-")
    try:
        # Hand-written run cases are checked alone first, so one the reference refuses
        # is recorded instead of failing the whole run.
        handwritten = [c for c in spec["cases"] if c["origin"] == "hand-written"]
        startup = check_startup(spec["startup"] + handwritten, workdir)
        refused = {r["name"] for r in startup if not r["starts"]}
        runnable = [c for c in spec["cases"] if c["name"] not in refused]

        run_dir = tempfile.mkdtemp(dir=workdir)
        started, env = start(run_dir, collector_config(runnable))
        if not started:
            raise RuntimeError("reference refused the combined config:\n" + stop(env)[-5000:])
        try:
            send(spec["inputs"], runnable)
            settle(os.path.join(run_dir, "out"))
            outputs = read_outputs(os.path.join(run_dir, "out"), runnable, input_observed_times(spec["inputs"]))
        finally:
            stop(env)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    with open(os.path.join(out, "startup.json"), "w", encoding="utf-8") as f:
        json.dump(startup, f, indent=1, sort_keys=True)
        f.write("\n")
    with open(os.path.join(out, "outputs.json"), "w", encoding="utf-8") as f:
        json.dump(outputs, f, indent=1, sort_keys=True)
        f.write("\n")
    with open(os.path.join(out, "run.yaml"), "w", encoding="utf-8") as f:
        f.write(f"reference_image: ghcr.io/fuseone-io/fusescope-otel-collector:{os.environ.get('REFERENCE_VERSION', '0.145.0')}\n")
        f.write(f"cases: {len(spec['cases'])}\nstartup_checks: {len(startup)}\nrefused_at_startup: {len(refused)}\n")
    print(f"{len(outputs)} cases observed, {len(refused)} refused at startup")


if __name__ == "__main__":
    main()
