#!/usr/bin/env python3
"""Builds valid.jsonl from the raw runs, never by hand.

A row is valid when the step completed and neither the gateway nor the agent was replaced or
restarted during its measured window (same pod UID and restart count at both ends). Every row
keeps "source": the raw file and line it comes from. Rows with refusals stay valid: they
measure where a limit is reached, and say so in their refusal fields.

Peaks include the window's first sample: bench.py up to test/sizing commit 3621037 left it out
of its maxima, but recorded it as the *_window_start fields, so the peak is the larger of the two.
"""
import glob, json, os

out = []
for path in sorted(glob.glob("runs/*/results.jsonl")):
    for n, line in enumerate(open(path), 1):
        row = json.loads(line)
        same = all(row[f"{c}_pod_at_window_start"]["uid"] == row[f"{c}_pod_at_window_end"]["uid"]
                   and row[f"{c}_pod_at_window_start"]["restarts"] == row[f"{c}_pod_at_window_end"]["restarts"]
                   for c in ("gateway", "agent"))
        if not same:
            continue
        for peak, start in (("gateway_rss_mib_max", "gateway_rss_mib_window_start"),
                            ("gateway_heap_mib_max", "gateway_heap_mib_window_start"),
                            ("agent_rss_mib_max", "agent_rss_mib_window_start")):
            row[peak] = max(row[peak], row[start])
        row["run"] = path.split(os.sep)[1]
        row["source"] = f"{path}:{n}"
        out.append(row)

with open("valid.jsonl", "w") as f:
    for row in out:
        f.write(json.dumps(row) + "\n")
print(f"{len(out)} valid rows")
