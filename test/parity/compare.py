"""Compares the rows the reference collector and the candidate wrote.

Usage: python3 compare.py <out_dir> <table>...

Rows are compared as multisets per table. Columns that differ by design are
dropped first: wall-clock insert times and random IDs. Writes <out_dir>/report.md
and exits 1 on any difference.
"""

import collections
import json
import os
import sys

IGNORED = {
    "*": {"inserted_at", "created_at"},
    "signoz_traces.signoz_error_index_v2": {"errorID"},
    "signoz_traces.span_attributes_keys": {"timestamp"},
    "signoz_traces.top_level_operations": {"time"},
    "signoz_logs.logs_v2": {"id"},
    "signoz_logs.logs_attribute_keys": {"timestamp"},
    "signoz_logs.logs_resource_keys": {"timestamp"},
    "signoz_metrics.time_series_v4": {"inserted_at_unix_milli"},
    "signoz_metrics.samples_v4": {"inserted_at_unix_milli"},
    # Fed from samples_v4_agg_5m with anyLast(last): when a series has two 5m rows in one
    # 30m bucket, the pick depends on the unordered GROUP BY output, in both collectors.
    "signoz_metrics.samples_v4_agg_30m": {"last"},
}

# The corpus record without any timestamp: each collector stamps it with its own ingest time.
INGEST_TIMED = "no time at all"
# The protobuf probe record has a time but no observed time, which also gets the ingest time
# (logs spec §3). Its body is invalid UTF-8, read with surrogateescape.
OBSERVED_INGEST_TIMED = "bad \udcff\udcfe utf8 \udce2\udc82"


def series_labels(out, side):
    """Maps each fingerprint to its labels: metric fingerprints differ by design (metrics spec §4)."""
    labels = {}
    with open(os.path.join(out, side, "signoz_metrics.time_series_v4.jsonl"), encoding="utf-8", errors="surrogateescape") as f:
        for line in f:
            if line.strip():
                row = json.loads(line, strict=False)
                labels[row["fingerprint"]] = row["labels"]
    return labels


def load(path, table, labels):
    drop = IGNORED["*"] | IGNORED.get(table, set())
    rows = collections.Counter()
    # Aggregate-function states (dependency_graph_minutes_v2) are raw bytes.
    with open(path, encoding="utf-8", errors="surrogateescape") as f:
        for line in f:
            if line.strip():
                row = {k: v for k, v in json.loads(line, strict=False).items() if k not in drop}
                if table == "signoz_logs.logs_v2" and row.get("body") == INGEST_TIMED:
                    for k in ("timestamp", "observed_timestamp", "ts_bucket_start"):
                        row.pop(k, None)
                if table == "signoz_logs.logs_v2" and row.get("body") == OBSERVED_INGEST_TIMED:
                    row.pop("observed_timestamp", None)
                if table.startswith("signoz_metrics."):
                    # The reference also writes span metrics (phase 4); the candidate does not yet.
                    if str(row.get("metric_name", "")).startswith("signoz_"):
                        continue
                    if "fingerprint" in row:
                        row["fingerprint"] = labels.get(row["fingerprint"], "unknown series")
                    # The reference's unit for .count metadata is arbitrary (metrics spec §5).
                    if table == "signoz_metrics.metadata" and str(row.get("metric_name")).endswith(".count"):
                        row.pop("unit", None)
                rows[json.dumps(row, sort_keys=True)] += 1
    return rows


def main():
    out, tables = sys.argv[1], sys.argv[2:]
    report, failed = ["# Parity report", ""], False
    labels = {side: series_labels(out, side) for side in ("reference", "candidate")}
    for table in tables:
        ref = load(os.path.join(out, "reference", f"{table}.jsonl"), table, labels["reference"])
        cand = load(os.path.join(out, "candidate", f"{table}.jsonl"), table, labels["candidate"])
        missing, extra = ref - cand, cand - ref
        status = "ok" if not missing and not extra else "DIFF"
        failed |= status == "DIFF"
        report.append(f"- `{table}`: {status} ({sum(ref.values())} reference rows, {sum(cand.values())} candidate rows)")
        for label, rows in (("only in reference", missing), ("only in candidate", extra)):
            for row in sorted(rows)[:12]:
                report.append(f"  - {label}: `{row[:600]}`")
    text = "\n".join(report) + "\n"
    with open(os.path.join(out, "report.md"), "w") as f:
        f.write(text)
    print(text)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
