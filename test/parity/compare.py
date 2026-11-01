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
}


def load(path, table):
    drop = IGNORED["*"] | IGNORED.get(table, set())
    rows = collections.Counter()
    # Aggregate-function states (dependency_graph_minutes_v2) are raw bytes.
    with open(path, encoding="utf-8", errors="surrogateescape") as f:
        for line in f:
            if line.strip():
                row = {k: v for k, v in json.loads(line, strict=False).items() if k not in drop}
                rows[json.dumps(row, sort_keys=True)] += 1
    return rows


def main():
    out, tables = sys.argv[1], sys.argv[2:]
    report, failed = ["# Parity report", ""], False
    for table in tables:
        ref = load(os.path.join(out, "reference", f"{table}.jsonl"), table)
        cand = load(os.path.join(out, "candidate", f"{table}.jsonl"), table)
        missing, extra = ref - cand, cand - ref
        status = "ok" if not missing and not extra else "DIFF"
        failed |= status == "DIFF"
        report.append(f"- `{table}`: {status} ({sum(ref.values())} reference rows, {sum(cand.values())} candidate rows)")
        for label, rows in (("only in reference", missing), ("only in candidate", extra)):
            for row in list(rows)[:5]:
                report.append(f"  - {label}: `{row[:600]}`")
    text = "\n".join(report) + "\n"
    with open(os.path.join(out, "report.md"), "w") as f:
        f.write(text)
    print(text)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
