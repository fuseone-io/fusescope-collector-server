"""The formal parity window of the cutover (CUTOVER.md): 168 hourly comparisons in a row, each
exiting 0, from the first one that leaves the collector's own series out (#80).

Reads /var/log/fusescope-shadow.log as the cron writes it, each run between
`=== run <UTC time>` and `=== exit <code>`. A run that exits non-zero, one without the internal
series filter, or a gap of more than GAP_MINUTES since the run before restarts the window at the
next run. Runs logged before the markers existed are ignored, and an unfinished last run is not
counted. Exits 0 when the window is complete.

    python3 -I deploy/shadow/window.py /var/log/fusescope-shadow.log
"""

import sys
from dataclasses import dataclass, field
from datetime import datetime

RUNS = 168
GAP_MINUTES = 90
FILTER_MARK = "internal series of"


@dataclass
class Run:
    at: str
    code: int | None = None
    filtered: bool = False


@dataclass
class Verdict:
    passed: bool
    green: int
    start: str | None
    resets: list = field(default_factory=list)


def parse(lines):
    runs, current = [], None
    for line in lines:
        line = line.rstrip("\n")
        if line.startswith("=== run "):
            current = Run(at=line.split()[2])
            runs.append(current)
        elif current is None:
            continue
        elif line.startswith("=== exit "):
            current.code = int(line.split()[2])
            current = None
        elif line.startswith(("ok    metric series", "DIFF  metric series")) and FILTER_MARK in line:
            current.filtered = True
    return runs


def _time(at):
    return datetime.strptime(at, "%Y-%m-%dT%H:%M:%SZ")


def evaluate(runs):
    runs = [r for r in runs if r.code is not None]
    first = next((i for i, r in enumerate(runs) if r.filtered), None)
    if first is None:
        return Verdict(passed=False, green=0, start=None)
    green, start, previous, resets = 0, None, None, []
    for run in runs[first:]:
        reason = None
        if not run.filtered:
            reason = "without the internal series filter"
        elif run.code != 0:
            reason = f"exit {run.code}"
        elif previous is not None and (_time(run.at) - _time(previous)).total_seconds() > GAP_MINUTES * 60:
            gap = int((_time(run.at) - _time(previous)).total_seconds() // 60)
            resets.append(f"{run.at} after a gap of {gap} min")
            green, start = 0, None
        previous = run.at
        if reason:
            resets.append(f"{run.at} {reason}")
            green, start = 0, None
            continue
        if start is None:
            start = run.at
        green += 1
    return Verdict(passed=green >= RUNS, green=green, start=start, resets=resets)


def main(path):
    with open(path, encoding="utf-8", errors="replace") as f:
        verdict = evaluate(parse(f))
    if verdict.start is None:
        print("no comparison with the internal series filter yet")
        return 1
    for reset in verdict.resets:
        print(f"restarted: {reset}")
    state = "complete" if verdict.passed else "open"
    print(f"window {state}: {verdict.green} of {RUNS} green runs in a row since {verdict.start}")
    return 0 if verdict.passed else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "/var/log/fusescope-shadow.log"))
