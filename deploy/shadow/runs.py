"""Operational monitoring of the hourly comparison (CUTOVER.md §3), not a parity window.

Reads /var/log/fusescope-shadow.log as the cron writes it, each run between
`=== run <UTC time>` and `=== exit <code>`, and lists every run that exited non-zero and
every gap of more than GAP_MINUTES between runs. Lines logged before the markers existed are
ignored. Exits 0 when the latest finished run exited 0 and is at most GAP_MINUTES old;
earlier failures and gaps are reported, for an explanation, but do not fail it.

    python3 -I deploy/shadow/runs.py /var/log/fusescope-shadow.log
"""

import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone

GAP_MINUTES = 90


@dataclass
class Run:
    at: str
    code: int | None = None


@dataclass
class Report:
    ok: bool
    latest: str | None
    findings: list = field(default_factory=list)


def parse(lines):
    parsed, current = [], None
    for line in lines:
        line = line.rstrip("\n")
        if line.startswith("=== run "):
            current = Run(at=line.split()[2])
            parsed.append(current)
        elif current is not None and line.startswith("=== exit "):
            current.code = int(line.split()[2])
            current = None
    return parsed


def _time(at):
    return datetime.strptime(at, "%Y-%m-%dT%H:%M:%SZ")


def _minutes(earlier, later):
    return int((later - earlier).total_seconds() // 60)


def report(parsed, now):
    finished = [r for r in parsed if r.code is not None]
    if not finished:
        return Report(ok=False, latest=None, findings=["no run with the markers yet"])
    findings, previous = [], None
    for run in finished:
        if previous is not None and _minutes(_time(previous), _time(run.at)) > GAP_MINUTES:
            findings.append(f"{run.at} after a gap of {_minutes(_time(previous), _time(run.at))} min")
        if run.code != 0:
            findings.append(f"{run.at} exit {run.code}")
        previous = run.at
    last = finished[-1]
    age = _minutes(_time(last.at), now)
    if age > GAP_MINUTES:
        findings.append(f"no run since {last.at} ({age} min)")
    return Report(ok=last.code == 0 and age <= GAP_MINUTES, latest=f"{last.at} exit {last.code}", findings=findings)


def main(path):
    with open(path, encoding="utf-8", errors="replace") as f:
        result = report(parse(f), now=datetime.now(timezone.utc).replace(tzinfo=None))
    for finding in result.findings:
        print(f"note  {finding}")
    print(f"{'ok  ' if result.ok else 'FAIL'}  latest run: {result.latest or 'none'}")
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "/var/log/fusescope-shadow.log"))
