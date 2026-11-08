"""python3 -m unittest deploy/shadow/runs_test.py"""

import os
import sys
import unittest
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
import runs  # noqa: E402

OK = "ok    metric series (480 rows; ignored 1037 internal series of fusescope-collector, /fusescope-otel-collector: 338 in fusescope, 699 in fork)"


def run(hour, code=0, day=10):
    return [f"=== run 2026-11-{day:02d}T{hour:02d}:05:01Z", "ok    spans per service and minute (450 rows)", OK,
            f"=== exit {code}"]


def log(*items):
    return [line for item in items for line in item]


def at(day, hour, minute=0):
    return datetime(2026, 11, day, hour, minute)


class ParseTest(unittest.TestCase):
    def test_lines_before_the_markers_ignored(self):
        parsed = runs.parse(["ok    duplicate spans (none)", "DIFF  metric series: 20 only in fusescope"] + run(1))
        self.assertEqual([(r.at, r.code) for r in parsed], [("2026-11-10T01:05:01Z", 0)])

    def test_run_without_exit_is_unfinished(self):
        parsed = runs.parse(run(1) + ["=== run 2026-11-10T02:05:01Z", OK])
        self.assertEqual([r.code for r in parsed], [0, None])


class ReportTest(unittest.TestCase):
    def test_latest_green_and_fresh_passes(self):
        report = runs.report(runs.parse(log(run(1), run(2))), now=at(10, 2, 30))
        self.assertTrue(report.ok)
        self.assertEqual(report.findings, [])
        self.assertEqual(report.latest, "2026-11-10T02:05:01Z exit 0")

    def test_earlier_failures_and_gaps_reported_but_do_not_fail(self):
        report = runs.report(runs.parse(log(run(1), run(2, code=1), run(5), run(6))), now=at(10, 6, 30))
        self.assertTrue(report.ok, "only the latest run gates; earlier ones are monitoring")
        self.assertEqual(report.findings, ["2026-11-10T02:05:01Z exit 1", "2026-11-10T05:05:01Z after a gap of 180 min"])

    def test_latest_non_zero_fails(self):
        report = runs.report(runs.parse(log(run(1), run(2, code=1))), now=at(10, 2, 30))
        self.assertFalse(report.ok)

    def test_latest_too_old_fails(self):
        report = runs.report(runs.parse(log(run(1))), now=at(10, 3, 0))
        self.assertFalse(report.ok)
        self.assertEqual(report.findings, ["no run since 2026-11-10T01:05:01Z (114 min)"])

    def test_unfinished_latest_is_judged_by_the_one_before(self):
        lines = log(run(1)) + ["=== run 2026-11-10T02:05:01Z", OK]
        report = runs.report(runs.parse(lines), now=at(10, 2, 6))
        self.assertTrue(report.ok)
        self.assertEqual(report.latest, "2026-11-10T01:05:01Z exit 0")

    def test_no_marked_run_fails(self):
        report = runs.report(runs.parse(["ok    duplicate spans (none)"]), now=at(10, 1))
        self.assertFalse(report.ok)
        self.assertIsNone(report.latest)

    def test_no_window_counted(self):
        # Monitoring only (CUTOVER.md §3): nothing in the report counts runs toward a window.
        report = runs.report(runs.parse(log(*[run(h) for h in range(3)])), now=at(10, 2, 10))
        self.assertFalse(hasattr(report, "green"))


if __name__ == "__main__":
    unittest.main()
