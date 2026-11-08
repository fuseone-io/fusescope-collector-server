"""python3 -m unittest deploy/shadow/window_test.py"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import window  # noqa: E402

FILTERED = "ok    metric series (480 rows; ignored 20 internal series of fusescope-collector: 20 in fusescope, 0 in fork)"
UNFILTERED = "DIFF  metric series: 20 only in fusescope, 0 only in fork"


def run(hour, code=0, series=FILTERED, day=10):
    return [f"=== run 2026-11-{day:02d}T{hour:02d}:05:01Z", "ok    spans per service and minute (450 rows)", series,
            f"=== exit {code}"]


def log(*runs):
    return [line for r in runs for line in r]


class ParseTest(unittest.TestCase):
    def test_runs_without_markers_ignored(self):
        lines = ["ok    duplicate spans (none)", UNFILTERED] + run(1)
        runs = window.parse(lines)
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0].code, 0)
        self.assertTrue(runs[0].filtered)

    def test_run_without_exit_is_incomplete(self):
        lines = run(1) + ["=== run 2026-11-10T02:05:01Z", FILTERED]
        runs = window.parse(lines)
        self.assertEqual([r.code for r in runs], [0, None])


class WindowTest(unittest.TestCase):
    def test_week_of_green_runs_passes(self):
        runs = [run(h % 24, day=10 + h // 24) for h in range(168)]
        verdict = window.evaluate(window.parse(log(*runs)))
        self.assertTrue(verdict.passed)
        self.assertEqual(verdict.green, 168)
        self.assertEqual(verdict.start, "2026-11-10T00:05:01Z")

    def test_six_days_do_not_pass(self):
        runs = [run(h % 24, day=10 + h // 24) for h in range(167)]
        self.assertFalse(window.evaluate(window.parse(log(*runs))).passed)

    def test_runs_before_the_filter_do_not_count(self):
        runs = [run(h, series=UNFILTERED, code=1) for h in range(5)] + [run(5), run(6)]
        verdict = window.evaluate(window.parse(log(*runs)))
        self.assertEqual(verdict.green, 2)
        self.assertEqual(verdict.start, "2026-11-10T05:05:01Z")

    def test_a_difference_restarts_the_window(self):
        runs = [run(1), run(2), run(3, code=1), run(4)]
        verdict = window.evaluate(window.parse(log(*runs)))
        self.assertEqual(verdict.green, 1)
        self.assertEqual(verdict.start, "2026-11-10T04:05:01Z")
        self.assertEqual(verdict.resets, ["2026-11-10T03:05:01Z exit 1"])

    def test_a_missed_hour_restarts_the_window(self):
        runs = [run(1), run(2), run(4), run(5)]
        verdict = window.evaluate(window.parse(log(*runs)))
        self.assertEqual(verdict.green, 2)
        self.assertEqual(verdict.start, "2026-11-10T04:05:01Z")
        self.assertEqual(verdict.resets, ["2026-11-10T04:05:01Z after a gap of 120 min"])

    def test_an_unfiltered_run_after_the_filter_restarts_the_window(self):
        runs = [run(1), run(2, series=UNFILTERED), run(3)]
        verdict = window.evaluate(window.parse(log(*runs)))
        self.assertEqual(verdict.green, 1)
        self.assertEqual(verdict.resets, ["2026-11-10T02:05:01Z without the internal series filter"])

    def test_an_incomplete_last_run_is_not_counted(self):
        lines = log(run(1), run(2)) + ["=== run 2026-11-10T03:05:01Z", FILTERED]
        verdict = window.evaluate(window.parse(lines))
        self.assertEqual(verdict.green, 2)

    def test_no_filtered_run_yet(self):
        verdict = window.evaluate(window.parse(log(run(1, series=UNFILTERED, code=1))))
        self.assertFalse(verdict.passed)
        self.assertIsNone(verdict.start)


if __name__ == "__main__":
    unittest.main()
