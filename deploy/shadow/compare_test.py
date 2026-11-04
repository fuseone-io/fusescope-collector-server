"""python3 -m unittest deploy/shadow/compare_test.py"""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import compare  # noqa: E402


def series(scope, version, collector="a"):
    labels = {"__name__": "m", "__scope.name__": scope, "__scope.version__": version, "signoz.collector.id": collector}
    return ("m", json.dumps(labels), {}, {"__scope.name__": scope, "__scope.version__": version}, {})


HOST = "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/cpuscraper"


class AnonymizeTest(unittest.TestCase):
    def test_collector_scope_version_ignored(self):
        self.assertEqual(compare.anonymize(series(HOST, "dev", "a")), compare.anonymize(series(HOST, "0.1.0-dev", "b")))

    def test_sdk_scope_versions_kept(self):
        demo = {compare.anonymize(series("io.opentelemetry.sdk", v)) for v in ("1.0", "2.0")}
        shadow = {compare.anonymize(series("io.opentelemetry.sdk", "1.0"))}
        self.assertEqual(len(demo - shadow), 1, "a series lost in the shadow must show as a difference")


class DuplicatesTest(unittest.TestCase):
    def test_none(self):
        self.assertEqual(compare.duplicates("duplicate spans", [], []), (False, ["ok    duplicate spans (none)"]))

    def test_demo_noted(self):
        failed, lines = compare.duplicates("duplicate spans", [("frontend", "2026-11-04 00:00:00", 13)], [])
        self.assertFalse(failed, "the demo's duplicates are not this collector's")
        self.assertTrue(lines[0].startswith("note  duplicate spans in the demo: 13 extra"))

    def test_shadow_fails(self):
        failed, lines = compare.duplicates("duplicate logs", [], [("payment", "2026-11-04 00:00:00", 1)])
        self.assertTrue(failed)
        self.assertTrue(lines[0].startswith("DIFF  duplicate logs in the shadow"))

    def test_after_the_swap(self):
        labels = {"demo": "fusescope", "shadow": "fork"}
        failed, lines = compare.duplicates("duplicate spans", [("frontend", "m", 2)], [("route", "m", 1)], ours="demo", labels=labels)
        self.assertTrue(failed, "this collector writes the demo's side after the swap")
        self.assertTrue(lines[0].startswith("DIFF  duplicate spans in the fusescope: 2 extra"))
        self.assertTrue(any(line.startswith("note  duplicate spans in the fork") for line in lines))
        failed, _ = compare.duplicates("duplicate spans", [], [("route", "m", 1)], ours="demo", labels=labels)
        self.assertFalse(failed, "the ghost's duplicates are not this collector's")


class LogIdentityTest(unittest.TestCase):
    def test_every_field_the_sender_sets(self):
        # Two logs equal in all but severity, trace flags or scope are two logs.
        for column in ("trace_flags", "severity_text", "severity_number", "scope_name", "scope_version", "scope_string",
                       "timestamp", "body", "trace_id", "span_id", "attributes_string", "attributes_number",
                       "attributes_bool", "resources_string"):
            self.assertIn(column, compare.LOG_IDENTITY)

    def test_not_what_each_collector_sets(self):
        for column in ("id", "observed_timestamp"):
            self.assertNotIn(column, compare.LOG_IDENTITY, "a retried log gets a new one")


if __name__ == "__main__":
    unittest.main()
