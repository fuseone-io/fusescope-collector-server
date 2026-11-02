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


if __name__ == "__main__":
    unittest.main()
