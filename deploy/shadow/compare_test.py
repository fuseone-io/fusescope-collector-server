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


def metric(name, service, where="resource"):
    labels = {"__name__": name}
    resource = {}
    if where == "resource":
        resource["service.name"] = service
    else:
        labels["service.name"] = service
    return (name, json.dumps(labels), {}, {}, resource)


INTERNAL = metric("otelcol.resourcedetection.attributes.detected", "fusescope-collector")
USER = metric("http.server.request.duration", "frontend")
LABELS = {"demo": "fusescope", "shadow": "fork"}


class MetricSeriesTest(unittest.TestCase):
    def run_check(self, demo, shadow):
        return compare.check("metric series", *compare.metric_series(demo, shadow, ("fusescope-collector",)), labels=LABELS)

    def test_internal_series_ignored_and_counted(self):
        failed, lines = self.run_check([USER, INTERNAL, metric("otelcol.process.uptime", "fusescope-collector", "labels")], [USER])
        self.assertFalse(failed, "the collector's own telemetry is not part of parity")
        self.assertEqual(lines, ["ok    metric series (1 rows; ignored 2 internal series of fusescope-collector: 2 in fusescope, 0 in fork)"])

    def test_no_internal_series_still_reported(self):
        _, lines = self.run_check([USER], [USER])
        self.assertEqual(lines[0], "ok    metric series (1 rows; ignored 0 internal series of fusescope-collector: 0 in fusescope, 0 in fork)")

    def test_user_difference_still_fails(self):
        lost = metric("http.client.request.duration", "checkout")
        failed, lines = self.run_check([USER, lost, INTERNAL], [USER])
        self.assertTrue(failed, "a user series lost on one side is a difference")
        self.assertTrue(lines[0].startswith("DIFF  metric series: 1 only in fusescope, 0 only in fork; ignored 1 internal series"))
        self.assertIn("http.client.request.duration", lines[1])

    def test_user_collector_otelcol_metrics_compared(self):
        # A user's own collector reports otelcol.* metrics under its own service.name.
        theirs = metric("otelcol.receiver.accepted_spans", "edge-gateway")
        failed, lines = self.run_check([USER], [USER, theirs])
        self.assertTrue(failed, "otelcol.* metrics of another service are user data")
        self.assertTrue(lines[0].startswith("DIFF  metric series: 0 only in fusescope, 1 only in fork"))


class InternalServicesTest(unittest.TestCase):
    def test_default_names_both_collectors(self):
        # Observed in both ClickHouses of the demo (deploy/shadow/README.md): the
        # earlier collector reports its own telemetry as /fusescope-otel-collector.
        self.assertEqual(compare.INTERNAL_SERVICES, ("fusescope-collector", "/fusescope-otel-collector"))

    def test_earlier_collector_series_ignored(self):
        own = metric("otelcol_exporter_sent_spans", "/fusescope-otel-collector")
        demo, shadow, ignored = compare.metric_series([USER], [USER, own])
        self.assertEqual((demo, ignored["shadow"]), (shadow, 1))

    def test_no_name_filter_in_sql(self):
        # The SQL used to drop otelcol_*, exporter_*, scrape_*, promhttp_* and up by
        # name, so user metrics of those names never reached the comparison.
        sql = compare.CHECKS["metric series"]
        for name in ("otelcol", "exporter", "scrape", "promhttp", "'up'", "NOT LIKE"):
            self.assertNotIn(name, sql)

    def test_user_metrics_with_collector_names_compared(self):
        for name in ("otelcol_receiver_accepted_spans", "exporter_queue_size", "scrape_duration_seconds",
                     "promhttp_metric_handler_requests_total", "up"):
            theirs = metric(name, "billing")
            failed, _ = compare.check("metric series", *compare.metric_series([USER, theirs], [USER]))
            self.assertTrue(failed, f"{name} of a user's service is user data")


class CheckTest(unittest.TestCase):
    def test_equal(self):
        self.assertEqual(compare.check("log attribute keys", {("k",)}, {("k",)}), (False, ["ok    log attribute keys (1 rows)"]))

    def test_difference(self):
        failed, lines = compare.check("log attribute keys", {("a",), ("b",)}, {("a",)}, labels={"demo": "demo", "shadow": "shadow"})
        self.assertTrue(failed)
        self.assertEqual(lines, ["DIFF  log attribute keys: 1 only in demo, 0 only in shadow", "        demo   ('b',)"])


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
