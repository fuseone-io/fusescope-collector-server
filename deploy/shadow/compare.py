"""Compares what the demo collector and the shadow collector wrote over the same
settled window, per minute where both see the same requests.

Real traffic batches differently in each collector, so rows are not compared
one by one as in test/parity: counts per service and minute must match for
spans and logs, per service for APM calls, and the sets of metric series and
attribute keys must match. Exits 1 on a difference.

Spans and logs are counted once each: an item can be stored twice on one
side, by a retried write or a sender's retry the other side refused (README,
Risks). Duplicates are a check of their own: on this collector's side (OURS,
the shadow's unless swapped) they fail the run, on the other they are noted.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

DEMO = os.environ.get("DEMO_CLICKHOUSE", "http://clickhouse:8123")
SHADOW = os.environ.get("SHADOW_CLICKHOUSE", "http://clickhouse-shadow:8123")
# Settled window: minutes that ended at least SETTLE_MINUTES ago.
WINDOW_MINUTES = int(os.environ.get("WINDOW_MINUTES", "60"))
SETTLE_MINUTES = int(os.environ.get("SETTLE_MINUTES", "5"))
# Names in the report. After the swap (README) this collector writes the
# demo's ClickHouse and the existing collector the shadow's.
DEMO_LABEL = os.environ.get("DEMO_LABEL", "demo")
SHADOW_LABEL = os.environ.get("SHADOW_LABEL", "shadow")
# The side whose duplicates fail the run: this collector's.
OURS = os.environ.get("OURS", "shadow")
# The service.name under which a collector reports its own telemetry. Its
# series are counted and left out of the metric series check; series of any
# other service, otelcol.* included (a user's own collector), are compared.
INTERNAL_SERVICES = tuple(s for s in os.environ.get("INTERNAL_SERVICES", "fusescope-collector").split(",") if s)
# The demo collector scrapes its own telemetry; the shadow collector does not.
SELF_METRICS = "metric_name NOT LIKE 'otelcol\\_%' AND metric_name NOT LIKE 'exporter\\_%' " \
    "AND metric_name NOT LIKE 'scrape\\_%' AND metric_name NOT LIKE 'promhttp\\_%' AND metric_name != 'up'"

# Fixed once per run, so both databases and every check see the same minutes.
_end = int(time.time()) // 60 * 60 - SETTLE_MINUTES * 60
WINDOW = (f"toDateTime({_end - WINDOW_MINUTES * 60}, 'UTC')", f"toDateTime({_end}, 'UTC')")
# Each collector stamps span metrics with its own identity (apm-metrics spec §6).
COLLECTOR_KEYS = ("signoz.collector.id", "resource_signoz.collector.id")

# What identifies a log: every field the sender sets. Not the log ID or the
# observed time, which each collector sets on receipt, so a retried log gets
# new ones; not body_v2 and body_promoted, derived from the body.
LOG_IDENTITY = (
    "timestamp", "trace_id", "span_id", "trace_flags", "severity_text", "severity_number", "body",
    "attributes_string", "attributes_number", "attributes_bool", "resources_string",
    "scope_name", "scope_version", "scope_string",
)
LOG_KEY = ", ".join(LOG_IDENTITY)
LOGS_IN_WINDOW = f"""timestamp >= toUnixTimestamp64Nano(toDateTime64({WINDOW[0]}, 9))
          AND timestamp < toUnixTimestamp64Nano(toDateTime64({WINDOW[1]}, 9))"""

CHECKS = {
    "spans per service and minute": f"""
        SELECT serviceName, toString(toStartOfMinute(timestamp)) AS m, uniqExact(traceID, spanID)
        FROM signoz_traces.signoz_index_v3
        WHERE timestamp >= {WINDOW[0]} AND timestamp < {WINDOW[1]}
        GROUP BY serviceName, m ORDER BY serviceName, m""",
    "error spans per service": f"""
        SELECT serviceName, uniqExact(traceID, spanID) FROM signoz_traces.signoz_error_index_v2
        WHERE timestamp >= {WINDOW[0]} AND timestamp < {WINDOW[1]}
        GROUP BY serviceName ORDER BY serviceName""",
    "span resource keys": f"""
        SELECT DISTINCT arrayJoin(mapKeys(resources_string)) AS k FROM signoz_traces.signoz_index_v3
        WHERE timestamp >= {WINDOW[0]} AND timestamp < {WINDOW[1]} ORDER BY k""",
    "logs per service and minute": f"""
        SELECT resources_string['service.name'] AS s, toString(toStartOfMinute(fromUnixTimestamp64Nano(toInt64(timestamp)))) AS m,
               uniqExact({LOG_KEY})
        FROM signoz_logs.logs_v2
        WHERE {LOGS_IN_WINDOW}
        GROUP BY s, m ORDER BY s, m""",
    "log attribute keys": f"""
        SELECT DISTINCT arrayJoin(arrayConcat(mapKeys(attributes_string), mapKeys(resources_string))) AS k
        FROM signoz_logs.logs_v2
        WHERE timestamp >= toUnixTimestamp64Nano(toDateTime64({WINDOW[0]}, 9))
          AND timestamp < toUnixTimestamp64Nano(toDateTime64({WINDOW[1]}, 9)) ORDER BY k""",
    "APM calls per service": f"""
        SELECT JSONExtractString(labels, 'service.name') AS s, sum(value)
        FROM signoz_metrics.samples_v4 AS sm
        INNER JOIN (SELECT DISTINCT fingerprint, labels FROM signoz_metrics.time_series_v4
                    WHERE metric_name = 'signoz_calls_total') AS ts USING fingerprint
        WHERE sm.metric_name = 'signoz_calls_total'
          AND sm.unix_milli >= toUnixTimestamp({WINDOW[0]}) * 1000 AND sm.unix_milli < toUnixTimestamp({WINDOW[1]}) * 1000
        GROUP BY s ORDER BY s""",
    # Series identities, not fingerprints, which differ by design (metrics spec §4).
    # Only series with samples in the window: series rows are hourly, and a
    # series newer than the window may reach one database first.
    "metric series": f"""
        SELECT DISTINCT metric_name, labels, attrs, scope_attrs, resource_attrs FROM signoz_metrics.time_series_v4
        WHERE {SELF_METRICS} AND unix_milli >= toUnixTimestamp({WINDOW[0]}) * 1000 - 3600000
          AND unix_milli < toUnixTimestamp({WINDOW[1]}) * 1000
          AND fingerprint IN (
            SELECT fingerprint FROM signoz_metrics.samples_v4
            WHERE {SELF_METRICS} AND unix_milli >= toUnixTimestamp({WINDOW[0]}) * 1000
              AND unix_milli < toUnixTimestamp({WINDOW[1]}) * 1000)""",
}

# Items stored more than once, per service and minute: (service, minute, extra copies).
DUPLICATES = {
    "duplicate spans": f"""
        SELECT serviceName, toString(toStartOfMinute(timestamp)) AS m, count() - uniqExact(traceID, spanID) AS extra
        FROM signoz_traces.signoz_index_v3
        WHERE timestamp >= {WINDOW[0]} AND timestamp < {WINDOW[1]}
        GROUP BY serviceName, m HAVING extra > 0 ORDER BY serviceName, m""",
    "duplicate logs": f"""
        SELECT resources_string['service.name'] AS s, toString(toStartOfMinute(fromUnixTimestamp64Nano(toInt64(timestamp)))) AS m,
               count() - uniqExact({LOG_KEY}) AS extra
        FROM signoz_logs.logs_v2
        WHERE {LOGS_IN_WINDOW}
        GROUP BY s, m HAVING extra > 0 ORDER BY s, m""",
}


# Scopes the collectors generate themselves carry each collector's build
# version; scopes received over OTLP carry the producer's and are kept.
SCOPE_NAME, SCOPE_VERSION = "__scope.name__", "__scope.version__"
COLLECTOR_SCOPES = ("github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/",)


def anonymize(row):
    """Replaces the collector identity, and the build version of collector-generated scopes, in a series row."""
    name, labels, attrs, scope_attrs, resource_attrs = row
    labels = json.loads(labels, strict=False)
    labels.update({k: "<collector>" for k in COLLECTOR_KEYS if k in labels})
    attrs, scope_attrs, resource_attrs = (
        {k: "<collector>" if k in COLLECTOR_KEYS else v for k, v in m.items()} for m in (attrs, scope_attrs, resource_attrs))
    if labels.get(SCOPE_NAME, "").startswith(COLLECTOR_SCOPES):
        for m in (labels, scope_attrs):
            if SCOPE_VERSION in m:
                m[SCOPE_VERSION] = "<build>"
    maps = [attrs, scope_attrs, resource_attrs]
    return (name, json.dumps([labels] + maps, sort_keys=True))


def internal(row, services=INTERNAL_SERVICES):
    """Whether a series row is a collector's own telemetry: its service.name, as a resource attribute or a label."""
    _, labels, _, _, resource_attrs = row
    service = resource_attrs.get("service.name") or json.loads(labels, strict=False).get("service.name")
    return service in services


def metric_series(demo, shadow, services=INTERNAL_SERVICES):
    """Series sets to compare, without the collectors' own telemetry, and how many series each side left out."""
    kept, ignored = {}, {"services": services}
    for side, rows in (("demo", demo), ("shadow", shadow)):
        rows = list(rows)
        own = [row for row in rows if internal(row, services)]
        ignored[side] = len(own)
        kept[side] = {anonymize(row) for row in rows if not internal(row, services)}
    return kept["demo"], kept["shadow"], ignored


def check(name, demo, shadow, ignored=None, labels=None):
    """Compares two sets of rows. Returns (failed, lines)."""
    labels = labels or {"demo": DEMO_LABEL, "shadow": SHADOW_LABEL}
    demo, shadow = set(demo), set(shadow)
    note = ""
    if ignored is not None:
        note = (f"ignored {ignored['demo'] + ignored['shadow']} internal series of {', '.join(ignored['services'])}: "
                f"{ignored['demo']} in {labels['demo']}, {ignored['shadow']} in {labels['shadow']}")
    if demo == shadow:
        return False, [f"ok    {name} ({len(demo)} rows{'; ' + note if note else ''})"]
    lines = [f"DIFF  {name}: {len(demo - shadow)} only in {labels['demo']}, {len(shadow - demo)} only in {labels['shadow']}"
             + (f"; {note}" if note else "")]
    lines += [f"        {labels['demo']:<6} {row}" for row in sorted(demo - shadow, key=str)[:10]]
    lines += [f"        {labels['shadow']:<6} {row}" for row in sorted(shadow - demo, key=str)[:10]]
    return True, lines


def query(base, sql):
    params = urllib.parse.urlencode({"readonly": "1"})
    req = urllib.request.Request(f"{base}/?{params}", data=(sql + " FORMAT JSONCompact").encode())
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return [tuple(row) for row in json.load(r)["data"]]
    except urllib.error.HTTPError as e:
        raise SystemExit(f"{base}: {e.read().decode(errors='replace').strip()}") from None


def duplicates(name, demo, shadow, ours=OURS, labels=None):
    """Reports duplicates: this collector's side fails, the other's is noted. Returns (failed, lines)."""
    labels = labels or {"demo": DEMO_LABEL, "shadow": SHADOW_LABEL}
    rows = {"demo": demo, "shadow": shadow}
    lines = []
    for side in sorted(rows, key=lambda side: side != ours):
        if rows[side]:
            word = "DIFF " if side == ours else "note "
            lines.append(f"{word} {name} in the {labels[side]}: {sum(r[2] for r in rows[side])} extra in {len(rows[side])} service-minutes")
            lines += [f"        {labels[side]:<6} {row}" for row in rows[side][:10]]
    if not lines:
        lines.append(f"ok    {name} (none)")
    return bool(rows[ours]), lines


def main():
    failed = 0
    for name, sql in DUPLICATES.items():
        bad, lines = duplicates(name, query(DEMO, sql), query(SHADOW, sql))
        failed += bad
        print("\n".join(lines))
    for name, sql in CHECKS.items():
        demo, shadow = query(DEMO, sql), query(SHADOW, sql)
        if name == "metric series":
            bad, lines = check(name, *metric_series(demo, shadow))
        else:
            bad, lines = check(name, demo, shadow)
        failed += bad
        print("\n".join(lines))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
