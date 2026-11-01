"""Deterministic OTLP/JSON corpus for observation and parity runs.

Usage: python3 corpus.py <t0_unix_nanos> <out_dir>

Every ID, name and value is fixed; only timestamps move, as offsets from t0, so
two collectors fed the same t0 must write the same rows.
"""

import json
import os
import sys

MS = 1_000_000


def t(t0, offset_ms):
    return str(t0 + offset_ms * MS)


def kv(key, value):
    if isinstance(value, bool):
        return {"key": key, "value": {"boolValue": value}}
    if isinstance(value, int):
        return {"key": key, "value": {"intValue": str(value)}}
    if isinstance(value, float):
        return {"key": key, "value": {"doubleValue": value}}
    if isinstance(value, list):
        return {"key": key, "value": {"arrayValue": {"values": [kv("", v)["value"] for v in value]}}}
    if isinstance(value, dict):
        return {"key": key, "value": {"kvlistValue": {"values": [kv(k, v) for k, v in value.items()]}}}
    if isinstance(value, bytes):
        import base64

        return {"key": key, "value": {"bytesValue": base64.b64encode(value).decode()}}
    return {"key": key, "value": {"stringValue": value}}


def attrs(d):
    return [kv(k, v) for k, v in d.items()]


def special_floats(prefix):
    """NaN and infinities, which OTLP/JSON carries as strings."""
    return [{"key": f"{prefix}.{name}", "value": {"doubleValue": v}}
            for name, v in (("nan", "NaN"), ("inf", "Infinity"), ("ninf", "-Infinity"))]


CHECKOUT = {
    "service.name": "checkout",
    "service.namespace": "shop",
    "service.version": "1.2.3",
    "deployment.environment": "prod",
    "host.name": "node-1",
    "k8s.cluster.name": "main",
    "k8s.namespace.name": "shop",
    "k8s.pod.name": "checkout-7d9f",
    "telemetry.sdk.language": "go",
}
PAYMENT = {
    "service.name": "payment",
    "deployment.environment": "prod",
    "host.name": "node-2",
    "telemetry.sdk.language": "java",
}
SCOPE = {"name": "observe/corpus", "version": "1.0.0"}
# Probe resources: which attributes enter the resource fingerprint, and in what order.
PROBE_RESOURCES = [
    {"service.name": "solo"},
    {"host.name": "node-3"},
    {
        "service.name": "inventory",
        "k8s.namespace.name": "shop",
        "k8s.deployment.name": "inventory",
        "k8s.node.name": "node-4",
        "container.name": "inventory",
        "cloud.region": "us-east-1",
        "host.name": "node-4",
        "process.pid": 4242,
    },
    {"service.name": "batch", "host.name": "node-5", "process.pid": 7, "os.type": "linux"},
    {"service.name": "edge", "k8s.cluster.name": "east", "cloud.region": "eu-west-1",
     "deployment.environment.name": "staging", "k8s.pod.name": "edge-1", "host.name": "node-6"},
]
ESCAPES = "<a href=\"x\">&amp;</a> ção ✓   \t tab"
LONG = "0123456789" * 600

TRACE_A = "0af7651916cd43dd8448eb211c80319c"
TRACE_B = "4bf92f3577b34da6a3ce929d0e0e4736"
S_ROOT, S_DB, S_ERR, S_LINK, S_PROD, S_PAY, S_CONS = (
    "b7ad6b7169203331",
    "00f067aa0ba902b7",
    "53995c3f42cd8ad8",
    "1a2b3c4d5e6f7081",
    "2b3c4d5e6f708192",
    "3c4d5e6f708192a3",
    "4d5e6f708192a3b4",
)


def span(trace, sid, parent, name, kind, start, end, a, t0, status=None, events=(), links=(), flags=None, state=None):
    s = {
        "traceId": trace,
        "spanId": sid,
        "name": name,
        "kind": kind,
        "startTimeUnixNano": t(t0, start),
        "endTimeUnixNano": t(t0, end),
        "attributes": attrs(a),
        "events": [
            {"timeUnixNano": t(t0, et), "name": en, "attributes": attrs(ea)} for et, en, ea in events
        ],
        "links": [{"traceId": lt, "spanId": ls, "attributes": attrs(la)} for lt, ls, la in links],
        "status": status or {},
    }
    if parent:
        s["parentSpanId"] = parent
    if flags is not None:
        s["flags"] = flags
    if state:
        s["traceState"] = state
    return s


TRACE_C = "5ce0e9a56015fec5aadfa328ae398115"


def probe_spans(t0):
    """Spans that pin derived columns: legacy vs current semconv, remote flags, escaped errors."""
    ids = iter(f"{i:016x}" for i in range(0xC0FFEE00, 0xC0FFEE40))
    root = next(ids)
    return [
        span(TRACE_C, root, None, "GET /legacy", 2, 0, 50, {
            "http.method": "GET",
            "http.url": "http://legacy.example.com/a?b=1",
            "http.status_code": 404,
            "http.host": "legacy.example.com",
            "http.target": "/a",
            "net.host.name": "legacy-host",
        }, t0, flags=0x301, state="vendor=a"),
        span(TRACE_C, next(ids), root, "GET upstream", 3, 5, 20, {
            "http.request.method": "PUT",
            "url.full": "https://api.partner.com/v1/items",
            "server.address": "api.partner.com",
            "http.response.status_code": 201,
            "peer.service": "partner-api",
        }, t0, flags=0x101),
        span(TRACE_C, next(ids), root, "legacy client", 3, 21, 30, {
            "http.method": "POST",
            "http.url": "http://old.partner.com/x",
            "net.peer.name": "old.partner.com",
        }, t0),
        span(TRACE_C, next(ids), root, "UPDATE stock", 3, 31, 40, {
            "db.system": "mysql",
            "db.name": "legacy_db",
            "db.operation": "UPDATE",
            "db.statement": "UPDATE stock SET n = n - 1",
            "net.peer.name": "mysql.internal",
        }, t0),
        span(TRACE_C, next(ids), root, "SELECT stock", 3, 41, 45, {
            "db.system.name": "mysql",
            "db.namespace": "current_db",
            "db.operation.name": "SELECT",
            "rpc.service": "Stock",
            "rpc.method": "Get",
        }, t0, status={"code": 1, "message": "ignored for ok"}),
        span(TRACE_C, next(ids), root, "escaped", 1, 46, 49, {}, t0,
             status={"code": 2, "message": "boom"}, events=[
                 (47, "exception", {"exception.type": "Boom", "exception.message": "boom", "exception.escaped": True}),
                 (48, "exception", {"exception.message": "message only"}),
                 (48, "plain", {}),
             ]),
        span(TRACE_C, next(ids), root, "precedence", 3, 50, 51, {
            "http.request.method": "PATCH",
            "http.method": "DELETE",
            "url.full": "https://new.example.com/p",
            "http.url": "https://old.example.com/p",
            "server.address": "new.example.com",
            "net.peer.name": "old.example.com",
            "http.host": "host.example.com",
            "http.response.status_code": 202,
            "http.status_code": 410,
            "db.namespace": "new_db",
            "db.name": "old_db",
            "db.operation.name": "INSERT",
            "db.operation": "DELETE",
        }, t0),
        span(TRACE_C, next(ids), root, "precedence-reversed", 3, 50, 51, {
            "http.method": "DELETE",
            "http.request.method": "PATCH",
            "http.url": "https://old.example.com/p",
            "url.full": "https://new.example.com/p",
            "http.host": "host.example.com",
            "net.peer.name": "old.example.com",
            "server.address": "new.example.com",
            "http.status_code": 410,
            "http.response.status_code": 202,
            "db.name": "old_db",
            "db.namespace": "new_db",
            "db.operation": "DELETE",
            "db.operation.name": "INSERT",
        }, t0),
        span(TRACE_C, next(ids), root, "external-host", 3, 50, 51, {
            "http.request.method": "GET",
            "url.full": "https://url-host.example.com/" + "p" * 300,
            "server.address": "address-host.example.com",
        }, t0),
        span(TRACE_C, next(ids), root, "external-url-only", 3, 50, 51, {
            "http.request.method": "GET",
            "url.full": "https://only-url.example.com:8443/x",
        }, t0),
        span(TRACE_C, next(ids), root, "collision-reversed", 1, 50, 51, {
            "rcoll.sub": "nested",
            "rcoll": "leaf",
        }, t0),
        span(TRACE_C, next(ids), root, "three-hours-old", 1, -3 * 3600 * 1000, -3 * 3600 * 1000 + 5, {
            "old.attr": "old",
        }, t0),
        span(TRACE_C, next(ids), root, "inverted-times", 1, 60, 55, {"inverted": True}, t0),
        dict(span(TRACE_C, next(ids), root, "special-floats", 1, 61, 62, {}, t0), attributes=special_floats("f")),
        span(TRACE_C, next(ids), root, "lengths", 1, 50, 51,
             {f"len.{n}": "x" * n for n in (128, 255, 256, 257, 511, 512, 513, 1023, 1024, 1025, 2048, 4096)}, t0),
        span(TRACE_C, next(ids), root, "strings", 0, 52, 53, {
            "escapes": ESCAPES,
            "long.value": LONG,
            "coll": "leaf",
            "coll.sub": "nested",
        }, t0, status={"code": 0, "message": "unset with message"}, events=[
            (52, "exception", {"exception.type": "Esc<&>", "exception.message": ESCAPES, "exception.stacktrace": LONG}),
        ], links=[(TRACE_A, S_ROOT, {})], state="k=" + ESCAPES),
    ]


def traces(t0):
    checkout_spans = [
        span(TRACE_A, S_ROOT, None, "GET /cart", 2, 0, 120, {
            "http.request.method": "GET",
            "http.route": "/cart",
            "http.response.status_code": 200,
            "url.full": "https://shop.example.com/cart?id=42",
            "server.address": "shop.example.com",
            "user_agent.original": "corpus/1.0",
        }, t0),
        span(TRACE_A, S_DB, S_ROOT, "SELECT cart", 3, 10, 30, {
            "db.system.name": "postgresql",
            "db.namespace": "shop",
            "db.query.text": "SELECT * FROM cart WHERE id = $1",
            "server.address": "db.internal",
            "server.port": 5432,
        }, t0),
        span(TRACE_A, S_ERR, S_ROOT, "applyDiscount", 1, 35, 60, {
            "discount.code": "FALL25",
        }, t0, status={"code": 2, "message": "discount expired"}, events=[
            (40, "exception", {
                "exception.type": "DiscountExpiredError",
                "exception.message": "discount FALL25 expired",
                "exception.stacktrace": "DiscountExpiredError: discount FALL25 expired\n    at applyDiscount (cart.go:42)",
            }),
            (55, "retry", {"attempt": 2}),
        ]),
        span(TRACE_A, S_LINK, S_ROOT, "typed-attributes", 1, 61, 70, {
            "attr.bool": True,
            "attr.int": -7,
            "attr.double": 3.25,
            "attr.string": "plain",
            "attr.int_array": [1, 2, 3],
            "attr.string_array": ["a", "b"],
            "attr.bytes": b"\x00\x01fusescope",
            "attr.map": {"inner": "value", "n": 1},
            "attr.empty": "",
        }, t0, links=[(TRACE_B, S_CONS, {"link.reason": "async"})]),
        span(TRACE_A, S_PROD, S_ROOT, "orders publish", 4, 71, 75, {
            "messaging.system": "kafka",
            "messaging.destination.name": "orders",
            "messaging.operation.type": "send",
        }, t0),
    ]
    payment_spans = [
        span(TRACE_A, S_PAY, S_DB, "POST /charge", 2, 80, 115, {
            "http.request.method": "POST",
            "http.route": "/charge",
            "http.response.status_code": 500,
            "rpc.system": "grpc",
        }, t0, status={"code": 2}),
        span(TRACE_B, S_CONS, None, "orders process", 5, 200, 260, {
            "messaging.system": "kafka",
            "messaging.destination.name": "orders",
            "messaging.operation.type": "process",
        }, t0, status={"code": 1}),
    ]
    return {"resourceSpans": [
        {"resource": {"attributes": attrs(CHECKOUT)}, "scopeSpans": [{"scope": SCOPE, "spans": checkout_spans}]},
        {"resource": {"attributes": attrs(PAYMENT)}, "scopeSpans": [{"scope": SCOPE, "spans": payment_spans}]},
        {"resource": {"attributes": attrs(PROBE_RESOURCES[2])}, "scopeSpans": [
            {"scope": {"name": "probe", "attributes": attrs({"scope.attr": "x"})}, "spans": probe_spans(t0)}]},
    ] + [
        {"resource": {"attributes": attrs(r)}, "scopeSpans": [{"scope": SCOPE, "spans": [
            span(f"{i + 1:032x}", f"{i + 1:016x}", None, "probe-resource", 1, 300, 301, {}, t0)]}]}
        for i, r in enumerate(PROBE_RESOURCES) if i != 2
    ]}


def logs(t0):
    def rec(ms, sev_num, sev_text, body, a, trace=None, sid=None):
        r = {
            "timeUnixNano": t(t0, ms),
            "observedTimeUnixNano": t(t0, ms + 1),
            "severityNumber": sev_num,
            "body": kv("", body)["value"],
            "attributes": attrs(a),
        }
        if sev_text:
            r["severityText"] = sev_text
        if trace:
            r["traceId"], r["spanId"], r["flags"] = trace, sid, 1
        return r

    records = [
        rec(5, 9, "INFO", "cart loaded", {"cart.items": 3}, TRACE_A, S_ROOT),
        rec(41, 17, "ERROR", "discount expired", {
            "exception.type": "DiscountExpiredError",
            "code.function": "applyDiscount",
        }, TRACE_A, S_ERR),
        rec(90, 13, "WARN", {"event": "slow_charge", "duration_ms": 35, "ok": False}, {"payment.provider": "acme"}),
        rec(95, 5, None, "debug without severity text", {"attr.double": 0.5, "attr.bool": True}),
        rec(96, 0, None, "", {}),
    ]
    probes = [
        rec(100, 9, "INFO", 42, {"body.kind": "int"}),
        rec(101, 9, "INFO", 2.5, {"body.kind": "double"}),
        rec(102, 9, "INFO", True, {"body.kind": "bool"}),
        rec(103, 9, "INFO", b"\x00\x01raw", {"body.kind": "bytes"}),
        rec(104, 9, "INFO", ["a", 1, {"k": "v"}], {"body.kind": "array"}),
        rec(105, 9, "INFO", {"z": 1, "a": {"nested": [1, 2]}, "html": "<&>"}, {"body.kind": "map"}),
        rec(106, 9, "INFO", "x" * 5000, {"body.kind": "long"}),
        rec(107, 21, "FATAL", ESCAPES, {
            "attr.map": {"inner": "value", "n": 1},
            "attr.int_array": [1, 2, 3],
            "attr.bytes": b"\x00\x01fusescope",
            "attr.empty": "",
            "coll": "leaf",
            "coll.sub": "nested",
            "long.value": LONG,
            "len.256": "y" * 256,
            "len.257": "y" * 257,
        }),
        rec(108, 0, "Warning", "text only severity", {}),
        rec(-3 * 3600 * 1000, 9, "INFO", "three hours old", {"old.attr": "old"}),
    ]
    no_time = rec(110, 9, "INFO", "observed time only", {})
    del no_time["timeUnixNano"]
    no_times = rec(111, 9, "INFO", "no time at all", {})
    del no_times["timeUnixNano"], no_times["observedTimeUnixNano"]
    probes += [no_time, no_times]
    special = rec(112, 9, "INFO", "special floats", {})
    special["attributes"] = special_floats("f")
    probes.append(special)
    first_old = rec(-5 * 3600 * 1000, 9, "INFO", "five hours old, first in batch", {"first.old": "yes"})
    return {"resourceLogs": [
        {"resource": {"attributes": attrs(PROBE_RESOURCES[0])}, "scopeLogs": [{"scope": SCOPE, "logRecords": [first_old]}]},
        {"resource": {"attributes": attrs(CHECKOUT)}, "scopeLogs": [{"scope": SCOPE, "logRecords": records}]},
        {"resource": {"attributes": attrs(PROBE_RESOURCES[2])}, "scopeLogs": [
            {"scope": {"name": "probe", "attributes": attrs({"scope.attr": "x", "scope.n": 2})}, "logRecords": probes}]},
    ]}


def metrics(t0):
    start = t(t0, -60_000)
    now = t(t0, 0)
    point = {"startTimeUnixNano": start, "timeUnixNano": now}
    ms = [
        {"name": "system.memory.usage", "unit": "By", "description": "Memory in use.",
         "gauge": {"dataPoints": [dict(point, asInt="1048576", attributes=attrs({"state": "used"}))]}},
        {"name": "http.server.request.count", "unit": "{request}",
         "sum": {"aggregationTemporality": 2, "isMonotonic": True,
                 "dataPoints": [dict(point, asInt="42", attributes=attrs({"http.route": "/cart", "http.response.status_code": 200}))]}},
        {"name": "queue.depth.change", "unit": "{item}",
         "sum": {"aggregationTemporality": 1, "isMonotonic": False,
                 "dataPoints": [dict(point, asDouble=-2.5, attributes=attrs({"queue": "orders"}))]}},
        {"name": "http.server.request.duration", "unit": "s",
         "histogram": {"aggregationTemporality": 2, "dataPoints": [dict(point,
             count="6", sum=0.73, min=0.01, max=0.4,
             bucketCounts=["1", "2", "2", "1"], explicitBounds=[0.025, 0.1, 0.25],
             attributes=attrs({"http.route": "/cart"}))]}},
        {"name": "rpc.server.duration", "unit": "ms",
         "exponentialHistogram": {"aggregationTemporality": 1, "dataPoints": [dict(point,
             count="5", sum=12.5, min=0.5, max=6.0, scale=2, zeroCount="1",
             positive={"offset": 1, "bucketCounts": ["1", "0", "2", "1"]},
             negative={"offset": 0, "bucketCounts": []},
             attributes=attrs({"rpc.method": "Charge"}))]}},
        {"name": "go.gc.pause", "unit": "s",
         "summary": {"dataPoints": [dict(point, count="10", sum=0.02, quantileValues=[
             {"quantile": 0.5, "value": 0.001}, {"quantile": 0.99, "value": 0.008}])]}},
    ]
    earlier = dict(point, timeUnixNano=t(t0, -30_000))
    probes = [
        {"name": "probe.bare", "gauge": {"dataPoints": [dict(point, asDouble=1.0)]}},
        {"name": "probe.one_attr", "gauge": {"dataPoints": [dict(point, asInt="2", attributes=attrs({"a": "b"}))]}},
        {"name": "probe.typed_attrs", "gauge": {"dataPoints": [dict(point, asInt="3", attributes=attrs({
            "i": 7, "d": 1.5, "bool": True, "arr": ["x", "y"], "empty": ""}))]}},
        {"name": "probe.two_points", "unit": "ms", "description": "Two points of one series.",
         "gauge": {"dataPoints": [dict(earlier, asDouble=4.0), dict(point, asDouble=5.0)]}},
        {"name": "probe.no_value", "gauge": {"dataPoints": [dict(point, flags=1)]}},
        {"name": "probe.nan", "gauge": {"dataPoints": [dict(point, asDouble="NaN")]}},
        {"name": "probe.inf", "gauge": {"dataPoints": [dict(point, asDouble="Infinity")]}},
        {"name": "probe.ninf", "gauge": {"dataPoints": [dict(point, asDouble="-Infinity")]}},
        {"name": "probe.monotonic_delta", "sum": {"aggregationTemporality": 1, "isMonotonic": True,
         "dataPoints": [dict(point, asInt="9")]}},
        {"name": "probe.cumulative_updown", "sum": {"aggregationTemporality": 2, "isMonotonic": False,
         "dataPoints": [dict(point, asInt="-4")]}},
        {"name": "probe.delta_histogram", "unit": "ms", "histogram": {"aggregationTemporality": 1, "dataPoints": [dict(point,
             count="3", sum=30.0, bucketCounts=["1", "2"], explicitBounds=[10.0])]}},
        {"name": "probe.histogram_no_minmax", "histogram": {"aggregationTemporality": 2, "dataPoints": [dict(point,
             count="0", bucketCounts=[], explicitBounds=[])]}},
        {"name": "probe.exp_cumulative", "exponentialHistogram": {"aggregationTemporality": 2, "dataPoints": [dict(point,
             count="2", sum=3.0, scale=0, zeroCount="0", positive={"offset": 0, "bucketCounts": ["1", "1"]})]}},
        {"name": "probe.extreme_bounds", "histogram": {"aggregationTemporality": 2, "dataPoints": [dict(point,
             count="3", sum=1.0, bucketCounts=["1", "1", "1"], explicitBounds=[0.00001, 1000000.0])]}},
        {"name": "probe.extreme_quantiles", "summary": {"dataPoints": [dict(point, count="1", sum=1.0, quantileValues=[
             {"quantile": 0.999, "value": 1.0}, {"quantile": 0.00001, "value": 0.5}, {"quantile": 1.0, "value": 2.0}])]}},
        {"name": "probe.dotted-Name_with:chars", "gauge": {"dataPoints": [dict(point, asDouble=6.0)]}},
    ]
    return {"resourceMetrics": [
        {"resource": {"attributes": attrs(CHECKOUT)}, "scopeMetrics": [{"scope": SCOPE, "metrics": ms}]},
        {"resource": {}, "scopeMetrics": [{"scope": {}, "metrics": probes}]},
        {"resource": {"attributes": attrs({"service.name": "probe-res", "process.pid": 42,
                                           "deployment.environment.name": "staging"})},
         "scopeMetrics": [{"scope": {"name": "probe", "version": "2", "attributes": attrs({"scope.n": 3})}, "metrics": [
             {"name": "probe.three_hours_old", "gauge": {"dataPoints": [
                 {"startTimeUnixNano": t(t0, -3 * 3600_000 - 1000), "timeUnixNano": t(t0, -3 * 3600_000), "asDouble": 7.0}]}},
             {"name": "probe.resource_typed", "gauge": {"dataPoints": [dict(point, asDouble=8.0)]}},
         ]}]},
    ]}


def _varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def _field(num, wire, payload):
    tag = _varint(num << 3 | wire)
    if wire == 2:
        return tag + _varint(len(payload)) + payload
    return tag + payload


def _string_kv(key, value):
    any_value = _field(1, 2, value)
    return _field(1, 2, key.encode()) + _field(2, 2, any_value)


def invalid_utf8(t0):
    """OTLP protobuf payloads with invalid UTF-8, which OTLP/JSON cannot carry."""
    bad = b"bad \xff\xfe utf8 \xe2\x82"
    resource = _field(1, 2, _string_kv("service.name", b"probe-utf8"))
    event = (_field(1, 1, (t0 + 5 * MS).to_bytes(8, "little")) + _field(2, 2, b"exception")
             + _field(3, 2, _string_kv("exception.message", bad)))
    span = (_field(1, 2, bytes.fromhex("6e6f74757466386e6f74757466380001")) + _field(2, 2, bytes.fromhex("7574663875746638"))
            + _field(5, 2, bad) + _field(6, 0, _varint(1))
            + _field(7, 1, t0.to_bytes(8, "little")) + _field(8, 1, (t0 + 10 * MS).to_bytes(8, "little"))
            + _field(9, 2, _string_kv("bad.attr", bad)) + _field(11, 2, event))
    traces = _field(1, 2, _field(1, 2, resource) + _field(2, 2, _field(2, 2, span)))
    record = (_field(1, 1, (t0 + 7 * MS).to_bytes(8, "little")) + _field(5, 2, _field(1, 2, bad))
              + _field(6, 2, _string_kv("bad.attr", bad)))
    logs = _field(1, 2, _field(1, 2, resource) + _field(2, 2, _field(2, 2, record)))
    return {"traces": traces, "logs": logs}


def main():
    t0, out = int(sys.argv[1]), sys.argv[2]
    os.makedirs(out, exist_ok=True)
    for name, build in (("traces", traces), ("logs", logs), ("metrics", metrics)):
        with open(os.path.join(out, f"{name}.json"), "w") as f:
            json.dump(build(t0), f, indent=2, sort_keys=True)
            f.write("\n")
    for name, payload in invalid_utf8(t0).items():
        with open(os.path.join(out, f"{name}-invalid-utf8.pb"), "wb") as f:
            f.write(payload)


if __name__ == "__main__":
    main()
