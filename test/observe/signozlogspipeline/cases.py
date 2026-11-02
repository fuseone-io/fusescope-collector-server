"""Builds the case file the signozlogspipeline observation runs.

Usage: python3 -I cases.py <backend_golden.json> <out cases.json>

Most cases come from the FuseScope backend's simulator contract golden
(`tests/integration/testdata/simulatorcontract/logspipelines.golden.json`, MIT):
its log sets and, per case, the processors the backend deploys for it, exactly
as `GenerateCollectorConfigWithPipelines` writes them (`$` escaped as `$$`).
The `X_` cases below are written here in the same shape (router, operators,
noop) to cover what the backend corpus does not: non-string bodies, record and
scope metadata, resource regrouping, expression errors and expression helpers.
`R_` cases only check whether the reference starts with a configuration.
"""

import base64
import json
import sys

IDX = "__signoz_input_idx__"


def kv(key, value):
    if isinstance(value, bool):
        return {"key": key, "value": {"boolValue": value}}
    if isinstance(value, int):
        return {"key": key, "value": {"intValue": str(value)}}
    if isinstance(value, float):
        return {"key": key, "value": {"doubleValue": value}}
    if isinstance(value, dict):
        return {"key": key, "value": {"kvlistValue": {"values": [kv(k, v) for k, v in value.items()]}}}
    if isinstance(value, list):
        return {"key": key, "value": {"arrayValue": {"values": [kv("", v)["value"] for v in value]}}}
    if isinstance(value, bytes):
        return {"key": key, "value": {"bytesValue": base64.b64encode(value).decode()}}
    return {"key": key, "value": {"stringValue": value}}


def anyvalue(value):
    return kv("", value)["value"]


def record(index, body=None, attrs=None, **extra):
    r = {"timeUnixNano": "1704164645000000000", "attributes": [kv(IDX, index)] + [kv(k, v) for k, v in (attrs or {}).items()]}
    if body is not None:
        r["body"] = anyvalue(body)
    r.update(extra)
    return r


def doc(records, resource=None, scope=None):
    return {"resourceLogs": [{"resource": {"attributes": [kv(k, v) for k, v in (resource or {}).items()]},
                              "scopeLogs": [{"scope": scope or {}, "logRecords": records}]}]}


def pipeline(ops, expr="body != nil"):
    """A processor shaped like pipelineBuilder.go's: router, the operators chained by output, noop."""
    ops = [dict(op) for op in ops]
    for i, op in enumerate(ops):
        op.setdefault("id", f"op{i + 1}")
    for i in range(len(ops) - 1):
        ops[i].setdefault("output", ops[i + 1]["id"])
    router = {"id": "router_signoz", "type": "router", "routes": [{"output": ops[0]["id"], "expr": expr}], "default": "noop"}
    return {"operators": [router] + ops + [{"id": "noop", "type": "noop"}]}


def single(ops, expr="body != nil"):
    return {"processors": {"signozlogspipeline/pipeline_x": pipeline(ops, expr)}, "processorNames": ["signozlogspipeline/pipeline_x"]}


EXTRA_INPUTS = {
    # One record per OTLP body type, the last without a body.
    "bodies": [
        doc([record(0, {"msg": "GET /a 200", "level": "warn", "n": 3, "nested": {"k": "v"}})], {"service.name": "svc"}),
        doc([record(1, 42)]),
        doc([record(2, b'{"level":"error","msg":"bytes json"}')]),
        doc([record(3, 1.5)]),
        doc([record(4, True)]),
        doc([record(5, ["a", 1])]),
        doc([record(6, '{"level":"info","msg":"string json","q":"\\"quoted\\""}')]),
        doc([record(7)]),
    ],
    # Two resources and two scopes in one request, with every record field set.
    "metadata": [{
        "resourceLogs": [
            {"resource": {"attributes": [kv("service.name", "a"), kv("k8s.pod.name", "a-1")], "droppedAttributesCount": 2},
             "schemaUrl": "https://opentelemetry.io/schemas/1.21.0",
             "scopeLogs": [
                 {"scope": {"name": "lib.one", "version": "1.0.0", "attributes": [kv("scope.attr", "s1")], "droppedAttributesCount": 1},
                  "schemaUrl": "https://opentelemetry.io/schemas/1.20.0",
                  "logRecords": [
                      record(0, "first", {"route": "yes"}, observedTimeUnixNano="1704164646000000000", severityNumber=17,
                             severityText="ERROR", flags=1, traceId="0102030405060708090a0b0c0d0e0f10", spanId="0102030405060708",
                             droppedAttributesCount=3, eventName="order.placed"),
                      record(1, "second", {"route": "no"}, observedTimeUnixNano="1704164647000000000"),
                  ]},
                 {"scope": {"name": "lib.two"},
                  "logRecords": [record(2, "third", {"route": "yes"})]},
             ]},
            {"resource": {"attributes": [kv("service.name", "b")]},
             "scopeLogs": [{"scope": {"name": "lib.one", "version": "1.0.0", "attributes": [kv("scope.attr", "s1")], "droppedAttributesCount": 1},
                            "logRecords": [record(3, "fourth", {"route": "yes"})]}]},
        ]}],
    "values": [
        doc([record(0, '{"a":null,"b":[1,null,{"c":null}],"d":{"e":null},"f":1e400}', {"s": "text", "n": 5, "q": "\"in quotes\"", "path": "a.b"})]),
        doc([record(1, "plain", {"s": "7", "n": "x"})]),
        # Severity numbers without text; JSON nulls.
        doc([record(2, '{"a":null,"b":[1,null,{"c":null}],"d":{"e":null}}', {"s": "nulls"}, severityNumber=13)]),
        doc([record(3, "info2", {"s": "sev"}, severityNumber=10)]),
    ],
}

CACHE_KEY = "__signozlogspipelineprocessor_internal__parsedBodyJson"

# The same JSON object as a string, bytes and map body, for parse_from body.<field>.
JSON_FIELDS = {"level": "warn", "ts": "1704164700", "trace": "0102030405060708090a0b0c0d0e0f10", "msg": "GET 200", "obj": {"k": "v"}}
EXTRA_INPUTS["jsonbodies"] = [
    doc([record(0, json.dumps(JSON_FIELDS, separators=(",", ":")))]),
    doc([record(1, json.dumps(JSON_FIELDS, separators=(",", ":")).encode())]),
    doc([record(2, JSON_FIELDS)]),
]


def body_field_cases():
    """Each operator reading or writing body.<field>, with and without an if that reads the body as JSON."""
    ops = {
        "Severity": {"type": "severity_parser", "parse_from": "body.level", "on_error": "send"},
        "Time": {"type": "time_parser", "parse_from": "body.ts", "layout_type": "epoch", "layout": "s", "on_error": "send"},
        "Trace": {"type": "trace_parser", "trace_id": {"parse_from": "body.trace"}, "on_error": "send"},
        "Regex": {"type": "regex_parser", "parse_from": "body.msg", "regex": "^(?P<verb>[A-Z]+)", "on_error": "send"},
        "JSON": {"type": "json_parser", "parse_from": "body.obj", "parse_to": "attributes.obj", "on_error": "send"},
        "Copy": {"type": "copy", "from": "body.msg", "to": "attributes.msg", "on_error": "send"},
        "Move": {"type": "move", "from": "body.msg", "to": "attributes.msg", "on_error": "send"},
        "Remove": {"type": "remove", "field": "body.msg", "on_error": "send"},
        "AddField": {"type": "add", "field": "body.added", "value": "x", "on_error": "send"},
        "MoveToBodyField": {"type": "move", "from": "attributes.src", "to": "body.moved", "on_error": "send"},
    }
    cases = []
    for name, op in ops.items():
        pre = [{"type": "add", "field": "attributes.src", "value": "s"}] if name == "MoveToBodyField" else []
        cases.append({"name": f"X_BodyField_{name}", "logSet": "jsonbodies", **single(pre + [op], "true")})
        cases.append({"name": f"X_BodyField_{name}_AfterIf", "logSet": "jsonbodies",
                      **single(pre + [dict(op, **{"if": "body?.level != nil"})], "true")})
    return cases

EXTRA_CASES = [
    {"name": "X_Bodies_Passthrough", "logSet": "bodies", **single([{"type": "add", "field": "attributes.seen", "value": "1"}])},
    {"name": "X_Bodies_RouterTypeOf", "logSet": "bodies", **single([{"type": "add", "field": "attributes.kind", "value": "EXPR(type(body))"}], "true")},
    {"name": "X_Bodies_Regex", "logSet": "bodies", **single([{
        "type": "regex_parser", "parse_from": "body", "parse_to": "attributes", "on_error": "send",
        "regex": "^(?P<verb>[A-Z]+)", "if": 'body != nil && body matches "^(?P<verb>[A-Z]+)"'}])},
    {"name": "X_Bodies_RegexNoIf", "logSet": "bodies", **single([{
        "type": "regex_parser", "parse_from": "body", "parse_to": "attributes", "on_error": "send", "regex": "^(?P<first>.)"}])},
    {"name": "X_Bodies_RegexNoIfDrop", "logSet": "bodies", **single([{
        "type": "regex_parser", "parse_from": "body", "parse_to": "attributes", "on_error": "drop", "regex": "^(?P<first>.)"}])},
    {"name": "X_Bodies_JSON", "logSet": "bodies", **single([{
        "type": "json_parser", "parse_from": "body", "parse_to": "attributes", "on_error": "send_quiet"}])},
    {"name": "X_Bodies_JSONToBodyFlatten", "logSet": "bodies", **single([{
        "type": "json_parser", "parse_from": "body", "parse_to": "body", "on_error": "send_quiet",
        "enable_flattening": True, "max_flattening_depth": 1, "enable_paths": True}])},
    {"name": "X_Bodies_Grok", "logSet": "bodies", **single([{
        "type": "grok_parser", "parse_from": "body", "parse_to": "attributes", "on_error": "send", "if": "body != nil",
        "pattern": "%{WORD:w}"}])},
    {"name": "X_Bodies_FieldAccess", "logSet": "bodies", **single([
        {"type": "add", "field": "attributes.lvl", "value": "EXPR(body.level)", "if": "body?.level != nil"},
        {"type": "move", "from": "body.msg", "to": "attributes.msg", "if": "body?.msg != nil"},
        {"type": "copy", "from": "body.nested", "to": "attributes.nested", "if": "body?.nested != nil"},
    ])},
    {"name": "X_Bodies_FilterOnBodyField", "logSet": "bodies", **single([{"type": "add", "field": "attributes.hit", "value": "1"}],
                                                                       'body.level == "error" || body.level == "warn"')},
    {"name": "X_Bodies_BodyFieldWithoutIf", "logSet": "bodies", **single([
        {"type": "move", "from": "body.msg", "to": "attributes.msg", "on_error": "send"}])},
    {"name": "X_Bodies_SeverityFromBody", "logSet": "bodies", **single([
        {"type": "severity_parser", "parse_from": "body.level", "on_error": "send", "overwrite_text": True,
         "if": 'body?.level != nil && ( type(body?.level) == "string" || ( type(body?.level) in ["int", "float"] && body?.level == float(int(body?.level)) ) )'}])},
    {"name": "X_Bodies_RemoveBody", "logSet": "bodies", **single([{"type": "remove", "field": "body", "if": "body != nil"}])},
    {"name": "X_Metadata_AddAttribute", "logSet": "metadata", **single([{"type": "add", "field": "attributes.hit", "value": "1"}],
                                                                       'attributes.route == "yes"')},
    {"name": "X_Metadata_SplitResource", "logSet": "metadata", **single([{"type": "add", "field": "resource.tier", "value": "gold"}],
                                                                        'attributes.route == "yes" && resource["service.name"] == "a"')},
    {"name": "X_Metadata_MoveToResource", "logSet": "metadata", **single([
        {"type": "move", "from": "attributes.route", "to": "resource.route", "if": "attributes.route != nil"}])},
    {"name": "X_Metadata_Untouched", "logSet": "metadata", **single([{"type": "add", "field": "attributes.never", "value": "1"}], "false")},
    {"name": "X_Metadata_TwoPipelines", "logSet": "metadata", "processors": {
        "signozlogspipeline/pipeline_a": pipeline([{"type": "add", "field": "resource.tier", "value": "gold"}], 'attributes.route == "yes"'),
        "signozlogspipeline/pipeline_b": pipeline([{"type": "add", "field": "attributes.b", "value": "EXPR(resource.tier)", "if": "(resource.tier != nil)"}]),
    }, "processorNames": ["signozlogspipeline/pipeline_a", "signozlogspipeline/pipeline_b"]},
    {"name": "X_Order_ServiceListWins", "logSet": "values", "processors": {
        "signozlogspipeline/pipeline_a": pipeline([{"type": "add", "field": "attributes.trail", "value": "EXPR((attributes.trail ?? \"\") + \"a\")"}]),
        "signozlogspipeline/pipeline_b": pipeline([{"type": "add", "field": "attributes.trail", "value": "EXPR((attributes.trail ?? \"\") + \"b\")"}]),
        "signozlogspipeline/pipeline_c": pipeline([{"type": "add", "field": "attributes.trail", "value": "EXPR((attributes.trail ?? \"\") + \"c\")"}]),
    }, "processorNames": ["signozlogspipeline/pipeline_c", "signozlogspipeline/pipeline_a", "signozlogspipeline/pipeline_b"]},
    {"name": "X_Expr_IfRuntimeErrorSend", "logSet": "values", **single([
        {"type": "add", "field": "attributes.r", "value": "1", "if": "attributes.n + 1 > 2", "on_error": "send"}])},
    {"name": "X_Expr_IfRuntimeErrorDrop", "logSet": "values", **single([
        {"type": "add", "field": "attributes.r", "value": "1", "if": "attributes.n + 1 > 2", "on_error": "drop"}])},
    {"name": "X_Expr_IfRuntimeErrorDefault", "logSet": "values", **single([
        {"type": "add", "field": "attributes.r", "value": "1", "if": "attributes.n + 1 > 2"}])},
    {"name": "X_Expr_RouteRuntimeError", "logSet": "values", **single([{"type": "add", "field": "attributes.r", "value": "1"}], "attributes.n + 1 > 2")},
    {"name": "X_Expr_ValueRuntimeError", "logSet": "values", **single([
        {"type": "add", "field": "attributes.r", "value": "EXPR(attributes.n + 1)", "on_error": "drop"}])},
    {"name": "X_Expr_ValueRuntimeErrorSend", "logSet": "values", **single([
        {"type": "add", "field": "attributes.r", "value": "EXPR(attributes.n + 1)", "on_error": "send"}])},
    {"name": "X_Expr_Helpers", "logSet": "values", **single([
        {"type": "add", "field": "attributes.is_json", "value": "EXPR(isJSON(body))"},
        {"type": "add", "field": "attributes.unquoted", "value": "EXPR(unquote(attributes.q))", "if": "attributes.q != nil"},
        {"type": "add", "field": "attributes.dollar", "value": "EXPR($$)"},
        {"type": "add", "field": "attributes.env", "value": "EXPR(env(\"OBSERVE_MARKER\"))"},
        {"type": "add", "field": "attributes.sev_text", "value": "EXPR(severity_text)"},
        {"type": "add", "field": "attributes.sev_num", "value": "EXPR(severity_number)"},
        {"type": "add", "field": "attributes.ts", "value": "EXPR(timestamp)"},
        {"type": "add", "field": "attributes.trace", "value": "EXPR(trace_id)"},
    ])},
    {"name": "X_Expr_BodyMapVariable", "logSet": "values", **single([
        {"type": "add", "field": "attributes.via_map", "value": "EXPR(body_map)"},
        {"type": "add", "field": "attributes.via_dot", "value": "EXPR(body.d)", "if": "body?.d != nil"},
    ])},
    {"name": "X_JSON_Nulls", "logSet": "values", **single([{
        "type": "json_parser", "parse_from": "body", "parse_to": "attributes.parsed", "on_error": "send_quiet"}])},
    {"name": "X_JSON_NullsFlatten", "logSet": "values", **single([{
        "type": "json_parser", "parse_from": "body", "parse_to": "attributes", "on_error": "send_quiet",
        "enable_flattening": True, "max_flattening_depth": 2, "enable_paths": True, "path_prefix": "p"}])},
    {"name": "X_ParsedBodyCache_Leak", "logSet": "bodies", **single([
        {"type": "add", "field": "attributes.lvl", "value": "EXPR(body.level)", "if": "body?.level != nil"},
        {"type": "copy", "from": "attributes", "to": "resource.snapshot", "if": "attributes != nil"},
    ])},
    {"name": "X_ParsedBodyCache_Visible", "logSet": "bodies", **single([
        {"type": "add", "field": "attributes.cache_seen", "value": f'EXPR("{CACHE_KEY}" in attributes)'},
        {"type": "add", "field": "attributes.n_attrs", "value": "EXPR(len(attributes))"},
    ], "body.level != nil")},
    {"name": "X_ParsedBodyCache_Stale", "logSet": "bodies", **single([
        {"type": "add", "field": "attributes.before", "value": "EXPR(body.level)"},
        {"type": "add", "field": "body", "value": '{"level":"changed"}'},
        {"type": "add", "field": "attributes.after", "value": "EXPR(body.level)"},
    ], "body.level != nil")},
    {"name": "X_ParsedBodyCache_AcrossPipelines", "logSet": "bodies", "processors": {
        "signozlogspipeline/pipeline_a": pipeline([
            {"type": "add", "field": "attributes.a", "value": "EXPR(body.level)"},
            {"type": "add", "field": "body", "value": '{"level":"changed"}'},
        ], "body.level != nil"),
        "signozlogspipeline/pipeline_b": pipeline([{"type": "add", "field": "attributes.b", "value": "EXPR(body.level)"}], "body.level != nil"),
    }, "processorNames": ["signozlogspipeline/pipeline_a", "signozlogspipeline/pipeline_b"]},
    {"name": "X_OnlyNoop", "logSet": "values", "processors": {
        "signozlogspipeline/pipeline_x": {"operators": [{"id": "noop", "type": "noop"}]}}, "processorNames": ["signozlogspipeline/pipeline_x"]},
    {"name": "X_IDDefaultsToType", "logSet": "values", "processors": {
        "signozlogspipeline/pipeline_x": {"operators": [{"type": "add", "field": "attributes.a", "value": "1"},
                                                        {"type": "copy", "from": "attributes.a", "to": "attributes.b"}]}},
     "processorNames": ["signozlogspipeline/pipeline_x"]},
    {"name": "X_ImplicitChain", "logSet": "values", "processors": {
        "signozlogspipeline/pipeline_x": {"operators": [{"id": "one", "type": "add", "field": "attributes.a", "value": "1"},
                                                        {"id": "two", "type": "add", "field": "attributes.b", "value": "2"}]}},
     "processorNames": ["signozlogspipeline/pipeline_x"]},
    {"name": "X_RouterNoDefault", "logSet": "values", "processors": {
        "signozlogspipeline/pipeline_x": {"operators": [
            {"id": "r", "type": "router", "routes": [{"expr": "attributes.s == \"text\"", "output": "a"}]},
            {"id": "a", "type": "add", "field": "attributes.routed", "value": "1"}]}},
     "processorNames": ["signozlogspipeline/pipeline_x"]},
    {"name": "X_StanzaIfOnly", "logSet": "values", **single([
        {"type": "add", "field": "attributes.has_s", "value": "1", "if": "attributes.s matches \"^te\""}])},
    {"name": "X_AddNonString", "logSet": "values", "processors": {
        "signozlogspipeline/pipeline_x": {"operators": [
            {"id": "a", "type": "add", "field": "attributes.i", "value": 3},
            {"id": "b", "type": "add", "field": "attributes.m", "value": {"k": "v"}},
            {"id": "c", "type": "add", "field": "attributes.l", "value": [1, "two"]}]}},
     "processorNames": ["signozlogspipeline/pipeline_x"]},
]

# Startup checks beyond the backend's rejected cases.
PROBE = {"id": "op", "type": "add", "field": "attributes.a", "value": "1"}
STARTUP = [
    ("R_UnknownOperatorField", {"operators": [dict(PROBE, foo="bar")]}),
    ("R_UnknownProcessorField", {"operators": [PROBE], "extra": 1}),
    ("R_NoOperatorsKey", {}),
    ("R_EmptyOperators", {"operators": []}),
    ("R_DuplicateID", {"operators": [PROBE, PROBE]}),
    ("R_OutputUnknown", {"operators": [dict(PROBE, output="missing")]}),
    ("R_Cycle", {"operators": [dict(PROBE, id="a", output="b"), dict(PROBE, id="b", output="a")]}),
    ("R_BadIfExpr", {"operators": [dict(PROBE, **{"if": "body =="})]}),
    ("R_BadRouteExpr", {"operators": [{"id": "r", "type": "router", "routes": [{"expr": "body ==", "output": "op"}]}, PROBE]}),
    ("R_BadOnError", {"operators": [dict(PROBE, on_error="bogus")]}),
    ("R_UnknownType_KeyValue", {"operators": [{"id": "kv", "type": "key_value_parser"}]}),
    ("R_UnknownType_Filter", {"operators": [{"id": "f", "type": "filter", "expr": "true"}]}),
    ("R_UnknownType_Flatten", {"operators": [{"id": "f", "type": "flatten", "field": "attributes.a"}]}),
    ("R_UnknownType_Bogus", {"operators": [{"id": "b", "type": "bogus"}]}),
    ("R_NoType", {"operators": [{"id": "b"}]}),
    ("R_SeverityBadMappingKey", {"operators": [{"id": "s", "type": "severity_parser", "parse_from": "attributes.l", "mapping": {"loud": ["x"]}}]}),
    ("R_BadField", {"operators": [dict(PROBE, field="nowhere.a")]}),
    ("R_GrokBadPattern", {"operators": [{"id": "g", "type": "grok_parser", "parse_from": "body", "pattern": "%{NOSUCHPATTERN:x}"}]}),
    ("R_GrokNoPattern", {"operators": [{"id": "g", "type": "grok_parser", "parse_from": "body"}]}),
    ("R_JSONBadDepthType", {"operators": [{"id": "j", "type": "json_parser", "max_flattening_depth": "deep"}]}),
    ("R_TimeBadLayoutType", {"operators": [{"id": "t", "type": "time_parser", "parse_from": "attributes.t", "layout_type": "moment", "layout": "x"}]}),
    ("R_AddNoValue", {"operators": [{"id": "a", "type": "add", "field": "attributes.a"}]}),
    ("R_AddBadExpr", {"operators": [{"id": "a", "type": "add", "field": "attributes.a", "value": "EXPR(1 +)"}]}),
    ("R_UnknownTraceField", {"operators": [{"id": "t", "type": "trace_parser", "trace_id": {"parse_from": "attributes.t", "x": 1}}]}),
    ("R_OK_Probe", {"operators": [PROBE]}),
]


def main():
    golden_path, out_path = sys.argv[1], sys.argv[2]
    with open(golden_path, encoding="utf-8") as f:
        golden = json.load(f)
    inputs = dict(golden["inputs"])
    inputs.update(EXTRA_INPUTS)
    cases, startup = [], []
    for case in golden["cases"]:
        collector = case.get("collector")
        if not collector:
            continue
        entry = {"name": case["name"], "origin": "backend", "processors": collector["processors"],
                 "processorNames": collector["processorNames"]}
        if collector.get("rejected"):
            startup.append(entry)
        else:
            cases.append(dict(entry, logSet=case["logSet"]))
    for case in EXTRA_CASES + body_field_cases():
        cases.append(dict(case, origin="hand-written"))
    for name, processor in STARTUP:
        startup.append({"name": name, "origin": "hand-written", "processors": {"signozlogspipeline/pipeline_x": processor},
                        "processorNames": ["signozlogspipeline/pipeline_x"]})
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"inputs": inputs, "cases": cases, "startup": startup}, f, indent=1, sort_keys=True)
        f.write("\n")
    print(f"{len(cases)} cases, {len(startup)} startup checks")


if __name__ == "__main__":
    main()
