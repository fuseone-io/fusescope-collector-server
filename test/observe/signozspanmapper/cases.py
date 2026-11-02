"""Deterministic cases for observing signozspanmapper as a black box.

Usage:
  python3 -I cases.py generate <out_dir>   writes cases.json, collector.yaml, startup/*.yaml
  python3 -I cases.py collect <out_dir>    reads out/<case>.json, writes outputs.json
  python3 -I cases.py startup-line <name> <result> <log file>

Accepted cases run in one collector, one pipeline per case, isolated by a
resource marker the pipeline drops before the mapper. Startup cases each run in
a collector of their own to see whether the reference accepts the config.
"""

import base64
import json
import os
import sys

MARKER = "observe.case"
HERE = os.path.dirname(os.path.abspath(__file__))


def value(v):
    if isinstance(v, bool):
        return {"boolValue": v}
    if isinstance(v, int):
        return {"intValue": str(v)}
    if isinstance(v, float):
        return {"doubleValue": v}
    if isinstance(v, list):
        return {"arrayValue": {"values": [value(x) for x in v]}}
    if isinstance(v, dict):
        return {"kvlistValue": {"values": kvs(v)}}
    if isinstance(v, bytes):
        return {"bytesValue": base64.b64encode(v).decode()}
    if v is None:
        return {}
    return {"stringValue": v}


def kvs(d):
    return [{"key": k, "value": value(v)} for k, v in d.items()]


def request(case, resources):
    """resources: [(resource_attrs, [[span, ...] per scope])]; span is attrs or (attrs, event_attrs)."""
    n = 0
    out = []
    for r, (res, scopes) in enumerate(resources):
        scope_spans = []
        for s, spans in enumerate(scopes):
            items = []
            for span in spans:
                attrs, events = (span if isinstance(span, tuple) else (span, None))
                item = {
                    "traceId": f"{n + 1:032x}",
                    "spanId": f"{n + 1:016x}",
                    "name": f"{case}/{n}",
                    "kind": 1,
                    "startTimeUnixNano": "1700000000000000000",
                    "endTimeUnixNano": "1700000001000000000",
                    "attributes": kvs(attrs),
                }
                if events is not None:
                    item["events"] = [{"name": "e", "timeUnixNano": "1700000000500000000", "attributes": kvs(events)}]
                items.append(item)
                n += 1
            scope_spans.append({"scope": {"name": f"scope{s}"}, "spans": items})
        out.append({"resource": {"attributes": kvs(res)}, "scopeSpans": scope_spans})
    return {"resourceSpans": out}


def group(gid, attrs=None, res=None, rules=()):
    exists = {}
    if attrs is not None:
        exists["attributes"] = attrs
    if res is not None:
        exists["resource"] = res
    return {"id": gid, "exists_any": exists, "attributes": list(rules)}


def rule(target, *sources, context="attribute"):
    r = {"target": target}
    if context is not None:
        r["context"] = context
    r["sources"] = [{"key": s} if isinstance(s, str) else {"key": s[0], "action": s[1]} for s in sources]
    return r


def cfg(*groups):
    return {"groups": list(groups)}


# Spans reused by several cases: one resource per span, as the backend preview builds them.
GENERIC = [
    ({"service.name": "svc", "res.a": "RA"}, [[{"src.a": "A", "src.b": "B", "other": "o"}]]),
    ({"res.a": "RA2"}, [[{"src.b": "B only"}]]),
    ({}, [[{"src.a": "A2", "target": "existing"}]]),
    ({"res.match": "m", "res.a": "RA3"}, [[{"unrelated": "u"}]]),
    ({}, [[{}]]),
    ({"target": "res-existing"}, [[{"SRC.A": "upper", "xsrc.a": "prefixed"}]]),
]

ACCEPTED = []


def case(name, config, resources):
    ACCEPTED.append({"name": name, "config": config, "input": request(name, resources)})


# Sources, priority and actions.
case("copy_single", cfg(group("g", ["src"], rules=[rule("target", "src.a")])), GENERIC)
case("move_single", cfg(group("g", ["src"], rules=[rule("target", ("src.a", "move"))])), GENERIC)
case("priority_first_present", cfg(group("g", ["src"], rules=[rule("target", "src.missing", "src.b", "src.a")])), GENERIC)
case("priority_move_only_winner", cfg(group("g", ["src"], rules=[rule("target", ("src.a", "move"), ("src.b", "move"))])), GENERIC)
case("action_copy_explicit", cfg(group("g", ["src"], rules=[rule("target", ("src.a", "copy"))])), GENERIC)
case("source_from_resource", cfg(group("g", ["src", "res"], rules=[rule("target", "resource.res.a")])), GENERIC)
case("source_from_resource_move", cfg(group("g", ["src", "res"], rules=[rule("target", ("resource.res.a", "move"))])), GENERIC)
case("source_resource_prefix_literal", cfg(group("g", ["resource."], rules=[rule("target", "resource.x")])), [
    ({}, [[{"resource.x": "attr-literal"}]]),
    ({"x": "from-resource"}, [[{"resource.x": "attr-literal"}]]),
])
case("source_empty_resource_key", cfg(group("g", ["k"], rules=[rule("target", "resource.", "k")])), [
    ({"": "empty-key"}, [[{"k": "v"}]]),
    ({}, [[{"k": "v"}]]),
])

# Target context.
case("target_resource", cfg(group("g", ["src"], rules=[rule("target", "src.a", context="resource")])), GENERIC)
case("target_resource_move", cfg(group("g", ["src"], rules=[rule("target", ("src.a", "move"), context="resource")])), GENERIC)
case("target_resource_from_resource", cfg(group("g", ["src", "res"], rules=[rule("target", "resource.res.a", context="resource")])), GENERIC)
case("target_context_omitted", cfg(group("g", ["src"], rules=[rule("target", "src.a", context=None)])), GENERIC)
case("target_context_empty", cfg(group("g", ["src"], rules=[rule("target", "src.a", context="")])), GENERIC)
case("action_empty", cfg(group("g", ["src"], rules=[rule("target", ("src.a", ""))])), GENERIC)
case("group_without_rules", cfg(group("g", ["src"]), group("g2", ["src"], rules=[rule("target", "src.a")])), GENERIC)
case("target_same_as_source", cfg(group("g", ["src"], rules=[rule("src.a", "src.b")])), GENERIC)
case("target_same_as_source_move", cfg(group("g", ["src"], rules=[rule("src.a", ("src.a", "move"))])), GENERIC)
case("target_prefixed_attribute", cfg(group("g", ["src"], rules=[rule("resource.target", "src.a")])), GENERIC)

# Values keep their type; an empty string or empty value is present.
TYPED = [({}, [[{"m.str": "s"}], [{"m.int": 7}], [{"m.dbl": 2.5}], [{"m.bool": False}], [{"m.arr": ["x", 1]}],
                [{"m.map": {"k": "v", "n": {"d": 1}}}], [{"m.bytes": b"\x00\x01"}], [{"m.empty": ""}], [{"m.none": None}]])]
case("values_typed", cfg(group("g", ["m."], rules=[rule("t", "m.str", "m.int", "m.dbl", "m.bool", "m.arr", "m.map", "m.bytes", "m.empty", "m.none")])),
     [({}, [[span] for scope in TYPED[0][1] for span in scope])])
case("values_typed_each", cfg(group("g", ["m."], rules=[
    rule("t.str", "m.str"), rule("t.int", "m.int"), rule("t.dbl", "m.dbl"), rule("t.bool", "m.bool"), rule("t.arr", "m.arr"),
    rule("t.map", "m.map"), rule("t.bytes", "m.bytes"), rule("t.empty", "m.empty"), rule("t.none", ("m.none", "move"))])), [
    ({}, [[{"m.str": "s", "m.int": 7, "m.dbl": 2.5, "m.bool": False, "m.arr": ["x", 1], "m.map": {"k": "v"},
            "m.bytes": b"\x00\x01", "m.empty": "", "m.none": None}]]),
])

# Conditions: substring of a key, never of a value; attributes or resource.
case("condition_resource_only", cfg(group("g", res=["res.m"], rules=[rule("target", "src.a")])), GENERIC + [
    ({"res.match": "m"}, [[{"src.a": "A-res-match"}]]),
])
case("condition_attributes_or_resource", cfg(group("g", ["unrel"], ["res.m"], rules=[rule("target", "resource.res.a")])), GENERIC)
case("condition_case_sensitive", cfg(group("g", ["SRC"], rules=[rule("target", "SRC.A", "src.a")])), GENERIC)
case("condition_inside_key", cfg(group("g", [".a"], rules=[rule("target", "src.a", "xsrc.a")])), GENERIC)
case("condition_value_not_key", cfg(group("g", ["only"], rules=[rule("target", "src.b")])), GENERIC)
case("condition_on_target_created", cfg(
    group("g1", ["src"], rules=[rule("mid.x", ("src.a", "move"))]),
    group("g2", ["mid."], rules=[rule("final", "mid.x")]),
    group("g3", ["src.a"], rules=[rule("after_move", "src.b")])), GENERIC)

# Groups and rules in order, each seeing the changes before it.
case("groups_same_target", cfg(group("g1", ["src"], rules=[rule("target", "src.b")]), group("g2", ["src"], rules=[rule("target", "src.a")])), GENERIC)
case("rules_same_source_copy_then_move", cfg(group("g", ["src"], rules=[rule("t1", "src.a"), rule("t2", ("src.a", "move"))])), GENERIC)
case("rules_same_source_move_then_copy", cfg(group("g", ["src"], rules=[rule("t1", ("src.a", "move")), rule("t2", "src.a")])), GENERIC)
case("rules_chained", cfg(group("g", ["src"], rules=[rule("t1", "src.a"), rule("t2", "t1")])), GENERIC)
case("rules_same_target", cfg(group("g", ["src"], rules=[rule("target", "src.b"), rule("target", "src.a")])), GENERIC)
case("groups_duplicate_id", cfg(group("g", ["src"], rules=[rule("t1", "src.a")]), group("g", ["src"], rules=[rule("t2", "src.b")])), GENERIC)
case("dotted_key_not_nested", cfg(group("g", ["a"], rules=[rule("target", "a.b")])), [
    ({}, [[{"a": {"b": "nested"}}]]),
    ({}, [[{"a.b": "flat", "a": {"b": "nested"}}]]),
])
case("groups_empty", cfg(), GENERIC)

# Several spans, scopes and resources in one request; events are not touched.
SHARED = [
    ({"res.a": "R1", "service.name": "svc"}, [
        [{"src.a": "first"}, {"src.a": "second"}, {"other": "x"}],
        [{"src.a": "third-scope"}],
    ]),
    ({"res.a": "R2"}, [[{"src.a": "second-resource"}, {"src.b": "b"}]]),
]
case("shared_resource_target", cfg(group("g", ["src", "other"], rules=[rule("target", "src.a", "src.b", context="resource")])), SHARED)
case("shared_resource_target_move", cfg(group("g", ["src", "other"], rules=[rule("target", ("src.a", "move"), context="resource")])), SHARED)
case("shared_resource_source_move", cfg(group("g", ["src", "other"], rules=[rule("target", ("resource.res.a", "move"))])), SHARED)
case("shared_resource_condition", cfg(group("g", res=["res.a"], rules=[rule("got", "resource.res.a"), rule("res.a", ("src.a", "move"), context="resource")])), SHARED)
case("shared_resource_condition_current", cfg(
    group("g1", ["src"], rules=[rule("flag.x", "src.a", context="resource")]),
    group("g2", res=["flag."], rules=[rule("seen", "resource.flag.x")])), [
    ({"res.a": "R1"}, [[{"other": "0"}, {"src.a": "one"}, {"other": "2"}], [{"other": "3"}]]),
])
case("shared_resource_condition_removed", cfg(
    group("g1", ["src"], rules=[rule("t", ("resource.res.a", "move"))]),
    group("g2", res=["res.a"], rules=[rule("seen", "other")])), [
    ({"res.a": "R1"}, [[{"src.a": "a", "other": "0"}, {"other": "1"}]]),
    ({"res.a": "R2"}, [[{"other": "2"}, {"src.a": "a", "other": "3"}, {"other": "4"}]]),
])
case("shared_attribute_spans", cfg(group("g", ["src"], rules=[rule("target", "src.a")])), SHARED)
case("events_untouched", cfg(group("g", ["src"], rules=[rule("target", ("src.a", "move"))])), [
    ({}, [[({"src.a": "span"}, {"src.a": "event"}), ({"other": "x"}, {"src.a": "event-only"})]]),
])
case("move_order", cfg(group("g", ["k"], rules=[rule("t", ("k2", "move"))])), [
    ({}, [[{"k1": 1, "k2": 2, "k3": 3, "k4": 4}]]),
])
case("move_order_resource", cfg(group("g", ["k"], rules=[rule("t", ("resource.r2", "move"), context="resource")])), [
    ({"r1": 1, "r2": 2, "r3": 3, "r4": 4}, [[{"k": 0}]]),
])


def system_cases():
    with open(os.path.join(HERE, "system.json"), encoding="utf-8") as f:
        fx = json.load(f)
    for name, docs in sorted(fx["spans"].items()):
        resource_spans = []
        for d in docs:
            resource_spans.extend(d["resourceSpans"])
        for i, rs in enumerate(resource_spans):
            for ss in rs["scopeSpans"]:
                for s in ss["spans"]:
                    s.update({"traceId": f"{i + 1:032x}", "spanId": f"{i + 1:016x}", "name": f"system_{name}/{i}", "kind": 1,
                              "startTimeUnixNano": "1700000000000000000", "endTimeUnixNano": "1700000001000000000"})
        ACCEPTED.append({"name": f"system_{name}", "config": fx["processor"], "input": {"resourceSpans": resource_spans}})


system_cases()

# Configs the reference may accept or refuse at start.
OK_GROUP = group("g", ["src"], rules=[rule("target", "src.a")])
STARTUP = {
    "empty_mapping": {},
    "null": None,
    "groups_empty": {"groups": []},
    "groups_null": {"groups": None},
    "valid": cfg(OK_GROUP),
    "unknown_top_field": {"groups": [OK_GROUP], "extra": 1},
    "unknown_group_field": cfg(dict(OK_GROUP, extra=1)),
    "unknown_exists_any_field": cfg(dict(OK_GROUP, exists_any={"attributes": ["src"], "extra": ["x"]})),
    "unknown_rule_field": cfg(dict(OK_GROUP, attributes=[dict(rule("target", "src.a"), extra=1)])),
    "unknown_source_field": cfg(dict(OK_GROUP, attributes=[{"target": "target", "context": "attribute", "sources": [{"key": "src.a", "extra": 1}]}])),
    "group_id_missing": cfg({k: v for k, v in OK_GROUP.items() if k != "id"}),
    "group_id_empty": cfg(dict(OK_GROUP, id="")),
    "group_id_duplicate": cfg(OK_GROUP, OK_GROUP),
    "exists_any_missing": cfg({k: v for k, v in OK_GROUP.items() if k != "exists_any"}),
    "exists_any_empty": cfg(dict(OK_GROUP, exists_any={})),
    "exists_any_empty_lists": cfg(dict(OK_GROUP, exists_any={"attributes": [], "resource": []})),
    "exists_any_empty_string": cfg(dict(OK_GROUP, exists_any={"attributes": [""]})),
    "exists_any_resource_empty_string": cfg(dict(OK_GROUP, exists_any={"resource": [""]})),
    "exists_any_one_empty_string": cfg(dict(OK_GROUP, exists_any={"attributes": ["src", ""]})),
    "exists_any_resource_only": cfg(dict(OK_GROUP, exists_any={"resource": ["service"]})),
    "rules_missing": cfg({k: v for k, v in OK_GROUP.items() if k != "attributes"}),
    "rules_empty": cfg(dict(OK_GROUP, attributes=[])),
    "target_missing": cfg(dict(OK_GROUP, attributes=[{"context": "attribute", "sources": [{"key": "src.a"}]}])),
    "target_empty": cfg(dict(OK_GROUP, attributes=[rule("", "src.a")])),
    "target_duplicate": cfg(dict(OK_GROUP, attributes=[rule("target", "src.a"), rule("target", "src.b")])),
    "sources_missing": cfg(dict(OK_GROUP, attributes=[{"target": "target", "context": "attribute"}])),
    "sources_empty": cfg(dict(OK_GROUP, attributes=[{"target": "target", "context": "attribute", "sources": []}])),
    "source_key_empty": cfg(dict(OK_GROUP, attributes=[rule("target", "")])),
    "source_key_missing": cfg(dict(OK_GROUP, attributes=[{"target": "target", "context": "attribute", "sources": [{"action": "move"}]}])),
    "source_key_resource_prefix_only": cfg(dict(OK_GROUP, attributes=[rule("target", "resource.")])),
    "context_omitted": cfg(dict(OK_GROUP, attributes=[rule("target", "src.a", context=None)])),
    "context_empty": cfg(dict(OK_GROUP, attributes=[rule("target", "src.a", context="")])),
    "context_span": cfg(dict(OK_GROUP, attributes=[rule("target", "src.a", context="span")])),
    "context_upper": cfg(dict(OK_GROUP, attributes=[rule("target", "src.a", context="Resource")])),
    "action_copy": cfg(dict(OK_GROUP, attributes=[rule("target", ("src.a", "copy"))])),
    "action_empty": cfg(dict(OK_GROUP, attributes=[rule("target", ("src.a", ""))])),
    "action_delete": cfg(dict(OK_GROUP, attributes=[rule("target", ("src.a", "delete"))])),
    "action_upper": cfg(dict(OK_GROUP, attributes=[rule("target", ("src.a", "MOVE"))])),
    "groups_not_list": {"groups": {"id": "g"}},
}


def config_yaml(processors, pipelines):
    """JSON is YAML; the collector reads it as is."""
    return json.dumps({
        "receivers": {"otlp": {"protocols": {"http": {"endpoint": "0.0.0.0:4318"}}}},
        "extensions": {"health_check": {"endpoint": "0.0.0.0:13133"}},
        "processors": processors,
        "exporters": {name: {"path": f"/out/{name.split('/', 1)[1]}.json", "flush_interval": "100ms"}
                      for p in pipelines.values() for name in p["exporters"]},
        "service": {"extensions": ["health_check"], "pipelines": pipelines},
    }, indent=1)


def generate(out):
    for d in ("startup", "requests"):
        os.makedirs(os.path.join(out, d), exist_ok=True)
    processors = {"resource/marker": {"attributes": [{"key": MARKER, "action": "delete"}]}}
    pipelines = {}
    for c in ACCEPTED:
        n = c["name"]
        processors[f"filter/{n}"] = {"error_mode": "ignore", "traces": {"span": [f'resource.attributes["{MARKER}"] != "{n}"']}}
        processors[f"signozspanmapper/{n}"] = c["config"]
        pipelines[f"traces/{n}"] = {"receivers": ["otlp"], "processors": [f"filter/{n}", "resource/marker", f"signozspanmapper/{n}"],
                                    "exporters": [f"file/{n}"]}
    with open(os.path.join(out, "collector.yaml"), "w", encoding="utf-8") as f:
        f.write(config_yaml(processors, pipelines))
    with open(os.path.join(out, "cases.json"), "w", encoding="utf-8") as f:
        json.dump({"accepted": ACCEPTED, "startup": STARTUP}, f, indent=1)
        f.write("\n")
    for c in ACCEPTED:
        marked = json.loads(json.dumps(c["input"]))
        for rs in marked["resourceSpans"]:
            rs.setdefault("resource", {}).setdefault("attributes", []).append({"key": MARKER, "value": {"stringValue": c["name"]}})
        with open(os.path.join(out, "requests", f"{c['name']}.json"), "w", encoding="utf-8") as f:
            json.dump(marked, f)
    for name, conf in STARTUP.items():
        with open(os.path.join(out, "startup", f"{name}.yaml"), "w", encoding="utf-8") as f:
            f.write(config_yaml({"signozspanmapper": conf},
                                {"traces/x": {"receivers": ["otlp"], "processors": ["signozspanmapper"], "exporters": ["file/x"]}}))


def collect(out):
    """The file exporter writes one OTLP/JSON document per line; key order is kept as written."""
    outputs = {}
    for c in ACCEPTED:
        path = os.path.join(out, "out", f"{c['name']}.json")
        resource_spans = []
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        resource_spans.extend(json.loads(line)["resourceSpans"])
        outputs[c["name"]] = {"resourceSpans": resource_spans}
    with open(os.path.join(out, "outputs.json"), "w", encoding="utf-8") as f:
        json.dump(outputs, f, indent=1)
        f.write("\n")
    counts = {n: sum(len(ss["spans"]) for rs in o["resourceSpans"] for ss in rs["scopeSpans"]) for n, o in outputs.items()}
    want = {c["name"]: sum(len(ss["spans"]) for rs in c["input"]["resourceSpans"] for ss in rs["scopeSpans"]) for c in ACCEPTED}
    bad = {n: (counts[n], want[n]) for n in want if counts[n] != want[n]}
    if bad:
        print(f"span counts differ (got, want): {bad}", file=sys.stderr)
        sys.exit(1)


def startup_line(name, result, log):
    """The fatal log entry's error says why the reference refused a config."""
    error = ""
    with open(log, encoding="utf-8", errors="replace") as f:
        for line in f:
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if entry.get("level") == "fatal":
                error = entry.get("error", entry.get("msg", ""))
    print(json.dumps({"name": name, "result": result, "error": error if result == "refused" else ""}))


if __name__ == "__main__":
    if sys.argv[1] == "startup-line":
        startup_line(*sys.argv[2:5])
    else:
        {"generate": generate, "collect": collect}[sys.argv[1]](sys.argv[2])
