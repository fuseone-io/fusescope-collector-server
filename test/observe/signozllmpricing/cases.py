"""Configurations and OTLP/JSON corpus for observing signozllmpricing.

Usage: python3 cases.py <out_dir>

Writes <out_dir>/configs/<case>.yaml, one reference collector configuration per
case (JSON, which YAML reads), and <out_dir>/corpus/traces.json. Every value is
fixed, so two runs send the same input. The processor block of each case is
shaped as the FuseScope backend pushes it (pkg/types/llmpricingruletypes).
"""

import copy
import json
import os
import sys

ATTRS = {
    "model": "gen_ai.request.model",
    "in": "gen_ai.usage.input_tokens",
    "out": "gen_ai.usage.output_tokens",
    "cache_read": "gen_ai.usage.cache_read.input_tokens",
    "cache_write": "gen_ai.usage.cache_creation.input_tokens",
}
OUTPUT_ATTRS = {
    "in": "signoz.gen_ai.usage.input_tokens.cost",
    "out": "signoz.gen_ai.usage.output_tokens.cost",
    "cache_read": "signoz.gen_ai.usage.cache_read.input_tokens.cost",
    "cache_write": "signoz.gen_ai.usage.cache_write.input_tokens.cost",
    "total": "signoz.gen_ai.usage.tokens.cost",
}


def rule(name, pattern, inp, out, cache=None):
    r = {"name": name, "pattern": pattern}
    if cache is not None:
        r["cache"] = cache
    r["in"] = inp
    r["out"] = out
    return r


# Order matters: it is the order the backend lists the rules in.
RULES = [
    rule("gpt-4o", ["gpt-4o*"], 5, 15, {"mode": "subtract", "read": 2.5, "write": 3.75}),
    # Listed after a pattern that also matches its models.
    rule("gpt-4o-mini", ["gpt-4o-mini*"], 0.15, 0.6, {"mode": "subtract", "read": 0.075, "write": 0}),
    # Listed before a pattern that also matches its model.
    rule("o1-mini", ["o1-mini"], 1.1, 4.4),
    rule("o1", ["o1*"], 15, 60),
    rule("claude-3-5-sonnet", ["claude-3-5-sonnet*", "anthropic/claude-3.5-sonnet"], 3, 15,
         {"mode": "additive", "read": 0.3, "write": 3.75}),
    # A cache block without mode (the backend's "unknown").
    rule("mistral-large", ["mistral-*"], 2, 6, {"read": 1, "write": 2}),
    rule("llama", ["llama-?-8b", "meta/*/llama-70b"], 0.1, 0.2),
    rule("gemini", ["gemini-[12].[05]-pro", "gemini-[!3]*-flash"], 1.25, 5),
    rule("literal-star", ["model\\*star"], 7, 7),
    rule("gpt-4o", ["dup-*"], 9, 9),
    rule("free", ["free-*"], 0, 0),
    rule("fp", ["fp-*"], 0.1, 0.3, {"mode": "subtract", "read": 0.07, "write": 0.33}),
    rule("fp-additive", ["fpa-*"], 0.1, 0.3, {"mode": "additive", "read": 0.07, "write": 0.33}),
    rule("numeric", ["42"], 1, 1),
    # One rule per cache mode, with prices that are powers of two.
    rule("mode-subtract", ["ms-*"], 1, 2, {"mode": "subtract", "read": 4, "write": 8}),
    rule("mode-additive", ["ma-*"], 1, 2, {"mode": "additive", "read": 4, "write": 8}),
    rule("mode-none", ["mu-*"], 1, 2, {"read": 4, "write": 8}),
    rule("no-cache", ["mn-*"], 1, 2),
]

# (input, cache read, cache write) for every cache mode; None leaves the attribute out.
CACHE_MATRIX = [
    ("below", 1000, 300, 50),
    ("equal", 350, 300, 50),
    ("read-below-sum-above", 320, 300, 50),
    ("read-equal", 300, 300, None),
    ("read-above", 200, 300, None),
    ("write-above", 40, None, 50),
    ("zero-input", 0, 300, 50),
    ("no-input", None, 300, 50),
    ("negative-read", 1000, -100, None),
    ("double-read", 1000, 300.5, None),
]


def processor(rules=None, attrs=None, output_attrs=None):
    return {
        "attrs": copy.deepcopy(attrs or ATTRS),
        "default_pricing": {"rules": copy.deepcopy(RULES if rules is None else rules)},
        "output_attrs": copy.deepcopy(output_attrs or OUTPUT_ATTRS),
    }


def with_(base, path, value):
    """Copy of base with the dotted path set to value, or removed if value is DELETE."""
    out = copy.deepcopy(base)
    node = out
    keys = path.split(".")
    for k in keys[:-1]:
        node = node[int(k)] if isinstance(node, list) else node[k]
    last = keys[-1]
    if value is DELETE:
        del node[last]
    elif isinstance(node, list):
        node[int(last)] = value
    else:
        node[last] = value
    return out


DELETE = object()
ONE = processor([rule("gpt-4o", ["gpt-4o*"], 5, 15, {"mode": "subtract", "read": 2.5, "write": 3.75})])

# Each case runs the reference on its own; "pipelines" overrides the signals
# the processor is placed in.
CASES = {
    "main": processor(),
    "no-rules": processor([]),
    "custom-attrs": processor(
        RULES[:1],
        {"model": "llm.model", "in": "llm.in", "out": "llm.out", "cache_read": "llm.cr", "cache_write": "llm.cw"},
        {"in": "c.in", "out": "c.out", "cache_read": "c.cr", "cache_write": "c.cw", "total": "c.total"},
    ),
    "empty-output-attrs": with_(with_(ONE, "output_attrs.cache_read", ""), "output_attrs.total", ""),
    "empty-attrs-model": with_(ONE, "attrs.model", ""),
    "empty-attrs-cache": with_(with_(ONE, "attrs.cache_read", ""), "attrs.cache_write", ""),
    "no-output-attrs": with_(ONE, "output_attrs", DELETE),
    "no-attrs": with_(ONE, "attrs", DELETE),
    "no-default-pricing": with_(ONE, "default_pricing", DELETE),
    "null-config": None,
    "unknown-field": with_(ONE, "default_pricing.rules.0.unit", "per_million_tokens"),
    "unknown-top-field": with_(ONE, "extra", True),
    "negative-in": with_(ONE, "default_pricing.rules.0.in", -1),
    "negative-cache-read": with_(ONE, "default_pricing.rules.0.cache.read", -1),
    "no-pattern": with_(ONE, "default_pricing.rules.0.pattern", DELETE),
    "empty-pattern-list": with_(ONE, "default_pricing.rules.0.pattern", []),
    "empty-pattern": with_(ONE, "default_pricing.rules.0.pattern", ["gpt-4o*", ""]),
    "bad-glob": with_(ONE, "default_pricing.rules.0.pattern", ["gpt-["]),
    "bad-cache-mode": with_(ONE, "default_pricing.rules.0.cache.mode", "foo"),
    "uppercase-cache-mode": with_(ONE, "default_pricing.rules.0.cache.mode", "Subtract"),
    "empty-cache-mode": with_(ONE, "default_pricing.rules.0.cache.mode", ""),
    "empty-cache-block": with_(ONE, "default_pricing.rules.0.cache", {}),
    "no-rule-name": with_(ONE, "default_pricing.rules.0.name", DELETE),
    "no-rule-out": with_(ONE, "default_pricing.rules.0.out", DELETE),
    "string-price": with_(ONE, "default_pricing.rules.0.in", "5"),
    "empty-output-cache-read": with_(ONE, "output_attrs.cache_read", ""),
    "empty-output-in": with_(ONE, "output_attrs.in", ""),
    "empty-attrs-in": with_(ONE, "attrs.in", ""),
    "empty-attrs-out": with_(ONE, "attrs.out", ""),
    "control-no-processor": {"pipelines": ["traces"], "processors": []},
    "logs-pipeline": {"pipelines": ["logs"], "config": ONE},
    "metrics-pipeline": {"pipelines": ["metrics"], "config": ONE},
}


def collector_config(case):
    pipelines, processors, cfg = ["traces"], ["signozllmpricing"], case
    if isinstance(case, dict) and "pipelines" in case:
        pipelines, processors, cfg = case["pipelines"], case.get("processors", processors), case.get("config", ONE)
    return {
        "receivers": {"otlp": {"protocols": {"http": {"endpoint": "0.0.0.0:4318"}}}},
        "processors": {"signozllmpricing": cfg} if processors else {},
        "exporters": {"file": {"path": "/out/output.jsonl"}},
        "extensions": {"health_check": {"endpoint": "0.0.0.0:13133"}},
        "service": {
            "extensions": ["health_check"],
            "pipelines": {
                p: {"receivers": ["otlp"], "processors": processors, "exporters": ["file"]}
                for p in pipelines
            },
        },
    }


def kv(key, value):
    if isinstance(value, bool):
        return {"key": key, "value": {"boolValue": value}}
    if isinstance(value, int):
        return {"key": key, "value": {"intValue": str(value)}}
    if isinstance(value, float):
        # OTLP/JSON carries NaN and infinities as strings.
        if value != value or value in (float("inf"), float("-inf")):
            return {"key": key, "value": {"doubleValue": {"inf": "Infinity", "-inf": "-Infinity"}.get(str(value), "NaN")}}
        return {"key": key, "value": {"doubleValue": value}}
    if isinstance(value, dict):
        return {"key": key, "value": {"kvlistValue": {"values": [kv(k, v) for k, v in value.items()]}}}
    return {"key": key, "value": {"stringValue": value}}


M, IN, OUT, CR, CW = (ATTRS[k] for k in ("model", "in", "out", "cache_read", "cache_write"))


def usage(model, inp=None, out=None, cr=None, cw=None, **extra):
    d = {}
    if model is not None:
        d[M] = model
    for k, v in ((IN, inp), (OUT, out), (CR, cr), (CW, cw)):
        if v is not None:
            d[k] = v
    d.update(extra)
    return d


# (span name, span attributes); the name says what the case probes.
SPANS = [
    ("subtract", usage("gpt-4o", 1000, 200, 300, 50)),
    ("subtract-no-cache-tokens", usage("gpt-4o", 1000, 200)),
    ("subtract-read-only", usage("gpt-4o", 1000, 200, 300)),
    ("subtract-write-only", usage("gpt-4o", 1000, 200, None, 50)),
    ("subtract-cache-exceeds-input", usage("gpt-4o", 100, 10, 300, 50)),
    ("additive", usage("claude-3-5-sonnet-20241022", 1000, 200, 300, 50)),
    ("additive-second-pattern", usage("anthropic/claude-3.5-sonnet", 1000, 200, 300, 50)),
    ("cache-without-mode", usage("mistral-large", 1000, 200, 300, 50)),
    ("rule-without-cache", usage("llama-3-8b", 1000, 200, 300, 50)),
    ("overlap-general-first", usage("gpt-4o-mini-2024-07-18", 1000, 200, 300, 50)),
    ("overlap-specific-first", usage("o1-mini", 1000, 200)),
    ("overlap-general", usage("o1-preview", 1000, 200)),
    ("glob-question-two-chars", usage("llama-31-8b", 1000, 200)),
    ("glob-star-one-segment", usage("meta/x/llama-70b", 1000, 200)),
    ("glob-star-two-segments", usage("meta/x/y/llama-70b", 1000, 200)),
    ("glob-anchored", usage("openai/gpt-4o", 1000, 200)),
    ("glob-class", usage("gemini-1.5-pro", 1000, 200)),
    ("glob-class-miss", usage("gemini-3.0-pro", 1000, 200)),
    ("glob-negated-class", usage("gemini-2.5-flash", 1000, 200)),
    ("glob-negated-class-miss", usage("gemini-3-flash", 1000, 200)),
    ("glob-escaped-star", usage("model*star", 1000, 200)),
    ("glob-escaped-star-miss", usage("modelXstar", 1000, 200)),
    ("case-sensitive", usage("GPT-4O", 1000, 200)),
    ("leading-space", usage(" gpt-4o", 1000, 200)),
    ("trailing-space", usage("gpt-4o ", 1000, 200)),
    ("duplicate-rule-name", usage("dup-1", 1000, 200)),
    ("zero-price", usage("free-1", 1000, 200)),
    ("unknown-model", usage("foo-model", 1000, 200)),
    ("empty-model", usage("", 1000, 200)),
    ("int-model", usage(42, 1000, 200)),
    ("no-model", usage(None, 1000, 200)),
    ("no-tokens", usage("gpt-4o")),
    ("output-only", usage("gpt-4o", None, 200)),
    ("input-only", usage("gpt-4o", 1000)),
    ("zero-tokens", usage("gpt-4o", 0, 0, 0, 0)),
    ("double-tokens", usage("gpt-4o", 1000.0, 200.5, 300.25, 50.75)),
    ("string-tokens", usage("gpt-4o", "1000", "200", "300", "50")),
    ("bad-string-tokens", usage("gpt-4o", "many", "200")),
    ("negative-tokens", usage("gpt-4o", -1000, -200, -300, -50)),
    ("bool-tokens", usage("gpt-4o", True, True)),
    ("huge-tokens", usage("gpt-4o", 9007199254740993, 1)),
    ("fp-subtract", usage("fp-1", 3, 7, 1, 2)),
    ("fp-subtract-large", usage("fp-2", 123457, 98765, 11111, 2222)),
    ("fp-additive", usage("fpa-1", 3, 7, 1, 2)),
    ("fp-additive-large", usage("fpa-2", 123457, 98765, 11111, 2222)),
    ("fp-sum", usage("fpa-3", 1, 1, 1, 1)),
    ("existing-cost", usage("gpt-4o", 1000, 200, **{
        OUTPUT_ATTRS["total"]: 99.0, OUTPUT_ATTRS["in"]: "x", OUTPUT_ATTRS["cache_read"]: 7})),
    ("existing-cost-unknown-model", usage("foo-model", 1000, 200, **{OUTPUT_ATTRS["total"]: 99.0})),
    ("response-model-only", {"gen_ai.response.model": "gpt-4o", IN: 1000, OUT: 200}),
    ("request-and-response-model", usage("foo-model", 1000, 200, **{"gen_ai.response.model": "gpt-4o"})),
    ("legacy-token-names", usage("gpt-4o", **{"gen_ai.usage.prompt_tokens": 1000, "gen_ai.usage.completion_tokens": 200})),
    ("custom-attr-names", {"llm.model": "gpt-4o", "llm.in": 1000, "llm.out": 200, "llm.cr": 300, "llm.cw": 50}),
    ("not-gen-ai", {"http.method": "GET"}),
    # Which token values make a span priced.
    ("gate-zero-in-out-with-cache", usage("gpt-4o", 0, 0, 300)),
    ("gate-cancelling", usage("gpt-4o", -5, 5)),
    ("gate-negative-out", usage("gpt-4o", 0, -1)),
    ("gate-cache-only", usage("gpt-4o", None, None, 300, 50)),
    ("gate-string-in-int-out", usage("gpt-4o", "1000", 200)),
    ("gate-double-zero", usage("gpt-4o", 0.0, 0.0)),
    ("gate-fraction", usage("gpt-4o", 0.5, 0)),
    # Which existing cost attributes are removed from a span the processor does not price.
    ("removal-no-model", {"http.method": "GET", OUTPUT_ATTRS["total"]: 99.0}),
    ("removal-no-tokens", usage("gpt-4o", **{OUTPUT_ATTRS["total"]: 99.0})),
    ("removal-zero-tokens", usage("gpt-4o", 0, 0, **{OUTPUT_ATTRS["total"]: 99.0})),
    ("removal-int-model", usage(42, 1000, 200, **{OUTPUT_ATTRS["total"]: 99.0})),
    ("removal-other-keys", usage("foo-model", 1000, 200, **{
        OUTPUT_ATTRS["cache_write"]: 5.0, OUTPUT_ATTRS["out"]: 5.0, "signoz.gen_ai.other": 1.0,
        "x.cost": 1.0, "c.total": 1.0, "c.in": 1.0})),
    ("removal-prefixes", usage("foo-model", 1000, 200, **{
        "signoz.other": 1.0, "signoz.gen_ai": 1.0, "signoz.gen_aix": 1.0, "Signoz.gen_ai.x": 1.0,
        "signoz.gen_ai.usage.input_tokens": 1.0, "gen_ai.usage.cost": 1.0})),
    # Maps whose flattened keys (as the ClickHouse exporter writes them) fall under the prefix.
    ("removal-nested-maps", usage("foo-model", 1000, 200, **{
        "signoz.gen_ai": {"usage.tokens.cost": 999.0, "x": {"y": 1.0}},
        "signoz": {"gen_ai.usage.input_tokens.cost": 999.0, "gen_ai": {"z": 1.0}, "other": 1.0},
        "other": {"signoz.gen_ai.w": 1.0}})),
    ("removal-on-priced-span",usage("gpt-4o", 1000, 200, **{"signoz.gen_ai.other": 1.0, "signoz.other": 1.0})),
    ("nan-tokens", usage("gpt-4o", float("nan"), 200)),
    ("infinite-tokens", usage("gpt-4o", 1000, float("inf"))),
] + [
    (f"cache-{mode}-{case}", usage(f"{prefix}-model", inp, 1000, cr, cw))
    for mode, prefix in (("subtract", "ms"), ("additive", "ma"), ("none", "mu"), ("no-block", "mn"))
    for case, inp, cr, cw in CACHE_MATRIX
]

TRACE = "5d1c0e3a7b9f4e2d8c6a4b2e0f1d3c5a"


def span(i, name, attributes):
    return {
        "traceId": TRACE,
        "spanId": f"{i + 1:016x}",
        "name": name,
        "kind": 3,
        "startTimeUnixNano": "1767225600000000000",
        "endTimeUnixNano": "1767225601000000000",
        "attributes": [kv(k, v) for k, v in attributes.items()],
    }


def corpus():
    spans = [span(i, name, a) for i, (name, a) in enumerate(SPANS)]
    resource_model = span(len(spans), "model-on-resource", {})
    # Cost-like attributes on an event and a link of a priced span.
    nested = span(len(spans) + 1, "removal-events-links", usage("gpt-4o", 1000, 200))
    nested["events"] = [{"timeUnixNano": "1767225600500000000", "name": "e",
                         "attributes": [kv("signoz.gen_ai.e", 1.0), kv(OUTPUT_ATTRS["total"], 1.0)]}]
    nested["links"] = [{"traceId": TRACE, "spanId": f"{1:016x}", "attributes": [kv("signoz.gen_ai.l", 1.0)]}]
    return {"resourceSpans": [
        {
            "resource": {"attributes": [kv("service.name", "llm-app")]},
            "scopeSpans": [{"scope": {"name": "observe"}, "spans": spans}],
        },
        # Model and tokens only on the resource.
        {
            "resource": {"attributes": [kv(k, v) for k, v in
                                        {"service.name": "llm-app-2", "signoz.gen_ai.x": 1.0, **usage("gpt-4o", 1000, 200)}.items()]},
            "scopeSpans": [{"scope": {"name": "observe", "attributes": [kv("signoz.gen_ai.s", 1.0)]},
                            "spans": [resource_model, nested]}],
        },
    ]}


def main(out):
    os.makedirs(os.path.join(out, "configs"), exist_ok=True)
    os.makedirs(os.path.join(out, "corpus"), exist_ok=True)
    for name, case in CASES.items():
        with open(os.path.join(out, "configs", name + ".yaml"), "w") as f:
            json.dump(collector_config(case), f, indent=2)
            f.write("\n")
    with open(os.path.join(out, "corpus", "traces.json"), "w") as f:
        json.dump(corpus(), f, indent=1)
        f.write("\n")
    # The logs and metrics cases get a log record and a metric carrying the same attributes.
    log = {"resourceLogs": [{"resource": {"attributes": [kv("service.name", "llm-app")]}, "scopeLogs": [{"scope": {"name": "observe"}, "logRecords": [
        {"timeUnixNano": "1767225600000000000", "body": {"stringValue": "llm call"},
         "attributes": [kv(k, v) for k, v in usage("gpt-4o", 1000, 200, 300, 50).items()]}]}]}]}
    metric = {"resourceMetrics": [{"resource": {"attributes": [kv("service.name", "llm-app")]}, "scopeMetrics": [{"scope": {"name": "observe"}, "metrics": [
        {"name": "llm.calls", "sum": {"aggregationTemporality": 1, "isMonotonic": True, "dataPoints": [
            {"timeUnixNano": "1767225600000000000", "asInt": "1",
             "attributes": [kv(k, v) for k, v in usage("gpt-4o", 1000, 200, 300, 50).items()]}]}}]}]}]}
    for name, doc in (("logs", log), ("metrics", metric)):
        with open(os.path.join(out, "corpus", name + ".json"), "w") as f:
            json.dump(doc, f, indent=1)
            f.write("\n")


if __name__ == "__main__":
    main(sys.argv[1])
