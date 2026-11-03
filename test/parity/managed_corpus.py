"""Corpus for the processors the FuseScope UI manages, appended to the parity corpus.

Usage: python3 managed_corpus.py <t0_unix_nanos> <corpus_dir>

Adds resources to <corpus_dir>/traces.json and logs.json, written by
test/observe/corpus.py, so they travel in the same requests. Each record names
the rule of managed-processors in the collector configs it exercises. Like the
base corpus, everything but the timestamps is fixed.

Kept out on purpose, as both collectors differ there by design:
- env() in a pipeline expression: refused here (signozlogspipeline spec §4.1);
- map-valued attributes that flatten under signoz.gen_ai.: stripped here, kept by the
  reference (signozllmpricing spec §5);
- configurations only this collector refuses (signozlogspipeline spec §1.2): the
  backend never generates them, and the reference would not start with them here.
"""

import datetime
import importlib.util
import json
import os
import sys

_spec = importlib.util.spec_from_file_location(
    "corpus", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "observe", "corpus.py"))
corpus = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(corpus)
attrs, kv, span, t, MS = corpus.attrs, corpus.kv, corpus.span, corpus.t, corpus.MS

SCOPE = {"name": "parity/managed", "version": "1.0.0", "attributes": attrs({"scope.kept": "no"})}
TRACE_LLM = "6f1d2c3b4a5968778695a4b3c2d1e0f1"
TRACE_BILL = "7a6b5c4d3e2f10293847566574839201"
TRACE_LOG = "8b7c6d5e4f3021304958677685940312"


def ids(base):
    return iter(f"{i:016x}" for i in range(base, base + 64))


def llm_spans(t0):
    """gen_ai.* mapping (signozspanmapper, shipped groups) feeding signozllmpricing."""
    n = ids(0xA11A0000)
    root = next(n)
    return [
        span(TRACE_LLM, root, None, "POST /chat", 2, 0, 900, {
            "http.request.method": "POST", "http.route": "/chat", "http.response.status_code": 200,
        }, t0),
        # OpenInference: gen_ai.llm runs (key "llm.model_name" contains "model"); tokens are
        # copied, input/output.value moved; gpt-4o-mini* wins over gpt-4o* (first match);
        # cache read 200 <= input 1200 is subtracted.
        span(TRACE_LLM, next(n), root, "ChatCompletion", 3, 10, 400, {
            "openinference.span.kind": "LLM",
            "llm.model_name": "gpt-4o-mini-2024-07-18",
            "llm.provider": "openai",
            "llm.token_count.prompt": 1200,
            "llm.token_count.completion": 300,
            "llm.token_count.prompt_details.cache_read": 200,
            "llm.input_messages.0.message.role": "user",
            "input.value": "[{\"role\":\"user\",\"content\":\"hi\"}]",
            "output.value": "hello",
            "session.id": "s-1",
        }, t0, events=[(20, "token", {"llm.model_name": "event-untouched"})]),
        # Vercel AI SDK: higher-priority sources win (ai.model.id over gen_ai.response.model);
        # claude-4-sonnet* is the rule's second pattern; additive cache read and write.
        span(TRACE_LLM, next(n), root, "ai.generateText", 3, 410, 600, {
            "ai.model.id": "claude-4-sonnet-20250514",
            "gen_ai.response.model": "claude-4-sonnet-20250514-r",
            "ai.model.provider": "anthropic",
            "ai.usage.promptTokens": 500,
            "ai.usage.completionTokens": 50,
            "gen_ai.usage.cache_read_input_tokens": 100,
            "gen_ai.usage.cache_creation_input_tokens": 40,
            "ai.prompt.messages": "[{\"role\":\"user\"}]",
            "ai.response.text": "ok",
            "ai.response.finishReason": "stop",
        }, t0),
        # Native gen_ai.*: the targets exist, so nothing is mapped (R2); a double input count;
        # a planted flat cost is removed, then replaced by the computed one.
        span(TRACE_LLM, next(n), root, "gemini", 3, 610, 700, {
            "gen_ai.request.model": "gemini-2.5-flash-001",
            "gen_ai.system": "gcp.gemini",
            "gen_ai.usage.input_tokens": 1000.0,
            "gen_ai.usage.output_tokens": 2000,
            "signoz.gen_ai.usage.tokens.cost": 999.0,
            "signoz.gen_ai.custom": "planted",
            "signoz.other": "kept",
        }, t0),
        # A model no rule prices: the backend drops llama-* for its negative price.
        span(TRACE_LLM, next(n), root, "llama", 3, 701, 720, {
            "model": "llama-3-70b-instruct",
            "input_tokens": 10,
            "output_tokens": 20,
            "signoz.gen_ai.usage.input_tokens.cost": 1.0,
        }, t0),
        # Priced model, all counts zero: no cost.
        span(TRACE_LLM, next(n), root, "zero tokens", 3, 721, 730, {
            "gen_ai.request.model": "gpt-4o-2024-08-06",
            "gen_ai.usage.input_tokens": 0,
        }, t0),
        # Cache read + write above input: the input is not reduced.
        span(TRACE_LLM, next(n), root, "cache above input", 3, 731, 740, {
            "gen_ai.request.model": "gpt-4o-2024-08-06",
            "gen_ai.usage.input_tokens": 100,
            "gen_ai.usage.output_tokens": 7,
            "gen_ai.usage.input_tokens.cached": 80,
            "cache_creation_tokens": 40,
        }, t0),
        # A rule without cache mode (the backend's "unknown"), matched by a ? glob.
        span(TRACE_LLM, next(n), root, "mistral", 3, 741, 750, {
            "llm.request.model": "mistral-large-2.1",
            "llm.usage.prompt_tokens": 2500,
            "llm.usage.completion_tokens": 125,
            "gen_ai.usage.cache_read_input_tokens": 500,
        }, t0),
        # A model string with a / never matches * (path.Match).
        span(TRACE_LLM, next(n), root, "routed model", 3, 751, 760, {
            "gen_ai.request.model": "gpt-4o/2024",
            "gen_ai.usage.input_tokens": 10,
        }, t0),
        # gen_ai.agent: "agent" in a key; final_result is moved.
        span(TRACE_LLM, next(n), root, "planner", 1, 761, 800, {
            "openinference.span.kind": "AGENT",
            "agent.name": "planner",
            "agent.description": "plans the answer",
            "final_result": "plan ready",
        }, t0),
        # gen_ai.tool: input.value is copied (moved only by gen_ai.llm, which does not run).
        span(TRACE_LLM, next(n), root, "web_search", 1, 801, 850, {
            "tool.name": "web_search",
            "tool.description": "searches the web",
            "input.value": "q=parity",
            "output.value": "3 results",
            "traceloop.span.kind": "tool",
        }, t0),
    ]


def billing_spans(t0):
    """The user group: conditions on span and resource keys, moves, resource targets."""
    n = ids(0xB1110000)
    root = next(n)
    return [
        # billing.tenant wins (priority 20) and moves; cost center moves into the resource.
        span(TRACE_BILL, root, None, "charge", 2, 0, 50, {
            "billing.tenant": "t-1",
            "billing.cost_center": "cc-42",
            "customer": "disabled source",
        }, t0),
        # Only the resource condition ("tenant" in a resource key): tenant.id from the
        # resource; the cost center is already in the shared resource (rule R2).
        span(TRACE_BILL, next(n), root, "refund", 1, 60, 80, {"refund.id": "r-9"}, t0),
    ]


def traces(t0):
    return [
        {"resource": {"attributes": attrs({
            "service.name": "chat-api",
            "deployment.environment": "staging",
            "telemetry.sdk.language": "python",
            # Resource attributes under signoz.gen_ai. are removed too.
            "signoz.gen_ai.usage.tokens.cost": 5.0,
        })}, "scopeSpans": [{"scope": SCOPE, "spans": llm_spans(t0)}]},
        {"resource": {"attributes": attrs({"service.name": "billing", "tenant": "acme", "host.name": "node-10"})},
         "scopeSpans": [{"scope": SCOPE, "spans": billing_spans(t0)}]},
    ]


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

    when = datetime.datetime.fromtimestamp(t0 // 1_000_000_000, datetime.timezone(datetime.timedelta(hours=-3)))
    httpdate = when.strftime("%d/%b/%Y:%H:%M:%S %z")
    epoch_s = t0 // 1_000_000_000
    epoch_ms = t0 // MS

    web = [
        # nginx-access: grok, strptime time with zone, severity from 2xx, remove, add, EXPR, copy, move.
        rec(300, 0, None, f'203.0.113.7 - alice [{httpdate}] "GET /api/cart HTTP/1.1" 200 512 '
            '"https://shop.example.com/" "Mozilla/5.0"', {"log.iostream": "stdout"}),
        rec(301, 0, None, f'198.51.100.2 - - [{httpdate}] "POST /api/login HTTP/2.0" 404 0 "-" "curl/8.5.0"', {}),
        rec(302, 9, "INFO", f'192.0.2.1 - bob [{httpdate}] "DELETE /api/cart/1 HTTP/1.1" 503 17 "-" "k6"', {}),
        # The filter matches, grok does not: an empty map is merged, later operators skip on their if.
        rec(303, 9, "INFO", "malformed HTTP/ line", {}),
        # The filter rejects (no "HTTP/"): unchanged but rebuilt (scope version and attributes dropped).
        rec(304, 13, "WARN", "upstream timed out", {}),
    ]
    orders = [
        # orders-json: flattening with paths, mapped severity/trace/span/message/service/env/host,
        # $-escaped value, regex on the moved message, epoch ms.
        rec(310, 0, None, json.dumps({
            "level": "warn", "msg": "order 1001 shipped", "trace_id": TRACE_LOG, "span_id": "0102030405060708",
            "service": "orders-worker", "env": "production", "host": "node-8", "ts_ms": str(epoch_ms + 5),
            "http": {"status": 200, "path": "/ship"}, "amount": 12.5, "note": None,
        }), {}),
        # A map body is parsed as it is; lvl ERR maps to error; message is not an order line.
        rec(311, 0, None, {"lvl": "ERR", "message": "payment declined", "retry": True}, {}),
        # Not JSON: json_parser fails quietly, the record goes on.
        rec(312, 9, "INFO", "plain text from orders", {}),
        # Both message keys: msg is moved last and wins; the record also enters audit (attribute
        # audit), whose body.user check no longer sees the JSON body.
        rec(313, 0, None, json.dumps({"level": "info", "message": "first", "msg": "order 1003 created",
                                      "user": "u-19"}), {"audit": "no"}),
    ]
    orders_dev = [rec(314, 9, "INFO", json.dumps({"level": "error", "msg": "dev only"}), {})]
    legacy = [
        # payments-legacy-: regex, epoch s.ms, severity mapping with overwrite, trace_parser,
        # move to body, remove, copy from the resource.
        rec(320, 0, None, f"{epoch_s}.123 WARNING [pool-1] trace={TRACE_LOG} span=1112131415161718 card declined code=51",
            {"log.type": "legacy"}),
        rec(321, 0, "orig", f"{epoch_s}.456 CRIT [main] trace={TRACE_LOG} span=2122232425262728 db down",
            {"log.type": "legacy"}),
        rec(322, 9, "INFO", "free text", {"log.type": "legacy"}),
    ]
    audit = [
        # audit: an "exists" filter on an attribute and on a body key, JSON string or map body.
        rec(330, 9, "INFO", json.dumps({"user": "u-17", "action": "delete"}), {"audit": "YES"}),
        rec(331, 9, "INFO", {"user": "u-18", "action": "read"}, {"audit": "yes"}),
        rec(332, 9, "INFO", json.dumps({"action": "noop"}), {"audit": "yes"}),
    ]
    return [
        {"resource": {"attributes": attrs({"service.name": "web", "host.name": "node-7"})},
         "scopeLogs": [{"scope": SCOPE, "logRecords": web}]},
        {"resource": {"attributes": attrs({"service.name": "orders", "deployment.environment": "prod"})},
         "scopeLogs": [{"scope": SCOPE, "logRecords": orders}]},
        {"resource": {"attributes": attrs({"service.name": "orders", "deployment.environment": "dev"})},
         "scopeLogs": [{"scope": SCOPE, "logRecords": orders_dev}]},
        {"resource": {"attributes": attrs({"service.name": "payments", "host.name": "node-9"})},
         "scopeLogs": [{"scope": SCOPE, "logRecords": legacy + audit}]},
    ]


def main():
    t0, out = int(sys.argv[1]), sys.argv[2]
    for name, key, build in (("traces", "resourceSpans", traces), ("logs", "resourceLogs", logs)):
        path = os.path.join(out, f"{name}.json")
        with open(path) as f:
            data = json.load(f)
        data[key] += build(t0)
        with open(path, "w") as f:
            json.dump(data, f, indent=2, sort_keys=True)
            f.write("\n")


if __name__ == "__main__":
    main()
