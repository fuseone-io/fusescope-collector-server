import json, time
now = str(time.time_ns())
def span(svc, name, attrs, sid):
    return {"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":svc}}]},
      "scopeSpans":[{"spans":[{"traceId":"5b8efff798038103d269b633813fc60d","spanId":sid,"name":name,"kind":1,
      "startTimeUnixNano":now,"endTimeUnixNano":now,"attributes":attrs}]}]}]}
s = lambda k,v: {"key":k,"value":{"stringValue":v}}
i = lambda k,v: {"key":k,"value":{"intValue":str(v)}}
probes = {
 "ssize10":   span("obs-ssize-10",   "x"*10,   [], "0000000000000001"),
 "ssize1010": span("obs-ssize-1010", "x"*1010, [], "0000000000000002"),
 "fp-ab":     span("obs-fp", "fp", [s("a","1"), s("b","x")], "0000000000000003"),
 "fp-ba":     span("obs-fp", "fp", [s("b","x"), s("a","1")], "0000000000000004"),
 "fp-int":    span("obs-fp", "fp", [i("a",1), s("b","x")], "0000000000000005"),
}
for k,v in probes.items(): open(f"p3c-{k}.json","w").write(json.dumps(v))
