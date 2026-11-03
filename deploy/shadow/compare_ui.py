"""Compares what the FuseScope backend serves for the APM pages from the demo's
ClickHouse and from the shadow's, over the same settled window: the Services
page (/api/v2/services), each service's top and entry-point operations, and
the service map (/api/v1/dependency_graph). Both backends are the same version;
only the ClickHouse behind them differs. The service map depends on how each
collector cuts its insert batches (traces spec §1), so its edges must match and
their call counts agree within EDGE_TOLERANCE; everything else exactly. Exits 1
on a difference.
"""

import json
import math
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

DEMO = os.environ.get("DEMO_API", "http://fusescope:8080")
SHADOW = os.environ.get("SHADOW_API", "http://fusescope-shadow-ui:8080")
WINDOW_MINUTES = int(os.environ.get("WINDOW_MINUTES", "60"))
SETTLE_MINUTES = int(os.environ.get("SETTLE_MINUTES", "5"))
EDGE_TOLERANCE = float(os.environ.get("EDGE_TOLERANCE", "0.05"))
_end = (int(time.time()) // 60 * 60 - SETTLE_MINUTES * 60) * 10**9
WINDOW = {"start": str(_end - WINDOW_MINUTES * 60 * 10**9), "end": str(_end), "tags": []}


def call(base, path, body=None, token=None):
    req = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(),
                                 headers={"content-type": "application/json"})
    if token:
        req.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        raise SystemExit(f"{base}{path}: {e.code} {e.read().decode(errors='replace')[:300]}") from None


def login(base, email, password, register=False):
    if register:
        try:
            call(base, "/api/v1/register", {"name": "shadow", "email": email, "password": password,
                                            "orgDisplayName": "shadow", "orgName": "shadow"})
        except SystemExit:
            pass  # already set up
    ctx = call(base, "/api/v2/sessions/context?" + urllib.parse.urlencode({"email": email, "ref": base}))
    org = ctx["data"]["orgs"][0]["id"]
    return call(base, "/api/v2/sessions/email_password", {"email": email, "password": password, "orgId": org})["data"]["accessToken"]


def data(r):
    """The payload: most endpoints wrap it in data, the service map does not."""
    return r.get("data") if isinstance(r, dict) and "data" in r else r


def normalize(v):
    """Sorts lists of objects so order differences do not count; rounds floats."""
    if isinstance(v, dict):
        return {k: normalize(x) for k, x in v.items()}
    if isinstance(v, list):
        items = [normalize(x) for x in v]
        return sorted(items, key=lambda x: json.dumps(x, sort_keys=True))
    if isinstance(v, float) and math.isfinite(v):
        return float(f"{v:.9g}")
    return v


def edges_match(d, s):
    """Same (parent, child) edges, call counts within EDGE_TOLERANCE (at least 2 calls)."""
    dm = {(e["parent"], e["child"]): e["callCount"] for e in d or []}
    sm = {(e["parent"], e["child"]): e["callCount"] for e in s or []}
    if dm.keys() != sm.keys():
        return False, f"edges only in demo {sorted(dm.keys() - sm.keys())}, only in shadow {sorted(sm.keys() - dm.keys())}"
    off = {k: (dm[k], sm[k]) for k in dm if abs(dm[k] - sm[k]) > max(2, EDGE_TOLERANCE * dm[k])}
    return not off, f"call counts (demo, shadow) beyond tolerance: {off}" if off else f"{len(dm)} edges"


def main():
    missing = [k for k in ("DEMO_EMAIL", "DEMO_PASSWORD", "SHADOW_UI_EMAIL", "SHADOW_UI_PASSWORD") if not os.environ.get(k)]
    if missing:
        raise SystemExit(f"set {', '.join(missing)} (README, APM pages)")
    demo = login(DEMO, os.environ["DEMO_EMAIL"], os.environ["DEMO_PASSWORD"])
    shadow = login(SHADOW, os.environ["SHADOW_UI_EMAIL"], os.environ["SHADOW_UI_PASSWORD"], register=True)
    checks = [("services", "/api/v2/services", WINDOW), ("service map", "/api/v1/dependency_graph", WINDOW)]
    services = data(call(DEMO, "/api/v2/services", WINDOW, demo)) or []
    for s in sorted({x.get("serviceName") for x in services if isinstance(x, dict)} - {None}):
        body = dict(WINDOW, service=s, limit=5000)
        checks += [(f"top operations {s}", "/api/v2/service/top_operations", body),
                   (f"entry points {s}", "/api/v2/service/entry_point_operations", body)]
    failed = 0
    for name, path, body in checks:
        d, s = normalize(data(call(DEMO, path, body, demo))), normalize(data(call(SHADOW, path, body, shadow)))
        if name == "service map":
            ok, detail = edges_match(d, s)
            print(f"{'ok  ' if ok else 'DIFF'}  service map ({detail})")
            failed += not ok
            continue
        if d == s:
            print(f"ok    {name} ({len(d) if isinstance(d, list) else 1} items)")
            continue
        failed += 1
        print(f"DIFF  {name}")
        print(f"        demo   {json.dumps(d, sort_keys=True)[:600]}")
        print(f"        shadow {json.dumps(s, sort_keys=True)[:600]}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
