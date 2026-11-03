# RUM receiver · contract with browsers, apps and the FuseScope backend

What `rumreceiver` must do to replace the FuseScope backend's RUM gateway for the SDKs and the
setup screen ([ADR 0006](../adr/0006-single-ingress.md), FuseScope ADR 0010 §6 and §10), and the
shape of the OpAMP ingest policy, ingest report and acknowledgement it depends on. Status:
implemented in `components/receiver/rumreceiver` and `components/pkg/ingestpolicy` without the
OpAMP transport, which is step 3 (issue
[#13](https://github.com/fuseone-io/fusescope-collector-server/issues/13), step 2).

## 0. Sources

Every rule cites one of:

- **[B]** the FuseScope backend, `fuseone-io/fusescope` at `6c8991257` (`origin/main`,
  2026-11-02). Paths are relative to it; `impl/` is
  `pkg/modules/rumapplication/implrumapplication/` and `rumtypes/` is `pkg/types/rumtypes/`.
- **[ADR3]**, **[ADR9]**, **[ADR10]**: FuseScope `docs/adr/0003-real-user-monitoring.md`,
  `0009-fusescope-collector.md`, `0010-single-ingress.md` at the same commit.
  **[ADR10 §10]**: the amendments on acceptance in
  [fuseone-io/fusescope#303](https://github.com/fuseone-io/fusescope/pull/303), merged as `4bee141`.
- **[SDK]** `fuseone-io/fusescope-sdk` at `2e16765`: `packages/browser/README.md` and
  `packages/browser/src/`. **[OTel-JS]** `@opentelemetry/otlp-exporter-base` 0.223.0, the
  exporter version the SDK resolves (README "Compatibility": OpenTelemetry JS 0.223).
  **[SDK-A]** `fuseone-io/fusescope-sdk-android` at `5296f46`, `README.md`.
- **[Fetch]** the WHATWG Fetch standard, CORS protocol (CORS-preflight request).
- **[G1]**–**[G6]**: the security gates of [ADR 0006](../adr/0006-single-ingress.md#security-gates).
- **[OTLP]**: the OTLP specification, OTLP/HTTP (response codes, `Retry-After`).
- **[M]**: a maintainer decision recorded in ADR 0006 or on PR #41, with no other source.
  **[M43]**: a maintainer decision in the review of PR #43 (step 2).

A rule marked **Differs** is a deliberate change from the backend, listed again in §12.

### Licensing

The gateway is FuseOne's own code in the MIT-licensed backend ([ADR3] lines 87–106; the
backend's `LICENSE` is MIT, with SigNoz's `ee/` and `cmd/enterprise/` trees excluded). ADR 0009
§4 (line 72) allows moving it here with its MIT notice kept.

History checked with `git log --follow` on `origin/main` for `impl/gateway.go`,
`impl/gateway_otlp.go`, `impl/otlp.go`, `impl/monitor.go`, `impl/keycache.go`,
`rumtypes/{application,key,pii}.go` and `pkg/ratelimit/{ratelimit,limiter}.go`: every file
was created by FuseOne in `e5df39536` (2026-10-21, "feat(rum): applications, public keys and
the ingestion gateway", whose message states it was written clean-room from FuseOne's own
interfaces, MIT code and public OTLP docs) or in later FuseOne commits (`4b9fcbf7f`,
`09508c60a`, `66f7b54b0`, `bcbeda1de`, `5fadb3193`, `740d24019`, `ed7704adc`). None is a
rename or copy of a SigNoz-era file; `pkg/ratelimit/limiter.go` is a rename of
`impl/ratelimit.go`, itself added in `e5df39536`.

The gateway does import SigNoz-era MIT helper packages of the backend: `pkg/errors`
(`c322fc72d`, 2024-08-22), `pkg/http/render` (`bd7d14b1c`, 2024-08-23), `pkg/factory`
(`42525b606`, 2025-01-17) and `pkg/valuer`. They are not ported: the receiver uses collector
APIs (`confighttp`, `consumererror`, `pdata`) for errors, responses and lifecycle. MIT would be
allowed (ADR 0005), but nothing needs them.

## 1. Routes and methods

- `POST` and `OPTIONS` on `/rum/v1/traces` and `/rum/v1/logs` ([B]
  `pkg/apiserver/signozapiserver/rum.go:58-65`; [SDK] `README.md:181-194`; [SDK-A] lines
  234–238). Other methods on these paths: 405.
- Traces carry an OTLP `ExportTraceServiceRequest`, logs an `ExportLogsServiceRequest`
  ([B] `impl/gateway_otlp.go:30-80`).
- Every response carries `Vary: Origin` ([B] `impl/gateway.go:76`).
- The order of checks is the backend's ([B] `impl/gateway.go:75-177`), after the connection
  and concurrency limits of §7.1: preflight; policy present (§11.4); key; origin; rate limits;
  content type; body; decoding; package names; processing; pipeline. A request refused at one
  step is not examined further.
- **TLS.** The listener serves TLS 1.3 only, with no plaintext option, its certificate and key read from files (`cert_file`, `key_file`; inline PEM is refused so no key reaches the effective configuration reported over OpAMP) ([ADR 0004](../adr/0004-security-baseline.md);
  [M43]). Unlike ClickHouse ([ADR 0007](../adr/0007-clickhouse-plaintext-for-migration.md)),
  no existing deployment needs an exception. Deployment: the public proxy (Caddy) terminates
  the client's TLS and opens a second TLS connection to the collector that validates the
  collector's certificate; mutual TLS on that hop is recommended. The proxy's addresses go in
  `trusted_proxies` (§7).
- The browser bundle route of the backend (`/rum/v1/sdk/browser/{version}/…`) is not served:
  the CDN serves it in this phase, with no new build dependency ([M], resolved 11; [ADR10 §10]).

## 2. Key transport and validation

- **Differs [G4].** The key is read only from the `FuseScope-RUM-Key` request header. The
  backend also reads a `fusescope_rum_key` query parameter, for `navigator.sendBeacon`, which
  cannot set headers ([B] `impl/gateway.go:17-22,207-212`). It is removed ([M], resolved 13;
  [ADR10 §10]): there are no users yet, and the browser SDK does not use `sendBeacon`. It sets
  the header on its OTLP exporters ([SDK] `src/config.ts:4,109-111`, `src/sdk.ts:41-42`), whose
  browser transport is `fetch` with the configured headers and `keepalive` when the browser's
  keepalive budget allows ([OTel-JS] `build/esm/configuration/create-legacy-browser-delegate.js:44-47`,
  `build/esm/transport/fetch-transport.js:52-72`). The Android SDK sends the header too ([SDK-A]
  line 238). A query parameter of that name is ignored, never read or logged; a request with no
  header has no key.
- A well-formed key starts with `rum_pub_`, is longer than the prefix and at most 128 bytes
  ([B] `rumtypes/key.go:18-25,147-149`). Anything else is refused as invalid without a lookup
  ([B] `impl/module_keys.go:111-114`).
- The verifier is the lowercase hex SHA-256 of the key as sent ([B]
  `rumtypes/key.go:142-145`), looked up in the ingest policy (§11). No per-request call to the
  backend ([ADR10] §4, line 71). `rumreceiver` does this lookup itself, against the same
  immutable policy store `ingestauth` uses ([M], resolved 2).
- A key absent from the policy is unknown. A key whose `expiresAt` is not after now is
  refused, attributed to its application ([B] `rumtypes/key.go:151-167`,
  `impl/module_keys.go:128-133`). Revoked keys are not published in the policy, so they are
  unknown ([M], resolved 7).
- Missing, malformed, unknown or expired: **401** ([B] `impl/gateway.go:89-94`,
  `pkg/http/render/render.go:103`; test `impl/gateway_test.go:108-109`).
- **Differs.** A valid key without the request's signal in its `signals` (for example a
  logs-only key on `/rum/v1/traces`) is refused with **403**, reason `signal_not_allowed`, on
  its application: authorisation, not authentication. The key is public, so telling it apart
  from an invalid key helps no enumeration ([M43]).
- Only keys of kind `public` are accepted on these routes; secret and runner keys are refused
  like unknown keys ([ADR10] §2 table, line 50: public keys for "RUM traces and logs, on the RUM
  routes only").
- Keys are always enforced here: `rumreceiver` has no `permissive` or `warn` mode, as the
  backend gateway has none ([B] `impl/gateway.go:89-94`; [ADR10] §5 modes apply to
  `ingestauth`).
- **[G4]** The key, the `FuseScope-RUM-Key` header and the request URL are never logged, never
  recorded in the collector's own spans, metrics or logs, and never reported. The backend keeps
  the RUM routes out of its request logs ([B] `pkg/apiserver/config.go:51-60`). Only the key ID
  and its verifier may appear anywhere.

## 3. Application lookup

- A key belongs to exactly one RUM application ([B] `rumtypes/key.go:55-67`; [ADR3] lines
  112–113). The application, with its org, name, environment, platform, origins, package names
  and PII policy, comes from the ingest policy (§11.1).
- Platforms are `web` and `android`; `ios` is reserved ([B]
  `rumtypes/application.go:30-40,344-352`). A policy with another platform is rejected (§11.2).

## 4. CORS and preflight

- **Preflight. Differs.** A CORS-preflight request carries the names of the request's
  non-safelisted headers in `Access-Control-Request-Headers`, never their values, and no
  credentials ([Fetch], CORS-preflight request), so it cannot carry the key. A preflight is
  allowed when it has a non-empty `Origin`, `Access-Control-Request-Method: POST`, and at least
  one RUM application in the policy allows the origin ([B] `impl/gateway.go:179-192`, minus the
  query-key branch removed in §2). The POST is then checked against its key's application.
- An allowed preflight answers **204** with `Access-Control-Allow-Origin: <origin>`,
  `Access-Control-Allow-Methods: POST`,
  `Access-Control-Allow-Headers: content-type, content-encoding, fusescope-rum-key` and
  `Access-Control-Max-Age: 86400` ([B] `impl/gateway.go:24-27,199-204`; [SDK]
  `README.md:190-193`). Any other preflight: **403** ([B] `impl/gateway.go:194-197`; tests
  `impl/gateway_test.go:190-193`).
- **POST with an `Origin`:** the key's application must allow it, else **403**, reason
  `origin_not_allowed` ([B] `impl/gateway.go:96-100`). When allowed,
  `Access-Control-Allow-Origin` echoes the origin ([B] `impl/gateway.go:102`). Responses
  refused before this check (key errors) carry no `Access-Control-Allow-Origin`. An `Origin`
  longer than 256 bytes matches nothing (§7.1).
- **POST without an `Origin`** skips the origin check ([B] `impl/gateway.go:96`; test
  `HeaderKeyNoOrigin_Accepted`, `impl/gateway_test.go:110`), for parity: `Origin` is a browser
  control, not authentication ([M], resolved 8). Mobile apps send this way ([SDK-A] line 239).
- **Origin matching** ([B] `rumtypes/application.go:172-203`): both sides lower-cased and parsed
  as URLs; a candidate without scheme or host never matches (so `Origin: null` is refused);
  `*` matches any origin; otherwise scheme and port must be equal, and the host must equal the
  entry's host, or, for an entry `*.example.com`, end in `.example.com` and be longer than it
  (subdomains only, never the apex). Tests: `impl/gateway_test.go:112-114`,
  `rumtypes/application_test.go`.
- Allowed origins are `*` or `scheme://host[:port]` with `http` or `https`, no path, query,
  fragment or user info, and `*` only as a leading `*.` label ([B]
  `rumtypes/application.go:418-434`); at most 50 entries ([B]
  `rumtypes/application.go:19,395-399`). A policy with any other entry is rejected (§11.2).
- No credentials are involved: `Access-Control-Allow-Credentials` is never sent ([SDK]
  `README.md:194`).

## 5. Android package names

- For an `android` application, every resource of the request must carry an
  `app.package.name` the application allows; a missing or refused one refuses the whole request
  with **403**, reason `package_not_allowed`, observing the first refused value (empty when
  missing) ([B] `impl/otlp.go:115-129`, `impl/gateway_otlp.go:109-116`; tests
  `impl/otlp_mobile_test.go:136-139`, `impl/gateway_test.go:378-380`).
- Matching ([B] `rumtypes/application.go:155-170`): an empty name never matches; `*` matches
  any; an exact entry matches itself; an entry `com.example.*` matches names starting with
  `com.example.` and longer than it, never `com.example` itself.
- Allowed entries are `*` or a package name matching
  `^[A-Za-z][A-Za-z0-9_]*(\.[A-Za-z][A-Za-z0-9_]*)+$` with an optional trailing `.*`, at most 50
  ([B] `rumtypes/application.go:36,363-383`).
- Android applications have no allowed origins ([B] `rumtypes/application.go:370-372`), so a
  request with an `Origin` header is refused with 403 `origin_not_allowed` ([ADR3] line 184;
  test `BrowserOrigin_Forbidden`, `impl/gateway_test.go:381`; [SDK-A] lines 217–219).
- The check runs after decoding, on the decoded resources ([B] `impl/gateway_otlp.go:30-45,56-71`).
  Web applications are not checked ([B] `impl/otlp.go:119-121`).

## 6. Content types, encodings and body size

- `Content-Type` is parsed as a media type, falling back to the trimmed, lower-cased raw value
  ([B] `impl/gateway_otlp.go:128-141`):
  - `application/x-protobuf`, `application/protobuf`: OTLP protobuf;
  - `application/json`, `text/plain`: OTLP JSON. `text/plain` existed for `sendBeacon` ([B]
    `impl/gateway_otlp.go:126-127`); it stays for parity, though no SDK needs it any more
    (follow-up, §13);
  - anything else, missing included: **400**, reason `invalid_payload`.
- `Content-Encoding` (trimmed, case-insensitive): empty or `identity` as is, `gzip` decoded;
  anything else **400** `invalid_payload`; an invalid gzip stream **400** ([B]
  `impl/gateway_otlp.go:162-186`; tests `impl/gateway_test.go:261,267`).
- **[G3]** One limit, `max_body_size`, default 4 MiB ([B]
  `pkg/modules/rumapplication/config.go:24-25,82`), applies three times ([B]
  `impl/gateway_otlp.go:162-197`):
  - a `Content-Length` above it is refused before reading;
  - at most limit + 1 bytes are read from the wire, and more is refused (chunked bodies, as the
    Android SDK sends, [SDK-A] line 237, have no `Content-Length`);
  - at most limit + 1 bytes are read from the gzip decoder, and more is refused.

  Over the limit: **413**, reason `too_large` (tests `TooLarge_PayloadTooLarge`,
  `GzipBomb_PayloadTooLarge`, `impl/gateway_test.go:265-266`). The body is never read without a
  bound.
- An OTLP payload that does not decode: **400** `invalid_payload` ([B]
  `impl/gateway_otlp.go:150-159`; tests `impl/gateway_test.go:262-263`).
- The response body is an empty `Export*ServiceResponse` in the request's encoding: JSON for
  JSON and `text/plain`, protobuf otherwise, with the matching `Content-Type`, status **200**
  ([B] `impl/gateway.go:118-120`, `impl/gateway_otlp.go:47-53,73-79,143-148`; tests
  `impl/gateway_test.go:259-261`).

## 7. Rate limits and bounded state

- Two token buckets, checked after the key and origin and before the body is read ([B]
  `impl/gateway.go:139-145`):
  - per key: every client of one key;
  - per key and client: identity `<key id>|<client group>` (below).
- Bucket semantics ([B] `pkg/ratelimit/limiter.go:10-65`): a new identity starts with a full
  bucket of `burst` tokens; tokens refill at `rate` per second up to `burst`; a request takes
  one token or is refused; buckets idle for more than 10 minutes are dropped.
- Over either limit: **429** with `Retry-After: 1`, reason `rate_limited` ([B]
  `impl/gateway.go:143-144`, `pkg/http/render/render.go:117-118,130-135`; test
  `impl/gateway_test.go:216`).
- **Differs (per-replica share).** The per-key limit is the key's `limit` in the ingest policy,
  already this replica's share of the key's limit ([ADR10] §4, line 73). When the policy gives
  none, the backend's default applies unshared: 500/s, burst 1000 ([B]
  `pkg/modules/rumapplication/config.go:86`). The per-client limit is local configuration,
  default 20/s, burst 60 ([B] `config.go:87`), enforced per replica with no sharing for now
  ([M], resolved 5).
- **Client IP. Differs [G2].** The backend takes the first `X-Forwarded-For` address from any
  peer when `trust_forwarded_for` is set ([B] `pkg/ratelimit/ratelimit.go:16-31`,
  `pkg/modules/rumapplication/config.go:67-68`). Here:
  - local configuration holds `trusted_proxies`, a list of CIDRs, empty by default;
  - when the peer address is not in it, the peer address is the client IP and
    `X-Forwarded-For` is ignored;
  - when it is, the `X-Forwarded-For` values (all header lines, comma-separated) are walked
    from right to left, skipping addresses in `trusted_proxies`; the first other address is the
    client IP. An entry that does not parse as an IP address ends the walk, and the last
    trusted address seen (or the peer) is used. The leftmost entry is never trusted on its own,
    since a client can write it.
- **Client group. Differs [G6].** An IPv4 address (IPv4-mapped IPv6 included) is its own group;
  an IPv6 address is grouped by its /64 prefix ([M], resolved 5).
- The client IP is held in memory for the bucket only, never stored, logged or reported ([B]
  `config.go:64`; [ADR3] line 148; [G5]).

### 7.1 Bounds **[G6]**

All state indexed by external input is bounded. Defaults, each overridable in local
configuration ([M]):

| State or limit | Default | Over the bound |
|---|---|---|
| Per-key-and-client buckets | 100 000 identities, global | least recently used evicted; idle ones still dropped after 10 min |
| Per-key buckets | one per key of the policy (≤ 50 000, §11.2) | none created for unknown keys |
| Session IDs per application per minute bucket | 10 000 ([B] `impl/monitor.go:14,58-62`), stored as 64-bit hashes (`hash/maphash`, per-process seed) | further sessions not counted |
| Per-application monitors | one per RUM application of the current policy (≤ 10 000), dropped when it leaves the policy | none for applications outside the policy |
| Unknown keys | one global counter and last-seen time; no per-key or per-origin state | — |
| Rejections per application | one entry per reason, seven reasons (§10.1) | — |
| Pending key usage for the report | 10 000 keys ([B] `impl/monitor.go:15`) | oldest use evicted, counted in `dropped.keys` |
| `Origin` | 256 bytes, checked before matching or storing | matches nothing; observed as empty |
| `app.package.name` | 256 bytes, checked before matching or storing | refused; observed as empty |
| SDK name / version | 64 / 32 bytes, checked before storing | stored as empty |
| Concurrent connections per listener | 4 096 | new connections wait in the accept backlog, then are refused |
| Concurrent requests per listener | 512 | 503 with `Retry-After: 1` |
| Header read timeout | 10 s | connection closed |
| Body read timeout | 30 s | connection closed |
| Write timeout | 30 s | connection closed |
| Idle keep-alive timeout | 120 s | connection closed |
| Request header bytes | 64 KiB | 431 |
| Resource groups per request | 1 000, after empty ones are dropped | 413 `too_large`; checked before any attribute is stamped |

Strings longer than their bound are never truncated into a stored value: a truncated origin or
package name could match or name something else.

## 8. Processing

Applied in place on the decoded request, after every check passed.

### 8.1 Resource stamping

For every resource ([B] `impl/otlp.go:90-113`; tests `impl/otlp_test.go:47-68`):

1. its attributes are scrubbed (§8.2);
2. `fusescope.rum.application.id` and `fusescope.rum.platform` are overwritten with the
   application's ID and platform, whatever the client sent;
3. `service.name` is set to the application's name when missing, empty, or equal to the
   application ID (an SDK default);
4. `deployment.environment.name` is set to the application's environment when missing (an
   empty value present is kept);
5. **new, from [ADR10] §4 (line 72):** `fusescope.ingest.key_id` is overwritten with the key's
   ID, and the key's org goes in the request's auth data (`client.Info`), never taken from the
   payload.

Attribute names: [B] `rumtypes/attributes.go:3-19`. Name and environment come from the policy
([M], resolved 3; [ADR10 §10]).

### 8.2 PII scrubbing

The application's PII policy ([B] `rumtypes/pii.go:17-30`): `dropQuery`, `queryAllowlist`,
`urlAllowlist`. The default is `dropQuery: true` with empty lists ([B] `rumtypes/pii.go:28-30`,
`rumtypes/application.go:270`).

- **Where:** the attributes of every resource, span, span event, span link and log record ([B]
  `impl/otlp.go:23-30,61,95`).
- **Which attributes** ([B] `impl/otlp.go:169-185`, `rumtypes/pii.go:10-15`):
  - string values of `url.full`, `http.url`, `http.target`, `url.original`, `location.href`,
    `page.url` go through *scrub URL*;
  - a string `url.query` goes through *scrub query*, and is removed when nothing is left;
  - `url.fragment` is always removed.
- **Scrub URL** ([B] `rumtypes/pii.go:32-60`): a value without `?` or `#` is unchanged. Else the
  part before the first `?` or `#` is the base; if the base starts with an entry of
  `urlAllowlist`, the value is kept verbatim. Otherwise the fragment is dropped, the query goes
  through *scrub query*, and the result is the base, plus `?` and the kept query when not
  empty.
- **Scrub query** ([B] `rumtypes/pii.go:61-81`): a leading `?` is trimmed. With `dropQuery`
  false, the query is kept as is. With it true, only parameters whose name (URL-unescaped when
  that succeeds) is in `queryAllowlist` are kept, in their original order and encoding.
- **Span names** ([B] `rumtypes/pii.go:83-96`, `impl/otlp.go:23`): a name containing `?` or `#`
  is split on single spaces and every token goes through *scrub URL* (for example
  `Navigation: /customers/392#tab?email=a@b.c`).
- Tests: `rumtypes/pii_test.go`, `impl/otlp_test.go:39`.
- Not scrubbed, for parity: log bodies, scope attributes, non-string values, other attributes
  ([M], resolved 9; follow-up in §13).
- `geoLookup`, `requireConsent` and `hashUserId` are in the backend's policy type ([B]
  `rumtypes/pii.go:23-25`) but not applied by its gateway: consent and user-ID hashing are
  SDK-side, and no geo lookup exists in the backend (`git grep GeoLookup` finds only the type).
  None is in policy schema v1, and the receiver does no geo lookup ([M], resolved 10).

### 8.3 Logs: `event.name`

- When a log record's `EventName` field is set and the `event.name` attribute is absent, the
  field is copied into the attribute, for every platform; storage keeps only the attribute ([B]
  `impl/otlp.go:47-48,63-67`; [ADR3] line 84; tests `impl/otlp_test.go:139-144`,
  `impl/gateway_test.go:311`).

### 8.4 Android normalisation

For `android` applications only, on log records, after the `event.name` copy ([B]
`impl/otlp.go:68-71,131-153`; tests `impl/otlp_mobile_test.go:37-78`):

- `event.name` `app.crash` becomes `exception`, with `fusescope.rum.error.source` = `crash`
  when missing or empty;
- `device.anr` becomes `exception`, with, when missing or empty, `fusescope.rum.error.source` =
  `anr`, `exception.type` = `ANR` and `exception.message` = `Application Not Responding`;
- then a record with source `anr` and `exception.type` `ApplicationNotResponding` (SDK
  0.1.0-beta.1) gets `exception.type` = `ANR`.

Names: [B] `rumtypes/attributes.go:21-39`, `rumtypes/timeline.go:16-19`,
`rumtypes/jvmstacktrace.go:17`; [SDK-A] lines 221–223.

### 8.5 Screen route

For `android` applications, on spans and log records: a non-empty `app.screen.name` is copied
into `fusescope.rum.view.route` when that is missing or empty ([B]
`impl/otlp.go:32-34,70,155-161`; [ADR3] line 192; tests `impl/otlp_mobile_test.go:67-72,109-110`).

### 8.6 What each accepted request contributes to the report

([B] `impl/otlp.go:10-88`, `rumtypes/status.go:37-42,61-68`)

- sessions: distinct non-empty `session.id` attribute values of spans and log records, kept as
  fixed-size hashes (§7.1), counted exactly per replica ([M], resolved 6);
- views: spans named `view`;
- errors: log records whose `event.name` is `exception` (after §8.4);
- vitals: log records whose `event.name` is `browser.web_vital` or `rum.vital`;
- SDK: per resource, `telemetry.distro.name`/`telemetry.distro.version` when the name is set,
  else `telemetry.sdk.name`/`telemetry.sdk.version`; the last resource with one wins ([B]
  `impl/otlp.go:106-112`); bounded as in §7.1.

## 9. Into the pipeline and status codes

The backend re-encodes the processed request as protobuf and forwards it to the collector's
OTLP/HTTP receiver ([B] `impl/gateway_otlp.go:199-223`). Here it goes to the next consumer of the
receiver's pipeline, with no extra hop ([ADR10] §6, line 94). The data is counted in the report
(§10) only once the consumer accepted it ([B] `impl/gateway.go:166-170`).

| Condition | Status | Counted as | Source |
|---|---|---|---|
| Too many concurrent requests | 503, `Retry-After: 1` | none | [G6], §7.1 |
| No valid ingest policy (fail closed) | 503, `Retry-After: 5`; preflight 403 | none | [G1], §11.4 |
| Key missing or malformed | 401 | none | §2 |
| Key unknown or of a wrong kind | 401 | global `unknownKeys` | §2, §10.1 |
| Key expired | 401 | `bad_key` on its application | §2 |
| Key valid, signal not in its `signals` | 403 | `signal_not_allowed` | §2 |
| Origin not allowed (incl. any `Origin` on Android) | 403 | `origin_not_allowed` | §4, §5 |
| Package name missing or not allowed | 403 | `package_not_allowed` | §5 |
| Rate limited | 429, `Retry-After: 1` | `rate_limited` | §7 |
| Unsupported type or encoding, bad gzip, bad OTLP | 400 | `invalid_payload` | §6 |
| Body over the limit, or over 1 000 non-empty resource groups | 413 | `too_large` | §6, §7.1 |
| Consumer error, permanent | 400 | none | **Differs**, below |
| Consumer error, not permanent (memory limiter, full queue, exporter backpressure) | 503, `Retry-After: 5` | none | **Differs**, below |
| Accepted | 200, empty export response | ingest | §6 |

- The backend maps its forward: unreachable collector 503, collector 429 or 503 kept, other
  4xx 400, 5xx 502; each retryable one with `Retry-After: 5` ([B]
  `impl/gateway_otlp.go:23,199-242`; tests `impl/gateway_test.go:334-338`). **Differs:** with no
  forward, the receiver maps consumer errors the way upstream OTLP receivers do: permanent
  (`consumererror.IsPermanent`) 400, anything else 503 with `Retry-After: 5`. OTLP clients retry
  429, 502, 503 and 504 and never 400 [OTLP], so client retry behaviour is unchanged.
- Error bodies are JSON with the error code (for example `rum_origin_not_allowed`), as today
  ([B] `impl/gateway.go:29-37`, `pkg/http/render/render.go:91-140`). They never echo the key.

## 10. Setup status, the ingest report and its acknowledgement

### 10.1 What the backend's setup screen needs

`GET /api/v1/rum/applications/{id}/status` ([B] `pkg/apiserver/signozapiserver/rum.go:117-120`,
`impl/module.go:146-172`, `rumtypes/status.go:27-59`) returns, over a 5-minute window ([B]
`rumtypes/status.go:10`):

- `firstSeenAt`: when data was first accepted, persisted on the application;
- `counts`: sessions, views, errors, vitals;
- `sdk`: name, version, and support (`supported`, `outdated`, `other`, `unknown`), computed by
  the backend from name and version ([B] `rumtypes/sdk.go:13-18,57-74`);
- `rejections`: per reason, `count`, `lastOrigin` and `lastSeenAt`, newest first ([B]
  `impl/monitor.go:83-96,135-140`).

The UI ([B] `frontend/src/components/RumApplicationSheet/components/`) shows first data
(`SetupStatus.tsx:33-66`), the counts (`StatusCounts.tsx:10-13`), the SDK (`SetupStatus.tsx:94-102`),
and one line per rejection; it shows `lastOrigin` only for `origin_not_allowed`,
`package_not_allowed` (as the package name) and `unknown_key` (`RejectionLine.tsx:30-76`). It also
shows each key's `lastUsedAt` (`KeyRow.tsx:19-20`), which the gateway feeds ([B]
`impl/monitor.go:72-74`, `impl/module_keys.go:145-172`).

Rejection attribution in the backend ([B] `impl/gateway.go:105-136`, `rumtypes/status.go:50-52`;
tests `impl/gateway_test.go:144-149`): an expired key counts `bad_key` on its application; an
unknown or malformed key with an `Origin` counts `unknown_key` on every application, of any org,
that allows that origin ([B] `impl/module.go:186-213`); every other refusal counts its reason on
the key's application.

**Differs.** Unknown keys are never attributed to applications: they increment one bounded
global counter, with no origin, which avoids one tenant's traffic showing up on another
tenant's setup screen ([M], resolved 12). The seven per-application reasons are `bad_key`,
`origin_not_allowed`, `package_not_allowed`, `rate_limited`, `too_large`, `invalid_payload`
([B] `rumtypes/status.go:12-20`, less `unknown_key`) and `signal_not_allowed` (§2, [M43]). The UI's unknown-key line needs a backend
follow-up to show the global counter instead.

Today each backend replica keeps this in memory and answers only for itself ([B]
`rumtypes/status.go:24-26`). [ADR10] §6 (line 98) replaces that with a report from each
collector, aggregated by the backend across replicas.

### 10.2 Report

Every 60 seconds ([ADR10] line 98), and at once after an OpAMP reconnect, the receiver sends a
report: one logical report, sent as one or more custom messages, capability
`io.fuseone.fusescope.ingest.v1`, type `report`, each a **page** of strict JSON (§11.2 rules
apply to its shape) of at most 256 KiB ([M43]):

```json
{
  "schemaVersion": 1,
  "reportSequence": 1874,
  "page": 1,
  "pages": 1,
  "generatedAt": "2026-11-03T12:01:00Z",
  "windowSeconds": 300,
  "policy": { "version": 42, "status": "ok" },
  "applications": [
    {
      "id": "0191c5e2-7f0a-7c3e-9a51-3c2b1d4e5f60",
      "firstSeenAt": "2026-11-03T11:58:12Z",
      "counts": { "sessions": 12, "views": 40, "errors": 1, "vitals": 33 },
      "sdk": { "name": "@fusescope/browser", "version": "0.2.0-beta.1" },
      "rejections": [
        { "reason": "origin_not_allowed", "count": 3, "lastOrigin": "https://staging.example.com", "lastSeenAt": "2026-11-03T12:00:41Z" }
      ]
    }
  ],
  "keys": [ { "id": "0191c5e2-8a1b-7d2e-b0c4-5e6f7a8b9c0d", "lastUsedAt": "2026-11-03T12:00:59Z" } ],
  "unknownKeys": { "count": 7, "lastSeenAt": "2026-11-03T12:00:12Z" },
  "dropped": { "keys": 0 }
}
```

- **Pages.** Every page of a report carries the same `reportSequence`, `pages`,
  `generatedAt`, `windowSeconds`, `policy`, `unknownKeys` and `dropped`; `page` runs from 1 to
  `pages`. `applications` and `keys` are split between the pages, each entry in exactly one
  page. The backend applies a report only once it holds every page of its sequence, as if it
  were one message; pages of an older sequence it has not completed are discarded. Every report
  carries every active application and every pending key, so none is starved by newer ones
  ([M43]).

- **Window values** (`counts`, `sdk`, `rejections`, `unknownKeys`) are this replica's view of
  the last `windowSeconds`, in one-minute buckets as the backend keeps them ([B]
  `impl/monitor.go:12-17,98-144,179-192`). Each report replaces the previous one from the same
  agent; the backend sums the latest report of every agent, keeping the newest
  `lastOrigin`/`lastSeenAt` and SDK. Field names are the backend's (`rumtypes/status.go:37-59`).
- **Durable values** (`firstSeenAt` per application, `lastUsedAt` per key) are pending until
  acknowledged (§10.4). Every report carries all pending values, so a report is an idempotent
  snapshot: the backend keeps the earliest `firstSeenAt` and the latest `lastUsedAt`, and
  applying the same report twice changes nothing ([B] `impl/monitor.go:72-80,146-168` keep and
  restore pending values the same way).
- **`policy`** is the revision held and whether the last policy received was accepted:
  `status` `ok` or `rejected`, with `error` set to one code of §11.2 when rejected. The backend
  compares `version` to decide whether to push again ([ADR10] line 64). It is not in the agent
  description, whose changes make the backend recompute the configuration
  ([`opamp.md`](opamp.md) §3).

### 10.3 Allowed fields and bounds **[G5]**

The report is minimised, not anonymous: origin, package and SDK strings come from clients and
can carry identifiers. Only these fields exist; anything else is a schema violation:

| Field | Bound |
|---|---|
| `schemaVersion`, `reportSequence`, `generatedAt`, `windowSeconds` | fixed types |
| `policy.version`, `policy.status`, `policy.error` | `error` one of the codes of §11.2 |
| `applications[]` | every application of the policy active in the window or with a pending `firstSeenAt` (so at most 10 000), most recently active first, across pages |
| `applications[].id` | UUID from the policy |
| `applications[].firstSeenAt` | timestamp |
| `applications[].counts.{sessions,views,errors,vitals}` | integers; sessions from hashes, never IDs |
| `applications[].sdk.name`, `.version` | 64 and 32 bytes (§7.1), else empty |
| `applications[].rejections[]` | at most 6, one per reason |
| `rejections[].reason`, `.count`, `.lastSeenAt` | enum, integer, timestamp |
| `rejections[].lastOrigin` | `origin_not_allowed`: the `Origin` when it parses as an `http`/`https` `scheme://host[:port]` origin of at most 256 bytes, lower-cased; `package_not_allowed`: the `app.package.name` when it matches the pattern of §5 and is at most 256 bytes; empty in every other case |
| `keys[]` | every pending key (at most 10 000, §7.1), most recent use first, across pages; `id` (UUID from the policy) and `lastUsedAt` |
| `unknownKeys.count`, `.lastSeenAt` | integer, timestamp; no key, verifier or origin |
| `dropped.keys` | integer: pending key uses evicted to keep the bound of §7.1, since start; never entries sent in another page |
| `page`, `pages` | integers, 1 ≤ `page` ≤ `pages` ≤ 128 |

- Never present: IP addresses, user agents, URL paths or queries, session or user IDs, attribute
  values, payload content, keys or verifiers ([G4], [G5]).
- Each encoded page is at most 256 KiB. Applications come first, then keys, packed in order
  into as few pages as fit. At the bounds (10 000 applications at their largest, 10 000 keys) a
  report takes 58 pages; 128 is the limit, which the backend may enforce.
- **Differs.** The backend stores `lastOrigin` for every reason ([B] `impl/gateway.go:107-112`)
  and for unknown keys; here it is set for two reasons only, validated and bounded.
- **Retention.** The collector holds window values in memory for the status window (5 min) and
  durable values until acknowledged; nothing is written to disk. The backend keeps
  `lastOrigin` and the SDK strings no longer than the status window; `firstSeenAt` and
  `lastUsedAt` are persisted on the application and the key, as today.

### 10.4 Acknowledgement

- `reportSequence` is a uint64, strictly increasing per agent across restarts: the collector
  persists the next value in its storage directory, next to the OpAMP instance UID
  ([ADR 0008](../adr/0008-opamp-remote-config.md) point 4), before sending a report with it.
- The backend acknowledges with a custom message of the same capability, type `reportAck`:

  ```json
  { "schemaVersion": 1, "reportSequence": 1874 }
  ```

- **Backend, before acknowledging** ([M]; pages [M43]): once it holds every page of the
  sequence, it applies the report idempotently (§10.2), then persists `(agentID, reportSequence)`, where `agentID` is the OpAMP instance UID, and only then
  sends the ack. A report whose sequence is not above the persisted one for that agent changes
  nothing and is acknowledged again. Acknowledging the same sequence twice changes nothing.
- **Collector:** a pending durable value is cleared only when the `reportAck` for a sequence
  whose report (any of its pages) carried it arrives, and only if the value has not changed since (a newer
  `lastUsedAt` stays pending). Until then every report repeats it. An ack for a sequence it
  never sent, or already processed, is ignored. Handing a report to the OpAMP client clears
  nothing.

## 11. The ingest policy

### 11.1 Message

An OpAMP custom message ([ADR10] §3, lines 62–65; [ADR9] line 80; [ADR10 §10]): the collector
declares the custom capability `io.fuseone.fusescope.ingest.v1`; the backend sends type
`policy` and `reportAck`, the collector sends type `report`. The `policy` data is a full
snapshot, never a delta, pushed on any change, when the collector's `policy.version` is behind,
and every 5 minutes regardless ([ADR10] line 64). It is sent only to collectors of role `ingress`
([ADR10] line 62; [ADR9] line 79).

The policy is the remote authority for ingest authentication and identity (ADR 0006 decision
3): it can add keys, widen origins and signals, set the org and the stamped name and
environment. It cannot touch listeners, pipelines, destinations or outbound credentials.

Encoding: JSON, schema v1, typed and strict ([M], resolved 1). Fields, from [ADR10] line 62,
[ADR10 §10] and what §§2–8 need:

| Field | Type | Rule |
|---|---|---|
| `schemaVersion` | integer | exactly `1` |
| `version` | uint64 ≥ 1 | monotonic, durable revision (§11.3); echoed as `policy.version` in reports |
| `issuedAt` | RFC 3339 | when the backend built it |
| `keys[]` | | every usable key: active, or expiring within its rotation overlap ([B] `rumtypes/key.go:151-167`); never a revoked one ([M], resolved 7); at most 50 000 |
| `keys[].id` | UUID | unique; stamped as `fusescope.ingest.key_id` |
| `keys[].sha256` | 64 lowercase hex | unique (§2) |
| `keys[].orgId` | UUID | org, for auth data |
| `keys[].kind` | `secret` \| `public` \| `runner` | [ADR10] §2 |
| `keys[].signals` | non-empty set of `traces`, `metrics`, `logs`, `profiles`, `artifacts` | no duplicates; `public`: a subset of `traces`, `logs` |
| `keys[].expiresAt` | RFC 3339 or null | end of a rotation overlap |
| `keys[].limit` | `{rate > 0, burst ≥ 1}` or absent | this replica's share (§7) |
| `keys[].applicationId` | UUID | required for `public`, absent otherwise; must name an application below |
| `rumApplications[]` | | at most 10 000 |
| `rumApplications[].id` | UUID | unique |
| `rumApplications[].orgId` | UUID | equal to the `orgId` of every key naming it |
| `rumApplications[].name` | string, 1–128 bytes | trimmed ([B] `rumtypes/application.go:17,385-388`) |
| `rumApplications[].environment` | string, 1–64 bytes | trimmed ([B] `rumtypes/application.go:18,389-391`) |
| `rumApplications[].platform` | `web` \| `android` | |
| `rumApplications[].allowedOrigins` | list, ≤ 50 | §4 rules; empty for `android` |
| `rumApplications[].allowedPackageNames` | list, ≤ 50 | §5 rules; empty for `web` |
| `rumApplications[].piiPolicy` | `{dropQuery, queryAllowlist, urlAllowlist}` | lists ≤ 50; query names non-empty; URL prefixes absolute `http`/`https` ([B] `rumtypes/application.go:395-415,436-442`); no other field in v1 |

`name` and `environment` go beyond the list in [ADR10] line 62; they are needed for parity
(§8.1) and added by [ADR10 §10] ([M], resolved 3). `geoLookup`, `requireConsent` and
`hashUserId` are not in v1 (§8.2), so the backend must not send them.

### 11.2 Validation: whole snapshot, strict

A snapshot is accepted whole or rejected whole; nothing is applied from a rejected one, and the
policy held, if any, stays in force ([M]). It is rejected, with the `policy.error` code
reported (§10.2) and a log line without content, when:

| Code | Condition |
|---|---|
| `too_large` | the data is over `max_policy_size`, 24 MiB: a global cap, independent of the counts of §11.1, which a policy at every count bound with a `limit` on every key fits (about 18 MB). The backend enforces the same cap ([M], resolved 1; raised from 16 MiB by [M43]; [ADR10] line 64 estimates 2 MB at 10 000 keys) |
| `parse_error` | it is not valid JSON of the v1 types |
| `schema_version` | `schemaVersion` is missing or not `1` |
| `unknown_field` | any object, at any depth, has a field v1 does not define: ignoring a new field could ignore a new restriction |
| `lower_version` | `version` is below the highest accepted (§11.3) |
| `version_conflict` | `version` equals the highest accepted but the content hash differs |
| `duplicate_id` | two keys, or two applications, share an `id` |
| `duplicate_verifier` | two keys share a `sha256` |
| `dangling_reference` | a `public` key's `applicationId` names no application, or a non-public key has one |
| `org_mismatch` | a key's `orgId` differs from its application's |
| `invalid_value` | any other rule of §11.1 fails (formats, enums, sizes, list lengths, origin and package rules) |
| `persist_error` | the snapshot is valid but its floor could not be persisted (§11.3): it is not accepted ([M43]) |

### 11.3 Revision and cache

- The content hash is the SHA-256 of the message's `data` bytes as received. The backend sends
  identical bytes for the same `version`.
- A snapshot whose `version` equals the highest accepted and whose hash matches is a no-op
  (it refreshes the policy's last-received time, for staleness). One with a higher version that
  passes §11.2 replaces the policy held.
- **Cache** ([ADR10] line 74; order [M43]): a valid policy of a higher version first has its
  floor written to `<storage>/ingest-policy.state` as `{version, sha256}`, the highest accepted
  revision, then its data bytes to `<storage>/ingest-policy.json`. Both mode `0600`, each
  written atomically (temporary file renamed over the old one, as
  [ADR 0008](../adr/0008-opamp-remote-config.md) point 4 does for pushes). They hold verifiers
  only, never a key, since the message carries none, and are never logged or reported.
- **Acceptance.** A revision is accepted, published and reported `ok` only once its floor is
  durable, so no restart can bring back an older revision and the keys it revoked. If the floor
  cannot be written, the revision is rejected with `persist_error`: the policy held, if any,
  stays in force and the receiver's readiness is unchanged (ready with that older policy,
  not ready with none), exactly as for any other rejected snapshot; the backend sees the older
  `policy.version` and pushes again. If the floor is written but the policy file is not, the
  revision is accepted and in force; the policy file is written again on the next identical
  push. A restart in between, like a crash between the two files, finds the files mismatched
  and starts with no policy (fail closed, §11.4), with the new floor refusing the older
  revision.
- A repeated identical push sets the policy file's modification time, which a restart reads as
  when the policy was last received, without rewriting either file.
- **At start** the state file is read first; the cached policy is used only if its hash
  equals the state's and it passes §11.2 (its version then equals the floor). It is used until
  the backend pushes. A lost OpAMP connection keeps the policy held in use ([ADR10] line 74).
  If the state file is missing or unreadable, the collector starts with no policy and no floor
  and logs an error; the next valid push sets both.

### 11.4 Fail closed and staleness

- **[G1].** With no valid policy (none received and none cached, or a cache rejected at start),
  `rumreceiver` refuses every request: POST with **503** and `Retry-After: 5`, preflight with
  **403**. Nothing is counted in the report beyond `policy.status`. `ingestauth` refuses
  likewise, in every mode. The collector reports not ready (health check), and the chart routes
  `/rum/v1` to it only once it holds a valid policy; the backend is deployed and publishing
  first ([M], resolved 4). **Differs** from [ADR10] line 75, amended by [ADR10 §10].
- **Readiness.** `rumreceiver` reports component status `RecoverableError` while it holds no
  valid policy (or one past `max_staleness`) and `OK` once it does. The chart (step 4)
  configures `healthcheckv2` with `component_health.include_recoverable_errors: true` and
  `recovery_duration: 0s`, and uses that endpoint for readiness only, never liveness, so a
  missing policy takes the pod out of traffic without a restart loop ([M43]).
- **Staleness** ([ADR10] line 74): the policy's age since last received is exported as a metric.
  An optional `max_staleness`, with no default, makes the receiver refuse POSTs with 503 and
  preflights with 403 once older, and report not ready ([M43]).
- Revocation reaches a connected collector within 10 s p95 ([ADR10] line 65); during an outage,
  revoked keys keep working from the cache until the connection returns or `max_staleness`
  passes.

## 12. Deliberate differences from the backend gateway

| # | Backend at `6c8991257` | Here | Why |
|---|---|---|---|
| 1 | Key lookup in the database with a 30 s cache; the gateway always answers | Keys from a strict, versioned policy; with none valid, every request refused (503) and not ready | [G1], [ADR10 §10] |
| 2 | Key in the header or the `fusescope_rum_key` query parameter | Header only; preflight allowed by origin alone | [G4], [Fetch], [ADR10 §10] |
| 3 | `X-Forwarded-For` first address, from any peer when `trust_forwarded_for` is on (`pkg/ratelimit/ratelimit.go:16-31`) | Only from peers in `trusted_proxies` (default none), walked right to left | [G2] |
| 4 | Per-key limit in each backend replica, full value; per-IP buckets per exact address, map unbounded until the idle sweep | Policy's per-replica share; IPv6 by /64; bucket map capped with LRU eviction | [ADR10] §4, [G6] |
| 5 | Session IDs stored as strings; strings stored unbounded | 64-bit hashes; strings bounded before storing; connection, request and timeout limits | [G6] |
| 6 | Forward to the collector, status mapped from its HTTP answer | Next consumer; permanent errors 400, others 503 | no extra hop, [ADR10] §6 |
| 7 | Setup status in memory per backend replica; `lastOrigin` kept for every reason; unknown keys counted on every application allowing the origin, across orgs | Report over OpAMP every minute, in pages, with sequence and ack; `lastOrigin` for two reasons, validated and bounded; unknown keys as one global counter | [ADR10] §6, [G5], [M], [M43] |
| 8 | Revoked keys known to the database, counted as `bad_key` | Not in the policy, so unknown; expired ones still `bad_key` while listed | [M], resolved 7 |
| 9 | No `fusescope.ingest.key_id`, no org in auth data | Both stamped | [ADR10] §4 |
| 10 | Browser bundle served at `/rum/v1/sdk/browser/…` | Not served; CDN | [M], resolved 11 |
| 11 | No per-signal key permission | A valid key without the signal: 403 `signal_not_allowed` | [M43] |

## 13. Decisions and follow-ups

Resolved by the maintainer on PR #41:

| # | Question | Decision |
|---|---|---|
| 1 | Policy encoding and limits | JSON schema v1, typed and strict, with `schemaVersion` and a monotonic revision, 16 MiB cap (§11), raised to 24 MiB in the review of PR #43. **Requirement for step 2:** an integration test that sends a policy at the maximum size through OpAMP end to end and checks it is accepted, cached and applied. |
| 2 | Who validates RUM keys | `rumreceiver`, against the same immutable policy store as `ingestauth` (§2) |
| 3 | `name` and `environment` in the policy | included, sizes validated; ADR 0010 amended (§11.1) |
| 4 | Upgrade order | backend first; a collector without a policy is not ready; the chart routes `/rum/v1` only after a valid policy (§11.4) |
| 5 | Rate limits across replicas | local per replica, per-key share from the policy; IPv6 by /64; bucket map globally bounded (§7, §7.1) |
| 6 | Sessions across replicas | exact local count over fixed-size hashes; the small over-count across replicas is accepted; no HyperLogLog now (§8.6) |
| 7 | Revoked keys | not published in `keys[]`, counted as unknown; revocation metadata may come later, in a structure that can never authorise (§2) |
| 8 | Requests without `Origin` | kept, for parity: `Origin` is not authentication (§4) |
| 9 | Scrubbing beyond the backend's | parity now (§8.2); separate improvement below |
| 10 | GeoIP | not implemented, not in schema v1 (§8.2) |
| 11 | Browser bundle | CDN at first; no new dependency in this phase (§1) |
| 12 | Unknown keys | never attributed to applications; one bounded global counter (§10.1) |
| 13 | Query-string key | removed (§2) |

Follow-ups, out of this spec:

- Extra scrubbing (log bodies, scope attributes, more URL-bearing attributes): a separate
  improvement, since it changes stored data.
- Dropping `text/plain` (§6), kept only for parity now that no SDK uses `sendBeacon`.
- Backend: consume `report`, send `reportAck` under §10.4, show the global unknown-key counter in
  the setup screen, and move its own rate limiter to trusted proxies only ([ADR10 §10]).

Still open:

- The defaults of §7.1 (connections, requests, bucket cap) are first estimates; confirm them
  with the step 2 load benchmark.

## 14. Tests to port

Table-driven, next to the code ([B] files, adapted to `rumreceiver`):
`impl/gateway_test.go` (auth and CORS, preflight, rate limit, decoding, `event.name`, consumer
failures, Android package names), `impl/otlp_test.go`, `impl/otlp_mobile_test.go`,
`impl/monitor_test.go`, `rumtypes/pii_test.go`, `rumtypes/application_test.go`,
`rumtypes/key_test.go`, `pkg/ratelimit/*_test.go`. Dropped: the query-key cases
(`QueryKeyForBeacon_Accepted`, `QueryKeyOtherOrigin_Forbidden`, `impl/gateway_test.go:111,194`)
become "query key ignored, 401" and "preflight by origin only", and the per-application
unknown-key cases (`impl/gateway_test.go:144-149`) become global-counter cases.

New ones: trusted proxies and IPv6 /64 grouping (§7); every bound of §7.1; the allowed report
fields and bounds (§10.3); the ack protocol, including repeated and unknown acks and values
changed after a report (§10.4); every rejection code of §11.2; revision, cache and floor
handling at start (§11.3); fail closed and readiness (§11.4); the maximum-size policy
integration test (§13, decision 1); and a test that no log line, span or metric of the receiver
contains a key, a verifier, the request URL or a client IP ([G4], [G5]).
