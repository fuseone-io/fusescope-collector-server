# RUM receiver · contract with browsers, apps and the FuseScope backend

What `rumreceiver` must do to replace the FuseScope backend's RUM gateway unchanged for the SDKs
and the setup screen ([ADR 0006](../adr/0006-single-ingress.md), FuseScope ADR 0010 §6), and the
shape of the OpAMP ingest policy and ingest report it depends on. Status: specification only;
no code yet (issue [#13](https://github.com/fuseone-io/fusescope-collector-server/issues/13),
step 1).

## 0. Sources

Every rule cites one of:

- **[B]** the FuseScope backend, `fuseone-io/fusescope` at `6c8991257` (`origin/main`,
  2026-11-02). Paths are relative to it; `impl/` is
  `pkg/modules/rumapplication/implrumapplication/` and `rumtypes/` is `pkg/types/rumtypes/`.
- **[ADR3]**, **[ADR9]**, **[ADR10]**: FuseScope `docs/adr/0003-real-user-monitoring.md`,
  `0009-fusescope-collector.md`, `0010-single-ingress.md` at the same commit.
- **[SDK]** `fuseone-io/fusescope-sdk` at `2e16765`, `packages/browser/README.md`;
  **[SDK-A]** `fuseone-io/fusescope-sdk-android` at `5296f46`, `README.md`.
- **[G1]**–**[G5]**: the security gates of [ADR 0006](../adr/0006-single-ingress.md#security-gates).
- **[OTLP]**: the OTLP specification, OTLP/HTTP (response codes, `Retry-After`).

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
  `pkg/apiserver/signozapiserver/rum.go:58-65`; [SDK] lines 181–194; [SDK-A] lines 234–238).
  Other methods on these paths: 405.
- Traces carry an OTLP `ExportTraceServiceRequest`, logs an `ExportLogsServiceRequest`
  ([B] `impl/gateway_otlp.go:30-80`).
- Every response carries `Vary: Origin` ([B] `impl/gateway.go:76`).
- The order of checks is the backend's ([B] `impl/gateway.go:75-177`): preflight; policy
  present (§11.3); key; origin; rate limits; content type; body; decoding; package names;
  processing; pipeline. A request refused at one step is not examined further.
- **Optional, open (§13):** `GET`/`HEAD` `/rum/v1/sdk/browser/{version}/fusescope.min.js` and
  `.map` serve the embedded browser bundle ([ADR10] line 99). Only the embedded version is
  served, anything else is 404; headers `Content-Type` (`application/javascript` or
  `application/json`), `Cache-Control: public, max-age=31536000, immutable`,
  `Access-Control-Allow-Origin: *`, `X-Content-Type-Options: nosniff`, `ETag` = the SRI value
  ([B] `pkg/modules/rumapplication/browsersdk/browsersdk.go:62-76`). No key is required.

## 2. Key transport and validation

- The key is read from the `FuseScope-RUM-Key` header, else from the `fusescope_rum_key` query
  parameter, which exists because `navigator.sendBeacon` cannot set headers ([B]
  `impl/gateway.go:17-22,207-212`).
- A well-formed key starts with `rum_pub_`, is longer than the prefix and at most 128 bytes
  ([B] `rumtypes/key.go:18-25,147-149`). Anything else is refused as invalid without a lookup
  ([B] `impl/module_keys.go:111-114`).
- The verifier is the lowercase hex SHA-256 of the key as sent ([B]
  `rumtypes/key.go:142-145`), looked up in the ingest policy (§11). No per-request call to the
  backend ([ADR10] §4, line 71).
- A key absent from the policy is unknown. A key whose `expiresAt` is not after now is
  refused, attributed to its application ([B] `rumtypes/key.go:151-167`,
  `impl/module_keys.go:128-133`). Revoked keys are not in the policy (it lists usable keys
  only, [ADR10] line 62), so they are unknown.
- Missing, malformed, unknown or expired: **401** ([B] `impl/gateway.go:89-94`,
  `pkg/http/render/render.go:103`; test `impl/gateway_test.go:108-109`).
- Only keys of kind `public` are accepted on these routes; secret and runner keys are refused
  like unknown keys ([ADR10] §2 table, line 50: public keys for "RUM traces and logs, on the RUM
  routes only").
- Keys are always enforced here: `rumreceiver` has no `permissive` or `warn` mode, as the
  backend gateway has none ([B] `impl/gateway.go:89-94`; [ADR10] §5 modes apply to
  `ingestauth`).
- **[G4]** The key, the full request URL (its query may hold the key) and the
  `FuseScope-RUM-Key` header are never logged, never recorded in the collector's own spans,
  metrics or logs, and never reported. The backend already keeps the RUM routes out of its
  request logs ([B] `pkg/apiserver/config.go:51-60`). Only the key ID and its verifier may
  appear anywhere.

## 3. Application lookup

- A key belongs to exactly one RUM application ([B] `rumtypes/key.go:55-67`; [ADR3] lines
  112–113). The application, with its org, name, environment, platform, origins, package names
  and PII policy, comes from the ingest policy (§11.1).
- Platforms are `web` and `android`; `ios` is reserved and refused at creation ([B]
  `rumtypes/application.go:30-40,344-352`). A policy entry with another platform is dropped
  (§11.2), so its keys are refused.

## 4. CORS and preflight

- **Preflight** (`OPTIONS`) is allowed only with a non-empty `Origin` and
  `Access-Control-Request-Method: POST` ([B] `impl/gateway.go:182-192`):
  - with `fusescope_rum_key` in the query, when that key is valid and its application allows
    the origin;
  - without it, when any application in the policy allows the origin. The preflight carries no
    header key, so the POST is checked again against the key's application ([B]
    `impl/gateway.go:179-181`).
- An allowed preflight answers **204** with `Access-Control-Allow-Origin: <origin>`,
  `Access-Control-Allow-Methods: POST`,
  `Access-Control-Allow-Headers: content-type, content-encoding, fusescope-rum-key` and
  `Access-Control-Max-Age: 86400` ([B] `impl/gateway.go:24-27,199-204`; [SDK] lines 190–193).
  Any other preflight: **403** ([B] `impl/gateway.go:194-197`; tests
  `impl/gateway_test.go:190-194`).
- **POST with an `Origin`:** the key's application must allow it, else **403**, reason
  `origin_not_allowed` ([B] `impl/gateway.go:96-100`). When allowed,
  `Access-Control-Allow-Origin` echoes the origin ([B] `impl/gateway.go:102`). Responses
  refused before this check (key errors) carry no `Access-Control-Allow-Origin`.
- **POST without an `Origin`** skips the origin check ([B] `impl/gateway.go:96`; test
  `HeaderKeyNoOrigin_Accepted`, `impl/gateway_test.go:110`). This is how mobile apps send
  ([SDK-A] line 239). See §13.
- **Origin matching** ([B] `rumtypes/application.go:172-203`): both sides lower-cased and parsed
  as URLs; a candidate without scheme or host never matches (so `Origin: null` is refused);
  `*` matches any origin; otherwise scheme and port must be equal, and the host must equal the
  entry's host, or, for an entry `*.example.com`, end in `.example.com` and be longer than it
  (subdomains only, never the apex). Tests: `impl/gateway_test.go:112-114`,
  `rumtypes/application_test.go`.
- Allowed origins are `*` or `scheme://host[:port]` with `http` or `https`, no path, query,
  fragment or user info, and `*` only as a leading `*.` label ([B]
  `rumtypes/application.go:418-434`); at most 50 entries ([B]
  `rumtypes/application.go:19,395-399`). Entries that fail these rules are dropped from the
  policy (§11.2).
- No credentials are involved: `Access-Control-Allow-Credentials` is never sent ([SDK] line 194).

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
  - `application/json`, `text/plain`: OTLP JSON. `text/plain` exists because `sendBeacon` sends
    only CORS-safelisted types ([B] `impl/gateway_otlp.go:126-127`; test
    `QueryKeyForBeacon_Accepted`, `impl/gateway_test.go:111`);
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

## 7. Rate limits

- Two token buckets, checked after the key and origin and before the body is read ([B]
  `impl/gateway.go:139-145`):
  - per key: every client of one key;
  - per key and client IP: identity `<key id>|<client ip>`.
- Bucket semantics ([B] `pkg/ratelimit/limiter.go:10-65`): a new identity starts with a full
  bucket of `burst` tokens; tokens refill at `rate` per second up to `burst`; a request takes
  one token or is refused; buckets idle for more than 10 minutes are dropped.
- Over either limit: **429** with `Retry-After: 1`, reason `rate_limited` ([B]
  `impl/gateway.go:143-144`, `pkg/http/render/render.go:117-118,130-135`; test
  `impl/gateway_test.go:216`).
- **Differs (per-replica share).** The per-key limit is the key's `limit` in the ingest policy,
  which is already this replica's share of the key's limit ([ADR10] §4, line 73). When the
  policy gives none, the backend's default applies unshared: 500/s, burst 1000 ([B]
  `pkg/modules/rumapplication/config.go:86`). The per-key-and-IP limit is local configuration,
  default 20/s, burst 60 ([B] `config.go:87`), and is enforced per replica (§13).
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
- The client IP is held in memory for the bucket only, never stored, logged or reported ([B]
  `config.go:64`; [ADR3] line 148; [G5]).

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

Attribute names: [B] `rumtypes/attributes.go:3-19`.

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
- Not scrubbed by the backend, kept as is here (§13): log bodies, scope attributes, non-string
  values, other attributes.
- `geoLookup`, `requireConsent` and `hashUserId` are part of the policy ([B]
  `rumtypes/pii.go:23-25`) but are not applied by the gateway: consent and user-ID hashing are
  SDK-side, and no geo lookup exists in the backend at this commit (`git grep GeoLookup` finds
  only the type). The receiver does no geo lookup (§13).

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

- sessions: distinct non-empty `session.id` attribute values of spans and log records;
- views: spans named `view`;
- errors: log records whose `event.name` is `exception` (after §8.4);
- vitals: log records whose `event.name` is `browser.web_vital` or `rum.vital`;
- SDK: per resource, `telemetry.distro.name`/`telemetry.distro.version` when the name is set,
  else `telemetry.sdk.name`/`telemetry.sdk.version`; the last resource with one wins ([B]
  `impl/otlp.go:106-112`).

## 9. Into the pipeline and status codes

The backend re-encodes the processed request as protobuf and forwards it to the collector's
OTLP/HTTP receiver ([B] `impl/gateway_otlp.go:199-223`). Here it goes to the next consumer of the
receiver's pipeline, with no extra hop ([ADR10] §6, line 94). The data is counted in the report
(§10) only once the consumer accepted it ([B] `impl/gateway.go:166-170`).

| Condition | Status | Reason counted | Source |
|---|---|---|---|
| No ingest policy (fail closed) | 503, `Retry-After: 5`; preflight 403 | none | [G1], §11.3 |
| Key missing, malformed, unknown, expired, wrong kind | 401 | `bad_key` / `unknown_key` (§10.1) | §2 |
| Origin not allowed (incl. any `Origin` on Android) | 403 | `origin_not_allowed` | §4, §5 |
| Package name missing or not allowed | 403 | `package_not_allowed` | §5 |
| Rate limited | 429, `Retry-After: 1` | `rate_limited` | §7 |
| Unsupported type or encoding, bad gzip, bad OTLP | 400 | `invalid_payload` | §6 |
| Body over the limit | 413 | `too_large` | §6 |
| Consumer error, permanent | 400 | none | **Differs**, below |
| Consumer error, not permanent (memory limiter, full queue, exporter backpressure) | 503, `Retry-After: 5` | none | **Differs**, below |
| Accepted | 200, empty export response | (counted as ingest) | §6 |

- The backend maps its forward: unreachable collector 503, collector 429 or 503 kept, other
  4xx 400, 5xx 502; each retryable one with `Retry-After: 5` ([B]
  `impl/gateway_otlp.go:23,199-242`; tests `impl/gateway_test.go:334-338`). **Differs:** with no
  forward, the receiver maps consumer errors the way upstream OTLP receivers do: permanent
  (`consumererror.IsPermanent`) 400, anything else 503 with `Retry-After: 5`. OTLP clients retry
  429, 502, 503 and 504 and never 400 [OTLP], so client retry behaviour is unchanged.
- Error bodies are JSON with the error code (for example `rum_origin_not_allowed`), as today
  ([B] `impl/gateway.go:29-37`, `pkg/http/render/render.go:91-140`). They never echo the key.

## 10. Setup status and the OpAMP ingest report

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

Rejection attribution ([B] `impl/gateway.go:105-136`, `rumtypes/status.go:50-52`; tests
`impl/gateway_test.go:144-149`):

- a refused key that names an application (expired): `bad_key` on it;
- an unknown or malformed key with an `Origin`: `unknown_key` on every application, of any org,
  that allows that origin ([B] `impl/module.go:186-213`); not counted when none does, when the
  key is missing, or when there is no `Origin`;
- every other refusal: its reason on the key's application.

Today each backend replica keeps this in memory and answers only for itself ([B]
`rumtypes/status.go:24-26`). [ADR10] §6 (line 98) replaces that with a report from each
collector, aggregated by the backend across replicas.

### 10.2 Report content

Every 60 seconds ([ADR10] line 98) the receiver sends one custom message, capability
`io.fuseone.fusescope.ingest.v1`, type `report` (§11.1), JSON:

```json
{
  "policyVersion": 42,
  "generatedAt": "2026-11-03T12:01:00Z",
  "windowSeconds": 300,
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
  "dropped": { "applications": 0, "keys": 0 }
}
```

- `counts`, `sdk` and `rejections` are this replica's view of the last `windowSeconds`, kept in
  one-minute buckets as the backend does ([B] `impl/monitor.go:12-17,98-144,179-192`). The
  backend sums counts and rejection counts over the latest report of each replica and keeps the
  newest `lastOrigin`/`lastSeenAt` and SDK. Field names are the backend's
  (`rumtypes/status.go:37-59`) so it can reuse its types.
- `firstSeenAt` is sent for an application the first time this process accepts data for it,
  and again in every report until one carrying it was handed to the OpAMP client; the backend
  keeps the earliest ([B] `impl/monitor.go:76-80,146-168`).
- `keys` lists keys used since the last report sent, with the latest use; the same
  hand-over-or-keep rule applies ([B] `impl/monitor.go:72-74,146-158`).
- `policyVersion` is the version of the policy the receiver holds, which the backend compares
  to decide whether to push again ([ADR10] line 64). It is not in the agent description,
  whose changes make the backend recompute the configuration ([`opamp.md`](opamp.md) §3).

### 10.3 Bounds and PII **[G5]**

- **No PII.** No IP address, user agent, URL, session ID, user ID, attribute value or payload
  content. `sessions` is a count; session IDs never leave the process. **Differs:** the backend
  counts distinct sessions in one process; summing replicas over-counts a session whose requests
  reached several replicas (§13).
- **SDK:** name at most 64 bytes, version at most 32; longer values are reported empty, and the
  backend then shows `unknown`.
- **`lastOrigin`, Differs.** Set only for the three reasons the setup screen shows it for:
  - `origin_not_allowed`, `unknown_key`: the request's `Origin`, only when it parses as an
    `http`/`https` `scheme://host[:port]` origin of at most 256 bytes, lower-cased; otherwise
    empty;
  - `package_not_allowed`: the refused `app.package.name`, only when it matches the package
    name pattern of §5 and is at most 256 bytes; otherwise empty.

  For every other reason it is empty. The backend stores the origin for all reasons ([B]
  `impl/gateway.go:107-112`) and returns it through the API.
- **Counts:** at most 10 000 distinct sessions per application per minute bucket ([B]
  `impl/monitor.go:14,58-62`); rejections at most one entry per reason, seven reasons ([B]
  `rumtypes/status.go:12-20`).
- **Entries:** at most 1 000 applications and 10 000 keys per report ([B]
  `impl/monitor.go:15` bounds pending key usage at 10 000). Applications are kept by most recent
  activity, keys by most recent use; the rest are counted in `dropped` and, for `keys` and
  `firstSeenAt`, kept for the next report.
- **Size:** the encoded report is at most 256 KiB; when over, keys are dropped first, then the
  least recently active applications, until it fits.
- **[G4]:** keys appear only by ID; no verifier is reported either.

## 11. The ingest policy

### 11.1 Message

An OpAMP custom message ([ADR10] §3, lines 62–65; [ADR9] line 80): the collector declares the
custom capability `io.fuseone.fusescope.ingest.v1`; the backend sends messages of type `policy`
(server to agent); the collector sends type `report` (§10). The `policy` data is a full
snapshot, never a delta, pushed on any change, when the collector's `policyVersion` is behind,
and every 5 minutes regardless ([ADR10] line 64). It is sent only to collectors of role `ingress`
([ADR10] line 62; [ADR9] line 79).

Encoding: JSON (§13). Fields, from [ADR10] line 62 and what §§2–8 need:

| Field | Type | Meaning |
|---|---|---|
| `version` | uint64 | the snapshot version; echoed as `policyVersion` in reports |
| `issuedAt` | RFC 3339 | when the backend built it |
| `keys[]` | | every usable key: active, or expiring within its rotation overlap ([B] `rumtypes/key.go:151-167`) |
| `keys[].id` | UUID | key ID, stamped as `fusescope.ingest.key_id` |
| `keys[].sha256` | 64 lowercase hex | verifier (§2) |
| `keys[].orgId` | UUID | org, for auth data |
| `keys[].kind` | `secret` \| `public` \| `runner` | [ADR10] §2 |
| `keys[].signals` | list of `traces`, `metrics`, `logs`, `profiles`, `artifacts` | for `public`: `traces`, `logs` |
| `keys[].expiresAt` | RFC 3339 or null | end of a rotation overlap |
| `keys[].limit` | `{rate, burst}` or absent | this replica's share of the key's limit (§7) |
| `keys[].applicationId` | UUID | `public` keys only: their RUM application |
| `rumApplications[]` | | |
| `rumApplications[].id`, `.orgId` | UUID | |
| `rumApplications[].name`, `.environment` | string | stamping (§8.1), at most 128 and 64 bytes ([B] `rumtypes/application.go:17-18`) |
| `rumApplications[].platform` | `web` \| `android` | |
| `rumApplications[].allowedOrigins` | list | web only (§4) |
| `rumApplications[].allowedPackageNames` | list | android only (§5) |
| `rumApplications[].piiPolicy` | `{dropQuery, queryAllowlist, urlAllowlist, …}` | §8.2; other fields ignored |

`name` and `environment` go beyond the list in [ADR10] line 62, which names origins, package
names, platform and PII policy; stamping (§8.1) needs them.

### 11.2 Validation: restrict only

- The policy can only restrict what is accepted ([ADR 0006](../adr/0006-single-ingress.md)
  decision 3). It never adds or changes receivers, exporters, extensions, endpoints or
  pipelines, and it is never part of the effective configuration the collector reports.
- A message that does not parse, or is larger than `max_policy_size` (local configuration,
  default 16 MiB; [ADR10] line 64 estimates 2 MB at 10 000 keys), is rejected whole: the policy
  held stays in force, and the rejection is logged without content.
- Within a parsed policy, an entry that fails validation is dropped, which only refuses more:
  a key with a malformed verifier, an unknown kind, or a `public` key whose application is
  missing or dropped; an application with an unknown platform, invalid origins or package
  names (§4, §5), or more than 50 entries in a list. Dropped entries are counted in a metric,
  never logged with their values.
- A valid policy replaces the one held, whatever its `version`: the backend is the authority,
  and the policy can only restrict.

### 11.3 Cache and fail closed

- **Cache** ([ADR10] line 74): every accepted policy is written to
  `<storage>/ingest-policy.json`, mode `0600`, atomically (temporary file renamed over the old
  one, as [ADR 0008](../adr/0008-opamp-remote-config.md) point 4 does for pushes). It holds the
  policy as received: verifiers only, never a key, since the message carries none. It is never
  logged or reported.
- **At start** the cache is read and validated like a pushed policy (§11.2) and used until the
  backend pushes. A lost OpAMP connection keeps the policy held in use ([ADR10] line 74).
- **[G1] Fail closed.** With no policy held, none received and none cached, or a cache that
  cannot be read or validated, `rumreceiver` refuses every request: POST with **503** and
  `Retry-After: 5`, preflight with **403**. Nothing is counted in the report, since no
  application is known. `ingestauth` refuses likewise, in every mode. **Differs** from
  [ADR10] line 75, which accepts in `permissive` and `warn`.
- **Staleness** ([ADR10] line 74): the policy's age is exported as a metric. An optional
  `max_staleness`, with no default, makes the receiver refuse with 503 once the policy is older.
- Revocation reaches a connected collector within 10 s p95 ([ADR10] line 65); during an outage,
  revoked keys keep working from the cache until the connection returns or `max_staleness`
  passes.

## 12. Deliberate differences from the backend gateway

| # | Backend at `6c8991257` | Here | Why |
|---|---|---|---|
| 1 | Key lookup in the database with a 30 s cache; first request after start always answers | Keys from the ingest policy; with no policy, every request refused (503) | [G1], [ADR10] §3–4 |
| 2 | `X-Forwarded-For` first address, from any peer when `trust_forwarded_for` is on (`pkg/ratelimit/ratelimit.go:16-31`) | Only from peers in `trusted_proxies` (default none), walked right to left | [G2] |
| 3 | Per-key limit in each backend replica, full value | Policy's per-replica share | [ADR10] §4 |
| 4 | Forward to the collector, status mapped from its HTTP answer | Next consumer; permanent errors 400, others 503 | no extra hop, [ADR10] §6 |
| 5 | Setup status in memory per backend replica, `lastOrigin` kept for every reason | Report over OpAMP every minute; `lastOrigin` only for the three reasons the UI shows, validated and bounded; no session IDs | [ADR10] §6, [G5] |
| 6 | Revoked or expired keys known to the database, counted as `bad_key` on their application | Revoked keys absent from the policy, counted as `unknown_key` by origin; expired ones still `bad_key` while listed | [ADR10] line 62 lists usable keys only |
| 7 | No `fusescope.ingest.key_id`, no org in auth data | Both stamped | [ADR10] §4 |
| 8 | Unknown-key preflight and attribution read applications of all orgs from the database | Same rule, from the policy | [B] `impl/module.go:186-213` |

## 13. Open questions

1. **Policy encoding and limits.** JSON as proposed, or protobuf? Confirm `max_policy_size`
   (16 MiB) against the OpAMP WebSocket message limits of both sides, and the report bounds of
   §10.3 (1 000 applications, 10 000 keys, 256 KiB) against the backend's needs.
2. **Who validates RUM keys:** `rumreceiver` against the shared policy (as specified), or
   `ingestauth` with a RUM key extractor (header or query)? Either way one policy store.
3. **Per-client-IP limit across replicas.** It stays a local, per-replica limit; with N replicas
   behind a round-robin balancer a client gets up to N times it. Accept, or have the backend
   send a share as for keys ([ADR10] open question on shared counters)? Bucket IPv6 clients by
   /64? Cap the number of buckets (the backend's limiter grows with distinct identities until
   the 10-minute sweep)?
4. **Sessions across replicas.** Summed per-replica distinct counts over-count; a mergeable
   sketch (HyperLogLog) would fix it without sending IDs. Needed for the setup screen?
5. **Revoked keys** (§12 row 6): should the policy list recently revoked keys for a while, so
   the screen keeps saying `bad_key` on the right application?
6. **Requests without `Origin` for web applications** are accepted (§4). Origins are a browser
   control and any non-browser client can forge them, so this is not a hole the backend closes
   either; confirm it stays.
7. **Unscrubbed fields:** log bodies and scope attributes are not scrubbed today (§8.2). Extend
   the scrubbing, which would change stored data, or keep parity?
8. **`geoLookup`:** stored but not implemented by the backend. Implement in the receiver (needs a
   GeoIP database and its licence, ADR 0005) or drop the flag?
9. **Browser bundle** (§1): serve it from the receiver ([ADR10] line 99) or rely on the CDN
   ([ADR10] open question)? Embedding the `@fusescope/browser` build (Apache-2.0) is a new
   dependency of the build, to be approved first.
10. **Cross-org attribution** of unknown keys (§10.1): an unknown key from origin X is counted
    on every org's application allowing X. Keep, or restrict to one org when the collector
    serves several?
11. **Upgrade order** (ADR 0006 consequences): with fail closed, the backend must publish the
    policy before `ingestauth` reaches an install, including in-cluster agents on the internal
    `permissive` listener. Does the chart gate on it?
12. **Query-string keys at the edge:** `fusescope_rum_key` in the URL can land in the access logs
    of a load balancer or ingress controller in front of the collector. Document it for
    operators, or ask the SDK to stop using the query parameter (the browser SDK already sends
    the header with `fetch(…, { keepalive: true })`, [SDK] lines 187–189)?

## 14. Tests to port

Table-driven, next to the code ([B] files, adapted to `rumreceiver`):
`impl/gateway_test.go` (auth and CORS, unknown-key attribution, preflight, rate limit,
decoding, `event.name`, consumer failures, Android package names), `impl/otlp_test.go`,
`impl/otlp_mobile_test.go`, `impl/monitor_test.go`, `rumtypes/pii_test.go`,
`rumtypes/application_test.go`, `rumtypes/key_test.go`, `pkg/ratelimit/*_test.go`. New ones for
§7 trusted proxies, §10.3 bounds and PII, §11 validation, cache and fail closed, and a test that
no log line, span or metric of the receiver contains the key, the query string or the client IP
([G4], [G5]).
