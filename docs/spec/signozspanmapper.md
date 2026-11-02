# signozspanmapper · span attribute mapping

What the `signozspanmapper` processor must do with the configuration the FuseScope backend
pushes for span attribute mapping (phase 5, [`opamp.md`](opamp.md) §5). Clean-room (ADR 0002).
Sources:

- **[be]**: the FuseScope backend (`fuseone-io/fusescope`, read at `c42fdd3db`); paths are
  relative to it and, unless they start otherwise, to `pkg/types/spantypes/`.
- **[obs]**: the reference `ghcr.io/fuseone-io/fusescope-otel-collector:0.145.0` run as a black
  box by [`test/observe/signozspanmapper/`](../../test/observe/signozspanmapper/). Inputs and
  configs are in [`observed/signozspanmapper/cases.json`](observed/signozspanmapper/cases.json),
  what the reference exported in `outputs.json`, and whether it started with each config in
  `startup.jsonl`. `[obs: x]` names the case.
- **[otel]**: the OpenTelemetry specification and the upstream `pdata` library
  (`go.opentelemetry.io/collector/pdata` v1.70.0).

## 1. Role

- The backend lets users and shipped definitions map span attributes onto target attributes,
  mostly the OpenTelemetry `gen_ai.*` conventions [be `../../modules/spanmapper/implspanmapper/fs/definitions/`],
  which its AI observability pages and LLM pricing read [be `../aiobservabilitytypes/keys.go:5-27`].
- It pushes one block, `processors.signozspanmapper`, and never places it in a pipeline
  [be `spanmapperprocessor.go:14,64-76`, `../../modules/spanmapper/implspanmapper/module.go:142-153`];
  the base configuration wires it into the traces pipeline ([`opamp.md`](opamp.md) §6).
- Traces only: the backend only maps span and resource attributes of spans
  [be `spanmapperprocessor.go:12`, `mapper.go:20-28`].

## 2. Configuration

The backend's shape [be `spanmapperprocessor.go:27-52`]:

```yaml
signozspanmapper:
  groups:
    - id: gen_ai.llm                      # group name
      exists_any:                         # the group runs when a key contains one of these
        attributes: [model, llm.input_messages]
        resource: [service.name]          # either list may be omitted
      attributes:                         # rules, in order
        - target: gen_ai.request.model
          context: attribute              # or resource: where the target is written
          sources:                        # in priority order, highest first
            - key: llm.model_name
            - key: resource.ai.model      # "resource." reads a resource attribute
              action: move                # omitted means copy
```

What the backend emits:

- `context` is always `attribute` or `resource` [be `spanmapperprocessor.go:161-169`, `mapper.go:27-28`].
- `action` is omitted for copy and `move` otherwise [be `spanmapperprocessor.go:136-138,154-157`,
  `mapper.go:35-36`].
- Sources are the enabled ones, sorted by priority, highest first; a source read from the
  resource carries the `resource.` prefix [be `spanmapperprocessor.go:140-153`].
- Groups with no enabled condition substring, and rules with no enabled source, are left out,
  because "the collector rejects an empty exists_any and empty sources"; a group left with no
  rule is left out too [be `spanmapperprocessor.go:93-109`]. `groups` may be empty
  [be `testdata/collector_no_groups.yaml`].

What the reference accepts at start (a refused config stops the collector) [obs: `startup.jsonl`]:

| Rule | Refused | Accepted |
|---|---|---|
| C1. Unknown keys | at any level (`extra` in the processor, a group, `exists_any`, a rule, a source) | |
| C2. `groups` | not a list | missing, `null`, `[]`; the whole block `{}` or `null` |
| C3. `id` | | missing, empty, repeated in another group |
| C4. `exists_any` | missing, `{}`, both lists empty; any empty substring (`""`), in either list | only `attributes`, only `resource` |
| C5. Rules (`attributes`) | | missing or empty (the group does nothing) |
| C6. `target` | missing or empty | repeated across rules or groups |
| C7. `sources` | missing or empty | |
| C8. `key` | missing or empty | `resource.` alone (reads the resource key `""`, rule P5) |
| C9. `context` | anything but `attribute`, `resource`, empty or missing (`span`, `Resource`) | empty or missing, as `attribute` |
| C10. `action` | anything but `copy`, `move`, empty or missing (`delete`, `MOVE`) | `copy`; empty or missing, as `copy` |

Values are case-sensitive (C9, C10) [obs: `context_upper`, `action_upper`]. Empty `context` and
`action` behave like `attribute` and `copy` [obs: `target_context_empty`, `target_context_omitted`,
`action_empty`]. An empty group does not stop later groups [obs: `group_without_rules`].

## 3. Processing

Spans are visited in order: resource spans, then scope spans, then spans [obs:
`shared_resource_target`]. For each span, every group runs in configuration order, and inside a
group every rule in order; each step sees the changes of the steps before it, including those
made for earlier spans of the same resource [be `spanmapperengine.go:12-41`; obs:
`condition_on_target_created`, `rules_chained`, `shared_resource_source_move`].

### Group condition

- G1. A group runs for a span when a **key** of the span's attributes contains one of
  `exists_any.attributes` as a substring, or a key of the resource's attributes contains one of
  `exists_any.resource` [be `spanmapperengine.go:34,43-55`; obs: `condition_inside_key`,
  `condition_resource_only`, `condition_attributes_or_resource`].
- G2. Matching is case-sensitive and never looks at values [obs: `condition_case_sensitive`,
  `condition_value_not_key`].
- G3. The span-attribute part is evaluated when the group runs, on the span as the earlier
  groups left it: a key an earlier group added makes it match, a key an earlier group moved
  away no longer does [obs: `condition_on_target_created`].
- G4. The resource part is evaluated **once per resource spans**, on the resource as received,
  before any of its spans is mapped: a resource key added or removed by a rule changes no
  group's resource match, neither for the span that changed it nor for later spans of the same
  resource, in any scope [obs: `shared_resource_condition_current`, `shared_resource_condition_removed`].
  The backend's preview evaluates it per span on the current resource
  [be `spanmapperengine.go:32-36`]; the two agree when each span has its own resource, which
  is how the preview builds its input [be `spanmappersimulator.go:84-99`], so the observed
  behaviour wins (§4).

### Rule

- R1. The target map is the span's attributes for `context: attribute` (or empty, missing), the
  resource's attributes for `context: resource` [be `spanmapperengine.go:58-61`; obs:
  `target_resource`, `target_context_omitted`]. The target name is literal, a `resource.`
  prefix included [obs: `target_prefixed_attribute`].
- R2. If the target key already exists in the target map, the rule does nothing, whatever its
  sources and actions: an existing value is never overwritten and nothing is moved
  [be `spanmapperengine.go:62-64`; obs: `copy_single` span 2, `target_same_as_source_move`,
  `rules_same_target`, `groups_same_target`].
- R3. Otherwise the sources are tried in order; the first one present wins and the rest are
  ignored [be `spanmapperengine.go:66-83`; obs: `priority_first_present`, `priority_move_only_winner`].
- R4. If no source is present, nothing changes [obs: `copy_single` span 1].

### Source

- P1. A key starting with `resource.` names the resource attribute whose key is the rest;
  any other key names a span attribute, with the whole key [be `spanmapperprocessor.go:150-153`,
  `spanmapperengine.go:67-70`; obs: `source_from_resource`, `source_resource_prefix_literal`]. A
  span attribute whose key itself starts with `resource.` cannot be a source.
- P2. Keys are exact: no case folding, and a dotted key never reaches into a map value
  (`a.b` does not read `{a: {b: …}}`) [obs: `condition_case_sensitive`, `dotted_key_not_nested`].
- P3. A source is present when the key exists, whatever its value: an empty string and an empty
  (unset) value count [obs: `values_typed` spans 7-8].
- P4. The value is copied to the target with its type, deep for maps and arrays [obs:
  `values_typed`, `values_typed_each`].
- P5. `resource.` alone reads the resource attribute with the empty key [obs:
  `source_empty_resource_key`].
- P6. With `action: move`, the source attribute is removed after the copy, from the span or the
  resource it was read from [be `spanmapperengine.go:79-81`; obs: `move_single`,
  `source_from_resource_move`, `target_resource_move`]. Only the winning source moves
  [obs: `priority_move_only_winner`]. A resource attribute moved for one span is gone for the
  later spans of that resource [obs: `shared_resource_source_move`].
- P7. Copies into the resource are shared by all spans of the resource: the first span that
  writes a resource target wins, later spans find it present (R2) [obs: `shared_resource_target`].

### Attribute order

Exported attribute order is observable and kept [obs: `move_order`, `move_order_resource`,
`target_resource_move`]:

- O1. A new target is appended after the existing attributes of its map.
- O2. A moved source is removed by moving the **last** attribute of its map into its place, as
  `pcommon.Map.Remove` does [otel `pdata` v1.70.0 `pcommon/map.go:78-89`]. So a move within one
  map leaves the target where the source was (`k1 k2 k3 k4` → `k1 t k3 k4`), and a move out of a
  map moves that map's last attribute into the source's slot (`a b c` → `c b`).

### Everything else

- E1. Span events, links, names, IDs, times, status and scope are untouched, including event
  attributes with a source's key [obs: `events_untouched`].
- E2. No span, scope or resource is added or dropped; with no group (`groups: []`, missing or
  `null`) the data passes unchanged [obs: `groups_empty`; `startup.jsonl`].
- E3. The shipped definitions (`gen_ai.llm`, `gen_ai.agent`, `gen_ai.tool`) applied to the
  backend's own example spans give the outputs of `system_llm`, `system_agent`, `system_tool`
  [obs; inputs and config from be `../../../tests/integration/testdata/simulatorcontract/spanmapper.golden.json`].

## 4. Differences from the backend's preview

The backend previews mappings with its own implementation and checks it against the reference
with one span per resource [be `spanmapperengine.go`, `simulator_corpus_test.go:24-29`]. The
collector follows the reference where they differ:

- the resource part of a group condition (G4).

## 5. Open questions

- The reference's internal telemetry for this processor (metrics, logs) was not observed; the
  collector emits none of its own.
- Whether the reference evaluates the resource condition when the resource spans is reached or
  for all resource spans of a request first cannot be told apart from OTLP: no rule can change
  another resource spans' resource.
- Whether the reference runs `signozspanmapper` in logs or metrics pipelines was not observed;
  the backend uses it for traces only (§1), and so does the collector.
