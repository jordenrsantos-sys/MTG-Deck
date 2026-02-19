# MTG Engine — UI Contract v1
Version: ui_contract_v1
Status: FROZEN
Applies To:
- structural_snapshot_v1
- graph_v1
- typed_graph_v1
- snapshot_preflight_v1
- canonical resolution layer
- build pipeline output

This document defines the stable UI read-only contract.

The UI MUST NOT:
- Modify engine schemas
- Infer missing fields
- Parse oracle text
- Add heuristics
- Mutate engine output
- Recompute engine logic client-side

The UI may ONLY render fields defined below.

---

# 1. Top-Level BuildResponse Contract

POST /build returns:

{
  engine_version: string
  ruleset_version: string
  bracket_definition_version: string
  game_changers_version: string
  db_snapshot_id: string
  profile_id: string
  bracket_id: string
  status: "OK" | "ERROR" | string
  deck_size_total: number
  deck_status: string | null
  cards_needed: number | null
  cards_to_cut: number | null
  build_hash_v1: string
  graph_hash_v2: string | null
  unknowns: UnknownV1[]
  result: BuildResultV1
}

UI may render all of the above.

UI MUST treat unknown fields as opaque.
Unknown/additive top-level fields are allowed and MUST be treated as opaque.
Unknown/additive result fields are allowed and MUST be treated as opaque.
Unknown fields MUST NOT cause contract compliance FAIL. They may be logged as WARN.

---

# 2. BuildResultV1 Contract

result: {

  ui_contract_version: "ui_contract_v1"

  available_panels_v1: { [panel_flag: string]: boolean }
  ui_index_v1?: object

  canonical_slots_all?: CanonicalSlotV1[]
  unknowns_canonical?: CanonicalUnknownV1[]

  structural_snapshot_v1?: StructuralSnapshotV1

  graph_v1?: GraphV1
  graph_nodes?: GraphNodeV1[]
  graph_edges?: GraphEdgeV1[]

  graph_typed_edges_total?: number
  graph_typed_nodes_total?: number
  graph_typed_components_total?: number

  snapshot_preflight_v1?: SnapshotPreflightReportV1

  scoring_summary_v1?: object

}

All fields are OPTIONAL except ui_contract_version and available_panels_v1.
result MUST always exist.
result MUST always include:
- ui_contract_version: "ui_contract_v1"
- available_panels_v1 (may be empty map {})
- ui_index_v1 (may be empty object {})

UI MUST gate panels using available_panels_v1.

---

# 3. CanonicalSlotV1

{
  slot_id: string
  input: string
  resolved_name: string | null
  resolved_oracle_id: string | null
  status: string
  codes?: string[]
}

Stable rules:
- slot_id is deterministic
- input is raw input string
- resolved_oracle_id is immutable for snapshot
- status is string enum but UI must not hardcode enum values

---

# 4. StructuralSnapshotV1

Opaque object.

UI MAY:
- Render JSON
- Render primitive totals
- Render commander dependency signal
- Render required primitives

UI MUST NOT:
- Derive new structural rules
- Recalculate primitives

Schema stability rule:
Fields may be added.
Existing fields may NOT be renamed or removed without:
- Bumping ui_contract_version
- Bumping structural_snapshot version

---

# 5. GraphV1

graph_v1: {
  candidate_edges_total?: number
  bounded?: boolean
  stats?: object
}

graph_nodes: [
  {
    node_id: string
    slot_id: string
    oracle_id: string
  }
]

graph_edges: [
  {
    source: string
    target: string
    edge_type: string
  }
]

Rules:
- node_id deterministic
- slot_id matches canonical slot_id
- edge_type opaque string

UI may:
- Render tables
- Count totals
- Show components summary

UI must NOT:
- Infer combo logic
- Detect loops
- Validate legality

---

# 6. SnapshotPreflightReportV1

{
  status: "OK" | "STALE" | "ERROR"
  thresholds?: object
  counts?: object
  rates?: object
  notes?: string[]
}

UI must render entire object raw.
UI must not interpret thresholds.

---

# 7. UnknownV1

{
  code: string
  message: string
  slot_id?: string
}

UI must display unknowns prominently.

Unknown codes are engine-defined.

---

# 8. Hash Guarantees

build_hash_v1:
- Deterministic for identical inputs + snapshot + profile + bracket

graph_hash_v2:
- Deterministic for identical graph state

UI may display and copy.
UI must not recompute.

---

# 9. Stability Rules

Any of the following require:
- ui_contract_version bump
- Documentation update
- UI compatibility audit

Breaking changes include:
- Renaming fields
- Removing fields
- Changing types
- Changing determinism behavior

Additive changes are allowed.
Unknown/additive fields are WARN-only for contract validation and MUST NOT cause FAIL.

---

# 10. Windsurf Verification Requirements

After implementing UI harness, Windsurf MUST:

1. Run a build request.
2. Print:
   - Full raw JSON response
   - ui_contract_version
   - available_panels_v1
   - build_hash_v1
   - graph_hash_v2
   - snapshot_preflight_v1 (if present)
   - structural_snapshot_v1 keys
   - graph_v1 stats summary

3. Confirm:
   - No client-side transformation
   - No schema mutation
   - No computed primitives
   - No oracle parsing

Windsurf must output:

UI_CONTRACT_V1_VALIDATION_REPORT:
- ui_contract_version detected:
- missing required fields:
- unknown fields encountered:
- panels rendered:
- deterministic hash observed:
- contract compliance: PASS | FAIL

The entire validation output must be pasted back to ChatGPT for audit before Phase 3 begins.

---

# 11. Prohibited UI Behaviors

The UI must never:
- Compute combos
- Parse oracle text
- Merge graph edges
- Guess bracket legality
- Apply power level heuristics
- Reorder engine arrays unless explicitly sorted by engine

All ordering must preserve engine order.

---

END OF CONTRACT
