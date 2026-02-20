---------------------------------------------------------------------

# ENGINE IMPLEMENTATION PLAN V1
Version: implementation_plan_v1_2

---------------------------------------------------------------------
DOCUMENT GOVERNANCE RULES
---------------------------------------------------------------------

This document is part of a version-locked governance system.

Any change to this file requires:
- Version bump (patch increment minimum)
- Entry added to CHANGE LOG section
- Cross-check against the other two governance docs
- Full unittest suite passing before marking COMPLETE tasks

No silent edits allowed.
---------------------------------------------------------------------

Governed by:
- docs/ENGINE_TASK_INVENTORY_V1.md (inventory_v1_4)
- docs/SUFFICIENCY_SPEC_V1.md (sufficiency_spec_v1_1)

Rule: Implement one step at a time. No step begins until prior step passes tests.

---------------------------------------------------------------------
0) GLOBAL ACCEPTANCE CRITERIA (ALL STEPS)
---------------------------------------------------------------------

Every new layer/engine MUST include:
- VERSION constant
- deterministic output (stable sort, no timestamps)
- status: OK | WARN | SKIP | ERROR
- SKIP includes reason_code
- WARN/ERROR include codes list (sorted)
- panel gate flag in available_panels_v1
- pipeline_versions pin(s) if new data pack or math contract
- unit tests + pipeline integration test + determinism repeat test
- repro bundle includes any rule/data files used at runtime

Forbidden:
- modifying ui_contract_v1 without version bump
- modifying structural_snapshot_v1 schema
- modifying graph_v1 schema
- runtime oracle_text parsing
- heuristic drift or non-deterministic scanning

---------------------------------------------------------------------
1) PHASE 3 STEP SEQUENCE
---------------------------------------------------------------------

STEP 1 — engine_requirement_detection_v1
Goal:
- Compute primitive-driven dependency flags from compiled primitive index only.

Inputs:
- primitive_index_by_slot
- slot_ids_by_primitive
- commander_slot_id (optional)

Outputs:
- result.engine_requirement_detection_v1 payload
- panel gate: has_engine_requirement_detection_v1
- pipeline_versions.dependency_signatures_version (new)

Acceptance:
- SKIP path when primitive index missing
- WARN when signatures contain unknown primitive ids
- Determinism repeat test passes

STOP CONDITIONS:
- If primitive ids required by signatures do not exist in taxonomy pack, do NOT invent.
  Emit unknowns and proceed deterministically.

Implementation Notes (2026-02-20):
- files added:
  - api/engine/data/sufficiency/dependency_signatures_v1.json
  - api/engine/dependency_signatures_v1.py
  - api/engine/layers/engine_requirement_detection_v1.py
  - tests/test_engine_requirement_detection_v1.py
  - tests/test_pipeline_engine_requirement_detection_v1.py
- files modified:
  - api/engine/pipeline_build.py
  - api/engine/repro_bundle_export_v1.py
  - tests/test_available_panels_v1.py
  - tests/test_repro_bundle_export_v1.py
  - docs/ENGINE_TASK_INVENTORY_V1.md
  - docs/ENGINE_IMPLEMENTATION_PLAN_V1.md
- tests added:
  - tests/test_engine_requirement_detection_v1.py
  - tests/test_pipeline_engine_requirement_detection_v1.py
- pipeline_versions additions:
  - dependency_signatures_version
- repro bundle additions:
  - rules/dependency_signatures_v1.json
- deterministic warning codes introduced:
  - UNKNOWN_PRIMITIVE_ID_IN_SIGNATURES
  - COMMANDER_SLOT_ID_MISSING
  - ENGINE_REQ_MANA_HUNGRY_UNIMPLEMENTED
  - ENGINE_REQ_SHUFFLE_UNIMPLEMENTED
  - ENGINE_REQ_PERMANENT_TYPE_UNIMPLEMENTED

STEP 2 — engine_coherence_v1
Goal:
- Detect dead slots, primitive concentration, overlap score.

Acceptance:
- Must not use oracle parsing or heuristics.

STEP 3 — mulligan_model_v1
Goal:
- Provide effective_n by checkpoint per spec.

Acceptance:
- Deterministic mapping, version pin.

STEP 4 — substitution_engine_v1
Goal:
- Implement effective_K per spec using bucket_substitutions_v1.json.

Acceptance:
- Deterministic ordering, bounded depth, explicit warnings for unimplemented paths.

STEP 5 — weight_multiplier_engine_v1
Goal:
- Apply weight_rules_v1.json driven by engine requirements.

Acceptance:
- No runtime expression eval; deterministic rule matcher only.

STEP 6 — probability_math_core_v1
Goal:
- Deterministic binomial/hypergeometric with overflow guards and half-up rounding.

Acceptance:
- K_int = floor(effective_K)
- Edge cases covered: K=0, K>=N, n=0, n>N

STEP 7 — probability_checkpoint_layer_v1
Goal:
- Compute probabilities at checkpoints 7/9/10/12 with mulligan adjustments.

STEP 8 — stress_model_definition_v1
Goal:
- Load and validate stress_models_v1.json.

STEP 9 — stress_transform_engine_v1
Goal:
- Apply stress transforms to effective_K and probability floors per spec.

STEP 10 — resilience_math_engine_v1
Goal:
- Compute resilience metrics per spec.

STEP 11 — commander_reliability_model_v1
Goal:
- Commander cast probability baseline and removal survival logic (deterministic, facet-based only).

STEP 12 — sufficiency_summary_v1
Goal:
- Aggregate PASS/WARN/FAIL using required effects, probabilities, stress, resilience, bracket compliance.

Acceptance:
- Missing calibration snapshot => SKIP (per spec)

---------------------------------------------------------------------
2) DOCUMENTATION & TRACKING RULES
---------------------------------------------------------------------

After each step completes:
- Mark the matching item COMPLETE in docs/ENGINE_TASK_INVENTORY_V1.md
- Add a short “Implementation Notes” entry in this plan:
  - files added/modified
  - tests added
  - pipeline_versions additions
  - any deterministic warning codes introduced

---------------------------------------------------------------------
DOCUMENT SYNC CHECKLIST (MANDATORY AFTER EACH STEP)
---------------------------------------------------------------------

When a Phase 3 step is marked COMPLETE, Windsurf MUST:

1) Mark the item COMPLETE in ENGINE_TASK_INVENTORY_V1.md
2) Add Implementation Notes under the step in this file:
   - files added
   - files modified
   - tests added
   - pipeline_versions additions
   - repro bundle additions
3) If math changed:
   - Update SUFFICIENCY_SPEC_V1.md
   - Bump spec version
   - Add change log entry
4) If data pack added:
   - Add to repro bundle list
   - Add to Inventory Section 3
5) Run:
   python -B -m unittest discover -s tests -p "test_*.py" -v
6) Confirm determinism repeat test passes
7) Only then mark COMPLETE.

Failure to perform this checklist invalidates completion status.

---------------------------------------------------------------------
3) REPRO BUNDLE COMPLETENESS CHECKLIST
---------------------------------------------------------------------

Every step that adds a runtime data file must update repro export to include it.

Required rule/data files for Phase 3 (expected):
- rules/dependency_signatures_v1.json
- rules/bucket_substitutions_v1.json
- rules/weight_rules_v1.json
- rules/stress_models_v1.json
- rules/calibration_snapshot_v1.json
- rules/mulligan_assumptions_v1.json

---------------------------------------------------------------------

DRIFT PREVENTION POLICY
---------------------------------------------------------------------

If:
- A Phase 3 runtime file exists but not listed in INVENTORY
- A data file exists but not listed in Inventory Section 3
- A pipeline_versions entry exists but not documented in Spec or Plan

Then:
- STOP implementation
- Add missing item to Inventory
- Add change log entry
- Resume work

---------------------------------------------------------------------

---------------------------------------------------------------------
CHANGE LOG
---------------------------------------------------------------------

Format:

## [version] - YYYY-MM-DD
- Short description of change
- Why it was needed
- What it impacts (inventory/spec/plan/runtime)

Entries must be append-only.
No deletions.

## [implementation_plan_v1_1] - 2026-02-20
- Added document governance rules, mandatory document sync checklist, drift prevention policy, and append-only change log requirements.
- Needed to prevent governance drift and enforce strict, synchronized completion protocol across inventory/spec/plan.
- Impacts inventory/spec/plan governance process (no runtime logic changes).

## [implementation_plan_v1_2] - 2026-02-20
- Recorded Step 1 implementation completion notes (files, tests, pipeline version pin, repro addition, warning codes) and updated governed inventory version.
- Needed to preserve deterministic governance traceability for completed Phase 3 work.
- Impacts inventory/plan/runtime tracking for Phase 3 governance.
