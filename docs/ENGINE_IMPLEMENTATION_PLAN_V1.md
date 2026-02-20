---------------------------------------------------------------------

# ENGINE IMPLEMENTATION PLAN V1
Version: implementation_plan_v1_16

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
- docs/ENGINE_TASK_INVENTORY_V1.md (inventory_v1_18)
- docs/SUFFICIENCY_SPEC_V1.md (sufficiency_spec_v1_15)

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

Implementation Notes (2026-02-20):
- requirements clarification completed first:
  - Added explicit engine_coherence_v1 contract to docs/SUFFICIENCY_SPEC_V1.md (sufficiency_spec_v1_3)
  - Defined formulas, status policy, ordering rules, and output schema before runtime implementation
- files added:
  - api/engine/layers/engine_coherence_v1.py
  - tests/test_engine_coherence_v1.py
  - tests/test_pipeline_engine_coherence_v1.py
- files modified:
  - api/engine/pipeline_build.py
  - tests/test_available_panels_v1.py
  - tests/test_schema_freeze_guard.py
  - docs/ENGINE_TASK_INVENTORY_V1.md
  - docs/ENGINE_IMPLEMENTATION_PLAN_V1.md
  - docs/SUFFICIENCY_SPEC_V1.md
- tests added:
  - tests/test_engine_coherence_v1.py
  - tests/test_pipeline_engine_coherence_v1.py
- pipeline_versions additions:
  - engine_coherence_version
- deterministic warning codes introduced:
  - DEAD_SLOTS_PRESENT
- specification/test alignment clarification (2026-02-20):
  - overlap_score definition in SUFFICIENCY_SPEC_V1.md promoted to authoritative pairwise Jaccard wording
  - explicit rule added: graph structure must NOT influence overlap_score in v1
  - governance and tests updated to guard against graph-based reinterpretation
  - runtime math logic and output schema unchanged

STEP 3 — mulligan_model_v1
Goal:
- Provide effective_n by checkpoint per spec.

Acceptance:
- Deterministic mapping, version pin.

Implementation Notes (2026-02-20):
- requirements clarification completed first:
  - Added explicit mulligan_model_v1 contract and fixed-path data pack schema to docs/SUFFICIENCY_SPEC_V1.md (sufficiency_spec_v1_5)
  - Defined deterministic checkpoint/policy ordering and SKIP/OK status behavior before runtime implementation
- files added:
  - api/engine/data/sufficiency/mulligan_assumptions_v1.json
  - api/engine/mulligan_assumptions_v1.py
  - api/engine/layers/mulligan_model_v1.py
  - tests/test_mulligan_model_v1.py
  - tests/test_pipeline_mulligan_model_v1.py
- files modified:
  - api/engine/pipeline_build.py
  - api/engine/repro_bundle_export_v1.py
  - tests/test_available_panels_v1.py
  - tests/test_repro_bundle_export_v1.py
  - tests/test_schema_freeze_guard.py
  - docs/ENGINE_TASK_INVENTORY_V1.md
  - docs/ENGINE_IMPLEMENTATION_PLAN_V1.md
  - docs/SUFFICIENCY_SPEC_V1.md
- tests added:
  - tests/test_mulligan_model_v1.py
  - tests/test_pipeline_mulligan_model_v1.py
- pipeline_versions additions:
  - mulligan_model_version
- repro bundle additions:
  - rules/mulligan_assumptions_v1.json

STEP 4 — substitution_engine_v1
Goal:
- Implement effective_K per spec using bucket_substitutions_v1.json.

Acceptance:
- Deterministic ordering, bounded depth, explicit warnings for unimplemented paths.

Implementation Notes (2026-02-20):
- requirements clarification completed first:
  - Added explicit substitution_engine_v1 contract and fixed-path data pack schema to docs/SUFFICIENCY_SPEC_V1.md (sufficiency_spec_v1_6)
  - Defined effective_K clamp policy, K_int=floor policy, deterministic ordering, and conditional requirement-flag behavior before runtime implementation
- files added:
  - api/engine/data/sufficiency/bucket_substitutions_v1.json
  - api/engine/bucket_substitutions_v1.py
  - api/engine/layers/substitution_engine_v1.py
  - tests/test_substitution_engine_v1.py
  - tests/test_pipeline_substitution_engine_v1.py
- files modified:
  - api/engine/pipeline_build.py
  - api/engine/repro_bundle_export_v1.py
  - tests/test_available_panels_v1.py
  - tests/test_repro_bundle_export_v1.py
  - tests/test_schema_freeze_guard.py
  - docs/ENGINE_TASK_INVENTORY_V1.md
  - docs/ENGINE_IMPLEMENTATION_PLAN_V1.md
  - docs/SUFFICIENCY_SPEC_V1.md
- tests added:
  - tests/test_substitution_engine_v1.py
  - tests/test_pipeline_substitution_engine_v1.py
- pipeline_versions additions:
  - substitution_rules_version
  - substitution_engine_version
- repro bundle additions:
  - rules/bucket_substitutions_v1.json

STEP 5 — weight_multiplier_engine_v1
Goal:
- Apply weight_rules_v1.json driven by engine requirements.

Acceptance:
- No runtime expression eval; deterministic rule matcher only.

Implementation Notes (2026-02-20):
- requirements clarification completed first:
  - Added explicit weight_multiplier_engine_v1 contract and fixed-path data pack schema to docs/SUFFICIENCY_SPEC_V1.md (sufficiency_spec_v1_7)
  - Defined deterministic multiplier stacking semantics, default bucket multipliers, and WARN/SKIP status policy before runtime implementation
- files added:
  - api/engine/data/sufficiency/weight_rules_v1.json
  - api/engine/weight_rules_v1.py
  - api/engine/layers/weight_multiplier_engine_v1.py
  - tests/test_weight_multiplier_engine_v1.py
  - tests/test_pipeline_weight_multiplier_engine_v1.py
- files modified:
  - api/engine/pipeline_build.py
  - api/engine/repro_bundle_export_v1.py
  - tests/test_available_panels_v1.py
  - tests/test_repro_bundle_export_v1.py
  - tests/test_schema_freeze_guard.py
  - docs/ENGINE_TASK_INVENTORY_V1.md
  - docs/ENGINE_IMPLEMENTATION_PLAN_V1.md
  - docs/SUFFICIENCY_SPEC_V1.md
- tests added:
  - tests/test_weight_multiplier_engine_v1.py
  - tests/test_pipeline_weight_multiplier_engine_v1.py
- pipeline_versions additions:
  - weight_rules_version
  - weight_multiplier_engine_version
- repro bundle additions:
  - rules/weight_rules_v1.json
- deterministic warning codes introduced:
  - WEIGHT_RULE_REQUIREMENT_FLAG_UNAVAILABLE

STEP 6 — probability_math_core_v1
Goal:
- Deterministic binomial/hypergeometric with overflow guards and half-up rounding.

Acceptance:
- K_int = floor(effective_K)
- Edge cases covered: K=0, K>=N, n=0, n>N

Implementation Notes (2026-02-20):
- requirements clarification completed first:
  - Added explicit probability_math_core_v1 core + layer contract and deterministic RuntimeError code convention to docs/SUFFICIENCY_SPEC_V1.md (sufficiency_spec_v1_8)
  - Defined deterministic layer status policy and K_int floor/clamp validation behavior before runtime implementation
- files added:
  - api/engine/probability_math_core_v1.py
  - api/engine/layers/probability_math_core_v1.py
  - tests/test_probability_math_core_v1.py
  - tests/test_pipeline_probability_math_core_v1.py
- files modified:
  - api/engine/pipeline_build.py
  - tests/test_available_panels_v1.py
  - tests/test_schema_freeze_guard.py
  - docs/ENGINE_TASK_INVENTORY_V1.md
  - docs/ENGINE_IMPLEMENTATION_PLAN_V1.md
  - docs/SUFFICIENCY_SPEC_V1.md
- tests added:
  - tests/test_probability_math_core_v1.py
  - tests/test_pipeline_probability_math_core_v1.py
- pipeline_versions additions:
  - probability_model_version
- deterministic error codes introduced:
  - PROBABILITY_MATH_CORE_V1_INVALID_INPUT
  - PROBABILITY_MATH_CORE_V1_INTERNAL_ERROR
  - PROBABILITY_MATH_BUCKET_EFFECTIVE_K_INVALID
  - PROBABILITY_MATH_BUCKET_K_INT_INVALID
  - PROBABILITY_MATH_K_INT_POLICY_VIOLATION
  - PROBABILITY_MATH_RUNTIME_ERROR

STEP 7 — probability_checkpoint_layer_v1
Goal:
- Compute probabilities at checkpoints 7/9/10/12 with mulligan adjustments.

Implementation Notes (2026-02-20):
- requirements clarification completed first:
  - Added explicit probability_checkpoint_layer_v1 contract to docs/SUFFICIENCY_SPEC_V1.md (sufficiency_spec_v1_9)
  - Defined checkpoint set [7,9,10,12], default mulligan policy integration, status policy, and deterministic output schema before runtime implementation
- files added:
  - api/engine/layers/probability_checkpoint_layer_v1.py
  - tests/test_probability_checkpoint_layer_v1.py
  - tests/test_pipeline_probability_checkpoint_layer_v1.py
- files modified:
  - api/engine/pipeline_build.py
  - tests/test_available_panels_v1.py
  - tests/test_schema_freeze_guard.py
  - docs/ENGINE_TASK_INVENTORY_V1.md
  - docs/ENGINE_IMPLEMENTATION_PLAN_V1.md
  - docs/SUFFICIENCY_SPEC_V1.md
- tests added:
  - tests/test_probability_checkpoint_layer_v1.py
  - tests/test_pipeline_probability_checkpoint_layer_v1.py
- pipeline_versions additions:
  - probability_checkpoint_version
- deterministic warning/error codes introduced:
  - PROBABILITY_CHECKPOINT_EFFECTIVE_N_FLOORED
  - PROBABILITY_CHECKPOINT_DEFAULT_POLICY_UNAVAILABLE
  - PROBABILITY_CHECKPOINT_POLICY_EFFECTIVE_N_INVALID
  - PROBABILITY_CHECKPOINT_BUCKET_EFFECTIVE_K_INVALID
  - PROBABILITY_CHECKPOINT_BUCKET_K_INT_INVALID
  - PROBABILITY_CHECKPOINT_K_INT_POLICY_VIOLATION
  - PROBABILITY_CHECKPOINT_MATH_RUNTIME_ERROR

STEP 8 — stress_model_definition_v1
Goal:
- Load and validate stress_models_v1.json.

Implementation Notes (2026-02-20):
- requirements clarification completed first:
  - Added explicit stress_model_definition_v1 contract to docs/SUFFICIENCY_SPEC_V1.md (sufficiency_spec_v1_10)
  - Defined deterministic stress model selection precedence (override/profile+bracket/profile/bracket/default), fixed data schema, and status policy before runtime implementation
- files added:
  - api/engine/data/sufficiency/stress_models_v1.json
  - api/engine/stress_models_v1.py
  - api/engine/layers/stress_model_definition_v1.py
  - tests/test_stress_model_definition_v1.py
  - tests/test_pipeline_stress_model_definition_v1.py
- files modified:
  - api/engine/pipeline_build.py
  - api/engine/repro_bundle_export_v1.py
  - tests/test_available_panels_v1.py
  - tests/test_repro_bundle_export_v1.py
  - tests/test_schema_freeze_guard.py
  - docs/ENGINE_TASK_INVENTORY_V1.md
  - docs/ENGINE_IMPLEMENTATION_PLAN_V1.md
  - docs/SUFFICIENCY_SPEC_V1.md
- tests added:
  - tests/test_stress_model_definition_v1.py
  - tests/test_pipeline_stress_model_definition_v1.py
- pipeline_versions additions:
  - stress_model_version
- repro bundle additions:
  - rules/stress_models_v1.json
- deterministic warning/error codes introduced:
  - STRESS_MODEL_OVERRIDE_UNKNOWN
  - STRESS_MODEL_SELECTED_ID_INVALID
  - STRESS_MODEL_PAYLOAD_INVALID

STEP 9 — stress_transform_engine_v1
Goal:
- Apply stress transforms to effective_K and probability floors per spec.

Implementation Notes (2026-02-20):
- requirements clarification completed first:
  - Added explicit stress_transform_engine_v1 contract to docs/SUFFICIENCY_SPEC_V1.md (sufficiency_spec_v1_11)
  - Defined deterministic transform stage policy (K-stage vs probability-stage), operator ordering, recomputation semantics, and per-stage rounding rules before runtime implementation
- files added:
  - api/engine/layers/stress_transform_engine_v1.py
  - tests/test_stress_transform_engine_v1.py
  - tests/test_pipeline_stress_transform_engine_v1.py
- files modified:
  - api/engine/pipeline_build.py
  - tests/test_available_panels_v1.py
  - tests/test_schema_freeze_guard.py
  - docs/ENGINE_TASK_INVENTORY_V1.md
  - docs/ENGINE_IMPLEMENTATION_PLAN_V1.md
  - docs/SUFFICIENCY_SPEC_V1.md
- tests added:
  - tests/test_stress_transform_engine_v1.py
  - tests/test_pipeline_stress_transform_engine_v1.py
- pipeline_versions additions:
  - stress_transform_version
- repro bundle additions:
  - none (no new runtime data pack required for Step 9)
- deterministic error codes introduced:
  - STRESS_TRANSFORM_BUCKET_EFFECTIVE_K_INVALID
  - STRESS_TRANSFORM_BUCKET_K_INT_INVALID
  - STRESS_TRANSFORM_K_INT_POLICY_VIOLATION
  - STRESS_TRANSFORM_CHECKPOINT_DRAW_INVALID
  - STRESS_TRANSFORM_OPERATOR_INVALID
  - STRESS_TRANSFORM_MATH_RUNTIME_ERROR

STEP 10 — resilience_math_engine_v1
Goal:
- Compute resilience metrics per spec.

Implementation Notes (2026-02-20):
- requirements clarification completed first:
  - Added explicit resilience_math_engine_v1 contract to docs/SUFFICIENCY_SPEC_V1.md (sufficiency_spec_v1_12)
  - Defined deterministic baseline/stress alignment, operator-impact metric formulas, commander fragility fallback behavior, and status/error policy before runtime implementation
- files added:
  - api/engine/layers/resilience_math_engine_v1.py
  - tests/test_resilience_math_engine_v1.py
  - tests/test_pipeline_resilience_math_engine_v1.py
- files modified:
  - api/engine/pipeline_build.py
  - tests/test_available_panels_v1.py
  - tests/test_schema_freeze_guard.py
  - docs/ENGINE_TASK_INVENTORY_V1.md
  - docs/ENGINE_IMPLEMENTATION_PLAN_V1.md
  - docs/SUFFICIENCY_SPEC_V1.md
- tests added:
  - tests/test_resilience_math_engine_v1.py
  - tests/test_pipeline_resilience_math_engine_v1.py
- pipeline_versions additions:
  - resilience_model_version
  - resilience_math_engine_version
- repro bundle additions:
  - none (no new runtime data pack required for Step 10)
- deterministic warning/error codes introduced:
  - RESILIENCE_COMMANDER_FRAGILITY_UNAVAILABLE
  - RESILIENCE_BASELINE_BUCKET_INVALID
  - RESILIENCE_STRESS_BUCKET_INVALID
  - RESILIENCE_BUCKET_ALIGNMENT_INVALID
  - RESILIENCE_CHECKPOINT_INVALID
  - RESILIENCE_PROBABILITY_INVALID
  - RESILIENCE_OPERATOR_IMPACTS_INVALID

STEP 11 — commander_reliability_model_v1
Goal:
- Commander cast probability baseline and removal survival logic (deterministic, facet-based only).

Implementation Notes (2026-02-20):
- requirements clarification completed first:
  - Added explicit commander_reliability_model_v1 contract to docs/SUFFICIENCY_SPEC_V1.md (sufficiency_spec_v1_13)
  - Defined closed-world turn-checkpoint mapping, RAMP-bucket cast proxy policy, protection primitive proxy policy, and commander fragility fallback behavior before runtime implementation
- files added:
  - api/engine/layers/commander_reliability_model_v1.py
  - tests/test_commander_reliability_model_v1.py
  - tests/test_pipeline_commander_reliability_model_v1.py
- files modified:
  - api/engine/pipeline_build.py
  - tests/test_available_panels_v1.py
  - tests/test_schema_freeze_guard.py
  - docs/ENGINE_TASK_INVENTORY_V1.md
  - docs/ENGINE_IMPLEMENTATION_PLAN_V1.md
  - docs/SUFFICIENCY_SPEC_V1.md
- tests added:
  - tests/test_commander_reliability_model_v1.py
  - tests/test_pipeline_commander_reliability_model_v1.py
- pipeline_versions additions:
  - commander_reliability_model_version
- repro bundle additions:
  - none (no new runtime data pack required for Step 11)
- deterministic warning/error codes introduced:
  - COMMANDER_RELIABILITY_RAMP_BUCKET_UNAVAILABLE
  - COMMANDER_RELIABILITY_PROTECTION_PROXY_UNAVAILABLE
  - COMMANDER_RELIABILITY_FRAGILITY_UNAVAILABLE
  - COMMANDER_RELIABILITY_CHECKPOINT_INVALID
  - COMMANDER_RELIABILITY_PROBABILITY_INVALID

STEP 12 — sufficiency_summary_v1
Goal:
- Aggregate PASS/WARN/FAIL using required effects, probabilities, stress, resilience, bracket compliance.

Acceptance:
- Missing calibration snapshot => SKIP (per spec)

Implementation Notes (2026-02-20):
- requirements clarification completed first:
  - Added explicit sufficiency_summary_v1 contract to docs/SUFFICIENCY_SPEC_V1.md (sufficiency_spec_v1_14)
  - Added explicit profile_thresholds_v1 data pack contract (fixed path, deterministic loader validation, profile selection policy)
  - Locked layer-specific status policy PASS/WARN/FAIL/SKIP for sufficiency_summary_v1
- files added:
  - api/engine/data/sufficiency/profile_thresholds_v1.json
  - api/engine/profile_thresholds_v1.py
  - api/engine/layers/sufficiency_summary_v1.py
  - tests/test_sufficiency_summary_v1.py
  - tests/test_pipeline_sufficiency_summary_v1.py
- files modified:
  - api/engine/pipeline_build.py
  - api/engine/repro_bundle_export_v1.py
  - tests/test_available_panels_v1.py
  - tests/test_repro_bundle_export_v1.py
  - tests/test_schema_freeze_guard.py
  - docs/ENGINE_TASK_INVENTORY_V1.md
  - docs/ENGINE_IMPLEMENTATION_PLAN_V1.md
  - docs/SUFFICIENCY_SPEC_V1.md
- tests added:
  - tests/test_sufficiency_summary_v1.py
  - tests/test_pipeline_sufficiency_summary_v1.py
- pipeline_versions additions:
  - profile_thresholds_version
  - calibration_snapshot_version
  - sufficiency_summary_version
- repro bundle additions:
  - rules/profile_thresholds_v1.json
- deterministic sufficiency codes introduced:
  - SUFFICIENCY_REQUIRED_UPSTREAM_UNAVAILABLE_*
  - SUFFICIENCY_PROFILE_THRESHOLDS_UNAVAILABLE
  - SUFFICIENCY_CALIBRATION_SNAPSHOT_UNAVAILABLE
  - SUFFICIENCY_* domain threshold/warn codes (sorted deterministic output)

STEP 13 — combo_pack_pipeline_v1
Goal:
- Replace legacy pair-only combo sourcing with closed-world, deterministic combo packs sourced offline from Commander Spellbook.

Acceptance:
- Runtime reads only local versioned JSON packs.
- Runtime performs no network fetches.
- two_card_combos_v2 is preferred with deterministic fallback to two_card_combos_v1.
- profile_bracket_enforcement_v1 handles combo runtime unavailability without pipeline crash.

Implementation Notes (2026-02-20):
- requirements clarification completed first:
  - Added explicit combo data pack and detector contract to docs/SUFFICIENCY_SPEC_V1.md (sufficiency_spec_v1_15)
  - Formalized closed-world runtime sourcing, deterministic loader validation, and bounded detector output policy before governance completion
- files added:
  - api/engine/combos/__init__.py
  - api/engine/combos/commander_spellbook_variants_v1.py
  - api/engine/combos/two_card_combos_v2.py
  - api/engine/data/combos/commander_spellbook_variants_v1.json
  - api/engine/data/combos/two_card_combos_v2.json
  - tools/update_combo_packs_from_spellbook.py
  - tests/test_commander_spellbook_variants_v1.py
  - tests/test_two_card_combos_v2.py
  - tests/test_update_combo_packs_from_spellbook.py
- files modified:
  - api/engine/layers/profile_bracket_enforcement_v1.py
  - api/engine/pipeline_build.py
  - api/engine/repro_bundle_export_v1.py
  - tests/test_available_panels_v1.py
  - tests/test_bracket_compliance_summary_v1.py
  - tests/test_docs_governance_gate.py
  - tests/test_pipeline_profile_bracket_enforcement_v1.py
  - tests/test_profile_bracket_enforcement_v1_layer.py
  - tests/test_repro_bundle_export_v1.py
  - tests/test_repro_bundle_manifest_v1.py
  - docs/ENGINE_TASK_INVENTORY_V1.md
  - docs/ENGINE_IMPLEMENTATION_PLAN_V1.md
  - docs/SUFFICIENCY_SPEC_V1.md
- tests added:
  - tests/test_commander_spellbook_variants_v1.py
  - tests/test_two_card_combos_v2.py
  - tests/test_update_combo_packs_from_spellbook.py
- pipeline_versions additions:
  - two_card_combos_version
  - spellbook_variants_version
- repro bundle additions:
  - rules/commander_spellbook_variants_v1.json
  - rules/two_card_combos_v2.json
- deterministic warning/error codes introduced:
  - SPELLBOOK_VARIANTS_V1_MISSING
  - SPELLBOOK_VARIANTS_V1_INVALID_JSON
  - SPELLBOOK_VARIANTS_V1_INVALID
  - TWO_CARD_COMBOS_V2_MISSING
  - TWO_CARD_COMBOS_V2_INVALID_JSON
  - TWO_CARD_COMBOS_V2_INVALID

---------------------------------------------------------------------
1A) ARCHITECTURE HARDENING PASS COMPLETED (2026-02-20)
---------------------------------------------------------------------

Scope completed before additional Phase 3 sufficiency layer work:
- graph_v1 schema drift prevention
- runtime safe mode guardrails for proof/oracle leakage
- structural payload isolation
- runtime import isolation checks
- deterministic anti-drift checks (timestamps/random/time/schema-freeze)

Files added:
- api/engine/layers/graph_v1_schema_assert_v1.py
- api/engine/runtime_mode_guard.py
- tests/test_graph_v1_schema_assert_v1.py
- tests/test_runtime_mode_guard.py
- tests/test_runtime_import_isolation.py
- tests/test_no_timestamp_fields.py
- tests/test_no_random_imports.py
- tests/test_schema_freeze_guard.py
- tests/test_structural_payload_isolation_v1.py

Files modified:
- api/engine/pipeline_build.py
- api/engine/layers/structural_v1.py
- tests/test_ui_contract_v1_validation.py
- docs/ENGINE_TASK_INVENTORY_V1.md
- docs/ENGINE_IMPLEMENTATION_PLAN_V1.md
- docs/SUFFICIENCY_SPEC_V1.md

Pipeline wiring and policy updates:
- Added run-time call to assert_runtime_safe_mode() at pipeline entry.
- Added graph_v1_schema_assert_v1 execution after graph_v1 assembly and before typed_graph_invariants_v1.
- Added hard ERROR return when graph_v1_schema_assert_v1 status is ERROR.
- Added pipeline_versions.graph_v1_schema_assert_version pin.
- structural_snapshot_v1 remains canonical structural payload; structural_v1 is legacy/deprecated and only emitted behind explicit legacy enable.

Test execution summary:
- Full suite: python -B -m unittest discover -s tests -p "test_*.py" -v
- Result: PASS (with expected environment-dependent skips).

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
- rules/profile_thresholds_v1.json
- rules/commander_spellbook_variants_v1.json
- rules/two_card_combos_v1.json
- rules/two_card_combos_v2.json

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

## [implementation_plan_v1_3] - 2026-02-20
- Recorded architecture hardening pass completion details, file/test inventory, and pipeline/runtime guard updates before next Phase 3 layers.
- Needed to harden graph schema enforcement, runtime safety boundaries, structural payload isolation, and determinism drift guards.
- Impacts inventory/spec/plan/runtime governance traceability.

## [implementation_plan_v1_4] - 2026-02-20
- Marked Step 2 (engine_coherence_v1) complete with implementation notes, tests, panel gating, and pipeline version pin.
- Needed to implement deterministic dead-slot/cohesion metrics after first formalizing missing step requirements in sufficiency_spec_v1_3.
- Impacts inventory/spec/plan/runtime governance traceability.

## [implementation_plan_v1_5] - 2026-02-20
- Recorded overlap_score spec/test clarification pass for engine_coherence_v1 and synchronized governed doc version references.
- Needed to codify pairwise Jaccard overlap as authoritative while explicitly preventing graph-based reinterpretation drift.
- Impacts inventory/spec/plan governance traceability (no runtime logic changes).

## [implementation_plan_v1_6] - 2026-02-20
- Marked Step 3 (mulligan_model_v1) complete with fixed-path assumptions loader, deterministic policy/checkpoint mapping, panel gate wiring, and pipeline version pin.
- Needed because mulligan assumptions and effective_n mapping were underspecified and had to be formalized in sufficiency_spec_v1_5 before implementation.
- Impacts inventory/spec/plan/runtime governance traceability.

## [implementation_plan_v1_7] - 2026-02-20
- Marked Step 4 (substitution_engine_v1) complete with fixed-path bucket substitutions loader, deterministic effective_K/K_int computation, conditional requirement-flag integration, panel gate wiring, and version pins.
- Needed because substitution bucket semantics and conditional rule activation behavior were underspecified and had to be formalized in sufficiency_spec_v1_6 before implementation.
- Impacts inventory/spec/plan/runtime governance traceability.

## [implementation_plan_v1_8] - 2026-02-20
- Marked Step 5 (weight_multiplier_engine_v1) complete with fixed-path weight rules loader, deterministic multiplicative rule stacking, panel gate wiring, and version pins.
- Needed because multiplier rule activation semantics and default bucket behavior were underspecified and had to be formalized in sufficiency_spec_v1_7 before implementation.
- Impacts inventory/spec/plan/runtime governance traceability.

## [implementation_plan_v1_9] - 2026-02-20
- Marked Step 6 (probability_math_core_v1) complete with deterministic integer combinatorics helpers, hypergeometric edge handling, layer wrapper status wiring, panel gate, and probability model version pin.
- Needed because probability math error conventions and layer return/status policy were underspecified and had to be formalized in sufficiency_spec_v1_8 before implementation.
- Impacts inventory/spec/plan/runtime governance traceability.

## [implementation_plan_v1_10] - 2026-02-20
- Marked Step 7 (probability_checkpoint_layer_v1) complete with deterministic checkpoint probability computation, mulligan default-policy draw integration, panel gate wiring, and checkpoint version pin.
- Needed because checkpoint output schema and mulligan-adjusted draw handling were underspecified and had to be formalized in sufficiency_spec_v1_9 before implementation.
- Impacts inventory/spec/plan/runtime governance traceability.

## [implementation_plan_v1_11] - 2026-02-20
- Marked Step 8 (stress_model_definition_v1) complete with fixed-path stress model loader validation, deterministic model selection wiring, panel gate, and stress model version pin.
- Needed because stress model data shape and profile/bracket/request override selection semantics were underspecified and had to be formalized in sufficiency_spec_v1_10 before implementation.
- Impacts inventory/spec/plan/runtime governance traceability.

## [implementation_plan_v1_12] - 2026-02-20
- Marked Step 9 (stress_transform_engine_v1) complete with deterministic stress operator execution, K/probability transform staging, panel gate wiring, and stress transform version pin.
- Needed because transform-order semantics and checkpoint probability recomputation policy were underspecified and had to be formalized in sufficiency_spec_v1_11 before implementation.
- Impacts inventory/spec/plan/runtime governance traceability.

## [implementation_plan_v1_13] - 2026-02-20
- Marked Step 10 (resilience_math_engine_v1) complete with deterministic resilience metric computation, operator-impact fallback behavior, panel gate wiring, and resilience version pins.
- Needed because resilience metric formulas, bucket/checkpoint alignment rules, and commander fragility fallback policy were ambiguous and had to be formalized in sufficiency_spec_v1_12 before implementation.
- Impacts inventory/spec/plan/runtime governance traceability.

## [implementation_plan_v1_14] - 2026-02-20
- Marked Step 11 (commander_reliability_model_v1) complete with deterministic commander reliability metrics, closed-world primitive-based protection proxy behavior, panel gate wiring, and commander reliability version pin.
- Needed because commander reliability checkpoint mapping, proxy formulas, and skip/warn/error semantics were ambiguous and had to be formalized in sufficiency_spec_v1_13 before implementation.
- Impacts inventory/spec/plan/runtime governance traceability.

## [implementation_plan_v1_15] - 2026-02-20
- Marked Step 12 (sufficiency_summary_v1) complete with deterministic domain verdict aggregation (PASS/WARN/FAIL/SKIP), fixed-path profile thresholds sourcing, panel gate wiring, and sufficiency/threshold version pins.
- Needed because sufficiency threshold sourcing policy, aggregate status semantics, and deterministic output contract were ambiguous and had to be formalized in sufficiency_spec_v1_14 before implementation.
- Impacts inventory/spec/plan/runtime governance traceability.

## [implementation_plan_v1_16] - 2026-02-20
- Marked Step 13 (combo_pack_pipeline_v1) complete with deterministic Commander Spellbook offline normalization, derived two-card combos v2 runtime loader/detector integration, panel/pipeline version updates, and repro bundle rule inclusion.
- Needed to enforce closed-world combo sourcing with deterministic local runtime behavior, while preserving two_card_combos_v1 fallback compatibility and graceful unavailability handling.
- Impacts inventory/spec/plan/runtime governance traceability.
