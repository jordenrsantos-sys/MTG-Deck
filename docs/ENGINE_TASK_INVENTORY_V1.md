# ENGINE TASK INVENTORY V1
Version: inventory_v1_19

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

Governed By: docs/SUFFICIENCY_SPEC_V1.md
Rule: Nothing in Phase 3 may be implemented unless listed here.
Modification Policy: Mark COMPLETE only after code + tests pass.

Completion Mark Format (Required):
[ ] task_name
or
[x] task_name  (COMPLETE - implementation_plan_vX.Y reference)

Note:
- Every [x] must reference implementation plan version used.

---------------------------------------------------------------------
SECTION 1 — VERIFIED CORE (COMPLETE)
---------------------------------------------------------------------

Core Pipeline:
- snapshot_preflight_v1
- canonicalization layers
- primitive_index generation
- graph_v1 expansion
- typed_graph_invariants_v1

Observability & Structural:
- graph_analytics_summary_v1
- graph_pathways_summary_v1
- disruption_surface_v1
- vulnerability_index_v1
- counterfactual_stress_test_v1
- structural_scorecard_v1

Policy:
- gc_limits_v1
- bracket_rules_v2
- profile_bracket_enforcement_v1
- bracket_compliance_summary_v1

Requirements:
- required_effects_v1
- required_effects_coverage_v1

Repro:
- repro_bundle_manifest_v1
- repro_bundle_export_v1

---------------------------------------------------------------------
SECTION 1A - ARCHITECTURE HARDENING PASS (COMPLETE)
---------------------------------------------------------------------

[x] graph_v1_schema_assert_v1  (COMPLETE - implementation_plan_v1_3 reference)
    - frozen graph_v1 schema assertion runs immediately after graph construction
    - hard failure on shape drift, without graph mutation

[x] runtime_mode_guard_v1  (COMPLETE - implementation_plan_v1_3 reference)
    - runtime build guard rejects proof/oracle-enable flags
    - proof/oracle text runtime paths remain disabled in safe mode

[x] structural_snapshot_isolation_v1  (COMPLETE - implementation_plan_v1_3 reference)
    - structural_snapshot_v1 remains canonical downstream structural payload
    - legacy structural_v1 is deprecated and only exposed via explicit legacy enable

[x] runtime_import_isolation_guard_v1  (COMPLETE - implementation_plan_v1_3 reference)
    - runtime modules are checked for forbidden imports from tools/snapshot_build/taxonomy_source

[x] determinism_hardening_guard_suite_v1  (COMPLETE - implementation_plan_v1_3 reference)
    - tests enforce no timestamp-like output keys in build payload
    - tests enforce no random/time nondeterministic runtime imports
    - tests enforce frozen top-level result key schema fingerprint

---------------------------------------------------------------------
SECTION 2 — PHASE 3: SUFFICIENCY ENGINE (SPEC-ALIGNED)
---------------------------------------------------------------------

2.1 Engine Requirement Detection
[x] engine_requirement_detection_v1  (COMPLETE - implementation_plan_v1_2 reference)
    - primitive-driven dependency flags
    - commander dependency classification
    - deterministic signature matching

2.2 Engine Coherence
[x] engine_coherence_v1  (COMPLETE - implementation_plan_v1_5 reference)
    - dead slot detection
    - primitive concentration index
    - overlap score

2.3 Mulligan Subsystem
[x] mulligan_model_v1  (COMPLETE - implementation_plan_v1_6 reference)
    - FRIENDLY
    - NORMAL
    - DRAW10_SHUFFLE3
    - effective_n adjustments

2.4 Substitution Engine
[x] substitution_engine_v1  (COMPLETE - implementation_plan_v1_7 reference)
    - effective_K calculation
    - conditional substitution logic
    - bounded depth

2.5 Weight Multiplier Engine
[x] weight_multiplier_engine_v1  (COMPLETE - implementation_plan_v1_8 reference)
    - rule evaluation
    - multiplier stacking
    - rounding normalization

2.6 Probability Math Core
[x] probability_math_core_v1  (COMPLETE - implementation_plan_v1_9 reference)
    - deterministic binomial coefficient engine
    - hypergeometric >=1
    - hypergeometric >=X
    - effective_K discretization policy (floor) tracked as part of the deliverable
    - overflow guards

2.7 Probability Checkpoint Layer
[x] probability_checkpoint_layer_v1  (COMPLETE - implementation_plan_v1_10 reference)
    - integrate effective_K
    - apply mulligan adjustments
    - checkpoint outputs

2.8 Stress Model Definition
[x] stress_model_definition_v1  (COMPLETE - implementation_plan_v1_11 reference)
    - load stress operators
    - validate structure

2.9 Stress Transform Engine
[x] stress_transform_engine_v1  (COMPLETE - implementation_plan_v1_12 reference)
    - K reduction
    - threshold inflation
    - graveyard penalty
    - wipe survival transforms

2.10 Resilience Math Engine
[x] resilience_math_engine_v1  (COMPLETE - implementation_plan_v1_13 reference)
    - engine_continuity_after_removal
    - rebuild_after_wipe
    - fragility deltas

2.11 Commander Reliability Model
[x] commander_reliability_model_v1  (COMPLETE - implementation_plan_v1_14 reference)
    - cast probability baseline
    - removal survival logic

2.12 Sufficiency Summary
[x] sufficiency_summary_v1  (COMPLETE - implementation_plan_v1_15 reference)
    - PASS/WARN/FAIL
    - profile threshold enforcement
    - version pin reporting

2.13 Combo Pack Pipeline
[x] combo_pack_pipeline_v1  (COMPLETE - implementation_plan_v1_16 reference)
    - commander_spellbook_variants_v1 deterministic loader
    - two_card_combos_v2 derivation + runtime detection
    - deterministic fallback to two_card_combos_v1
    - offline updater tool + pipeline/repro integration

2.14 Decklist Ingestion + Engine Viewer
[x] decklist_ingestion_v1  (COMPLETE - implementation_plan_v1_17 reference)
    - deterministic raw decklist parser (counts/comments/section headers)
    - snapshot-locked card resolution with explicit unknown queue
    - canonicalization-only /deck/validate endpoint (no pipeline execution)

[x] engine_viewer_v0  (COMPLETE - implementation_plan_v1_17 reference)
    - ui_harness upgraded to call /deck/validate before /build
    - unknown queue gating disables Build when unresolved names remain
    - 3-column viewer layout (run history, build header, panel explorer)

---------------------------------------------------------------------
SECTION 3 — DATA PACKS REQUIRED
---------------------------------------------------------------------

[x] dependency_signatures_v1.json  (COMPLETE - implementation_plan_v1_2 reference)
[x] mulligan_assumptions_v1.json  (COMPLETE - implementation_plan_v1_6 reference)
[x] weight_rules_v1.json  (COMPLETE - implementation_plan_v1_8 reference)
[x] bucket_substitutions_v1.json  (COMPLETE - implementation_plan_v1_7 reference)
[x] stress_models_v1.json  (COMPLETE - implementation_plan_v1_11 reference)
[x] profile_thresholds_v1.json  (COMPLETE - implementation_plan_v1_15 reference)
[x] commander_spellbook_variants_v1.json  (COMPLETE - implementation_plan_v1_16 reference)
[x] two_card_combos_v2.json  (COMPLETE - implementation_plan_v1_16 reference)
[x] two_card_combos_v1.json  (COMPLETE - implementation_plan_v1_16 reference, legacy fallback)
[ ] calibration_snapshot_v1.json

---------------------------------------------------------------------
SECTION 4 — VERSION LOCKS REQUIRED
---------------------------------------------------------------------

[x] probability_model_version  (COMPLETE - implementation_plan_v1_9 reference)
[x] probability_checkpoint_version  (COMPLETE - implementation_plan_v1_10 reference)
[x] stress_model_version  (COMPLETE - implementation_plan_v1_11 reference)
[x] stress_transform_version  (COMPLETE - implementation_plan_v1_12 reference)
[x] resilience_model_version  (COMPLETE - implementation_plan_v1_13 reference)
[x] resilience_math_engine_version  (COMPLETE - implementation_plan_v1_13 reference)
[x] commander_reliability_model_version  (COMPLETE - implementation_plan_v1_14 reference)
[x] profile_thresholds_version  (COMPLETE - implementation_plan_v1_15 reference)
[x] calibration_snapshot_version  (COMPLETE - implementation_plan_v1_15 reference)
[x] sufficiency_summary_version  (COMPLETE - implementation_plan_v1_15 reference)
[x] mulligan_model_version  (COMPLETE - implementation_plan_v1_6 reference)
[x] substitution_rules_version  (COMPLETE - implementation_plan_v1_7 reference)
[x] substitution_engine_version  (COMPLETE - implementation_plan_v1_7 reference)
[x] weight_rules_version  (COMPLETE - implementation_plan_v1_8 reference)
[x] weight_multiplier_engine_version  (COMPLETE - implementation_plan_v1_8 reference)
[x] two_card_combos_version  (COMPLETE - implementation_plan_v1_16 reference)
[x] spellbook_variants_version  (COMPLETE - implementation_plan_v1_16 reference)

---------------------------------------------------------------------
SECTION 5 — DETERMINISM SAFETY
---------------------------------------------------------------------

[ ] global rounding policy
[ ] float normalization enforcement
[ ] stable sorting enforcement
[ ] overflow guards
[ ] iteration bounds

---------------------------------------------------------------------
SECTION 6 — DEFINITION OF DONE (PHASE 3)
---------------------------------------------------------------------

A Phase 3 component is COMPLETE only if:

- VERSION constant exported
- Input contract matches spec
- Output contract matches spec
- SKIP logic implemented
- Deterministic ordering verified
- Unit tests written
- Integration test written
- Determinism repeat test passes
- Pipeline wiring complete
- Panel gate added
- Version pin included

---------------------------------------------------------------------
SECTION 7 — EXECUTION ORDER
---------------------------------------------------------------------

1) engine_requirement_detection_v1
2) engine_coherence_v1
3) mulligan_model_v1
4) substitution_engine_v1
5) weight_multiplier_engine_v1
6) probability_math_core_v1
7) probability_checkpoint_layer_v1
8) stress_model_definition_v1
9) stress_transform_engine_v1
10) resilience_math_engine_v1
11) commander_reliability_model_v1
12) sufficiency_summary_v1
13) combo_pack_pipeline_v1

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

## [inventory_v1_3] - 2026-02-20
- Added document governance rules, strict completion mark format requirements, and append-only change log policy.
- Needed to prevent drift and enforce synchronized documentation updates across governance files.
- Impacts inventory/spec/plan governance process (no runtime logic changes).

## [inventory_v1_4] - 2026-02-20
- Marked Phase 3 Step 1 (engine_requirement_detection_v1) and dependency_signatures_v1.json as complete with implementation plan reference.
- Needed to record completion status after deterministic implementation, panel gate wiring, version pinning, and test validation.
- Impacts inventory/plan/runtime tracking for Phase 3 governance.

## [inventory_v1_5] - 2026-02-20
- Added Section 1A architecture hardening completion items covering graph schema assertion, runtime safe mode guard, structural payload isolation, import isolation, and determinism guard tests.
- Needed to formalize pre-Phase-3 risk burn-down before continuing sufficiency layer implementation.
- Impacts inventory/plan/spec/runtime governance traceability.

## [inventory_v1_6] - 2026-02-20
- Marked Phase 3 Step 2 (engine_coherence_v1) complete with implementation plan reference.
- Needed to record deterministic implementation of dead-slot detection, primitive concentration index, overlap score, and pipeline panel/version wiring.
- Impacts inventory/spec/plan/runtime governance traceability.

## [inventory_v1_7] - 2026-02-20
- Recorded overlap_score spec clarification alignment for engine_coherence_v1 and updated plan reference.
- Needed to codify pairwise Jaccard overlap as authoritative and prevent future graph-based reinterpretation drift.
- Impacts inventory/spec/plan governance traceability (no runtime logic changes).

## [inventory_v1_8] - 2026-02-20
- Marked Phase 3 Step 3 (mulligan_model_v1), mulligan_assumptions_v1.json, and mulligan_model_version lock as complete with implementation plan reference.
- Needed to record deterministic mulligan policy/checkpoint assumptions integration, panel/version wiring, and fixed-path runtime data pack loading.
- Impacts inventory/spec/plan/runtime governance traceability.

## [inventory_v1_9] - 2026-02-20
- Marked Phase 3 Step 4 (substitution_engine_v1), bucket_substitutions_v1.json, substitution_rules_version, and substitution_engine_version locks as complete with implementation plan reference.
- Needed to record deterministic substitution bucket integration, conditional requirement-flag activation behavior, and effective_K/K_int pipeline wiring.
- Impacts inventory/spec/plan/runtime governance traceability.

## [inventory_v1_10] - 2026-02-20
- Marked Phase 3 Step 5 (weight_multiplier_engine_v1), weight_rules_v1.json, weight_rules_version, and weight_multiplier_engine_version locks as complete with implementation plan reference.
- Needed to record deterministic multiplier rule stacking, requirement-flag activation behavior, and fixed-path weight rules runtime integration.
- Impacts inventory/spec/plan/runtime governance traceability.

## [inventory_v1_11] - 2026-02-20
- Marked Phase 3 Step 6 (probability_math_core_v1) and probability_model_version lock as complete with implementation plan reference.
- Needed to record deterministic combinatorics/hypergeometric core integration, K_int policy validation, and runtime panel/version wiring.
- Impacts inventory/spec/plan/runtime governance traceability.

## [inventory_v1_12] - 2026-02-20
- Marked Phase 3 Step 7 (probability_checkpoint_layer_v1) and probability_checkpoint_version lock as complete with implementation plan reference.
- Needed to record deterministic checkpoint probability integration across mulligan-adjusted draws with panel/version wiring.
- Impacts inventory/spec/plan/runtime governance traceability.

## [inventory_v1_13] - 2026-02-20
- Marked Phase 3 Step 8 (stress_model_definition_v1), stress_models_v1.json, and stress_model_version lock as complete with implementation plan reference.
- Needed to record deterministic stress model loader/selection integration, panel gate wiring, and stress model version pin.
- Impacts inventory/spec/plan/runtime governance traceability.

## [inventory_v1_14] - 2026-02-20
- Marked Phase 3 Step 9 (stress_transform_engine_v1) and stress_transform_version lock as complete with implementation plan reference.
- Needed to record deterministic stress transform execution ordering, K/probability stage policy, and pipeline panel/version wiring.
- Impacts inventory/spec/plan/runtime governance traceability.

## [inventory_v1_15] - 2026-02-20
- Marked Phase 3 Step 10 (resilience_math_engine_v1) and resilience version locks as complete with implementation plan reference.
- Needed to record deterministic resilience metric integration (removal continuity, wipe rebuild, graveyard fragility, commander fragility fallback), panel gate wiring, and pipeline version pins.
- Impacts inventory/spec/plan/runtime governance traceability.

## [inventory_v1_16] - 2026-02-20
- Marked Phase 3 Step 11 (commander_reliability_model_v1) and commander reliability version lock as complete with implementation plan reference.
- Needed to record deterministic commander reliability metric integration (turn-checkpoint cast reliability proxies, protection coverage proxy, commander fragility delta fallback policy), panel gate wiring, and pipeline version pin.
- Impacts inventory/spec/plan/runtime governance traceability.

## [inventory_v1_17] - 2026-02-20
- Marked Phase 3 Step 12 (sufficiency_summary_v1), profile_thresholds_v1.json, and sufficiency threshold/pin locks as complete with implementation plan reference.
- Needed to record deterministic sufficiency aggregation wiring (PASS/WARN/FAIL/SKIP), fixed-path threshold sourcing, panel gate integration, repro bundle rule inclusion, and pipeline version pins.
- Impacts inventory/spec/plan/runtime governance traceability.

## [inventory_v1_18] - 2026-02-20
- Marked Phase 3 Step 13 (combo_pack_pipeline_v1) complete, including Commander Spellbook-derived combo data packs, runtime combo detector migration to two_card_combos_v2 with v1 fallback, and offline updater governance coverage.
- Needed to record closed-world deterministic combo sourcing updates and ensure combo runtime/version lock artifacts are explicitly tracked in inventory sections 2/3/4.
- Impacts inventory/spec/plan/runtime governance traceability.

## [inventory_v1_19] - 2026-02-20
- Marked Phase 3 Step 14 decklist_ingestion_v1 and engine_viewer_v0 complete, including deterministic raw decklist parse/resolve/ingest modules, canonicalization-only /deck/validate endpoint, and ui_harness Engine Viewer v0 validate-first flow.
- Needed to close missing usable ingestion/UI path while preserving frozen schema contracts and deterministic unknown-surfacing policy.
- Impacts inventory/spec/plan/runtime governance traceability.
