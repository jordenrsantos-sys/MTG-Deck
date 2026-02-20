# ENGINE TASK INVENTORY V1
Version: inventory_v1_4

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
SECTION 2 — PHASE 3: SUFFICIENCY ENGINE (SPEC-ALIGNED)
---------------------------------------------------------------------

2.1 Engine Requirement Detection
[x] engine_requirement_detection_v1  (COMPLETE - implementation_plan_v1_2 reference)
    - primitive-driven dependency flags
    - commander dependency classification
    - deterministic signature matching

2.2 Engine Coherence
[ ] engine_coherence_v1
    - dead slot detection
    - primitive concentration index
    - overlap score

2.3 Mulligan Subsystem
[ ] mulligan_model_v1
    - FRIENDLY
    - NORMAL
    - DRAW10_SHUFFLE3
    - effective_n adjustments

2.4 Substitution Engine
[ ] substitution_engine_v1
    - effective_K calculation
    - conditional substitution logic
    - bounded depth

2.5 Weight Multiplier Engine
[ ] weight_multiplier_engine_v1
    - rule evaluation
    - multiplier stacking
    - rounding normalization

2.6 Probability Math Core
[ ] probability_math_core_v1
    - deterministic binomial coefficient engine
    - hypergeometric >=1
    - hypergeometric >=X
    - effective_K discretization policy (floor) tracked as part of the deliverable
    - overflow guards

2.7 Probability Checkpoint Layer
[ ] probability_checkpoint_layer_v1
    - integrate effective_K
    - apply mulligan adjustments
    - checkpoint outputs

2.8 Stress Model Definition
[ ] stress_model_definition_v1
    - load stress operators
    - validate structure

2.9 Stress Transform Engine
[ ] stress_transform_engine_v1
    - K reduction
    - threshold inflation
    - graveyard penalty
    - wipe survival transforms

2.10 Resilience Math Engine
[ ] resilience_math_engine_v1
    - engine_continuity_after_removal
    - rebuild_after_wipe
    - fragility deltas

2.11 Commander Reliability Model
[ ] commander_reliability_model_v1
    - cast probability baseline
    - removal survival logic

2.12 Sufficiency Summary
[ ] sufficiency_summary_v1
    - PASS/WARN/FAIL
    - profile threshold enforcement
    - version pin reporting

---------------------------------------------------------------------
SECTION 3 — DATA PACKS REQUIRED
---------------------------------------------------------------------

[x] dependency_signatures_v1.json  (COMPLETE - implementation_plan_v1_2 reference)
[ ] weight_rules_v1.json
[ ] bucket_substitutions_v1.json
[ ] stress_models_v1.json
[ ] calibration_snapshot_v1.json
[ ] mulligan_assumptions_v1.json

---------------------------------------------------------------------
SECTION 4 — VERSION LOCKS REQUIRED
---------------------------------------------------------------------

[ ] probability_model_version
[ ] stress_model_version
[ ] resilience_model_version
[ ] mulligan_model_version
[ ] substitution_rules_version
[ ] weight_rules_version
[ ] calibration_snapshot_version

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
