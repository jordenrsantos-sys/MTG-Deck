# SUFFICIENCY SPECIFICATION V1
Version: sufficiency_spec_v1_15

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

Referenced By: ENGINE_TASK_INVENTORY_V1.md

All Phase 3 layers must conform to this document.

---------------------------------------------------------------------
I. GLOBAL RULES
---------------------------------------------------------------------

Deck Size:
N = 99

Rounding:
- 6 decimal places
- half-up rounding
- deterministic float normalization
- half-up must be implemented via Decimal quantize ROUND_HALF_UP or an equivalent deterministic helper (do not rely on Python round()).

All lists sorted deterministically.

All layers emit status: OK | WARN | SKIP | ERROR unless a layer-specific
contract explicitly defines a different closed-world status set.
- SKIP must include reason_code (string)
- WARN/ERROR must include codes list (sorted, deterministic)

Layer-specific status exception:
- sufficiency_summary_v1 emits: PASS | WARN | FAIL | SKIP

---------------------------------------------------------------------
II. MATHEMATICAL CONTRACTS
---------------------------------------------------------------------

Hypergeometric >=1:
P(>=1) = 1 - (C(N-K,n) / C(N,n))

Hypergeometric >=X:
P(>=x) = sum i=x..min(K,n) [
  C(K,i) * C(N-K,n-i) / C(N,n)
]

Effective_K:
effective_K = K_primary + Σ(weight_i * K_substitute_i)

Clamp:
0 ≤ effective_K ≤ N

K DISCRETIZATION POLICY:
- K_primary is integer slot count
- effective_K may be float after substitution
- hypergeometric uses K_int = floor(effective_K) (deterministic)
- output should report both effective_K (float) and K_int (int)

---------------------------------------------------------------------
III. STRESS TRANSFORMS
---------------------------------------------------------------------

TARGETED_REMOVAL:
effective_K_after = max(0, effective_K - removal_weight * count)

BOARD_WIPE:
effective_K_after = effective_K * surviving_engine_fraction

GRAVEYARD_HATE_WINDOW:
effective_K_after = effective_K * graveyard_penalty

STAX_TAX:
probability_floor *= inflation_factor

All weights sourced from calibration_snapshot_v1.

---------------------------------------------------------------------
IV. RESILIENCE DEFINITIONS
---------------------------------------------------------------------

This section is conceptual shorthand only.
Authoritative runtime resilience behavior is defined by:
- XIV. RESILIENCE_MATH_ENGINE_V1 CONTRACT

---------------------------------------------------------------------
V. ARCHITECTURE HARDENING PASS COMPLETED (2026-02-20)
---------------------------------------------------------------------

Hardening constraints locked before additional Phase 3 sufficiency layers:

1) graph_v1 schema freeze guard
- graph_v1 must pass strict frozen-schema assertion (graph_v1_schema_assert_v1)
- assertion executes immediately after graph_v1 construction and before downstream typed invariant analysis
- schema assertion failure returns ERROR and halts downstream processing

2) runtime safe mode guard
- runtime build path must reject proof/oracle enable flags
- proof/oracle text runtime parsing remains disabled in runtime safe mode

3) structural payload isolation
- structural_snapshot_v1 is the canonical downstream structural payload
- structural_v1 is legacy/deprecated and only emitted when explicitly legacy-enabled

4) runtime import isolation
- runtime engine modules must not import update-tooling modules from tools/, snapshot_build/, or taxonomy_source/

5) determinism drift guards
- no timestamp-like keys in build payload output
- no random/time nondeterministic runtime imports in build runtime modules
- frozen top-level result key schema fingerprint must match committed guard snapshot

---------------------------------------------------------------------
VI. ENGINE_COHERENCE_V1 CONTRACT
---------------------------------------------------------------------

Purpose:
- Detect dead playable slots, compute primitive concentration index, and compute slot overlap score.

Inputs:
- primitive_index_by_slot: Dict[str, List[str]]
- deck_slot_ids_playable: List[str]

Normalization rules:
- deck_slot_ids_playable must be normalized to sorted unique non-empty slot IDs.
- primitive lists per slot must be normalized to sorted unique non-empty primitive IDs.
- No oracle_text parsing is allowed.

Definitions:
- dead_slot:
  - A playable slot with zero normalized primitives.
- primitive_concentration_index:
  - Let non_dead_slots_total = count of playable slots with >=1 primitive.
  - For each primitive P, compute coverage(P) = number of non-dead playable slots containing P.
  - primitive_concentration_index = max_P(coverage(P) / non_dead_slots_total).
  - If non_dead_slots_total == 0, primitive_concentration_index = 0.0.

Overlap Score Definition (Authoritative)

overlap_score is defined as the average pairwise Jaccard similarity
across all unordered pairs of engine-relevant playable slots.

For two slots A and B:

J(A,B) = |Primitives(A) ∩ Primitives(B)|
         --------------------------------
         |Primitives(A) ∪ Primitives(B)|

Rules:
- Only slots with >=1 primitive are considered.
- If fewer than 2 engine-relevant slots exist:
  overlap_score = 0.0
- Result is clamped to [0,1].
- Result is rounded to 6 decimal places using half-up rounding.
- Deterministic ordering of slot ids must be enforced before pair iteration.
- Graph structure must NOT influence overlap_score in v1.

Future layers must treat engine_coherence_v1.overlap_score as authoritative and must not reinterpret overlap using graph edges.

Rounding:
- primitive_concentration_index, overlap_score, and share values MUST be rounded to 6 decimals using deterministic half-up rounding.

Status policy:
- SKIP with reason_code="PRIMITIVE_INDEX_UNAVAILABLE" when primitive_index_by_slot is not a dict
  or deck_slot_ids_playable is not a list.
- WARN with codes containing "DEAD_SLOTS_PRESENT" when dead_slots_total > 0.
- OK otherwise.
- codes must always be sorted deterministically.

Output contract:
- version: "engine_coherence_v1"
- status: "OK" | "WARN" | "SKIP"
- reason_code: string | null
- codes: sorted List[str]
- summary:
  - playable_slots_total: int
  - non_dead_slots_total: int
  - dead_slots_total: int
  - dead_slot_ratio: float
  - primitive_concentration_index: float
  - overlap_score: float
- dead_slots: List[{
    "slot_id": str,
    "primitive_count": int,
    "primitives": List[str]
  }], sorted by slot_id ascending.
- top_primitive_concentration: List[{
    "primitive": str,
    "slots_with_primitive": int,
    "share": float
  }], sorted by share descending then primitive ascending, capped at top 8 rows.

---------------------------------------------------------------------
VII. MULLIGAN_MODEL_V1 CONTRACT
---------------------------------------------------------------------

Purpose:
- Provide deterministic effective_n values by checkpoint for mulligan policies.

Inputs:
- format: str
- mulligan_assumptions_payload: Dict[str, Any]

Data pack (fixed path) contract:
- Runtime loader path must be fixed to:
  - api/engine/data/sufficiency/mulligan_assumptions_v1.json
- Root fields:
  - version: non-empty string
  - format_defaults: object
- format_defaults.<format> fields:
  - default_policy: non-empty string
  - policies: object containing exactly the policy ids:
    - FRIENDLY
    - NORMAL
    - DRAW10_SHUFFLE3
- policies.<policy>.effective_n_by_checkpoint fields:
  - keys: 7, 9, 10, 12
  - values: numeric effective_n assumptions

Computation:
- effective_n values are sourced directly from mulligan_assumptions_v1.json (no runtime heuristics).
- For each policy and checkpoint:
  - Clamp effective_n to [0, N] where N=99.
  - Round to 6 decimal places using deterministic half-up rounding.

Determinism:
- format token normalization must be deterministic.
- policy ordering must be deterministic (sorted by policy id ascending).
- checkpoint iteration order must be deterministic and fixed as [7, 9, 10, 12].

Status policy:
- SKIP with reason_code="MULLIGAN_ASSUMPTIONS_UNAVAILABLE" when mulligan_assumptions_payload is not a dict.
- SKIP with reason_code="FORMAT_ASSUMPTIONS_UNAVAILABLE" when format has no assumptions entry.
- OK otherwise.
- codes must always be sorted deterministically.

Output contract:
- version: "mulligan_model_v1"
- status: "OK" | "SKIP"
- reason_code: string | null
- codes: sorted List[str]
- assumptions_version: string | null
- format: string
- default_policy: string | null
- checkpoints: List[int] with [7, 9, 10, 12]
- policy_effective_n: List[{
    "policy": str,
    "effective_n_by_checkpoint": List[{
      "checkpoint": int,
      "effective_n": float
    }]
  }], sorted by policy ascending.

No oracle_text parsing is allowed.

---------------------------------------------------------------------
VIII. SUBSTITUTION_ENGINE_V1 CONTRACT
---------------------------------------------------------------------

Purpose:
- Compute deterministic effective_K per substitution bucket using fixed-path bucket_substitutions_v1 rules and engine requirement flags.

Inputs:
- primitive_index_by_slot: Dict[str, List[str]]
- deck_slot_ids_playable: List[str]
- engine_requirement_detection_v1_payload: Dict[str, Any]
- format: str
- bucket_substitutions_payload: Dict[str, Any]

Data pack (fixed path) contract:
- Runtime loader path must be fixed to:
  - api/engine/data/sufficiency/bucket_substitutions_v1.json
- Root fields:
  - version: non-empty string
  - format_defaults: object
- format_defaults.<format> fields:
  - buckets: object
- buckets.<bucket> fields:
  - primary_primitives: non-empty List[str]
  - base_substitutions: List[{"primitive": str, "weight": number}]
  - conditional_substitutions: List[{
      "requirement_flag": str,
      "substitutions": List[{"primitive": str, "weight": number}]
    }]
- substitution weight rules:
  - weight must be numeric
  - weight must be in [0.0, 1.0]

Computation:
- playable slots must be normalized to sorted unique non-empty slot ids.
- primitive ids per slot must be normalized to sorted unique non-empty ids.
- For each bucket:
  - K_primary = count of playable slots containing any primary primitive for that bucket.
  - Active substitutions = base_substitutions + conditionals whose requirement_flag is True in engine_requirement_detection_v1_payload.engine_requirements_v1.
  - Active substitution rows with the same primitive are aggregated by summing weight.
  - For each aggregated primitive:
    - K_substitute = count of playable slots containing that primitive.
    - contribution = weight * K_substitute.
  - effective_K_raw = K_primary + sum(contribution for all aggregated substitution primitives).
  - Clamp effective_K to [0, N] where N=99.
  - Round effective_K to 6 decimal places using deterministic half-up rounding.
  - K_int = floor(effective_K).

Determinism:
- format token normalization must be deterministic.
- bucket ordering must be deterministic (sorted by bucket id ascending).
- substitution_terms ordering must be deterministic (sorted by primitive ascending).
- active_requirement_flags ordering must be deterministic (sorted ascending).
- conditional depth is bounded to the single conditional_substitutions level (no recursive expansion).
- codes must always be sorted deterministically.

Status policy:
- SKIP with reason_code="BUCKET_SUBSTITUTIONS_UNAVAILABLE" when bucket_substitutions_payload is not a dict.
- SKIP with reason_code="FORMAT_BUCKET_SUBSTITUTIONS_UNAVAILABLE" when format has no bucket entry.
- SKIP with reason_code="PRIMITIVE_INDEX_UNAVAILABLE" when primitive_index_by_slot is not a dict or deck_slot_ids_playable is not a list.
- WARN with code "ENGINE_REQUIREMENTS_UNAVAILABLE" when engine_requirement_detection_v1_payload.engine_requirements_v1 is unavailable.
- WARN with code "SUBSTITUTION_REQUIREMENT_FLAG_UNAVAILABLE" when a conditional rule references a missing or non-bool requirement flag.
- OK otherwise.

Output contract:
- version: "substitution_engine_v1"
- status: "OK" | "WARN" | "SKIP"
- reason_code: string | null
- codes: sorted List[str]
- substitutions_version: string | null
- format: string
- buckets: List[{
    "bucket": str,
    "k_primary": int,
    "effective_K": float,
    "K_int": int,
    "active_requirement_flags": List[str],
    "substitution_terms": List[{
      "primitive": str,
      "weight": float,
      "k_substitute": int,
      "contribution": float
    }]
  }], sorted by bucket ascending.

No oracle_text parsing is allowed.

---------------------------------------------------------------------
IX. WEIGHT_MULTIPLIER_ENGINE_V1 CONTRACT
---------------------------------------------------------------------

Purpose:
- Apply deterministic per-bucket weight multipliers from fixed-path weight_rules_v1 rules.
- Multipliers default to 1.0 for buckets with no active rules.

Inputs:
- engine_requirement_detection_v1_payload: Dict[str, Any]
- substitution_engine_v1_payload: Dict[str, Any] (optional)
- format: str
- weight_rules_payload: Dict[str, Any]

Data pack (fixed path) contract:
- Runtime loader path must be fixed to:
  - api/engine/data/sufficiency/weight_rules_v1.json
- Root fields:
  - version: non-empty string
  - format_defaults: object
- format_defaults.<format> fields:
  - rules: List[{
      "rule_id": str,
      "target_bucket": str,
      "requirement_flag": str,
      "multiplier": number
    }]
- rule constraints:
  - rule_id must be non-empty and unique within a format rule list
  - target_bucket must be a non-empty string
  - requirement_flag must be a non-empty string
  - multiplier must be numeric and >= 0.0

Computation:
- format token normalization must be deterministic.
- Candidate buckets are the deterministic union of:
  - substitution_engine_v1_payload.buckets[*].bucket (when available)
  - weight rules target_bucket values.
- Initialize multiplier to 1.0 for each candidate bucket.
- A rule is active only when requirement_flag is exactly True in
  engine_requirement_detection_v1_payload.engine_requirements_v1.
- Active rules stack multiplicatively by bucket:
  - multiplier_bucket = product(active_rule.multiplier).
- multipliers_by_bucket values are rounded to 6 decimals using deterministic half-up rounding.
- applied_rules includes only active rules.
- Runtime expression evaluation is forbidden; only explicit field-based matching is allowed.

Determinism:
- candidate bucket ordering must be deterministic (sorted by bucket id ascending).
- rule evaluation order must be deterministic (target_bucket ascending, then rule_id ascending).
- applied_rules ordering must be deterministic (target_bucket ascending, then rule_id ascending).
- codes must always be sorted deterministically.

Status policy:
- SKIP with reason_code="WEIGHT_RULES_UNAVAILABLE" when weight_rules_payload is not a dict.
- SKIP with reason_code="FORMAT_WEIGHT_RULES_UNAVAILABLE" when format has no rules entry.
- WARN with code "ENGINE_REQUIREMENTS_UNAVAILABLE" when engine_requirement_detection_v1_payload.engine_requirements_v1 is unavailable.
- WARN with code "WEIGHT_RULE_REQUIREMENT_FLAG_UNAVAILABLE" when a rule references a missing or non-bool requirement flag.
- OK otherwise.

Output contract:
- version: "weight_multiplier_engine_v1"
- status: "OK" | "WARN" | "SKIP"
- reason_code: string | null
- codes: sorted List[str]
- weight_rules_version: string | null
- format: string
- multipliers_by_bucket: List[{
    "bucket": str,
    "multiplier": float
  }], sorted by bucket ascending.
- applied_rules: List[{
    "rule_id": str,
    "target_bucket": str,
    "requirement_flag": str,
    "multiplier": float
  }], sorted by target_bucket ascending then rule_id ascending.

No oracle_text parsing is allowed.

---------------------------------------------------------------------
X. PROBABILITY_MATH_CORE_V1 CONTRACT
---------------------------------------------------------------------

Purpose:
- Provide deterministic integer-combinatorics probability primitives for downstream sufficiency layers.
- Enforce K discretization policy continuity from substitution outputs.

Module location:
- api/engine/probability_math_core_v1.py

Required exports:
- PROBABILITY_MATH_CORE_V1_VERSION
- comb(n, k) -> int
- hypergeom_p_ge_1(N, K_int, n) -> float
- hypergeom_p_ge_x(N, K_int, n, x) -> float

Input domain:
- N, K_int, n, x must be integers (bool is invalid).
- Domain constraints:
  - 0 <= K_int <= N
  - 0 <= n <= N
  - 0 <= x <= n

Computation:
- comb(n, k) must use deterministic integer math only (no float intermediate arithmetic).
- Hypergeometric formulas:
  - P(>=1) = 1 - (C(N-K,n) / C(N,n))
  - P(>=x) = sum i=x..min(K,n) [ C(K,i) * C(N-K,n-i) / C(N,n) ]
- Edge policy:
  - x == 0 => return 1.0
  - x > min(K_int, n) => return 0.0
- Probability outputs are rounded to 6 decimals using deterministic half-up rounding.
- Output probabilities must be clamped to [0.0, 1.0] after deterministic arithmetic.

Error convention (core math module):
- Core math functions MUST raise RuntimeError with explicit deterministic code prefix.
- Required codes:
  - PROBABILITY_MATH_CORE_V1_INVALID_INPUT
  - PROBABILITY_MATH_CORE_V1_INTERNAL_ERROR

Layer wrapper location:
- api/engine/layers/probability_math_core_v1.py

Layer wrapper purpose:
- Expose runtime readiness metadata for probability math primitives.
- Validate substitution K discretization policy consistency where substitution buckets are available.

Layer wrapper inputs:
- substitution_engine_v1_payload: Dict[str, Any] (optional)

Layer wrapper status policy:
- SKIP with reason_code="SUBSTITUTION_ENGINE_UNAVAILABLE" when substitution bucket rows are unavailable.
- ERROR with code "PROBABILITY_MATH_BUCKET_EFFECTIVE_K_INVALID" for missing/non-numeric effective_K.
- ERROR with code "PROBABILITY_MATH_BUCKET_K_INT_INVALID" for missing/non-int K_int.
- ERROR with code "PROBABILITY_MATH_K_INT_POLICY_VIOLATION" when K_int != floor(clamp(effective_K, 0, N)).
- ERROR with code "PROBABILITY_MATH_RUNTIME_ERROR" when core-math deterministic self-check raises a RuntimeError.
- OK otherwise.

Layer wrapper output contract:
- version: "probability_math_core_v1"
- status: "OK" | "SKIP" | "ERROR"
- reason_code: string | null
- codes: sorted List[str]
- math_backend: "int_comb"
- available_functions: sorted List[str]
- validated_buckets: int

Determinism:
- codes list must be sorted deterministically.
- available_functions must be sorted deterministically.
- No random/time/timestamp input is permitted.

No oracle_text parsing is allowed.

---------------------------------------------------------------------
XI. PROBABILITY_CHECKPOINT_LAYER_V1 CONTRACT
---------------------------------------------------------------------

Purpose:
- Compute deterministic probability checkpoints from substitution bucket K values and mulligan-adjusted draw assumptions.

Module location:
- api/engine/layers/probability_checkpoint_layer_v1.py

Required exports:
- PROBABILITY_CHECKPOINT_LAYER_V1_VERSION
- run_probability_checkpoint_layer_v1(...)

Inputs:
- format: str
- substitution_engine_v1_payload: Dict[str, Any]
- mulligan_model_v1_payload: Dict[str, Any]
- probability_math_core_v1 functions: hypergeom_p_ge_1 (and hypergeom_p_ge_x optional)

Checkpoint definitions (fixed):
- opening_7 -> checkpoint 7
- turn_3 -> checkpoint 9
- turn_4 -> checkpoint 10
- turn_6 -> checkpoint 12
- Deterministic checkpoint order MUST be [7, 9, 10, 12].

Mulligan policy integration:
- Use mulligan_model_v1.default_policy as the selected mulligan type.
- Resolve effective_n values for checkpoints [7, 9, 10, 12] from the selected policy row.
- For each checkpoint:
  - clamp effective_n to [0, N] where N=99
  - round effective_n to 6 decimals via deterministic half-up rounding
  - discretize draw count as n_int = floor(effective_n)

Computation:
- For each substitution bucket:
  - validate effective_K numeric and K_int integer (bool invalid)
  - enforce K policy continuity:
    - expected_k_int = floor(clamp(effective_K, 0, N))
    - K_int must equal expected_k_int
  - compute p_ge_1 at each checkpoint using:
    - hypergeom_p_ge_1(N=99, K_int, n=n_int)
- p_ge_1 outputs MUST be deterministic and normalized to 6 decimals half-up.

Status policy:
- SKIP with reason_code="SUBSTITUTION_ENGINE_UNAVAILABLE" when substitution buckets are unavailable.
- SKIP with reason_code="MULLIGAN_MODEL_UNAVAILABLE" when mulligan default-policy checkpoint draws are unavailable.
- WARN with code "PROBABILITY_CHECKPOINT_EFFECTIVE_N_FLOORED" when any checkpoint has n_int != effective_n.
- ERROR with deterministic codes when validation/math fails:
  - PROBABILITY_CHECKPOINT_DEFAULT_POLICY_UNAVAILABLE
  - PROBABILITY_CHECKPOINT_POLICY_EFFECTIVE_N_INVALID
  - PROBABILITY_CHECKPOINT_BUCKET_EFFECTIVE_K_INVALID
  - PROBABILITY_CHECKPOINT_BUCKET_K_INT_INVALID
  - PROBABILITY_CHECKPOINT_K_INT_POLICY_VIOLATION
  - PROBABILITY_CHECKPOINT_MATH_RUNTIME_ERROR
- codes must always be sorted deterministically.

Output contract:
- version: "probability_checkpoint_layer_v1"
- status: "OK" | "WARN" | "SKIP" | "ERROR"
- reason_code: string | null
- codes: sorted List[str]
- format: string
- deck_size_N: 99
- default_policy: string | null
- checkpoints: List[int] with [7, 9, 10, 12]
- checkpoint_draws: List[{
    "checkpoint": int,
    "effective_n": float,
    "n_int": int
  }], sorted by checkpoint ascending.
- probabilities_by_bucket: List[{
    "bucket": str,
    "effective_K": float,
    "K_int": int,
    "probabilities_by_checkpoint": List[{
      "checkpoint": int,
      "effective_n": float,
      "n_int": int,
      "p_ge_1": float
    }] sorted by checkpoint ascending
  }], sorted by bucket ascending.

Determinism:
- checkpoints ordering is fixed [7, 9, 10, 12].
- probabilities_by_bucket ordering is bucket ascending.
- No random/time/timestamp inputs are permitted.

No oracle_text parsing is allowed.

---------------------------------------------------------------------
XII. STRESS_MODEL_DEFINITION_V1 CONTRACT
---------------------------------------------------------------------

Purpose:
- Resolve a deterministic stress model definition from fixed-path stress_models_v1 data.

Module locations:
- Loader: api/engine/stress_models_v1.py
- Layer: api/engine/layers/stress_model_definition_v1.py

Required exports:
- stress_models_v1.py:
  - load_stress_models_v1()
- stress_model_definition_v1.py:
  - STRESS_MODEL_DEFINITION_V1_VERSION
  - run_stress_model_definition_v1(...)

Layer inputs:
- format: str
- bracket_id: str | null
- profile_id: str | null
- request_override_model_id: str | null (optional request-level override)
- stress_models_payload: Dict[str, Any]

Data pack (fixed path) contract:
- Runtime loader path must be fixed to:
  - api/engine/data/sufficiency/stress_models_v1.json
- Root fields:
  - version: non-empty string
  - format_defaults: object
- format_defaults.<format> fields:
  - selection: object
    - default_model_id: non-empty string | null
    - by_profile_id: object map profile_id -> model_id
    - by_bracket_id: object map bracket_id -> model_id
    - by_profile_bracket: List[{"profile_id": str, "bracket_id": str, "model_id": str}]
  - models: object keyed by model_id
- models.<model_id> fields:
  - operators: non-empty List[operator]
- Operator schema (v1):
  - TARGETED_REMOVAL:
    - op: "TARGETED_REMOVAL"
    - count: int >= 0
  - BOARD_WIPE:
    - op: "BOARD_WIPE"
    - by_turn: int >= 0
    - surviving_engine_fraction: number in [0.0, 1.0]
  - GRAVEYARD_HATE_WINDOW:
    - op: "GRAVEYARD_HATE_WINDOW"
    - turns: non-empty List[int >= 0]
    - graveyard_penalty: number in [0.0, 1.0]
  - STAX_TAX:
    - op: "STAX_TAX"
    - by_turn: int >= 0
    - inflation_factor: number >= 0.0

Selection policy (authoritative, deterministic):
1) Normalize format/profile_id/bracket_id/request_override_model_id as stripped strings.
2) If request_override_model_id is non-empty and exists in format models, use it.
3) Else if request_override_model_id is non-empty but missing, add WARN code:
   - STRESS_MODEL_OVERRIDE_UNKNOWN
   and continue fallback selection.
4) Fallback selection order:
   - by_profile_bracket exact match (profile_id + bracket_id)
   - by_profile_id match
   - by_bracket_id match
   - selection.default_model_id
5) If no candidate exists after fallback order, SKIP with reason_code:
   - STRESS_MODEL_SELECTION_UNAVAILABLE
6) If selected candidate model_id is not present in models, ERROR with code:
   - STRESS_MODEL_SELECTED_ID_INVALID

Determinism:
- format key ordering must be deterministic (ascending).
- by_profile_id and by_bracket_id maps must be normalized with key-ascending ordering.
- by_profile_bracket rows must be normalized and sorted by:
  - profile_id asc, bracket_id asc, model_id asc.
- models map must be normalized with model_id-ascending ordering.
- operators in output must be canonical and deterministic:
  - sorted by op asc, then canonical parameter tuple asc.
- codes must always be sorted deterministically.

Status policy:
- SKIP with reason_code="STRESS_MODELS_UNAVAILABLE" when stress_models_payload is not a dict.
- SKIP with reason_code="FORMAT_STRESS_MODELING_UNAVAILABLE" when format has no stress model entry.
- SKIP with reason_code="STRESS_MODEL_SELECTION_UNAVAILABLE" when no model selection is configured/resolved.
- WARN with code "STRESS_MODEL_OVERRIDE_UNKNOWN" when request override is supplied but unknown and fallback succeeds.
- ERROR with deterministic codes when selection/payload is invalid:
  - STRESS_MODEL_SELECTED_ID_INVALID
  - STRESS_MODEL_PAYLOAD_INVALID

Output contract:
- version: "stress_model_definition_v1"
- status: "OK" | "WARN" | "SKIP" | "ERROR"
- reason_code: string | null
- codes: sorted List[str]
- stress_models_version: string | null
- format: string
- profile_id: string
- bracket_id: string
- request_override_model_id: string | null
- selected_model_id: string | null
- selection_source: "override" | "profile_bracket" | "profile" | "bracket" | "default" | null
- operators: List[operator], canonical deterministic ordering.

No oracle_text parsing is allowed.

---------------------------------------------------------------------
XIII. STRESS_TRANSFORM_ENGINE_V1 CONTRACT
---------------------------------------------------------------------

Purpose:
- Apply deterministic stress operators to substitution bucket K values and checkpoint probabilities.

Module location:
- api/engine/layers/stress_transform_engine_v1.py

Required exports:
- STRESS_TRANSFORM_ENGINE_V1_VERSION
- run_stress_transform_engine_v1(...)

Inputs:
- substitution_engine_v1_payload: Dict[str, Any]
- probability_checkpoint_layer_v1_payload: Dict[str, Any]
- stress_model_definition_v1_payload: Dict[str, Any]
- probability_math_core_v1_payload: Dict[str, Any]

Transform application policy (authoritative):
- Hybrid model in v1:
  - K-stage operators (mutate effective_K, then recompute probabilities):
    - TARGETED_REMOVAL
    - BOARD_WIPE
    - GRAVEYARD_HATE_WINDOW
  - Probability-stage operators (mutate probabilities directly, K unchanged):
    - STAX_TAX

Operator ordering:
- Operators MUST be applied in deterministic canonical order:
  - op asc,
  - then operator parameter tuple asc.
- No source-order dependence is allowed.

Checkpoint/turn metadata policy for v1:
- by_turn and turns fields are retained in operator metadata but DO NOT gate checkpoint application in v1.
- Each operator applies to the full checkpoint set [7, 9, 10, 12].

K-stage operator semantics:
- Let current effective_K be K_cur.
- TARGETED_REMOVAL:
  - K_next = clamp(K_cur - count, 0, N)
- BOARD_WIPE:
  - K_next = clamp(K_cur * surviving_engine_fraction, 0, N)
- GRAVEYARD_HATE_WINDOW:
  - K_next = clamp(K_cur * graveyard_penalty, 0, N)
- After each K-stage operator:
  - effective_K is rounded to 6 decimals half-up
  - K_int = floor(effective_K)
  - checkpoint probabilities are recomputed using hypergeom_p_ge_1(N=99, K_int, n_int)

Probability-stage operator semantics:
- STAX_TAX:
  - For each checkpoint probability p:
    - p_next = clamp(p * inflation_factor, 0.0, 1.0)
    - p_next rounded to 6 decimals half-up
  - effective_K and K_int remain unchanged for this stage.

Determinism:
- checkpoints ordering fixed as [7, 9, 10, 12]
- bucket ordering ascending by bucket id
- operator_impacts ordering follows deterministic operator order
- codes sorted deterministically

Rounding and clamping rules:
- effective_K round/clamp/floor applied after each K-stage operator
- probability round/clamp applied after each recomputation stage and after each probability-stage operator

Status policy:
- SKIP with reason_code="SUBSTITUTION_ENGINE_UNAVAILABLE" when substitution buckets unavailable.
- SKIP with reason_code="PROBABILITY_CHECKPOINT_UNAVAILABLE" when checkpoint draws unavailable.
- SKIP with reason_code="STRESS_MODEL_DEFINITION_UNAVAILABLE" when selected stress model operators unavailable.
- SKIP with reason_code="PROBABILITY_MATH_CORE_UNAVAILABLE" when probability math core payload unavailable/non-OK.
- ERROR with deterministic codes when validation/math fails:
  - STRESS_TRANSFORM_BUCKET_EFFECTIVE_K_INVALID
  - STRESS_TRANSFORM_BUCKET_K_INT_INVALID
  - STRESS_TRANSFORM_K_INT_POLICY_VIOLATION
  - STRESS_TRANSFORM_CHECKPOINT_DRAW_INVALID
  - STRESS_TRANSFORM_OPERATOR_INVALID
  - STRESS_TRANSFORM_MATH_RUNTIME_ERROR

Output contract:
- version: "stress_transform_engine_v1"
- status: "OK" | "SKIP" | "ERROR"
- reason_code: string | null
- codes: sorted List[str]
- format: string
- deck_size_N: 99
- selected_model_id: string | null
- operators_applied: List[operator], deterministic order
- checkpoints: [7, 9, 10, 12]
- checkpoint_draws: List[{checkpoint, effective_n, n_int}] ordered by checkpoint asc
- stress_adjusted_effective_K: List[{
    "bucket": str,
    "effective_K_before": float,
    "K_int_before": int,
    "effective_K_after": float,
    "K_int_after": int
  }], bucket asc
- stress_adjusted_probabilities_by_bucket: List[{
    "bucket": str,
    "effective_K_after": float,
    "K_int_after": int,
    "probabilities_by_checkpoint": List[{
      "checkpoint": int,
      "effective_n": float,
      "n_int": int,
      "p_ge_1": float
    }] checkpoint asc
  }], bucket asc
- operator_impacts: List[{
    "operator_index": int,
    "operator": operator,
    "bucket_impacts": List[{
      "bucket": str,
      "effective_K_before": float,
      "effective_K_after": float,
      "K_int_before": int,
      "K_int_after": int,
      "probabilities_before": List[{"checkpoint": int, "p_ge_1": float}],
      "probabilities_after": List[{"checkpoint": int, "p_ge_1": float}]
    }] bucket asc
  }] deterministic operator order.

No oracle_text parsing is allowed.

---------------------------------------------------------------------
XIV. RESILIENCE_MATH_ENGINE_V1 CONTRACT
---------------------------------------------------------------------

Purpose:
- Compute deterministic resilience metrics by comparing baseline checkpoint probabilities
  against stress-adjusted checkpoint probabilities.

Module location:
- api/engine/layers/resilience_math_engine_v1.py

Required exports:
- RESILIENCE_MATH_ENGINE_V1_VERSION
- run_resilience_math_engine_v1(...)

Inputs:
- probability_checkpoint_layer_v1_payload: Dict[str, Any]
- stress_transform_engine_v1_payload: Dict[str, Any]
- engine_requirement_detection_v1_payload: Dict[str, Any] (optional for commander fragility)

Normalization and alignment policy:
- Checkpoints are fixed and must be validated as [7, 9, 10, 12].
- Baseline probabilities source:
  - probability_checkpoint_layer_v1_payload.probabilities_by_bucket
- Stress-adjusted probabilities source:
  - stress_transform_engine_v1_payload.stress_adjusted_probabilities_by_bucket
- Bucket alignment is closed-world and deterministic:
  - bucket ids must be non-empty strings
  - baseline and stress bucket id sets must match exactly
  - bucket ordering in output must be ascending by bucket id

Metric definitions (authoritative):
- engine_continuity_after_removal:
  - If TARGETED_REMOVAL operator impacts are available in
    stress_transform_engine_v1_payload.operator_impacts:
    - per bucket ratio = K_after_last_targeted / K_before_first_targeted
  - Else fallback per bucket ratio = stress_effective_K_after / baseline_effective_K
  - Deck metric = arithmetic mean of per bucket ratios.
- rebuild_after_wipe:
  - If BOARD_WIPE operator impacts are available:
    - per bucket ratio = K_after_last_wipe / K_before_first_wipe
  - Else fallback value is 1.0.
  - Deck metric = arithmetic mean of per bucket ratios (or fallback scalar).
- graveyard_fragility_delta:
  - If GRAVEYARD_HATE_WINDOW operator impacts are available:
    - per bucket/checkpoint delta = max(0, p_before_first_graveyard - p_after_last_graveyard)
  - Else fallback value is 0.0.
  - Deck metric = arithmetic mean over all bucket/checkpoint deltas (or fallback scalar).
- commander_fragility_delta:
  - Uses engine_requirement_detection_v1_payload.engine_requirements_v1.commander_dependent.
  - If commander_dependent == "LOW": commander_fragility_delta = 0.0.
  - Otherwise commander_fragility_delta is null and WARN code is emitted:
    - RESILIENCE_COMMANDER_FRAGILITY_UNAVAILABLE

Rounding and clamping policy:
- All output metric floats are rounded to 6 decimals, half-up.
- Ratio metrics are clamped to [0.0, 1.0] before rounding.
- Probability deltas are clamped to [0.0, 1.0] before rounding.
- Safe zero-denominator ratio handling:
  - numerator <= 0 and denominator <= 0 => 1.0
  - numerator > 0 and denominator <= 0 => 0.0

Determinism:
- Bucket ordering ascending by bucket id.
- Checkpoint ordering fixed as [7, 9, 10, 12].
- codes ordering sorted ascending.
- No random/time/timestamp inputs are permitted.

Status policy:
- SKIP with reason_code="PROBABILITY_CHECKPOINT_UNAVAILABLE" when baseline payload/status/data is unavailable.
- SKIP with reason_code="STRESS_TRANSFORM_UNAVAILABLE" when stress payload/status/data is unavailable.
- WARN when deterministic non-fatal conditions exist (e.g., commander fragility unavailable).
- ERROR when deterministic validation fails with any of:
  - RESILIENCE_BASELINE_BUCKET_INVALID
  - RESILIENCE_STRESS_BUCKET_INVALID
  - RESILIENCE_BUCKET_ALIGNMENT_INVALID
  - RESILIENCE_CHECKPOINT_INVALID
  - RESILIENCE_PROBABILITY_INVALID
  - RESILIENCE_OPERATOR_IMPACTS_INVALID

Output contract:
- version: "resilience_math_engine_v1"
- status: "OK" | "WARN" | "SKIP" | "ERROR"
- reason_code: string | null
- codes: sorted List[str]
- format: string
- checkpoints: [7, 9, 10, 12]
- commander_dependency: string | null
- metrics: {
    "engine_continuity_after_removal": float | null,
    "rebuild_after_wipe": float | null,
    "graveyard_fragility_delta": float | null,
    "commander_fragility_delta": float | null
  }
- bucket_metrics: List[{
    "bucket": str,
    "baseline_effective_K": float,
    "stress_effective_K": float,
    "baseline_p_ge_1_mean": float,
    "stress_p_ge_1_mean": float,
    "stress_delta_p_ge_1_mean": float
  }], sorted by bucket asc.

No oracle_text parsing is allowed.

---------------------------------------------------------------------
XV. COMMANDER_RELIABILITY_MODEL_V1 CONTRACT
---------------------------------------------------------------------

Purpose:
- Compute deterministic, closed-world commander reliability metrics using only
  compiled primitives and existing probability artifacts.

Module location:
- api/engine/layers/commander_reliability_model_v1.py

Required exports:
- COMMANDER_RELIABILITY_MODEL_V1_VERSION
- run_commander_reliability_model_v1(...)

Inputs:
- commander_slot_id: str | None
- probability_checkpoint_layer_v1_payload: Dict[str, Any]
- stress_transform_engine_v1_payload: Dict[str, Any]
- engine_requirement_detection_v1_payload: Dict[str, Any]
- primitive_index_by_slot: Dict[str, Any] | None (optional)
- deck_slot_ids_playable: List[str] | None (optional)

Checkpoint mapping (fixed for v1):
- turn t3 -> checkpoint 9
- turn t4 -> checkpoint 10
- turn t6 -> checkpoint 12

Bucket selection policy:
- Cast and fragility proxy metrics use the "RAMP" bucket only.
- No mana-curve inference or oracle-text parsing is permitted.
- If RAMP bucket probabilities are unavailable, cast metrics are null and WARN code is emitted.

Metric definitions (authoritative):
- cast_reliability_t3:
  - baseline RAMP checkpoint probability at checkpoint 9.
- cast_reliability_t4:
  - baseline RAMP checkpoint probability at checkpoint 10.
- cast_reliability_t6:
  - baseline RAMP checkpoint probability at checkpoint 12.
- protection_coverage_proxy:
  - Define protection primitive set:
    - HEXPROOF_PROTECTION
    - INDESTRUCTIBLE_PROTECTION
  - Evaluate playable non-commander slots only.
  - Let protected_slots be count of playable non-commander slots with at least one
    protection primitive in primitive_index_by_slot.
  - Let eligible_slots be total playable non-commander slots.
  - If eligible_slots > 0:
    - protection_coverage_proxy = protected_slots / eligible_slots
  - Else metric is null and WARN code is emitted.
- commander_fragility_delta:
  - commander_dependent comes from
    engine_requirement_detection_v1_payload.engine_requirements_v1.commander_dependent.
  - If commander_dependent == "LOW":
    - commander_fragility_delta = 0.0
  - Else if both baseline and stress RAMP bucket checkpoint probabilities are available:
    - baseline_mean = mean([p9_baseline, p10_baseline, p12_baseline])
    - stress_mean = mean([p9_stress, p10_stress, p12_stress])
    - commander_fragility_delta = max(0, baseline_mean - stress_mean)
  - Else commander_fragility_delta is null and WARN code is emitted.

Rounding and clamping policy:
- All metric floats rounded to 6 decimals, half-up.
- All probability-derived metrics clamped to [0.0, 1.0] before rounding.

Determinism:
- checkpoint mapping is fixed and static.
- notes list sorted ascending, unique strings only.
- codes list sorted ascending.
- No random/time/timestamp inputs are permitted.

Status policy:
- SKIP with reason_code="COMMANDER_SLOT_UNAVAILABLE" when commander_slot_id is missing/blank.
- SKIP with reason_code="PROBABILITY_CHECKPOINT_UNAVAILABLE" when baseline checkpoint payload/status/data unavailable.
- SKIP with reason_code="STRESS_TRANSFORM_UNAVAILABLE" when stress transform payload/status/data unavailable.
- WARN when non-fatal metric omissions occur:
  - COMMANDER_RELIABILITY_RAMP_BUCKET_UNAVAILABLE
  - COMMANDER_RELIABILITY_PROTECTION_PROXY_UNAVAILABLE
  - COMMANDER_RELIABILITY_FRAGILITY_UNAVAILABLE
- ERROR when deterministic payload validation fails:
  - COMMANDER_RELIABILITY_CHECKPOINT_INVALID
  - COMMANDER_RELIABILITY_PROBABILITY_INVALID

Output contract:
- version: "commander_reliability_model_v1"
- status: "OK" | "WARN" | "SKIP" | "ERROR"
- reason_code: string | null
- codes: sorted List[str]
- commander_dependent: string | null
- checkpoint_mapping: {"t3": 9, "t4": 10, "t6": 12}
- metrics: {
    "cast_reliability_t3": float | null,
    "cast_reliability_t4": float | null,
    "cast_reliability_t6": float | null,
    "protection_coverage_proxy": float | null,
    "commander_fragility_delta": float | null
  }
- notes: sorted List[str]

No oracle_text parsing is allowed.

---------------------------------------------------------------------
XVI. SUFFICIENCY_SUMMARY_V1 CONTRACT
---------------------------------------------------------------------

Purpose:
- Aggregate deterministic Phase 3 sufficiency verdicts using precomputed
  closed-world layer outputs and fixed threshold policy.

Module location:
- api/engine/layers/sufficiency_summary_v1.py

Required exports:
- SUFFICIENCY_SUMMARY_V1_VERSION
- run_sufficiency_summary_v1(...)

Inputs:
- format: str
- profile_id: str
- profile_thresholds_v1_payload: Dict[str, Any]
- engine_requirement_detection_v1_payload: Dict[str, Any]
- engine_coherence_v1_payload: Dict[str, Any]
- mulligan_model_v1_payload: Dict[str, Any]
- substitution_engine_v1_payload: Dict[str, Any]
- weight_multiplier_engine_v1_payload: Dict[str, Any]
- probability_math_core_v1_payload: Dict[str, Any]
- probability_checkpoint_layer_v1_payload: Dict[str, Any]
- stress_model_definition_v1_payload: Dict[str, Any]
- stress_transform_engine_v1_payload: Dict[str, Any]
- resilience_math_engine_v1_payload: Dict[str, Any]
- commander_reliability_model_v1_payload: Dict[str, Any]
- required_effects_coverage_v1_payload: Dict[str, Any] (optional)
- bracket_compliance_summary_v1_payload: Dict[str, Any] (optional)
- pipeline_versions: Dict[str, Any] (optional)

Required upstream readiness policy:
- Required upstream payloads are:
  - engine_requirement_detection_v1_payload
  - engine_coherence_v1_payload
  - mulligan_model_v1_payload
  - substitution_engine_v1_payload
  - weight_multiplier_engine_v1_payload
  - probability_math_core_v1_payload
  - probability_checkpoint_layer_v1_payload
  - stress_model_definition_v1_payload
  - stress_transform_engine_v1_payload
  - resilience_math_engine_v1_payload
  - commander_reliability_model_v1_payload
- A required upstream payload is ready only if:
  - payload is an object
  - payload.status is a non-empty string
  - payload.status in {"OK", "WARN"}
- If any required upstream payload is not ready, summary emits:
  - status="SKIP"
  - reason_code="UPSTREAM_PHASE3_UNAVAILABLE"

Threshold source policy (closed-world):
- Runtime threshold source is fixed-path local data pack:
  - api/engine/data/sufficiency/profile_thresholds_v1.json
- Loader/resolver contract:
  - api/engine/profile_thresholds_v1.py
  - deterministic local file load only (no network)
  - deterministic validation errors via RuntimeError codes
- Resolver output must include:
  - profile_thresholds_version
  - calibration_snapshot_version
  - format
  - requested_profile_id
  - selected_profile_id
  - selection_source
  - domains thresholds object
- If threshold payload unavailable/unresolved:
  - status="SKIP"
  - reason_code="PROFILE_THRESHOLDS_UNAVAILABLE"
- If calibration_snapshot_version missing/blank:
  - status="SKIP"
  - reason_code="CALIBRATION_SNAPSHOT_UNAVAILABLE"

Threshold data pack contract (profile_thresholds_v1):
- Root fields:
  - version: non-empty string
  - calibration_snapshot_version: non-empty string
  - format_defaults: object
- format_defaults.<format> fields:
  - default_profile_id: non-empty string
  - profiles: object
- profiles.<profile_id>.domains must include exactly:
  - required_effects:
    - max_missing: int >= 0
    - max_unknowns: int >= 0
  - baseline_prob:
    - cast_reliability_t3_min: float in [0,1]
    - cast_reliability_t4_min: float in [0,1]
    - cast_reliability_t6_min: float in [0,1]
  - stress_prob:
    - engine_continuity_after_removal_min: float in [0,1]
    - rebuild_after_wipe_min: float in [0,1]
    - graveyard_fragility_delta_max: float in [0,1]
  - coherence:
    - dead_slot_ratio_max: float in [0,1]
    - overlap_score_min: float in [0,1]
  - resilience:
    - commander_fragility_delta_max: float in [0,1]
  - commander:
    - protection_coverage_proxy_min: float in [0,1]
    - commander_fragility_delta_max: float in [0,1]

Domain evaluation policy (authoritative):
- required_effects domain:
  - source: required_effects_coverage_v1_payload
  - missing_total = len(missing)
  - unknowns_total = len(unknowns)
  - FAIL if missing_total > required_effects.max_missing
  - WARN if unknowns_total > required_effects.max_unknowns
- baseline_prob domain:
  - source: commander_reliability_model_v1_payload.metrics
  - compare cast_reliability_t3/t4/t6 against baseline_prob mins
  - missing metric => WARN
  - below min => FAIL
- stress_prob domain:
  - source: resilience_math_engine_v1_payload.metrics
  - compare:
    - engine_continuity_after_removal >= min
    - rebuild_after_wipe >= min
    - graveyard_fragility_delta <= max
  - missing metric => WARN
  - threshold breach => FAIL
- coherence domain:
  - source: engine_coherence_v1_payload.summary
  - compare:
    - dead_slot_ratio <= max
    - overlap_score >= min
  - missing metric => WARN
  - threshold breach => FAIL
- resilience domain:
  - source: resilience_math_engine_v1_payload.metrics.commander_fragility_delta
  - compare commander_fragility_delta <= resilience.commander_fragility_delta_max
  - missing metric => WARN
  - threshold breach => FAIL
- commander domain:
  - source: commander_reliability_model_v1_payload
  - evaluate protection_coverage_proxy only when commander_dependent != "LOW"
  - compare:
    - protection_coverage_proxy >= commander.protection_coverage_proxy_min
    - commander_fragility_delta <= commander.commander_fragility_delta_max
  - missing required metric => WARN
  - threshold breach => FAIL

Per-domain verdict policy:
- Each domain emits:
  - status: PASS | WARN | FAIL
  - codes: sorted List[str]

Aggregate status policy:
- status="SKIP" for skip conditions defined above.
- status="FAIL" if one or more domain verdicts are FAIL.
- status="WARN" if no domain FAIL and one or more domain WARN.
- status="PASS" otherwise.

Determinism:
- Domain key order is fixed:
  - required_effects, baseline_prob, stress_prob, coherence, resilience, commander
- codes list sorted ascending unique strings.
- failures list sorted ascending unique strings.
- warnings list sorted ascending unique strings.
- thresholds_used and versions_used keys are deterministic fixed-order objects.
- No random/time/timestamp inputs are permitted.

Output contract:
- version: "sufficiency_summary_v1"
- status: "PASS" | "WARN" | "FAIL" | "SKIP"
- reason_code: string | null
- codes: sorted List[str]
- failures: sorted List[str]
- warnings: sorted List[str]
- domain_verdicts: {
    "required_effects": {"status": str, "codes": List[str]},
    "baseline_prob": {"status": str, "codes": List[str]},
    "stress_prob": {"status": str, "codes": List[str]},
    "coherence": {"status": str, "codes": List[str]},
    "resilience": {"status": str, "codes": List[str]},
    "commander": {"status": str, "codes": List[str]}
  }
- thresholds_used: {
    "profile_thresholds_version": str | null,
    "calibration_snapshot_version": str | null,
    "format": str,
    "requested_profile_id": str,
    "selected_profile_id": str,
    "selection_source": str,
    "domains": Dict[str, Any]
  }
- versions_used: deterministic subset of pipeline_versions containing sufficiency/model pins.

No oracle_text parsing is allowed.

---------------------------------------------------------------------
XVII. COMBO_PACK_PIPELINE_V1 CONTRACT
---------------------------------------------------------------------

Purpose:
- Define deterministic, closed-world runtime combo detection sourced from local
  versioned combo packs, with offline-only Commander Spellbook ingestion.

Runtime module locations:
- api/engine/combos/commander_spellbook_variants_v1.py
- api/engine/combos/two_card_combos_v2.py
- api/engine/layers/profile_bracket_enforcement_v1.py (consumer)

Runtime data packs (fixed local paths):
- api/engine/data/combos/commander_spellbook_variants_v1.json
- api/engine/data/combos/two_card_combos_v2.json
- api/engine/data/combos/two_card_combos_v1.json (legacy fallback source only)

Offline updater tool location:
- tools/update_combo_packs_from_spellbook.py

Closed-world runtime sourcing policy:
- Runtime engine code must read combo definitions only from local fixed-path JSON files.
- Runtime engine code must not perform HTTP/network calls for combo data.
- Network access for combo data is allowed only in tools/update_combo_packs_from_spellbook.py.

Data pack contract (commander_spellbook_variants_v1):
- Root fields:
  - version: must equal "commander_spellbook_variants_v1"
  - source: non-empty string
  - generated_from: non-empty string
  - variants: List[variant]
- variant fields:
  - variant_id: non-empty string, unique across variants
  - cards: List[str], canonicalized lower-case card keys, sorted unique, length >= 2
  - result: optional non-empty string
  - tags: optional List[str], sorted unique non-empty strings

Data pack contract (two_card_combos_v2):
- Root fields:
  - version: must equal "two_card_combos_v2"
  - pairs: List[pair]
- pair fields:
  - a: canonical non-empty card key
  - b: canonical non-empty card key
  - canonical ordering must hold: a < b
  - a and b must be distinct
  - variant_ids: sorted unique non-empty strings, length >= 1
- pairs must be unique by (a,b) and sorted deterministically by (a,b).

Derivation contract (variants -> two_card_combos_v2):
- Derivation considers only variants that normalize to exactly two unique card keys.
- Each two-card pair aggregates all contributing variant_id values.
- Output pair ordering and variant_ids ordering must be deterministic.

Version preference and fallback policy:
- Runtime combo detector must prefer loading two_card_combos_v2.
- Fallback to normalized two_card_combos_v1 is allowed only when v2 loader returns
  TWO_CARD_COMBOS_V2_MISSING.
- Any other v2 loader error must not silently fallback to v1.

Detector contract (two_card_combos_v2):
- Function: detect_two_card_combos(deck_card_keys, max_matches=25)
- Input:
  - deck_card_keys may be a list of card keys or a slot->card mapping.
  - values are canonicalized deterministically; blanks/non-strings are ignored.
- Output:
  - version: string | null
  - supported: bool
  - count: int | null
  - matches: bounded deterministic List[match]
  - error_code: string (only when supported=false due to loader/runtime error)
- match fields:
  - a: str
  - b: str
  - variant_ids: optional List[str]
- max_matches handling:
  - defaults to 25 when invalid
  - clamps to 0 when negative
  - matches list is bounded to max_matches while count reports full match total

Error code policy:
- commander_spellbook_variants_v1 loader runtime codes:
  - SPELLBOOK_VARIANTS_V1_MISSING
  - SPELLBOOK_VARIANTS_V1_INVALID_JSON
  - SPELLBOOK_VARIANTS_V1_INVALID
- two_card_combos_v2 loader/derivation runtime codes:
  - TWO_CARD_COMBOS_V2_MISSING
  - TWO_CARD_COMBOS_V2_INVALID_JSON
  - TWO_CARD_COMBOS_V2_INVALID

Pipeline integration policy:
- pipeline_versions must include:
  - two_card_combos_version = "two_card_combos_v2"
  - spellbook_variants_version = "commander_spellbook_variants_v1"
- profile_bracket_enforcement_v1 category support resolution must:
  - consume detect_two_card_combos output
  - degrade gracefully when combo support unavailable
  - never crash pipeline on combo loader/detector RuntimeError
- repro bundle export must include deterministic combo files:
  - rules/commander_spellbook_variants_v1.json
  - rules/two_card_combos_v2.json
  - rules/two_card_combos_v1.json (legacy compatibility)

Offline updater contract (tools/update_combo_packs_from_spellbook.py):
- Supports deterministic offline refresh of both combo packs from Commander Spellbook API.
- Must support dry-run behavior and optional partial write policy.
- Writes must be deterministic JSON (sorted keys, canonical separators, stable ordering).
- Runtime engine behavior must be unchanged regardless of updater availability.

Determinism and safety constraints:
- No randomness/timestamps/time-derived output.
- Stable sorting for variants, pairs, tags, variant_ids, and match outputs.
- Fixed bounded loops for pagination via max-pages.

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

## [sufficiency_spec_v1_1] - 2026-02-20
- Added document governance rules and append-only change log requirements.
- Needed to prevent drift and enforce synchronized updates across governance docs.
- Impacts inventory/spec/plan governance process (no runtime logic changes).

## [sufficiency_spec_v1_2] - 2026-02-20
- Added architecture hardening completion section covering graph schema assertion, runtime safe mode, structural payload isolation, import isolation, and determinism drift guards.
- Needed to formalize hardening constraints before continuing Phase 3 sufficiency implementation.
- Impacts inventory/spec/plan/runtime governance traceability.

## [sufficiency_spec_v1_3] - 2026-02-20
- Added explicit engine_coherence_v1 contract (inputs, formulas, status policy, output schema, sorting, and deterministic half-up rounding).
- Needed because step-level coherence requirements were ambiguous in inventory/plan and had to be fixed before implementation.
- Impacts inventory/spec/plan/runtime governance traceability.

## [sufficiency_spec_v1_4] - 2026-02-20
- Clarified overlap_score as the authoritative pairwise Jaccard average over unordered non-dead slot pairs, including deterministic ordering and explicit graph-independence requirement.
- Needed to codify the already-implemented runtime method and prevent future graph-based reinterpretation drift.
- Impacts inventory/spec/plan governance traceability (no runtime math change).

## [sufficiency_spec_v1_5] - 2026-02-20
- Added explicit mulligan_model_v1 contract, fixed-path mulligan_assumptions_v1 data pack schema, deterministic checkpoint ordering, and status/output rules.
- Needed because mulligan assumptions and effective_n checkpoint mapping were underspecified before Step 3 implementation.
- Impacts inventory/spec/plan/runtime governance traceability.

## [sufficiency_spec_v1_6] - 2026-02-20
- Added explicit substitution_engine_v1 contract, fixed-path bucket_substitutions_v1 data pack schema, conditional requirement-flag integration rules, and effective_K/K_int output policy.
- Needed because substitution bucket semantics and conditional rule application were ambiguous before Step 4 implementation.
- Impacts inventory/spec/plan/runtime governance traceability.

## [sufficiency_spec_v1_7] - 2026-02-20
- Added explicit weight_multiplier_engine_v1 contract, fixed-path weight_rules_v1 data pack schema, multiplicative stacking rules, and deterministic output/status policy.
- Needed because rule activation semantics and multiplier stacking behavior were ambiguous before Step 5 implementation.
- Impacts inventory/spec/plan/runtime governance traceability.

## [sufficiency_spec_v1_8] - 2026-02-20
- Added explicit probability_math_core_v1 contract, deterministic integer-combinatorics requirements, hypergeometric edge behavior, and runtime error conventions.
- Needed because probability math error/return behavior and layer status policy were ambiguous before Step 6 implementation.
- Impacts inventory/spec/plan/runtime governance traceability.

## [sufficiency_spec_v1_9] - 2026-02-20
- Added explicit probability_checkpoint_layer_v1 contract defining checkpoint set [7,9,10,12], mulligan default-policy draw integration, output schema, and deterministic status/error conventions.
- Needed because checkpoint probability output shape and mulligan-adjusted draw policy were not explicitly frozen before Step 7 implementation.
- Impacts inventory/spec/plan/runtime governance traceability.

## [sufficiency_spec_v1_10] - 2026-02-20
- Added explicit stress_model_definition_v1 contract defining fixed-path stress_models_v1 schema, deterministic model selection precedence (override/profile+bracket/profile/bracket/default), status policy, and canonical operator output ordering.
- Needed because stress model selection semantics and payload contract were ambiguous before Step 8 implementation.
- Impacts inventory/spec/plan/runtime governance traceability.

## [sufficiency_spec_v1_11] - 2026-02-20
- Added explicit stress_transform_engine_v1 contract defining transform stage order, K-vs-probability operator policy, per-stage recomputation and rounding rules, deterministic operator ordering, and output schema.
- Needed because stress transform execution policy and checkpoint recomputation semantics were ambiguous before Step 9 implementation.
- Impacts inventory/spec/plan/runtime governance traceability.

## [sufficiency_spec_v1_12] - 2026-02-20
- Added explicit resilience_math_engine_v1 contract defining required inputs, deterministic bucket/checkpoint alignment, metric formulas (removal continuity, wipe rebuild, graveyard fragility, commander fragility fallback), and status/error conventions.
- Needed because resilience formulas, operator-impact usage, and commander fragility behavior were ambiguous before Step 10 implementation.
- Impacts inventory/spec/plan/runtime governance traceability.

## [sufficiency_spec_v1_13] - 2026-02-20
- Added explicit commander_reliability_model_v1 contract defining closed-world cast reliability checkpoints, protection proxy primitive policy, commander fragility derivation, and deterministic status/error conventions.
- Needed because commander reliability formulas, input dependencies, and WARN/SKIP behavior were not explicitly frozen before Step 11 implementation.
- Impacts inventory/spec/plan/runtime governance traceability.

## [sufficiency_spec_v1_14] - 2026-02-20
- Added explicit sufficiency_summary_v1 contract defining required upstream readiness gates, deterministic domain verdict aggregation (PASS/WARN/FAIL/SKIP), and output schema including thresholds_used and versions_used.
- Added closed-world profile_thresholds_v1 data pack contract and deterministic threshold sourcing/selection rules, including calibration snapshot version gating.
- Needed because Step 12 formulas, threshold sourcing policy, and status semantics were not explicitly frozen before implementation.
- Impacts inventory/spec/plan/runtime governance traceability.

## [sufficiency_spec_v1_15] - 2026-02-20
- Added explicit combo_pack_pipeline_v1 contract defining closed-world runtime combo sourcing, Commander Spellbook variants/two-card pack schemas, deterministic v2->v1 fallback policy, detector output bounds, and pipeline/repro integration requirements.
- Needed because combo data sourcing boundaries, runtime fallback semantics, and deterministic detector contract were not explicitly frozen before Step 13 governance completion.
- Impacts inventory/spec/plan/runtime governance traceability.
