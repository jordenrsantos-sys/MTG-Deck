# SUFFICIENCY SPECIFICATION V1
Version: sufficiency_spec_v1_1

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

All layers emit status: OK | WARN | SKIP | ERROR
- SKIP must include reason_code (string)
- WARN/ERROR must include codes list (sorted, deterministic)

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

engine_continuity_after_removal =
  stress_adjusted_K / baseline_K

rebuild_after_wipe =
  surviving_engine_fraction

graveyard_fragility_delta =
  P_baseline - P_after_graveyard_hate

commander_fragility_delta =
  P_with_commander - P_without_commander

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
