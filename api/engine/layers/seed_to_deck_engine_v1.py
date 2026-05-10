"""Seed-to-deck orchestrator (Phase 3.1 — full deck-build delegation).

Wraps existing engine layers into a single end-to-end pipeline that takes N
seed cards (1 ≤ N ≤ 99 + commander) + active bracket and returns a deck +
sufficiency summary + swap suggestions + repro_bundle.

**Phase 3.1 changes (vs Phase 3 v1):**
- ADDED `canonical_deck_input` + `baseline_build_result` kwargs so callers
  with pre-computed pipeline state can drive end-to-end deck-build via
  `deck_complete_engine_v1`.
- REPLACED single-tune-call with sufficiency-loop iteration (hard cap 3
  iterations per safety #2 of Phase 3.1 spec); each iteration calls
  `deck_tune_runner` and re-evaluates sufficiency; on persistent FAIL after
  the cap, returns STATUS_FAIL_AFTER_TUNE.
- ADDED `repro_bundle` field to envelope; populated via
  `build_repro_bundle_manifest_v1` (or injected `repro_bundle_runner`).
- SHRUNK `phase_3_1_deferred` field to enumerate ONLY the work that's still
  open after Phase 3.1 (commander pool expansion post-Phase-2.1; native
  pipeline_build integration that avoids the recursion guard).
- Production deck-build path: when `canonical_deck_input` AND
  `baseline_build_result` are provided AND no `deck_build_runner` is
  injected, the orchestrator calls `run_deck_complete_engine_v1` directly.
- When called WITHOUT pre-computed state (e.g., from pipeline_build's mid-
  pipeline integration where the late-stage state isn't yet available), the
  orchestrator returns STATUS_OK_DECK_BUILD_DEFERRED (graceful no-op
  preserving the architectural slot; pipeline_build's analysis layers don't
  need a fully-built deck).

**Output envelope (extended in Phase 3.1):**

    {
        "version": "seed_to_deck_engine_v1",
        "status": "OK" | "INSUFFICIENT_SIGNAL" | "INVALID_REQUEST" |
                  "OK_DECK_BUILD_DEFERRED" | "FAIL_AFTER_TUNE",
        "commander": {"oracle_id": str, "name": Optional[str]} | None,
        "deck_99": List[str],
        "sufficiency_summary": Optional[Dict],
        "swap_suggestions": List[Dict],   # aggregated across loop iterations
        "reason_code": Optional[str],
        "phase_3_1_deferred": List[str],
        "repro_bundle": Optional[Dict],   # NEW in Phase 3.1
        "sufficiency_loop_iterations": int,  # NEW in Phase 3.1
    }

Determinism: same `(seed_oracle_ids, commander_oracle_id, snapshot_id,
bracket_id, profile_id)` + injected runners → bytewise-identical output.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from api.engine.layers.commander_recommendation_v1 import (
    run_commander_recommendation_v1,
)
from api.engine.layers.repro_bundle_manifest_v1 import (
    build_repro_bundle_manifest_v1,
)

SEED_TO_DECK_ENGINE_V1_VERSION = "seed_to_deck_engine_v1"

DEFAULT_BRACKET_ID = "B2"
DEFAULT_PROFILE_ID = "balanced"
DEFAULT_DECK_SIZE = 99
DEFAULT_MULLIGAN_MODEL_ID = "default"
DEFAULT_MAX_ADDS = 200
DEFAULT_MAX_SWAPS = 10
DEFAULT_LAND_TARGET_MODE = "auto"
DEFAULT_SUFFICIENCY_LOOP_MAX = 3

STATUS_OK = "OK"
STATUS_INSUFFICIENT_SIGNAL = "INSUFFICIENT_SIGNAL"
STATUS_INVALID_REQUEST = "INVALID_REQUEST"
STATUS_OK_DECK_BUILD_DEFERRED = "OK_DECK_BUILD_DEFERRED"
STATUS_FAIL_AFTER_TUNE = "FAIL_AFTER_TUNE"

REASON_EMPTY_SEED_NO_COMMANDER = "empty_seed_no_commander"
REASON_SEED_OVERSIZED = "seed_oversized"
REASON_NO_COMMANDER_RECOMMENDABLE = "no_commander_recommendable"
REASON_DECK_BUILD_DEFERRED = "deck_build_deferred_no_pre_computed_state"
REASON_FAIL_AFTER_TUNE = "sufficiency_fail_after_loop_cap"

# Phase 3.1 shrunk this list — items previously deferred are now implemented.
# Only commander_pool_expansion remains (gated on Phase 2.1 curation work).
PHASE_3_1_DEFERRED = (
    "commander_pool_expansion_post_phase_2_1",
    "native_pipeline_build_integration_without_recursion_guard",
)


def _envelope(
    *,
    status: str,
    commander: Optional[Dict[str, Any]] = None,
    deck_99: Optional[List[str]] = None,
    sufficiency_summary: Optional[Dict[str, Any]] = None,
    swap_suggestions: Optional[List[Dict[str, Any]]] = None,
    reason_code: Optional[str] = None,
    repro_bundle: Optional[Dict[str, Any]] = None,
    sufficiency_loop_iterations: int = 0,
) -> Dict[str, Any]:
    return {
        "version": SEED_TO_DECK_ENGINE_V1_VERSION,
        "status": status,
        "commander": commander,
        "deck_99": list(deck_99 or []),
        "sufficiency_summary": sufficiency_summary,
        "swap_suggestions": list(swap_suggestions or []),
        "reason_code": reason_code,
        "phase_3_1_deferred": list(PHASE_3_1_DEFERRED),
        "repro_bundle": repro_bundle,
        "sufficiency_loop_iterations": sufficiency_loop_iterations,
    }


def _validate_request(
    *,
    seed_oracle_ids: List[str],
    commander_oracle_id: Optional[str],
) -> Optional[Dict[str, Any]]:
    if not isinstance(seed_oracle_ids, list):
        return _envelope(
            status=STATUS_INVALID_REQUEST,
            reason_code="seed_oracle_ids_not_list",
        )
    if len(seed_oracle_ids) > DEFAULT_DECK_SIZE:
        return _envelope(
            status=STATUS_INVALID_REQUEST,
            reason_code=REASON_SEED_OVERSIZED,
        )
    if not seed_oracle_ids and (commander_oracle_id is None or commander_oracle_id == ""):
        return _envelope(
            status=STATUS_INSUFFICIENT_SIGNAL,
            reason_code=REASON_EMPTY_SEED_NO_COMMANDER,
        )
    return None


def _resolve_commander(
    *,
    commander_oracle_id: Optional[str],
    seed_oracle_ids: List[str],
    snapshot_id: str,
    commander_recommendation_runner: Optional[Callable],
) -> Optional[Dict[str, Any]]:
    if isinstance(commander_oracle_id, str) and commander_oracle_id != "":
        return {"oracle_id": commander_oracle_id, "name": None}

    rec_runner = commander_recommendation_runner or run_commander_recommendation_v1
    rec = rec_runner(
        seed_primitive_ids=[],
        seed_color_identity=[],
        snapshot_id=snapshot_id,
    )
    if not isinstance(rec, dict):
        return None
    if rec.get("status") != "OK":
        return None
    candidates = rec.get("candidates") or []
    if not candidates:
        return None
    top = candidates[0]
    return {
        "oracle_id": top.get("oracle_id"),
        "name": top.get("name"),
    }


def _default_deck_build_runner(
    *,
    canonical_deck_input: Any,
    baseline_build_result: Dict[str, Any],
    snapshot_id: str,
    bracket_id: str,
    profile_id: str,
    mulligan_model_id: str,
    target_deck_size: int,
    max_adds: int,
    allow_basic_lands: bool,
    land_target_mode: str,
) -> Dict[str, Any]:
    """Production deck-build runner — wraps run_deck_complete_engine_v1.

    Imports lazily to avoid module-load coupling.
    """
    from api.engine.deck_complete_engine_v1 import run_deck_complete_engine_v1
    return run_deck_complete_engine_v1(
        canonical_deck_input=canonical_deck_input,
        baseline_build_result=baseline_build_result,
        db_snapshot_id=snapshot_id,
        bracket_id=bracket_id,
        profile_id=profile_id,
        mulligan_model_id=mulligan_model_id,
        target_deck_size=target_deck_size,
        max_adds=max_adds,
        allow_basic_lands=allow_basic_lands,
        land_target_mode=land_target_mode,
    )


def _extract_deck_build_outputs(
    build_result: Dict[str, Any],
) -> Dict[str, Any]:
    """Normalise the deck_build_runner result into our envelope subset.

    Phase 3.1 soft-safety #3 (sufficiency shape variance): best-effort field
    mapping. Looks for `deck_99`, `sufficiency_summary`, `swap_suggestions`
    in either the top level or nested under common deck_complete_engine_v1
    output shapes.
    """
    if not isinstance(build_result, dict):
        return {"deck_99": [], "sufficiency_summary": None, "swap_suggestions": []}

    def _first_list(keys):
        for k in keys:
            v = build_result.get(k)
            if isinstance(v, list):
                return list(v)
        return []

    def _first_dict(keys):
        for k in keys:
            v = build_result.get(k)
            if isinstance(v, dict):
                return v
        return None

    deck_99 = _first_list(["deck_99", "completed_deck", "deck_oracle_ids", "result_deck"])
    sufficiency_summary = _first_dict(["sufficiency_summary", "sufficiency_summary_v1"])
    swap_suggestions = _first_list(["swap_suggestions", "swaps", "tune_swaps"])
    return {
        "deck_99": deck_99,
        "sufficiency_summary": sufficiency_summary,
        "swap_suggestions": swap_suggestions,
    }


def _run_sufficiency_loop(
    *,
    deck_99: List[str],
    sufficiency_summary: Optional[Dict[str, Any]],
    seed_aggregated_swaps: List[Dict[str, Any]],
    commander: Dict[str, Any],
    bracket_id: str,
    profile_id: str,
    deck_tune_runner: Optional[Callable],
    sufficiency_loop_max: int,
) -> Dict[str, Any]:
    """Iterate up to `sufficiency_loop_max` tune rounds while sufficiency FAILS.

    Per Phase 3.1 spec stage 2 + soft-safety #10: hard cap at 3 iterations;
    return FAIL_AFTER_TUNE if sufficiency persistently FAILs through the cap.
    Aggregates swap_suggestions across all iterations.
    """
    iterations = 0
    aggregated = list(seed_aggregated_swaps)

    if deck_tune_runner is None:
        # No tune capability; return current state with iteration count 0.
        return {
            "deck_99": deck_99,
            "sufficiency_summary": sufficiency_summary,
            "swap_suggestions": aggregated,
            "iterations": iterations,
        }

    while iterations < sufficiency_loop_max:
        if not isinstance(sufficiency_summary, dict):
            break
        if sufficiency_summary.get("status") != "FAIL":
            break

        tune_result = deck_tune_runner(
            deck_99=deck_99,
            commander=commander,
            sufficiency_summary=sufficiency_summary,
            bracket_id=bracket_id,
            profile_id=profile_id,
            iteration=iterations,
        )
        iterations += 1
        if not isinstance(tune_result, dict):
            break

        swaps = tune_result.get("swap_suggestions")
        if isinstance(swaps, list):
            aggregated.extend(swaps)

        new_deck = tune_result.get("deck_99")
        if isinstance(new_deck, list):
            deck_99 = list(new_deck)

        new_summary = tune_result.get("sufficiency_summary")
        if isinstance(new_summary, dict):
            sufficiency_summary = new_summary

    return {
        "deck_99": deck_99,
        "sufficiency_summary": sufficiency_summary,
        "swap_suggestions": aggregated,
        "iterations": iterations,
    }


def _build_repro_bundle(
    *,
    deck_99: List[str],
    commander: Dict[str, Any],
    sufficiency_summary: Optional[Dict[str, Any]],
    snapshot_id: str,
    bracket_id: str,
    profile_id: str,
    repro_bundle_runner: Optional[Callable],
) -> Dict[str, Any]:
    """Synthesize a build_result-shaped dict + invoke the repro bundle layer.

    Phase 3.1 soft-safety #8 (repro_bundle shape variance): pass a Phase-3-
    specific lightweight build_result. The layer's `_strip_ui_redacted_path_
    fields` + `_layer_is_included` helpers tolerate missing layer keys.
    """
    runner = repro_bundle_runner or build_repro_bundle_manifest_v1
    synthesized_build_result: Dict[str, Any] = {
        "seed_to_deck_v1": {
            "deck_99": list(deck_99),
            "commander": dict(commander) if isinstance(commander, dict) else commander,
            "snapshot_id": snapshot_id,
            "bracket_id": bracket_id,
            "profile_id": profile_id,
        },
    }
    if isinstance(sufficiency_summary, dict):
        synthesized_build_result["sufficiency_summary_v1"] = sufficiency_summary
    return runner(synthesized_build_result)


def run_seed_to_deck_engine_v1(
    *,
    seed_oracle_ids: List[str],
    snapshot_id: str,
    commander_oracle_id: Optional[str] = None,
    bracket_id: str = DEFAULT_BRACKET_ID,
    profile_id: str = DEFAULT_PROFILE_ID,
    canonical_deck_input: Optional[Any] = None,
    baseline_build_result: Optional[Dict[str, Any]] = None,
    mulligan_model_id: str = DEFAULT_MULLIGAN_MODEL_ID,
    target_deck_size: int = DEFAULT_DECK_SIZE,
    max_adds: int = DEFAULT_MAX_ADDS,
    max_swaps: int = DEFAULT_MAX_SWAPS,
    allow_basic_lands: bool = True,
    land_target_mode: str = DEFAULT_LAND_TARGET_MODE,
    sufficiency_loop_max: int = DEFAULT_SUFFICIENCY_LOOP_MAX,
    commander_recommendation_runner: Optional[Callable] = None,
    deck_build_runner: Optional[Callable] = None,
    sufficiency_runner: Optional[Callable] = None,  # reserved for Phase 3.2
    deck_tune_runner: Optional[Callable] = None,
    repro_bundle_runner: Optional[Callable] = None,
) -> Dict[str, Any]:
    """End-to-end seed-to-deck orchestration (Phase 3.1)."""
    early = _validate_request(
        seed_oracle_ids=seed_oracle_ids,
        commander_oracle_id=commander_oracle_id,
    )
    if early is not None:
        return early

    commander = _resolve_commander(
        commander_oracle_id=commander_oracle_id,
        seed_oracle_ids=seed_oracle_ids,
        snapshot_id=snapshot_id,
        commander_recommendation_runner=commander_recommendation_runner,
    )
    if commander is None or not commander.get("oracle_id"):
        return _envelope(
            status=STATUS_INSUFFICIENT_SIGNAL,
            reason_code=REASON_NO_COMMANDER_RECOMMENDABLE,
        )

    # Choose the deck-build runner.
    runner = deck_build_runner
    if runner is None and canonical_deck_input is not None and baseline_build_result is not None:
        # Production path: pre-computed pipeline state available -> default
        # runner wraps run_deck_complete_engine_v1.
        def runner(*, seed_oracle_ids, commander, snapshot_id, bracket_id, profile_id):
            return _default_deck_build_runner(
                canonical_deck_input=canonical_deck_input,
                baseline_build_result=baseline_build_result,
                snapshot_id=snapshot_id,
                bracket_id=bracket_id,
                profile_id=profile_id,
                mulligan_model_id=mulligan_model_id,
                target_deck_size=target_deck_size,
                max_adds=max_adds,
                allow_basic_lands=allow_basic_lands,
                land_target_mode=land_target_mode,
            )

    if runner is None:
        # No pre-computed state and no injected runner -> defer gracefully.
        return _envelope(
            status=STATUS_OK_DECK_BUILD_DEFERRED,
            commander=commander,
            deck_99=list(seed_oracle_ids),
            reason_code=REASON_DECK_BUILD_DEFERRED,
        )

    build_result = runner(
        seed_oracle_ids=seed_oracle_ids,
        commander=commander,
        snapshot_id=snapshot_id,
        bracket_id=bracket_id,
        profile_id=profile_id,
    )
    if not isinstance(build_result, dict):
        return _envelope(
            status=STATUS_INVALID_REQUEST,
            commander=commander,
            reason_code="deck_build_runner_returned_invalid",
        )

    extracted = _extract_deck_build_outputs(build_result)
    deck_99 = extracted["deck_99"]
    sufficiency_summary = extracted["sufficiency_summary"]
    swap_suggestions: List[Dict[str, Any]] = list(extracted["swap_suggestions"])

    loop_result = _run_sufficiency_loop(
        deck_99=deck_99,
        sufficiency_summary=sufficiency_summary,
        seed_aggregated_swaps=swap_suggestions,
        commander=commander,
        bracket_id=bracket_id,
        profile_id=profile_id,
        deck_tune_runner=deck_tune_runner,
        sufficiency_loop_max=sufficiency_loop_max,
    )
    deck_99 = loop_result["deck_99"]
    sufficiency_summary = loop_result["sufficiency_summary"]
    swap_suggestions = loop_result["swap_suggestions"]
    iterations = loop_result["iterations"]

    sufficiency_status = (
        sufficiency_summary.get("status")
        if isinstance(sufficiency_summary, dict)
        else None
    )
    final_status = (
        STATUS_FAIL_AFTER_TUNE if sufficiency_status == "FAIL" else STATUS_OK
    )
    final_reason_code = (
        REASON_FAIL_AFTER_TUNE if final_status == STATUS_FAIL_AFTER_TUNE else None
    )

    repro_bundle = _build_repro_bundle(
        deck_99=deck_99,
        commander=commander,
        sufficiency_summary=sufficiency_summary,
        snapshot_id=snapshot_id,
        bracket_id=bracket_id,
        profile_id=profile_id,
        repro_bundle_runner=repro_bundle_runner,
    )

    return _envelope(
        status=final_status,
        commander=commander,
        deck_99=deck_99,
        sufficiency_summary=sufficiency_summary,
        swap_suggestions=swap_suggestions,
        reason_code=final_reason_code,
        repro_bundle=repro_bundle,
        sufficiency_loop_iterations=iterations,
    )
