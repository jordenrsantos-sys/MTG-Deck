"""Tiered self-learning loop for Phase 5b offline playtest framework
(stage 5b.9).

Per Decision 7 of OFFLINE_PLAYTEST_DESIGN.md + CALIBRATION_BOUNDARY safety #4:
three-tier evaluation of per-batch drift.

- Auto-adjust: drift within ±0.03 per 100-game batch; cumulative ≤ ±0.15 per
  pack per month. Lands automatically; logged to playtest_changelog_v1.json.
- Queue-for-review: drift beyond auto cap. Surfaced on dashboard at
  `05_VALIDATION/PLAYTEST_DASHBOARD.md`; user reviews when convenient.
- Halt: architectural shifts (new bracket, new combo type, corpus
  discontinuity, determinism check fails). Always halts; immediate user
  attention.

Calibration-only module.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

SELF_LEARNING_LOOP_V1_VERSION = "self_learning_loop_v1"

PER_BATCH_CAP_DEFAULT = 0.03
CUMULATIVE_CAP_DEFAULT = 0.15

TIER_AUTO_ADJUST = "auto_adjust"
TIER_QUEUE_FOR_REVIEW = "queue_for_review"
TIER_HALT = "halt"

ARCHITECTURAL_SHIFT_REASONS = (
    "new_bracket_id",
    "new_combo_type",
    "corpus_discontinuity",
    "determinism_check_failed",
    "manifest_drift",
)


@dataclass(frozen=True)
class DriftEvaluation:
    deck_id: str
    metric: str
    old_value: float
    new_value: float
    drift: float
    cumulative_drift: float
    tier: str
    reason: str


def evaluate_drift(
    *,
    deck_id: str,
    metric: str,
    old_value: float,
    new_value: float,
    cumulative_drift: float = 0.0,
    architectural_shift: Optional[str] = None,
    per_batch_cap: float = PER_BATCH_CAP_DEFAULT,
    cumulative_cap: float = CUMULATIVE_CAP_DEFAULT,
) -> DriftEvaluation:
    """Classify a single drift signal into auto_adjust / queue_for_review / halt."""
    drift = new_value - old_value

    if architectural_shift is not None:
        return DriftEvaluation(
            deck_id=deck_id,
            metric=metric,
            old_value=old_value,
            new_value=new_value,
            drift=drift,
            cumulative_drift=cumulative_drift,
            tier=TIER_HALT,
            reason=architectural_shift,
        )

    if abs(drift) <= per_batch_cap and abs(cumulative_drift + drift) <= cumulative_cap:
        return DriftEvaluation(
            deck_id=deck_id,
            metric=metric,
            old_value=old_value,
            new_value=new_value,
            drift=drift,
            cumulative_drift=cumulative_drift + drift,
            tier=TIER_AUTO_ADJUST,
            reason="within_per_batch_cap",
        )

    return DriftEvaluation(
        deck_id=deck_id,
        metric=metric,
        old_value=old_value,
        new_value=new_value,
        drift=drift,
        cumulative_drift=cumulative_drift + drift,
        tier=TIER_QUEUE_FOR_REVIEW,
        reason=(
            "exceeds_per_batch_cap" if abs(drift) > per_batch_cap
            else "exceeds_cumulative_cap"
        ),
    )


def make_changelog_entry(
    eval: DriftEvaluation,
    *,
    timestamp_utc: str,
    game_count: int,
    run_id: str,
) -> Dict[str, Any]:
    """Format a DriftEvaluation as a changelog entry per the pack schema."""
    return {
        "timestamp_utc": timestamp_utc,
        "deck_id": eval.deck_id,
        "metric": eval.metric,
        "old_value": eval.old_value,
        "new_value": eval.new_value,
        "drift": eval.drift,
        "tier": eval.tier,
        "game_count": game_count,
        "run_id": run_id,
        "reason": eval.reason,
    }


def evaluate_batch(
    drifts: Sequence[Dict[str, Any]],
    *,
    cumulative_drift_by_pair: Optional[Dict[Tuple[str, str], float]] = None,
    architectural_shift_total_cap_multiplier: float = 5.0,
) -> Tuple[List[DriftEvaluation], bool]:
    """Evaluate a batch of drifts; flag halt if cumulative exceeds 5× the cap.

    Per safety #9: halt and surface if cumulative drift exceeds 5× the per-
    batch cap on first canonical run.

    Returns: (list of evaluations, halt_required_flag)
    """
    if cumulative_drift_by_pair is None:
        cumulative_drift_by_pair = {}

    evaluations: List[DriftEvaluation] = []
    total_abs_drift = 0.0
    halt_required = False

    for d in drifts:
        cum_key = (d["deck_id"], d["metric"])
        cum = cumulative_drift_by_pair.get(cum_key, 0.0)
        eval = evaluate_drift(
            deck_id=d["deck_id"],
            metric=d["metric"],
            old_value=d["old_value"],
            new_value=d["new_value"],
            cumulative_drift=cum,
            architectural_shift=d.get("architectural_shift"),
        )
        evaluations.append(eval)
        cumulative_drift_by_pair[cum_key] = eval.cumulative_drift
        total_abs_drift += abs(eval.drift)
        if eval.tier == TIER_HALT:
            halt_required = True

    # Safety #9 cumulative check.
    halt_threshold = PER_BATCH_CAP_DEFAULT * architectural_shift_total_cap_multiplier * len(drifts)
    if total_abs_drift > halt_threshold and len(drifts) > 0:
        halt_required = True

    return evaluations, halt_required
