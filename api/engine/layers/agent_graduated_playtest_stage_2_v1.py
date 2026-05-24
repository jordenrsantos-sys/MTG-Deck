"""agent_graduated_playtest_stage_2_v1 — sub-mega-task C Phase 5.

Stitches Stage 2 (sub-C cycle runner) into the existing Stage 1
graduated_playtest_v1 orchestrator. Pillar D's dispatcher calls
`run_stage_1_then_stage_2(...)` instead of run_graduated_sweep when
the user has set `enable_stage_2=True` on the build.

Flow:
  1. Always run Stage 1 (statistical approximator) via
     run_graduated_sweep. Reports GraduationReport.
  2. If enable_stage_2 is True AND Stage 1's overall_status is
     "graduated" or "graduated_partial" (i.e., Stage 1 didn't
     immediately RED out at Tier 0): run Stage 2 via
     run_stage_two_cycle.
  3. Stitch Stage 1 + Stage 2 results into a CombinedReport with a
     unified `stage_2_recommendation` (GREEN / YELLOW / RED /
     INCOMPLETE / SKIPPED).
  4. Append a calibration delta record to
     `repo/api/engine/data/agent/stage_calibration_log.json` for the
     calibration team to retune Stage 1 if Stage 1 vs Stage 2 deltas
     trend > 15% on an archetype.

The dispatcher reads CombinedReport.stage_2_recommendation:
  - GREEN -> ship immediately
  - YELLOW -> ship with summary panel + ask user to confirm
  - RED -> revise the build (return to Pillar D for re-pick)
  - INCOMPLETE -> show cost-halt message, ship as Stage 1 alone
  - SKIPPED -> user did not opt-in; ship as Stage 1 alone

Opt-in default: enable_stage_2=False. Stage 2 costs ~$150/cycle live
per scoping; user must explicitly enable per build.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from api.engine.layers.agent_graduated_playtest_v1 import (
    GraduationReport, run_graduated_sweep,
)
from api.engine.pillar_f.v0_2.playtest.aggregation import StageTwoReport
from api.engine.pillar_f.v0_2.playtest.cycle import run_stage_two_cycle
from api.engine.pillar_f.v0_2.playtest.orchestrator import (
    StageTwoDeck, StageTwoCycleConfig,
)


STAGE_2_INTEGRATION_VERSION = "agent_graduated_playtest_stage_2_v1.0"

# Default calibration log location (relative to repo root).
DEFAULT_CALIBRATION_LOG = Path("api/engine/data/agent/stage_calibration_log.json")

# Delta threshold above which the calibration team should review.
CALIBRATION_DELTA_THRESHOLD = 0.15


@dataclass
class CombinedReport:
    """Stitched Stage 1 + Stage 2 result the dispatcher consumes."""
    stage_1_report: GraduationReport
    stage_2_report: Optional[StageTwoReport]      # None when skipped
    stage_2_recommendation: str                    # "GREEN" | "YELLOW" | "RED" | "INCOMPLETE" | "SKIPPED"
    stage_2_recommendation_reason: str
    calibration_delta: Optional[float] = None      # Stage1_winrate - Stage2_winrate
    enable_stage_2: bool = False
    version: str = STAGE_2_INTEGRATION_VERSION


def _stage_1_pod_winrate_baseline(
    stage_1_report: GraduationReport,
) -> Optional[float]:
    """Compute Stage 1's overall winrate baseline -- a weighted
    average across tiers reached. For calibration delta, we pick
    the highest-tier pod_winrate the deck achieved (or Tier 0 if
    Tier 1+ wasn't reached)."""
    if not stage_1_report.tier_results:
        return None
    return stage_1_report.tier_results[-1].pod_winrate


def _append_calibration_log(
    *,
    deck_id: str, deck_archetype: str, bracket: str,
    stage_1_winrate: Optional[float],
    stage_2_winrate: Optional[float],
    stage_2_recommendation: str,
    log_path: Path,
) -> Dict[str, Any]:
    """Append a calibration record to the JSON log. Creates the file
    + parent dir if needed. Returns the record that was written."""
    delta = None
    if stage_1_winrate is not None and stage_2_winrate is not None:
        delta = round(stage_1_winrate - stage_2_winrate, 3)
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "deck_id": deck_id,
        "deck_archetype": deck_archetype,
        "bracket": bracket,
        "stage_1_winrate": stage_1_winrate,
        "stage_2_winrate": stage_2_winrate,
        "delta": delta,
        "delta_exceeds_threshold": (
            delta is not None and abs(delta) > CALIBRATION_DELTA_THRESHOLD
        ),
        "stage_2_recommendation": stage_2_recommendation,
        "version": STAGE_2_INTEGRATION_VERSION,
    }
    log_path.parent.mkdir(parents=True, exist_ok=True)
    if log_path.exists():
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
            if not isinstance(existing, list):
                existing = []
        except (json.JSONDecodeError, OSError):
            existing = []
    else:
        existing = []
    existing.append(record)
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, default=str)
    return record


def run_stage_1_then_stage_2(
    *,
    deck: Sequence[Dict[str, Any]],
    bracket: str,
    enable_stage_2: bool = False,
    stage_2_deck: Optional[StageTwoDeck] = None,
    stage_2_control_pool: Optional[List[StageTwoDeck]] = None,
    stage_2_n_games: int = 30,
    stage_2_output_dir: Optional[Path] = None,
    stage_2_cycle_cost_ceiling_usd: float = 200.0,
    llm_client: Optional[Any] = None,
    db_snapshot_id: Optional[str] = None,
    calibration_log_path: Optional[Path] = None,
) -> CombinedReport:
    """Run Stage 1 then optionally Stage 2; return a CombinedReport.

    Args:
        deck, bracket: standard Stage 1 inputs.
        enable_stage_2: opt-in flag. If False, returns CombinedReport
            with stage_2_recommendation="SKIPPED".
        stage_2_deck: StageTwoDeck for the deck-under-test. Required
            when enable_stage_2=True.
        stage_2_control_pool: List of StageTwoDeck for opposition.
            Required when enable_stage_2=True.
        stage_2_n_games: cycle size (default 30 per scoping).
        stage_2_output_dir: where per-game JSONs + cycle report go.
        stage_2_cycle_cost_ceiling_usd: per-cycle ceiling (default $200).
        llm_client: AnthropicClient or compatible mock. Required when
            enable_stage_2=True.
        db_snapshot_id: passed through to Stage 1.
        calibration_log_path: override the default log location.
    """
    # Stage 1 always.
    stage_1 = run_graduated_sweep(
        deck=deck, bracket=bracket, db_snapshot_id=db_snapshot_id,
    )

    if not enable_stage_2:
        return CombinedReport(
            stage_1_report=stage_1,
            stage_2_report=None,
            stage_2_recommendation="SKIPPED",
            stage_2_recommendation_reason=(
                "enable_stage_2 flag is False (opt-in default)."
            ),
            enable_stage_2=False,
        )

    if stage_2_deck is None or stage_2_control_pool is None \
            or llm_client is None:
        return CombinedReport(
            stage_1_report=stage_1,
            stage_2_report=None,
            stage_2_recommendation="SKIPPED",
            stage_2_recommendation_reason=(
                "enable_stage_2=True but stage_2_deck/"
                "stage_2_control_pool/llm_client not provided."
            ),
            enable_stage_2=True,
        )

    # Stage 1 must have reached at least Tier 0 with some signal --
    # otherwise spending $150 on Stage 2 for a definitively bad deck
    # wastes budget. Gate: Stage 1 must NOT be stalled_tier_0 with 0
    # winrate.
    if stage_1.overall_status == "stalled_tier_0" \
            and stage_1.tier_results \
            and stage_1.tier_results[0].pod_winrate < 0.10:
        return CombinedReport(
            stage_1_report=stage_1,
            stage_2_report=None,
            stage_2_recommendation="RED",
            stage_2_recommendation_reason=(
                f"Stage 1 stalled at Tier 0 with pod_winrate="
                f"{stage_1.tier_results[0].pod_winrate:.2f} < 0.10. "
                f"Skipping Stage 2 to save budget; deck needs Pillar D "
                f"revision before re-test."
            ),
            enable_stage_2=True,
        )

    # Run Stage 2.
    cycle_config = StageTwoCycleConfig(
        deck_under_test=stage_2_deck,
        control_pool=stage_2_control_pool,
        n_games=stage_2_n_games,
        output_dir=stage_2_output_dir,
        cycle_cost_ceiling_usd=stage_2_cycle_cost_ceiling_usd,
    )
    stage_2 = run_stage_two_cycle(cycle_config, llm_client=llm_client)

    # Append calibration log record.
    log_path = calibration_log_path or DEFAULT_CALIBRATION_LOG
    record = _append_calibration_log(
        deck_id=stage_2_deck.deck_id,
        deck_archetype=stage_2_deck.archetype_hint,
        bracket=bracket,
        stage_1_winrate=_stage_1_pod_winrate_baseline(stage_1),
        stage_2_winrate=stage_2.win_rate,
        stage_2_recommendation=stage_2.pass_recommendation,
        log_path=log_path,
    )

    return CombinedReport(
        stage_1_report=stage_1,
        stage_2_report=stage_2,
        stage_2_recommendation=stage_2.pass_recommendation,
        stage_2_recommendation_reason=stage_2.recommendation_reason,
        calibration_delta=record["delta"],
        enable_stage_2=True,
    )
