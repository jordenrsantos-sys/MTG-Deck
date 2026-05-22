"""
agent_graduated_playtest_v1 — Mega-task v5 Phase 12.

Stage 1 of the graduated playtest framework. Runs the Pillar F v0.1
statistical approximator against tiered opposition pods (Tier 0 =
precon-equivalent, Tier 1 = mid-tier, Tier 2 = high-tier / cEDH) and
emits a `GraduationReport` showing the deck's pod winrate at each tier
it was advanced through, plus advancement status.

Advancement rule (per kickoff):
  - Run Tier 0. If pod_winrate >= TIER_ADVANCE_THRESHOLD (0.55), advance.
  - Run Tier 1. If pod_winrate >= 0.55, advance.
  - Run Tier 2. Regardless of result, this is the final tier.

The Pillar F approximator currently keys off the opponent's `bracket`
field only. To get a meaningful gradient across tiers within the same
bracket, this module maps (bracket, tier) → an *effective* bracket
passed to the approximator (tier 0 = one step down, tier 1 = same, tier
2 = one step up). When Phase F v0.2 ships per-deck simulation this
becomes obsolete.

Public API:
  - run_graduated_sweep(*, deck, bracket, db_snapshot_id=None,
        tier_advance_threshold=0.55) -> GraduationReport
  - GraduationReport dataclass
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from api.engine.layers.agent_statistical_approximator_v1 import (
    approximate_pod_winrate, PodWinrateReport,
)
from api.engine.playtest.opposition_decks_v1 import filter_by_bracket_and_tier


GRADUATED_PLAYTEST_VERSION = "agent_graduated_playtest_v1.0"
TIER_ADVANCE_THRESHOLD = 0.55

_TIER_LABELS = {
    0: "Tier 0 (precon-equivalent)",
    1: "Tier 1 (mid-tier)",
    2: "Tier 2 (high-tier / cEDH)",
}


def _effective_bracket(bracket: str, tier: int) -> str:
    """Map (bracket, tier) to a synthetic bracket used by the approximator
    so tier-on-tier gradient is meaningful even with a bracket-only model."""
    try:
        base = int(bracket[1:])
    except (ValueError, IndexError):
        base = 3
    adj = {0: -1, 1: 0, 2: 1}.get(tier, 0)
    new = max(1, min(5, base + adj))
    return f"B{new}"


@dataclass
class TierResult:
    tier: int
    label: str
    bracket: str
    effective_bracket: str
    pod_winrate: float
    per_opponent_winrate: Dict[str, float] = field(default_factory=dict)
    opponents: List[Dict[str, Any]] = field(default_factory=list)
    advanced: bool = False
    reason: str = ""


@dataclass
class GraduationReport:
    bracket: str
    final_tier_reached: int  # 0, 1, or 2
    tier_results: List[TierResult]
    suggested_tweaks: List[str] = field(default_factory=list)
    overall_status: str = "unknown"  # "graduated", "stalled_tier_0", "stalled_tier_1", "graduated_partial"
    threshold_used: float = TIER_ADVANCE_THRESHOLD
    version: str = GRADUATED_PLAYTEST_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _summarize_opp(opp: Dict[str, Any]) -> Dict[str, Any]:
    """Trim the opposition entry down to the keys callers care about."""
    return {
        "corpus_id": opp.get("corpus_id"),
        "commander": opp.get("commander"),
        "bracket": opp.get("bracket"),
        "archetype_hint": opp.get("archetype_hint"),
        "role_tag": opp.get("role_tag"),
        "opposition_tier": opp.get("opposition_tier"),
    }


def _build_opposition_for_tier(
    bracket: str, tier: int,
) -> List[Dict[str, Any]]:
    """Load opposition entries for (bracket, tier), assigning each the
    effective bracket so the approximator picks up tier gradient."""
    entries = filter_by_bracket_and_tier(bracket, tier)
    eff_b = _effective_bracket(bracket, tier)
    out: List[Dict[str, Any]] = []
    for e in entries:
        opp = dict(e)
        # Override the bracket the approximator sees, but keep the
        # original bracket on a different key for the report.
        opp["bracket_displayed"] = opp.get("bracket")
        opp["bracket"] = eff_b
        out.append(opp)
    # Take the first 3 — opposition_decks_v1 is curated, the order is the
    # canonical "representative trio."
    return out[:3]


def _generate_suggested_tweaks(
    report_so_far: List[TierResult],
) -> List[str]:
    """Stage-3 hook: produce free-text tweak suggestions based on which
    tier blocked advancement. Stage 1 just emits short heuristic notes;
    Stage 3 (game-simulation) will produce concrete card swaps.
    """
    if not report_so_far:
        return []
    last = report_so_far[-1]
    if last.advanced:
        return []
    if last.tier == 0:
        return [
            "Tier 0 (precon-equivalent) stall: deck loses to bracket-baseline opponents. "
            "Likely causes: missing mass-removal, missing card-draw engine, "
            "or core curve too high. Check pillar_e_v0_3_curve_check and "
            "pillar_e_v0_4_interaction_check in the build summary.",
        ]
    if last.tier == 1:
        return [
            "Tier 1 (mid-tier) stall: deck handles precons but loses to community-standard "
            "competition. Likely causes: speed gap (mid-tier wins on T6-T7 — deck needs "
            "faster win-path) or interaction gap (need more counterspells / spot removal).",
        ]
    if last.tier == 2:
        return [
            "Tier 2 (high-tier) stall: deck competes at mid-tier but falls behind cEDH. "
            "Expected for B1-B4 decks. For B5 specifically, this suggests the deck "
            "is fundamentally a mid-tier B5 build, not a cEDH-tier B5 build.",
        ]
    return []


def run_graduated_sweep(
    *,
    deck: Sequence[Dict[str, Any]],
    bracket: str,
    db_snapshot_id: Optional[str] = None,
    tier_advance_threshold: float = TIER_ADVANCE_THRESHOLD,
) -> GraduationReport:
    """Run the deck through Tier 0 → Tier 1 → Tier 2 with the
    statistical approximator. Returns a `GraduationReport`.

    Advancement: pod_winrate at tier N must meet or exceed
    `tier_advance_threshold` (default 0.55) to attempt tier N+1.

    When no opposition entries exist for a (bracket, tier) cell, that
    tier's TierResult will have empty opponents + winrate 0.0 +
    advanced=False with reason="no_opposition_in_registry".
    """
    target = (bracket or "").strip().upper()
    tier_results: List[TierResult] = []
    final_tier_reached = -1

    for tier in (0, 1, 2):
        eff_b = _effective_bracket(target, tier)
        opposition = _build_opposition_for_tier(target, tier)
        if not opposition:
            tier_results.append(TierResult(
                tier=tier,
                label=_TIER_LABELS[tier],
                bracket=target,
                effective_bracket=eff_b,
                pod_winrate=0.0,
                advanced=False,
                reason="no_opposition_in_registry",
            ))
            break

        report = approximate_pod_winrate(
            deck=deck,
            opponents=opposition,
            db_snapshot_id=db_snapshot_id,
        )
        advanced = report.pod_winrate >= tier_advance_threshold
        tier_result = TierResult(
            tier=tier,
            label=_TIER_LABELS[tier],
            bracket=target,
            effective_bracket=eff_b,
            pod_winrate=round(report.pod_winrate, 3),
            per_opponent_winrate=report.per_opponent_winrate,
            opponents=[_summarize_opp(o) for o in opposition],
            advanced=advanced,
            reason="winrate_ge_threshold" if advanced else "winrate_lt_threshold",
        )
        tier_results.append(tier_result)
        final_tier_reached = tier
        if not advanced:
            break

    # Overall status.
    if not tier_results:
        overall = "no_data"
    elif final_tier_reached == 2 and tier_results[-1].advanced:
        overall = "graduated"  # advanced past Tier 2 — top of the line
    elif final_tier_reached == 2:
        overall = "graduated_partial"  # reached Tier 2 but didn't beat it
    elif final_tier_reached == 1:
        overall = "stalled_tier_1"
    elif final_tier_reached == 0:
        overall = "stalled_tier_0"
    else:
        overall = "no_data"

    return GraduationReport(
        bracket=target,
        final_tier_reached=max(0, final_tier_reached),
        tier_results=tier_results,
        suggested_tweaks=_generate_suggested_tweaks(tier_results),
        overall_status=overall,
        threshold_used=tier_advance_threshold,
    )
