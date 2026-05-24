"""StageTwoReport aggregation + GREEN/YELLOW/RED recommendation.

Sub-C Phase 4. Per scoping doc section 5b.

Input: List[StageTwoGameResult] from a cycle's run_single_game calls.
Output: StageTwoReport dataclass that the dispatcher's
stage_2_recommendation flag reads.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


AGGREGATOR_VERSION = "pillar_f_v0_2_playtest_aggregator_v1"


# Recommendation thresholds per scoping doc section 5b.
GREEN_WINRATE = 0.30
YELLOW_WINRATE = 0.20
GREEN_AVG_TURN_ELIMINATED = 12.0  # deck survives mid-game
YELLOW_AVG_TURN_ELIMINATED = 8.0


@dataclass
class StageTwoReport:
    """Aggregated cycle result."""
    deck_under_test_id: str
    deck_under_test_archetype: str
    games_completed: int
    games_halted_for_cost: int
    win_rate: float
    avg_turn_eliminated_when_lost: float
    avg_damage_dealt: float            # iter-11 stub (politics log proxy)
    avg_damage_taken: float
    politics_summary: Dict[str, Any]
    cost_summary: Dict[str, float]
    combat_summary: Dict[str, int]
    pass_recommendation: str           # "GREEN" | "YELLOW" | "RED" | "INCOMPLETE"
    recommendation_reason: str
    per_game_brief: List[Dict[str, Any]] = field(default_factory=list)


def aggregate_cycle(
    *,
    deck_under_test_id: str,
    deck_under_test_archetype: str,
    game_results: List[Any],          # List[StageTwoGameResult]
    halted_for_cycle_cost: bool = False,
) -> StageTwoReport:
    """Aggregate a list of per-game results into a single StageTwoReport.

    Args:
        deck_under_test_id, deck_under_test_archetype: from the cycle
            config's deck_under_test.
        game_results: results from run_single_game (one per game).
        halted_for_cycle_cost: True if the cycle runner aborted early
            due to per-cycle ceiling -> report tagged INCOMPLETE.
    """
    n_total = len(game_results)
    n_halted = sum(1 for r in game_results if r.halted_for_cost)
    n_completed = n_total - n_halted

    # Win-rate: games where deck-under-test pid was the winner.
    wins = sum(
        1 for r in game_results
        if r.winner_pid == r.deck_under_test_pid
    )
    win_rate = wins / n_total if n_total else 0.0

    # Avg turn eliminated when lost.
    lost_turns: List[int] = []
    for r in game_results:
        elims = [
            t for (pid, t, _cause) in r.elimination_order
            if pid == r.deck_under_test_pid
        ]
        if elims:
            lost_turns.extend(elims)
    avg_turn_lost = (
        sum(lost_turns) / len(lost_turns) if lost_turns else 0.0
    )

    # Damage dealt/taken (iter-11 proxy via politics damage_log counts).
    damage_dealt_total = 0
    damage_taken_total = 0
    for r in game_results:
        # Politics summary has alliance_distribution etc; the damage
        # log lives on each player's politics_state, captured at game
        # end. The runner snapshots damage_log_count.
        dlc = r.politics_summary.get("damage_log_count", 0)
        # Use damage_log_count as a coarse proxy; iter-12+ refines.
        damage_dealt_total += dlc  # symmetric (one event = dealt by A taken by B)
        damage_taken_total += dlc
    avg_damage_dealt = damage_dealt_total / n_total if n_total else 0.0
    avg_damage_taken = damage_taken_total / n_total if n_total else 0.0

    # Politics summary: aggregate alliance distributions across games.
    total_ally = total_neutral = total_rival = 0
    total_deals_made = total_deals_honored = 0
    games_with_alliance_transition = 0
    games_with_deals = 0
    for r in game_results:
        any_transition_this_game = False
        adist = r.politics_summary.get("alliance_distribution") or {}
        for pid_alliances in adist.values():
            total_ally += pid_alliances.get("ally", 0)
            total_neutral += pid_alliances.get("neutral", 0)
            total_rival += pid_alliances.get("rival", 0)
            if pid_alliances.get("ally", 0) or pid_alliances.get("rival", 0):
                any_transition_this_game = True
        if any_transition_this_game:
            games_with_alliance_transition += 1
        total_deals_made += r.politics_summary.get("deals_made_count", 0)
        total_deals_honored += r.politics_summary.get(
            "deals_honored_count", 0,
        )
        if r.politics_summary.get("deals_made_count", 0) > 0:
            games_with_deals += 1
    politics_agg: Dict[str, Any] = {
        "total_ally": total_ally,
        "total_neutral": total_neutral,
        "total_rival": total_rival,
        "total_deals_made": total_deals_made,
        "total_deals_honored": total_deals_honored,
        "games_with_alliance_transition": games_with_alliance_transition,
        "games_with_deals": games_with_deals,
    }

    # Cost summary.
    spend_list = [r.total_spend_usd for r in game_results]
    cost_summary: Dict[str, float] = {
        "total_spend": sum(spend_list),
        "per_game_avg": (sum(spend_list) / n_total) if n_total else 0.0,
        "per_game_max": max(spend_list) if spend_list else 0.0,
        "games_halted_for_cost": float(n_halted),
        "total_fallback_events": float(sum(
            len(r.fallback_events) for r in game_results
        )),
    }

    # Combat summary.
    combat_summary: Dict[str, int] = {
        "games_with_combat": 0,
        "games_with_multi_block": 0,
        "total_attacker_decisions": 0,
        "total_blocker_decisions": 0,
        "total_combat_fallbacks": 0,
    }
    for r in game_results:
        atk_count = sum(
            1 for d in r.combat_decisions_log if d.get("phase") == "attackers"
        )
        blk_count = sum(
            1 for d in r.combat_decisions_log if d.get("phase") == "blockers"
        )
        if atk_count > 0:
            combat_summary["games_with_combat"] += 1
        # Multi-block: any blocker decision with >1 blocker indices in
        # a single attack slot. Approximate via final_count > attacker
        # count for that decision; iter-12+ tracks more precisely.
        for d in r.combat_decisions_log:
            if d.get("phase") == "blockers" and d.get("final_count", 0) > 1:
                combat_summary["games_with_multi_block"] += 1
                break  # one game = one increment
        combat_summary["total_attacker_decisions"] += atk_count
        combat_summary["total_blocker_decisions"] += blk_count
        combat_summary["total_combat_fallbacks"] += sum(
            1 for d in r.combat_decisions_log if d.get("fallback_used")
        )

    # Recommendation.
    # A deck that never loses (lost_turns empty) satisfies the
    # "survives mid-game" criterion vacuously -- if it's never
    # eliminated, it cannot fold early. Treat no-losses as satisfying
    # the GREEN_AVG_TURN_ELIMINATED gate.
    survives_mid_game = (
        not lost_turns or avg_turn_lost >= GREEN_AVG_TURN_ELIMINATED
    )
    survives_yellow = (
        not lost_turns or avg_turn_lost >= YELLOW_AVG_TURN_ELIMINATED
    )
    if halted_for_cycle_cost or n_completed == 0:
        pass_rec = "INCOMPLETE"
        reason = (
            f"Cycle halted for cost or no completed games "
            f"({n_completed}/{n_total} completed)."
        )
    elif win_rate >= GREEN_WINRATE and survives_mid_game:
        pass_rec = "GREEN"
        reason = (
            f"win_rate={win_rate:.2f} >= {GREEN_WINRATE} AND "
            f"(no losses OR avg_turn_eliminated={avg_turn_lost:.1f} >= "
            f"{GREEN_AVG_TURN_ELIMINATED})."
        )
    elif win_rate >= YELLOW_WINRATE or survives_yellow:
        pass_rec = "YELLOW"
        reason = (
            f"win_rate={win_rate:.2f} (need {GREEN_WINRATE} for GREEN) "
            f"OR avg_turn_eliminated={avg_turn_lost:.1f} "
            f"(need {GREEN_AVG_TURN_ELIMINATED}); mid-tier."
        )
    else:
        pass_rec = "RED"
        reason = (
            f"win_rate={win_rate:.2f} < {YELLOW_WINRATE} AND "
            f"avg_turn_eliminated={avg_turn_lost:.1f} < "
            f"{YELLOW_AVG_TURN_ELIMINATED}; deck likely folds early."
        )

    # Per-game brief for the report's "notable games" section.
    per_game_brief: List[Dict[str, Any]] = []
    for i, r in enumerate(game_results):
        per_game_brief.append({
            "game_idx": getattr(r, "game_idx", i),
            "seed": r.seed,
            "winner_pid": r.winner_pid,
            "turns_run": r.turns_run,
            "halted_for_cost": r.halted_for_cost,
            "halted_reason": r.halted_reason,
            "spend_usd": r.total_spend_usd,
            "llm_calls": r.total_llm_calls,
            "final_life_totals": dict(r.final_life_totals),
            "deck_under_test_outcome": (
                "WIN" if r.winner_pid == r.deck_under_test_pid
                else ("LOSS_ELIMINATED" if any(
                    pid == r.deck_under_test_pid
                    for (pid, _t, _c) in r.elimination_order
                ) else "DRAW_OR_HALT")
            ),
        })

    return StageTwoReport(
        deck_under_test_id=deck_under_test_id,
        deck_under_test_archetype=deck_under_test_archetype,
        games_completed=n_completed,
        games_halted_for_cost=n_halted,
        win_rate=win_rate,
        avg_turn_eliminated_when_lost=avg_turn_lost,
        avg_damage_dealt=avg_damage_dealt,
        avg_damage_taken=avg_damage_taken,
        politics_summary=politics_agg,
        cost_summary=cost_summary,
        combat_summary=combat_summary,
        pass_recommendation=pass_rec,
        recommendation_reason=reason,
        per_game_brief=per_game_brief,
    )
