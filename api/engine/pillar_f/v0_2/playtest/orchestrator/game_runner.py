"""Per-game runner for Stage 2 cycles.

Sub-C Phase 3. Implements:

- build_game_state(config) -> GameState
- run_single_game(config, llm_client) -> StageTwoGameResult

The runner orchestrates:
  1. Build GameState with 4 PlayerState seats + libraries from decks
  2. Mulligan phase (sub-B Phase 6 mulligan_setup with LLM deciders)
  3. Turn loop (sub-B priority responder + sub-C combat hook glue)
  4. Game-end detection (3 of 4 has_lost OR max_turns OR cost ceiling)
  5. StageTwoGameResult population (winner, elimination order, threat
     vectors, politics summary, cost summary, action log)
"""
from __future__ import annotations

import random
import time
from typing import Any, Dict, List, Optional, Tuple

from api.engine.pillar_f.v0_2.state import (
    Card, GameState, PlayerState, PlayerZones, Step, Phase,
)
from api.engine.pillar_f.v0_2.stack import (
    priority_round, resolve_top,
)
from api.engine.pillar_f.v0_2.turn import (
    STEP_ORDER, advance_step, start_step, step_opens_priority,
    untap_step, draw_step, cleanup_step,
    mulligan_setup,
)
from api.engine.pillar_f.v0_2.policy import (
    make_llm_priority_responder,
    make_llm_mulligan_decider, make_llm_bottom_picker,
    update_politics_state, roll_damage_log_for_turn,
    compute_threat_vector,
)
from api.engine.pillar_f.v0_2.policy.cost import CostTracker
from api.engine.pillar_f.v0_2.playtest.combat_glue import (
    make_llm_attacker_decider, make_llm_blocker_decider,
    run_llm_combat_phase, CombatDecisionRecord,
)
from api.engine.pillar_f.v0_2.playtest.orchestrator.types import (
    StageTwoGameConfig, StageTwoGameResult, StageTwoDeck,
)
from api.engine.pillar_f.v0_2.playtest.orchestrator.card_factory import (
    make_card_from_name,
)


GAME_RUNNER_VERSION = "pillar_f_v0_2_playtest_game_runner_v1"


def build_game_state(config: StageTwoGameConfig) -> GameState:
    """Construct a fresh GameState from a per-game config.

    Each player_id 0..3 gets a PlayerState with:
    - life_total = config.starting_life
    - library = shuffled card_ids from their deck's mainboard
                (commander not included in iter-11 simplification --
                commander gets played from command zone in iter-12+)

    Cards are constructed via make_card_from_name. Unknown cards
    become vanilla placeholders so deck construction never crashes.
    """
    rng = random.Random(config.seed)
    gs = GameState()
    for pid in range(4):
        deck = config.decks[pid]
        next_opp = (pid + 1) % 4
        ps = PlayerState(
            player_id=pid, name=f"P{pid}-{deck.deck_id}",
            life_total=config.starting_life,
            zones=PlayerZones(),
        )
        # Build cards from mainboard names.
        for cname in deck.mainboard:
            c = make_card_from_name(
                cname, owner=pid, next_opponent_pid=next_opp,
            )
            gs.add_card(c)
            ps.zones.library.append(c.card_id)
        # Shuffle library deterministically with this game's seed.
        rng.shuffle(ps.zones.library)
        gs.players.append(ps)
    gs.active_player = 0
    gs.turn_number = 1
    gs.step = Step.UNTAP
    return gs


def _apply_politics_for_resolved_entry(
    state: GameState, entry, *, prev_life_totals: List[int],
) -> List[str]:
    """When a damage-dealing spell resolves, log the damage in each
    defender's politics_state so threat-vector + alliance bumps fire."""
    lines: List[str] = []
    if entry is None:
        return lines
    payment = entry.payment or {}
    if payment.get("resolver") != "deal_damage_to_player":
        return lines
    amount = int(payment.get("amount", 0) or 0)
    if amount <= 0:
        return lines
    for t in entry.targets or []:
        if not isinstance(t, int):
            continue
        if not (0 <= t < len(state.players)):
            continue
        cur_life = state.players[t].life_total
        prev_life = prev_life_totals[t] if t < len(prev_life_totals) else cur_life
        delta = prev_life - cur_life
        if delta <= 0:
            continue
        update_politics_state(state, viewer_id=t, event={
            "type": "combat_damage",
            "from": entry.controller,
            "amount": delta,
        })
        lines.append(
            f"politics: P{t} logs {delta} damage from P{entry.controller}"
        )
    return lines


def _detect_alive_players(state: GameState) -> List[int]:
    return [p.player_id for p in state.players if not p.has_lost]


def _detect_winner(state: GameState) -> Optional[int]:
    alive = _detect_alive_players(state)
    if len(alive) == 1:
        return alive[0]
    return None


def _record_eliminations(
    state: GameState, prev_alive: List[int],
    turn_number: int, elim_log: List[Tuple[int, int, str]],
) -> List[int]:
    """Detect new eliminations between two SBA passes and append to log."""
    new_alive = _detect_alive_players(state)
    for pid in prev_alive:
        if pid not in new_alive:
            # Determine cause.
            ps = state.players[pid]
            cause = "unknown"
            if ps.life_total <= 0:
                cause = "life_total_zero"
            elif ps.has_drawn_from_empty_library:
                cause = "drew_from_empty"
            elim_log.append((pid, turn_number, cause))
    return new_alive


def run_single_game(
    config: StageTwoGameConfig,
    *,
    llm_client: Any,
) -> StageTwoGameResult:
    """Run one complete Stage 2 game and return the result.

    Args:
        config: per-game configuration (4 decks + ceilings + seed).
        llm_client: an AnthropicClient (or compatible mock with
            is_available + call_with_budget interface).
    """
    t_start = time.perf_counter()
    gs = build_game_state(config)

    deck_archetype_hints = {
        pid: config.decks[pid].archetype_hint
        for pid in range(4)
    }

    ct = CostTracker(
        per_turn_ceiling_usd=config.per_turn_cost_ceiling_usd,
        per_game_ceiling_usd=config.per_game_cost_ceiling_usd,
    )
    politics_state_by_player = {
        pid: {"threats": {}, "alliances": {}, "deals": []}
        for pid in range(4)
    }
    rationale_history_by_player = {pid: [] for pid in range(4)}
    action_log: List[str] = []
    politics_log: List[str] = []
    combat_decisions: List[CombatDecisionRecord] = []

    # --- Mulligan phase ---
    decider = make_llm_mulligan_decider(
        llm_client=llm_client, cost_tracker=ct,
        deck_archetype_hint_by_player=deck_archetype_hints,
    )
    picker = make_llm_bottom_picker(
        llm_client=llm_client, cost_tracker=ct,
        deck_archetype_hint_by_player=deck_archetype_hints,
    )
    mulligan_setup(
        gs, decider_fn=decider, bottom_picker_fn=picker,
        max_mulligans=config.max_mulligans,
        seed_per_player={pid: config.seed + pid for pid in range(4)},
    )

    # --- Priority responder + combat deciders ---
    responder = make_llm_priority_responder(
        llm_client=llm_client, cost_tracker=ct,
        action_log=action_log,
        politics_state_by_player=politics_state_by_player,
        deck_archetype_hint_by_player=deck_archetype_hints,
        rationale_history_by_player=rationale_history_by_player,
    )
    atk_decider = make_llm_attacker_decider(
        llm_client=llm_client, cost_tracker=ct,
        politics_state_by_player=politics_state_by_player,
        deck_archetype_hint_by_player=deck_archetype_hints,
        decision_log=combat_decisions,
    )
    blk_decider = make_llm_blocker_decider(
        llm_client=llm_client, cost_tracker=ct,
        politics_state_by_player=politics_state_by_player,
        decision_log=combat_decisions,
    )

    # --- Turn loop ---
    elimination_order: List[Tuple[int, int, str]] = []
    turns_run = 0
    halted_reason: Optional[str] = None
    for turn in range(1, config.max_turns + 1):
        # Roll damage decay at turn start.
        for pid in range(4):
            roll_damage_log_for_turn(gs, viewer_id=pid, current_turn=turn)
        # Check if game over before turn loop.
        winner = _detect_winner(gs)
        if winner is not None:
            halted_reason = "win"
            turns_run = turn - 1
            break

        for ap in range(4):
            if gs.players[ap].has_lost:
                continue
            gs.active_player = ap
            gs.turn_number = turn
            prev_alive_at_turn_start = _detect_alive_players(gs)

            for step in STEP_ORDER:
                start_step(gs, step)
                if step == Step.UNTAP:
                    untap_step(gs)
                elif step == Step.DRAW:
                    draw_step(gs)
                elif step == Step.CLEANUP:
                    cleanup_step(gs)
                elif step == Step.DECLARE_ATTACKERS and config.enable_combat:
                    # Substitute LLM-driven combat for the manual
                    # declare_attackers/blockers walk. Run the entire
                    # combat phase end-to-end here; then advance past
                    # the combat substeps the substrate would otherwise
                    # iterate.
                    if not ct.game_halted_for_cost:
                        try:
                            run_llm_combat_phase(
                                gs, active_player=ap,
                                attacker_decider=atk_decider,
                                blocker_decider=blk_decider,
                            )
                        except ValueError as e:
                            action_log.append(
                                f"T{turn} P{ap} combat error: {e}"
                            )
                    # advance_step continues normally; declare_blockers
                    # / first_strike / combat_damage steps still happen
                    # but with empty combat_state (substrate handles
                    # gracefully).

                if step_opens_priority(step) and not ct.game_halted_for_cost:
                    priority_round(gs, responder)
                    while gs.stack:
                        prev_life = [p.life_total for p in gs.players]
                        resolved = resolve_top(gs)
                        more = _apply_politics_for_resolved_entry(
                            gs, resolved, prev_life_totals=prev_life,
                        )
                        politics_log.extend(more)
                        if not ct.game_halted_for_cost:
                            priority_round(gs, responder)

                advance_step(gs)
                if ct.game_halted_for_cost:
                    halted_reason = "cost"
                    break

            # End of turn: detect eliminations.
            prev_alive_at_turn_start = _record_eliminations(
                gs, prev_alive_at_turn_start, turn, elimination_order,
            )

            if ct.game_halted_for_cost:
                break
            if _detect_winner(gs) is not None:
                break

        turns_run = turn
        if ct.game_halted_for_cost:
            break
        if _detect_winner(gs) is not None:
            halted_reason = "win"
            break
    else:
        halted_reason = "max_turns"

    # --- Result aggregation ---
    elapsed = time.perf_counter() - t_start
    winner_pid = _detect_winner(gs)
    halted_for_cost = bool(ct.game_halted_for_cost)
    # If cost halted: pick tie-break winner = highest life among survivors.
    if halted_for_cost and winner_pid is None:
        alive = _detect_alive_players(gs)
        if alive:
            winner_pid = max(alive, key=lambda pid: gs.players[pid].life_total)

    # Threat vectors at game end.
    threat_vectors: Dict[int, Dict[int, float]] = {}
    for pid in range(4):
        if gs.players[pid].has_lost:
            continue
        tv: Dict[int, float] = {}
        for opp in range(4):
            if opp == pid or gs.players[opp].has_lost:
                continue
            v = compute_threat_vector(gs, viewer_id=pid, opponent_id=opp)
            tv[opp] = round(v["score"], 3)
        threat_vectors[pid] = tv

    # Politics summary.
    politics_summary: Dict[str, Any] = {
        "alliance_distribution": {},
        "deals_made_count": 0,
        "deals_honored_count": 0,
        "damage_log_count": len(politics_log),
    }
    for pid in range(4):
        ps_pol = gs.players[pid].politics_state or {}
        alliances = ps_pol.get("alliances") or {}
        ally_n = sum(1 for v in alliances.values() if v == "ally")
        neutral_n = sum(1 for v in alliances.values() if v == "neutral")
        rival_n = sum(1 for v in alliances.values() if v == "rival")
        politics_summary["alliance_distribution"][pid] = {
            "ally": ally_n, "neutral": neutral_n, "rival": rival_n,
        }
        deals = ps_pol.get("deals") or []
        politics_summary["deals_made_count"] += len(deals)
        politics_summary["deals_honored_count"] += sum(
            1 for d in deals if d.get("kept")
        )

    # Fallback events from CostTracker.events.
    fallback_events = [
        e for e in ct.events
        if e.get("event") in ("COST_CEILING_HIT", "GAME_COST_CEILING_EXCEEDED")
    ]

    # Combat decisions diagnostics — pack to dicts for JSON serializability.
    combat_diag: List[Dict[str, Any]] = []
    for rec in combat_decisions:
        combat_diag.append({
            "phase": rec.phase,
            "player_id": rec.player_id,
            "turn_number": rec.turn_number,
            "eligible_count": rec.eligible_count,
            "llm_calls_made": rec.llm_calls_made,
            "parse_failures": rec.parse_failures,
            "fallback_used": rec.fallback_used,
            "final_count": rec.final_count,
            "rationale": rec.rationale,
        })

    return StageTwoGameResult(
        game_idx=0,  # cycle runner sets this
        seed=config.seed,
        deck_under_test_pid=config.deck_under_test_pid,
        deck_ids=[d.deck_id for d in config.decks],
        winner_pid=winner_pid,
        turns_run=turns_run,
        halted_for_cost=halted_for_cost,
        halted_reason=halted_reason,
        elimination_order=elimination_order,
        final_life_totals={
            pid: gs.players[pid].life_total for pid in range(4)
        },
        final_threat_vectors=threat_vectors,
        politics_summary=politics_summary,
        action_log=list(action_log),
        combat_decisions_log=combat_diag,
        total_spend_usd=ct.total_spend(),
        total_llm_calls=len(ct.events),
        fallback_events=fallback_events,
        elapsed_seconds=elapsed,
    )
