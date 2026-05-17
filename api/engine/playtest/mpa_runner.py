"""mpa_runner — Phase 5b.1d. Turn loop runner + 5b.3 blocking layer."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .mpa_game_state import GameState, Phase, Step, CardInGame
from .mpa_actions import Action, ActionType, enumerate_legal_actions
from .mpa_policy import choose_action, should_mulligan, choose_blocks


RUNNER_VERSION = "mpa_runner_v0.2_blocking"
MAX_TURNS = 50
MAX_ACTIONS_PER_PRIORITY = 100


@dataclass
class GameResult:
    winner_seat: Optional[int]
    loss_reason: Optional[str]
    final_turn: int
    actions_taken: int
    decision_log: List[Dict[str, Any]]


def run_game(state, *, max_turns=MAX_TURNS, record_decisions=False):
    decisions = []
    counter = {"actions": 0}
    while not state.game_over and state.turn_number <= max_turns:
        _execute_turn(state, decisions, record_decisions, counter)
        _check_state_based(state)
        state.turn_number += 1
        state.active_player_index = (state.active_player_index + 1) % len(state.players)
        state.priority_player_index = state.active_player_index
        for seat in range(len(state.players)):
            state.lands_played_this_turn_by_seat[seat] = 0
            state.mana_pool_by_seat[seat] = {}
    if not state.game_over:
        state.game_over = True
        state.loss_reason = "turn_cap_reached"
    return GameResult(
        winner_seat=state.winner_seat,
        loss_reason=state.loss_reason,
        final_turn=state.turn_number - 1,
        actions_taken=counter["actions"],
        decision_log=decisions,
    )


def _execute_turn(state, decisions, record_decisions, counter):
    active = state.active_player_index
    state.phase = Phase.BEGINNING
    state.step = Step.UNTAP
    _untap_all(state, active)
    state.step = Step.UPKEEP
    state.step = Step.DRAW
    if state.turn_number > 1 or active != 0:
        state.draw(active, 1)
        if state.players[active].life <= -50:
            state.game_over = True
            state.loss_reason = "deck_out"
            return
    state.phase = Phase.PRECOMBAT_MAIN
    state.step = Step.PRECOMBAT_MAIN
    _action_loop(state, active, decisions, record_decisions, "precombat_main", counter)
    if state.game_over:
        return
    # Combat with blocking layer (Phase 5b.3 blocking)
    state.phase = Phase.COMBAT
    state.step = Step.DECLARE_ATTACKERS
    attackers = _gather_attackers(state, active)
    if attackers:
        opp_seats = state.opponent_seats(active)
        if opp_seats:
            target_seat = opp_seats[0]
            for atk in attackers:
                atk.tapped = True
            state.step = Step.DECLARE_BLOCKERS
            blocks = choose_blocks(state, target_seat, attackers)
            blocked_ids = {b[0].instance_id for b in blocks}
            state.step = Step.COMBAT_DAMAGE
            unblocked_damage = sum((a.power or 0) for a in attackers if a.instance_id not in blocked_ids)
            state.players[target_seat].life -= unblocked_damage
            deaths_atk = []
            deaths_blk = []
            for atk, blk in blocks:
                atk_pow = atk.power or 0
                atk_tou = atk.toughness or 0
                blk_pow = blk.power or 0
                blk_tou = blk.toughness or 0
                if blk_pow >= atk_tou:
                    deaths_atk.append(atk)
                if atk_pow >= blk_tou:
                    deaths_blk.append(blk)
            for c in deaths_atk:
                if c in state.players[active].battlefield:
                    state.players[active].battlefield.remove(c)
                    state.players[active].graveyard.append(c)
            for c in deaths_blk:
                if c in state.players[target_seat].battlefield:
                    state.players[target_seat].battlefield.remove(c)
                    state.players[target_seat].graveyard.append(c)
            if record_decisions:
                decisions.append({
                    "turn": state.turn_number, "step": "combat_damage",
                    "attacker_seat": active, "defender_seat": target_seat,
                    "unblocked_damage": unblocked_damage,
                    "attackers": [a.name for a in attackers],
                    "blocks": [(a.name, b.name) for a, b in blocks],
                    "deaths_attacker": [c.name for c in deaths_atk],
                    "deaths_blocker": [c.name for c in deaths_blk],
                })
            _check_state_based(state)
            if state.game_over:
                return
    state.phase = Phase.POSTCOMBAT_MAIN
    state.step = Step.POSTCOMBAT_MAIN
    _action_loop(state, active, decisions, record_decisions, "postcombat_main", counter)
    if state.game_over:
        return
    state.phase = Phase.ENDING
    state.step = Step.END
    for c in state.players[active].battlefield:
        c.summoning_sick = False


def _action_loop(state, seat, decisions, record_decisions, phase_label, counter):
    for _ in range(MAX_ACTIONS_PER_PRIORITY):
        legal = enumerate_legal_actions(state, seat)
        legal_for_policy = [a for a in legal if a.type != ActionType.TAP_FOR_MANA]
        action, conf, rationale = choose_action(state, seat, legal_for_policy)
        counter["actions"] += 1
        if record_decisions:
            decisions.append({
                "turn": state.turn_number, "phase": phase_label,
                "action": action.type.value, "source": action.source_instance_id,
                "rationale": rationale, "confidence": conf,
            })
        if action.type == ActionType.PASS_PRIORITY:
            return
        _apply_action(state, action)
        _check_state_based(state)
        if state.game_over:
            return


def _apply_action(state, action):
    p = state.players[action.seat_index]
    if action.type == ActionType.PLAY_LAND:
        for i, c in enumerate(p.hand):
            if c.instance_id == action.source_instance_id:
                land = p.hand.pop(i)
                land.tapped = False
                p.battlefield.append(land)
                state.lands_played_this_turn_by_seat[action.seat_index] = (
                    state.lands_played_this_turn_by_seat.get(action.seat_index, 0) + 1
                )
                return
    elif action.type == ActionType.CAST_FROM_HAND:
        for i, c in enumerate(p.hand):
            if c.instance_id == action.source_instance_id:
                _auto_tap_for_cost(state, action.seat_index, c.cmc or 0, mana_cost_string=c.mana_cost)
                spell = p.hand.pop(i)
                if spell.is_creature() or spell.is_artifact() or "Enchantment" in spell.type_line:
                    spell.summoning_sick = spell.is_creature()
                    p.battlefield.append(spell)
                else:
                    p.graveyard.append(spell)
                return
    elif action.type == ActionType.CAST_COMMANDER:
        for i, c in enumerate(p.command_zone):
            if c.instance_id == action.source_instance_id:
                total_cost = (c.cmc or 0) + p.commander_tax_paid
                _auto_tap_for_cost(state, action.seat_index, total_cost, mana_cost_string=c.mana_cost)
                cmdr = p.command_zone.pop(i)
                cmdr.summoning_sick = True
                p.battlefield.append(cmdr)
                p.commander_tax_paid += 2
                return


def _auto_tap_for_cost(state, seat, total_cost, mana_cost_string=None):
    """Tap lands to cover a cost. Uses mana_cost parser when string given."""
    if mana_cost_string:
        try:
            from .mpa_mana_cost import parse_mana_cost, select_lands_to_tap
            from .mpa_actions import _land_color
            parsed = parse_mana_cost(mana_cost_string)
            lands_by_color = {"W": [], "U": [], "B": [], "R": [], "G": [], "C": []}
            for c in state.players[seat].battlefield:
                if c.is_land() and not c.tapped:
                    color = _land_color(c) or "C"
                    lands_by_color.setdefault(color, []).append(c)
            picks = select_lands_to_tap(parsed, lands_by_color)
            if picks:
                for land in picks:
                    land.tapped = True
                return
        except Exception:
            pass
    tapped = 0
    for c in state.players[seat].battlefield:
        if tapped >= total_cost:
            return
        if c.is_land() and not c.tapped:
            c.tapped = True
            tapped += 1


def _untap_all(state, seat):
    for c in state.players[seat].battlefield:
        c.tapped = False
        c.summoning_sick = False


def _gather_attackers(state, seat):
    from .mpa_policy import _should_attack
    if not _should_attack(state, seat):
        return []
    return [c for c in state.players[seat].battlefield if c.is_creature() and not c.tapped and not c.summoning_sick]


def _check_state_based(state):
    for p in state.players:
        if p.life <= 0:
            state.game_over = True
            state.loss_reason = "life_to_zero"
            for other in state.players:
                if other.seat_index != p.seat_index and not (other.life <= 0 or other.poison >= 10):
                    state.winner_seat = other.seat_index
            return
        if p.poison >= 10:
            state.game_over = True
            state.loss_reason = "poison"
            for other in state.players:
                if other.seat_index != p.seat_index and not (other.life <= 0 or other.poison >= 10):
                    state.winner_seat = other.seat_index
            return
