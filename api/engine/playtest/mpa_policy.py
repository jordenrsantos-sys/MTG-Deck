"""mpa_policy — Phase 5b.1c heuristic baseline + 5b.3 block selection."""
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
from .mpa_game_state import GameState, Phase, Step
from .mpa_actions import Action, ActionType

POLICY_VERSION = "mpa_policy_v0.2_with_blocking"


def choose_action(state, seat_index, legal_actions):
    if not legal_actions:
        return Action(type=ActionType.PASS_PRIORITY, seat_index=seat_index), 1.0, "no_legal_actions"
    if state.game_over:
        return Action(type=ActionType.PASS_PRIORITY, seat_index=seat_index), 1.0, "game_over"
    me = state.players[seat_index]
    land_actions = [a for a in legal_actions if a.type == ActionType.PLAY_LAND]
    if land_actions:
        return land_actions[0], 0.95, "always_play_land_t1"
    commander_actions = [a for a in legal_actions if a.type == ActionType.CAST_COMMANDER]
    if commander_actions:
        return commander_actions[0], 0.85, "cast_commander_on_curve"
    cast_actions = [a for a in legal_actions if a.type == ActionType.CAST_FROM_HAND]
    if cast_actions:
        def cmc_for(action):
            for c in me.hand:
                if c.instance_id == action.source_instance_id:
                    return c.cmc or 0
            return 0
        chosen = max(cast_actions, key=cmc_for)
        return chosen, 0.7, "cast_highest_cmc_spell"
    attack_actions = [a for a in legal_actions if a.type == ActionType.DECLARE_ATTACKER]
    if attack_actions and _should_attack(state, seat_index):
        return attack_actions[0], 0.65, "attack_when_opp_low_life_or_open"
    block_actions = [a for a in legal_actions if a.type == ActionType.DECLARE_BLOCKER]
    if block_actions and _should_block(state, seat_index):
        return block_actions[0], 0.65, "block_to_prevent_damage"
    return Action(type=ActionType.PASS_PRIORITY, seat_index=seat_index), 0.9, "no_productive_action"


def should_mulligan(state, seat_index, mulligans_taken):
    me = state.players[seat_index]
    if mulligans_taken >= 3:
        return False, "max_mulligans_reached"
    land_count = sum(1 for c in me.hand if c.is_land())
    castable = sum(1 for c in me.hand if not c.is_land() and (c.cmc or 0) <= 4)
    if land_count < 2:
        return True, "only_" + str(land_count) + "_lands_in_opening_hand"
    if land_count > 5:
        return True, "flooded_" + str(land_count) + "_lands"
    if castable == 0:
        return True, "no_castable_threats_in_opening_hand"
    return False, "keep_hand"


def _should_attack(state, seat_index):
    opps = state.opponent_seats(seat_index)
    if not opps:
        return False
    my_life = state.players[seat_index].life
    for o in opps:
        if state.players[o].life <= my_life:
            return True
    for o in opps:
        if not any(c.is_creature() for c in state.players[o].battlefield):
            return True
    return False


def _should_block(state, seat_index):
    incoming = 0
    for obj in state.stack:
        if obj.get("type") == "attack" and obj.get("defender_seat") == seat_index:
            incoming += int(obj.get("damage", 0))
    my_life = state.players[seat_index].life
    if incoming >= my_life:
        return True
    if incoming >= 10:
        return True
    return False


def choose_blocks(state, defender_seat, attackers):
    """Phase 5b.3 block selection. Returns list of (attacker, blocker) tuples.

    Heuristic: profitable blocks first (blocker survives + kills attacker);
    chump-block lethal swings; otherwise let damage through. Always returns
    a list (possibly empty); never None.
    """
    if not attackers:
        return []
    defender = state.players[defender_seat]
    available = [c for c in defender.battlefield if c.is_creature() and not c.tapped]
    if not available:
        return []
    total_incoming = sum((a.power or 0) for a in attackers)
    is_lethal_swing = total_incoming >= defender.life
    threats = sorted(attackers, key=lambda a: -(a.power or 0))
    blockers_pool = sorted(available, key=lambda b: (b.toughness or 0))
    assignments = []
    used = set()
    for atk in threats:
        atk_pow = atk.power or 0
        atk_tou = atk.toughness or 0
        best = None
        for b in blockers_pool:
            if b.instance_id in used:
                continue
            b_pow = b.power or 0
            b_tou = b.toughness or 0
            if b_tou > atk_pow and b_pow >= atk_tou:
                best = b
                break
        if best is None and is_lethal_swing:
            for b in blockers_pool:
                if b.instance_id in used:
                    continue
                best = b
                break
        if best is not None:
            assignments.append((atk, best))
            used.add(best.instance_id)
    return assignments
