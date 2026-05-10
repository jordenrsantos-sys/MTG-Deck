"""Turn loop scaffolding for Phase 5b offline playtest framework (stage 5b.3).

Per Decision 2 of OFFLINE_PLAYTEST_DESIGN.md: abstracted 5-phase turn structure
(untap / upkeep / draw / main+combat / end) with deterministic stack ordering.
The loop drives a GameState forward one phase at a time; pilot decision
callbacks are invoked at each phase to mutate state. Per-turn duration target
<= 50 ms.

Calibration-only module.
"""
from __future__ import annotations

from dataclasses import replace
from typing import Any, Callable, Dict, List, Optional

from .game_state import (
    GameState,
    PHASES,
    PHASE_DRAW,
    PHASE_END,
    PHASE_MAIN_COMBAT,
    PHASE_UNTAP,
    PHASE_UPKEEP,
    PLAYER_COUNT,
    PlayerState,
    alive_player_count,
    update_player_state,
    winner,
)

TURN_LOOP_V1_VERSION = "turn_loop_v1"

MAX_TURNS_DEFAULT = 30
PHASE_INDEX = {phase: idx for idx, phase in enumerate(PHASES)}


PilotDecisionFn = Callable[[GameState, int], GameState]
"""Pilot decision callback: takes (state, player_id) -> new state."""


def _draw_card(state: GameState, player_id: int, count: int = 1) -> GameState:
    """Move count cards from a player's library to their hand. No-op when empty."""
    p = state.players[player_id]
    if not p.is_alive:
        return state
    drawn = p.library_oracle_ids[:count]
    remaining = p.library_oracle_ids[count:]
    new_hand = p.hand_oracle_ids + drawn
    return update_player_state(
        state,
        player_id,
        library_oracle_ids=remaining,
        hand_oracle_ids=new_hand,
    )


def _check_loss_conditions(state: GameState) -> GameState:
    """Mark players is_alive=False if they meet any loss condition."""
    new_players: List[PlayerState] = []
    for p in state.players:
        if not p.is_alive:
            new_players.append(p)
            continue
        if p.life <= 0:
            new_players.append(replace(p, is_alive=False))
            continue
        # Commander damage check.
        any_lethal_cmdr = any(
            dmg >= 21 for _, dmg in p.commander_damage_dealt
        )
        if any_lethal_cmdr:
            new_players.append(replace(p, is_alive=False))
            continue
        new_players.append(p)
    return replace(state, players=tuple(new_players))


def _next_active_player(state: GameState) -> int:
    """Find the next alive player after the current active_player."""
    start = state.active_player
    for offset in range(1, PLAYER_COUNT + 1):
        candidate = (start + offset) % PLAYER_COUNT
        if state.players[candidate].is_alive:
            return candidate
    return start


def advance_phase(
    state: GameState,
    *,
    decision_callback: Optional[PilotDecisionFn] = None,
) -> GameState:
    """Advance the GameState by one phase.

    On phase transitions:
    - untap: refresh active player's mana_available based on battlefield count
    - upkeep: trigger upkeep effects (delegated to decision_callback)
    - draw: active player draws one card
    - main_combat: pilot decision for plays + attacks (delegated)
    - end: cleanup; advance turn counter + active_player when end resolves

    The decision_callback is called during upkeep + main_combat for the
    active player only; other players' triggers fold into the same callback
    via priority_holder rotation if needed (deferred to stage 5b.5+).
    """
    state = _check_loss_conditions(state)
    if winner(state) is not None:
        return state

    phase = state.phase
    active = state.active_player
    new_state = state

    if phase == PHASE_UNTAP:
        # Refresh mana available equal to the count of permanents on battlefield
        # (highly abstracted; real mana is color-aware + tap-aware in stage 5b.5+).
        p = new_state.players[active]
        new_state = update_player_state(
            new_state, active,
            mana_available=len(p.battlefield_oracle_ids),
        )
        new_state = replace(new_state, phase=PHASE_UPKEEP, priority_holder=active)
        return new_state

    if phase == PHASE_UPKEEP:
        if decision_callback is not None:
            new_state = decision_callback(new_state, active)
        new_state = replace(new_state, phase=PHASE_DRAW, priority_holder=active)
        return new_state

    if phase == PHASE_DRAW:
        # First turn of the game (turn 1, active player 0 in standard 4-player)
        # skips draw step per Commander rules; sim ignores this nuance.
        new_state = _draw_card(new_state, active)
        new_state = replace(new_state, phase=PHASE_MAIN_COMBAT, priority_holder=active)
        return new_state

    if phase == PHASE_MAIN_COMBAT:
        if decision_callback is not None:
            new_state = decision_callback(new_state, active)
        new_state = replace(new_state, phase=PHASE_END, priority_holder=active)
        return new_state

    if phase == PHASE_END:
        # Cleanup; advance turn + active_player.
        next_player = _next_active_player(new_state)
        new_turn = new_state.turn + (1 if next_player <= active else 1)
        # Increment turn counter when the table wraps OR every turn — for
        # simplicity: increment each time active_player rotates (so turn
        # counter advances 4× per round, once per player turn). Rename
        # comment: turn counter measures *player turns*, not round number.
        new_state = replace(
            new_state,
            phase=PHASE_UNTAP,
            active_player=next_player,
            priority_holder=next_player,
            turn=new_turn,
        )
        return new_state

    raise ValueError(f"Unknown phase: {phase}")


def advance_full_turn(
    state: GameState,
    *,
    decision_callback: Optional[PilotDecisionFn] = None,
) -> GameState:
    """Advance the GameState through one full player-turn (5 phases)."""
    for _ in PHASES:
        state = advance_phase(state, decision_callback=decision_callback)
        if winner(state) is not None:
            break
    return state


def play_until_winner_or_max_turns(
    state: GameState,
    *,
    decision_callback: Optional[PilotDecisionFn] = None,
    max_turns: int = MAX_TURNS_DEFAULT,
) -> GameState:
    """Drive the GameState forward until a winner emerges or max_turns hit."""
    while alive_player_count(state) > 1 and state.turn < max_turns:
        state = advance_full_turn(state, decision_callback=decision_callback)
    return state
