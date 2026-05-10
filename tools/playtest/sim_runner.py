"""Single-game simulation runner for Phase 5b offline playtest framework (stage 5b.5).

MVP: takes 4 decks + 4 personalities + seed -> produces per-game record
(winner, turns_to_resolution, key_decision_log, win_condition_used,
replay_seed). Per safety #6: each sim game must complete in <2 seconds wall-
clock; halt at >5 seconds.

Per Decision 10 of OFFLINE_PLAYTEST_DESIGN.md: per-game record format with
bounded key_decision_log (top 20 decisions) for storage discipline at 100-
game scale.

Calibration-only module.
"""
from __future__ import annotations

import hashlib
import random
import time
from dataclasses import dataclass, field, replace
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .game_state import (
    GameState,
    PHASE_END,
    PHASE_MAIN_COMBAT,
    PHASE_UNTAP,
    PHASE_UPKEEP,
    PLAYER_COUNT,
    STARTING_HAND_SIZE,
    PlayerState,
    alive_player_count,
    make_initial_game_state,
    make_initial_player_state,
    state_hash,
    update_player_state,
    winner,
)
from .pilot import PilotPersonality, evaluate_pilot_decision, make_decision_callback
from . import turn_loop

SIM_RUNNER_V1_VERSION = "sim_runner_v1"

DEFAULT_MAX_TURNS = 80
DEFAULT_PER_GAME_TIMEOUT_SEC = 5.0
KEY_DECISION_LOG_MAX = 20

# Win-condition dispatch table keys. Match win_condition_extractor.WIN_LINE_*.
WIN_CONDITION_COMBAT = "combat"
WIN_CONDITION_COMBO = "combo"
WIN_CONDITION_MILL = "mill"
WIN_CONDITION_ALT_WIN = "alt_win"
WIN_CONDITION_VALUE_GRIND = "value_grind"

# Combo-primary handler tuning.
COMBO_FINISH_BASE_PROB = 0.05
COMBO_FINISH_PER_CHEAP_TUTOR = 0.03
COMBO_FINISH_PER_FAST_MANA = 0.02
COMBO_FINISH_DAMAGE = 60  # Lethal-ish from 40 life

# Value-grind handler tuning.
VALUE_GRIND_LATE_GAME_TURN = 8
VALUE_GRIND_LATE_BONUS = 5


@dataclass(frozen=True)
class DeckProfile:
    deck_id: str
    commander_oracle_ids: Tuple[str, ...]
    deck_oracle_ids: Tuple[str, ...]


@dataclass(frozen=True)
class GameSetup:
    decks: Tuple[DeckProfile, ...]
    personalities: Tuple[PilotPersonality, ...]
    seed: int


def _shuffled_library(deck: DeckProfile, seed: int) -> Tuple[str, ...]:
    """Deterministic library shuffle for a deck given a seed."""
    library = list(deck.deck_oracle_ids)
    rng = random.Random(seed)
    rng.shuffle(library)
    return tuple(library)


def _initial_state_for_setup(setup: GameSetup) -> GameState:
    """Build the initial GameState from a GameSetup with mulligan-free 7-card hands."""
    if len(setup.decks) != PLAYER_COUNT:
        raise ValueError(f"GameSetup needs exactly {PLAYER_COUNT} decks")
    if len(setup.personalities) != PLAYER_COUNT:
        raise ValueError(f"GameSetup needs exactly {PLAYER_COUNT} personalities")

    players = []
    for i, deck in enumerate(setup.decks):
        # Per-player seed derived from base seed + player index for determinism.
        player_seed = (setup.seed << 8) ^ i
        shuffled = _shuffled_library(deck, player_seed)
        # Draw the opening 7.
        hand = shuffled[:STARTING_HAND_SIZE]
        library = shuffled[STARTING_HAND_SIZE:]
        p = make_initial_player_state(
            player_id=i,
            deck_id=deck.deck_id,
            commander_oracle_ids=deck.commander_oracle_ids,
            library_oracle_ids=library,
        )
        p = replace(p, hand_oracle_ids=hand)
        players.append(p)
    return make_initial_game_state(players=tuple(players), seed=setup.seed)


def _select_target(
    *,
    state: GameState,
    active: int,
    personality,
    rng: random.Random,
) -> Optional[PlayerState]:
    """Pick a target opponent per personality's removal_targeting axis."""
    alive_opponents = [
        op for op in state.players
        if op.is_alive and op.player_id != active
    ]
    if not alive_opponents:
        return None
    if personality.removal_targeting >= 0.7:
        return max(alive_opponents, key=lambda op: (op.life, -op.player_id))
    if personality.removal_targeting <= 0.3:
        return rng.choice(alive_opponents)
    return alive_opponents[(state.turn + active) % len(alive_opponents)]


def _apply_damage_to_target(
    state: GameState,
    target_id: int,
    damage: int,
) -> GameState:
    p = state.players[target_id]
    return update_player_state(state, target_id, life=max(p.life - damage, 0))


def _combat_primary_handler(
    state: GameState,
    *,
    active: int,
    personality,
    envelope: Optional[Dict[str, Any]],
    seed: int,
) -> Tuple[GameState, List[Dict[str, Any]]]:
    """Existing aggression × battlefield × 2 model (combat-primary baseline)."""
    decisions: List[Dict[str, Any]] = []
    p = state.players[active]
    if len(p.battlefield_oracle_ids) == 0:
        return state, decisions
    rng = random.Random((seed << 16) ^ (state.turn << 8) ^ active)
    base_damage = int(personality.aggression * len(p.battlefield_oracle_ids) * 2)
    if base_damage <= 0:
        return state, decisions
    damage = min(base_damage, 12)
    target = _select_target(state=state, active=active, personality=personality, rng=rng)
    if target is None:
        return state, decisions
    new_state = _apply_damage_to_target(state, target.player_id, damage)
    decisions.append({
        "turn": state.turn, "actor": active, "decision": "deal_damage",
        "target": target.player_id, "damage": damage,
        "rationale_code": "COMBAT_PRIMARY",
    })
    return new_state, decisions


def _combo_primary_handler(
    state: GameState,
    *,
    active: int,
    personality,
    envelope: Optional[Dict[str, Any]],
    seed: int,
) -> Tuple[GameState, List[Dict[str, Any]]]:
    """Combo-primary: per-turn combo-finish probability based on enablers.

    On success: deal COMBO_FINISH_DAMAGE to ALL alive opponents (typically
    lethal from 40 life). On failure: small combat damage as fallback.
    """
    decisions: List[Dict[str, Any]] = []
    rng = random.Random((seed << 16) ^ (state.turn << 8) ^ active)

    signals = (envelope or {}).get("signals_used", {}) if envelope else {}
    cheap_tutor = int(signals.get("cheap_tutor", 0))
    fast_mana = int(signals.get("fast_mana", 0))

    finish_prob = (
        COMBO_FINISH_BASE_PROB
        + COMBO_FINISH_PER_CHEAP_TUTOR * cheap_tutor
        + COMBO_FINISH_PER_FAST_MANA * fast_mana
    )
    finish_prob = min(finish_prob, 0.95)

    if rng.random() < finish_prob:
        # Combo goes off — deal lethal to every alive opponent.
        new_state = state
        for op in state.players:
            if op.is_alive and op.player_id != active:
                new_state = _apply_damage_to_target(
                    new_state, op.player_id, COMBO_FINISH_DAMAGE,
                )
        decisions.append({
            "turn": state.turn, "actor": active, "decision": "combo_finish",
            "damage": COMBO_FINISH_DAMAGE, "finish_prob": finish_prob,
            "rationale_code": "COMBO_FINISH",
        })
        return new_state, decisions

    # Combo not assembled this turn — fall back to small combat pressure.
    p = state.players[active]
    if len(p.battlefield_oracle_ids) == 0:
        return state, decisions
    base_damage = int(personality.aggression * len(p.battlefield_oracle_ids))
    if base_damage <= 0:
        return state, decisions
    damage = min(base_damage, 6)
    target = _select_target(state=state, active=active, personality=personality, rng=rng)
    if target is None:
        return state, decisions
    new_state = _apply_damage_to_target(state, target.player_id, damage)
    decisions.append({
        "turn": state.turn, "actor": active, "decision": "deal_damage",
        "target": target.player_id, "damage": damage,
        "rationale_code": "COMBO_FALLBACK_COMBAT",
    })
    return new_state, decisions


def _value_grind_handler(
    state: GameState,
    *,
    active: int,
    personality,
    envelope: Optional[Dict[str, Any]],
    seed: int,
) -> Tuple[GameState, List[Dict[str, Any]]]:
    """Value-grind: combat + value_staple bonus + late-game inevitability."""
    decisions: List[Dict[str, Any]] = []
    p = state.players[active]
    if len(p.battlefield_oracle_ids) == 0:
        return state, decisions
    rng = random.Random((seed << 16) ^ (state.turn << 8) ^ active)
    signals = (envelope or {}).get("signals_used", {}) if envelope else {}
    value_staple = int(signals.get("value_staple", 0))

    base_damage = int(personality.aggression * len(p.battlefield_oracle_ids) * 2)
    bonus = value_staple
    if state.turn >= VALUE_GRIND_LATE_GAME_TURN:
        bonus += VALUE_GRIND_LATE_BONUS
    if base_damage + bonus <= 0:
        return state, decisions
    damage = min(base_damage + bonus, 14)
    target = _select_target(state=state, active=active, personality=personality, rng=rng)
    if target is None:
        return state, decisions
    new_state = _apply_damage_to_target(state, target.player_id, damage)
    decisions.append({
        "turn": state.turn, "actor": active, "decision": "deal_damage",
        "target": target.player_id, "damage": damage,
        "rationale_code": "VALUE_GRIND",
    })
    return new_state, decisions


# Dispatch table keyed on win_condition_primary. Mill + alt_win defer to
# combat (extractor never produces them on current corpus).
_WIN_CONDITION_DISPATCH = {
    WIN_CONDITION_COMBAT: _combat_primary_handler,
    WIN_CONDITION_COMBO: _combo_primary_handler,
    WIN_CONDITION_VALUE_GRIND: _value_grind_handler,
    WIN_CONDITION_MILL: _combat_primary_handler,
    WIN_CONDITION_ALT_WIN: _combat_primary_handler,
}


def _apply_combat_damage(
    state: GameState,
    *,
    setup: GameSetup,
    seed: int,
    envelopes_by_player: Optional[Sequence[Optional[Dict[str, Any]]]] = None,
) -> Tuple[GameState, List[Dict[str, Any]]]:
    """Apply combat damage via win-condition dispatch table.

    Caller invokes this after the main_combat phase advance; phase is now
    PHASE_END. Per stage 5b.11: dispatch on the active player's
    win_condition_primary type. Defaults to combat-primary if no envelope
    provided (preserves pre-5b.11 behavior).
    """
    active = state.active_player
    p = state.players[active]
    if not p.is_alive:
        return state, []
    personality = setup.personalities[active]

    envelope = None
    if envelopes_by_player is not None and active < len(envelopes_by_player):
        envelope = envelopes_by_player[active]
    primary_type = (envelope or {}).get("primary_type") if envelope else None
    if primary_type is None:
        primary_type = WIN_CONDITION_COMBAT

    handler = _WIN_CONDITION_DISPATCH.get(primary_type, _combat_primary_handler)
    return handler(
        state, active=active, personality=personality,
        envelope=envelope, seed=seed,
    )


def _step_phase_with_combat(
    state: GameState,
    *,
    setup: GameSetup,
    decision_callback,
    seed: int,
) -> Tuple[GameState, List[Dict[str, Any]]]:
    """One phase step + combat damage application during main_combat."""
    decisions: List[Dict[str, Any]] = []
    state = turn_loop.advance_phase(state, decision_callback=decision_callback)
    # If we just transitioned INTO main_combat, apply combat damage.
    if state.phase == PHASE_END:
        # turn_loop.advance_phase already moved past main_combat into end; we
        # need to do the combat damage step BEFORE transitioning out. So we
        # apply combat in the intermediate state — we do this by reconstructing
        # the main_combat phase from the prior state. Simpler approach: apply
        # combat damage during main_combat, before turn_loop advances out.
        pass
    return state, decisions


def run_single_game(
    setup: GameSetup,
    *,
    max_turns: int = DEFAULT_MAX_TURNS,
    per_game_timeout_sec: float = DEFAULT_PER_GAME_TIMEOUT_SEC,
    envelopes_by_player: Optional[Sequence[Optional[Dict[str, Any]]]] = None,
) -> Dict[str, Any]:
    """Run one 4-player game and return a per-game record.

    Per safety #6: halt with `runtime_exceeded=True` if wall-clock exceeds
    per_game_timeout_sec. Per Decision 10: bounded decision log.
    """
    start_time = time.monotonic()
    state = _initial_state_for_setup(setup)
    initial_state_hash = state_hash(state)

    personalities_by_player = {i: setup.personalities[i] for i in range(PLAYER_COUNT)}
    pilot_callback = make_decision_callback(personalities_by_player, seed=setup.seed)

    decision_log: List[Dict[str, Any]] = []
    runtime_exceeded = False
    max_turns_hit = False

    while alive_player_count(state) > 1:
        if state.turn >= max_turns:
            max_turns_hit = True
            break
        elapsed = time.monotonic() - start_time
        if elapsed > per_game_timeout_sec:
            runtime_exceeded = True
            break

        # advance_phase with pilot callback at upkeep + main_combat
        state = turn_loop.advance_phase(state, decision_callback=pilot_callback)
        # Apply combat damage AT main_combat phase end (just before transitioning to end).
        if state.phase == PHASE_END:
            state, combat_decisions = _apply_combat_damage(
                state, setup=setup, seed=setup.seed,
                envelopes_by_player=envelopes_by_player,
            )
            for d in combat_decisions:
                if len(decision_log) < KEY_DECISION_LOG_MAX:
                    decision_log.append(d)

    elapsed = time.monotonic() - start_time
    win_player = winner(state)
    if win_player is None and not runtime_exceeded and not max_turns_hit:
        # Shouldn't happen; defensive.
        max_turns_hit = True

    final_state_hash = state_hash(state)
    deck_state_payload = {
        "deck_ids": [d.deck_id for d in setup.decks],
        "commander_oracle_ids": [list(d.commander_oracle_ids) for d in setup.decks],
        "deck_oracle_ids_lengths": [len(d.deck_oracle_ids) for d in setup.decks],
    }
    deck_state_hash = hashlib.sha256(
        repr(sorted(deck_state_payload.items())).encode("utf-8")
    ).hexdigest()

    record = {
        "version": SIM_RUNNER_V1_VERSION,
        "game_id": hashlib.sha256(
            f"{setup.seed}-{deck_state_hash}-{initial_state_hash}".encode("utf-8")
        ).hexdigest(),
        "deck_ids": [d.deck_id for d in setup.decks],
        "personality_names": [p.name for p in setup.personalities],
        "winner_player_id": win_player,
        "winner_deck_id": setup.decks[win_player].deck_id if win_player is not None else None,
        "turns_to_resolution": state.turn,
        "max_turns_hit": max_turns_hit,
        "runtime_exceeded": runtime_exceeded,
        "wall_clock_sec": elapsed,
        "key_decision_log": decision_log[:KEY_DECISION_LOG_MAX],
        "win_condition_used": None,  # populated in stage 5b.7
        "replay_seed": setup.seed,
        "initial_state_hash": initial_state_hash,
        "final_state_hash": final_state_hash,
        "deck_state_hash": deck_state_hash,
    }
    return record


def make_mock_deck(deck_id: str, deck_size: int = 99, *, prefix: str = "card") -> DeckProfile:
    """Helper for tests: produce a synthetic DeckProfile with simple oracle_ids."""
    return DeckProfile(
        deck_id=deck_id,
        commander_oracle_ids=(f"{prefix}-{deck_id}-cmdr",),
        deck_oracle_ids=tuple(f"{prefix}-{deck_id}-{i}" for i in range(deck_size)),
    )
