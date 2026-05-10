"""Game-state data model for Phase 5b offline playtest framework (stage 5b.3).

Per Decision 1 of OFFLINE_PLAYTEST_DESIGN.md: abstracted decision-point state,
not full board state. Tap states / summoning sickness / attached auras /
counters tracked only when a decision rule consults them; full CR-fidelity
rules engine is out of scope.

Calibration-only module — never imported by runtime layers. Frozen dataclasses
for determinism; same input -> bitwise-identical output.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, replace
from typing import Any, Dict, FrozenSet, Iterable, List, Optional, Tuple

GAME_STATE_V1_VERSION = "game_state_v1"

PLAYER_COUNT = 4
COMMANDER_DAMAGE_LETHAL = 21
STARTING_LIFE = 40
STARTING_HAND_SIZE = 7

PHASE_UNTAP = "untap"
PHASE_UPKEEP = "upkeep"
PHASE_DRAW = "draw"
PHASE_MAIN_COMBAT = "main_combat"
PHASE_END = "end"
PHASES = (PHASE_UNTAP, PHASE_UPKEEP, PHASE_DRAW, PHASE_MAIN_COMBAT, PHASE_END)


@dataclass(frozen=True)
class PlayerState:
    player_id: int
    deck_id: str
    life: int
    library_oracle_ids: Tuple[str, ...]
    hand_oracle_ids: Tuple[str, ...]
    battlefield_oracle_ids: Tuple[str, ...]
    graveyard_oracle_ids: Tuple[str, ...]
    exile_oracle_ids: Tuple[str, ...]
    command_zone_oracle_ids: Tuple[str, ...]
    mana_available: int
    commander_tax: int
    commander_damage_dealt: Tuple[Tuple[int, int], ...]  # ((opp_id, damage), ...)
    is_alive: bool


@dataclass(frozen=True)
class GameState:
    version: str
    turn: int
    phase: str
    priority_holder: int
    active_player: int
    players: Tuple[PlayerState, ...]
    seed: int
    history_summary: Tuple[str, ...]


def make_initial_player_state(
    *,
    player_id: int,
    deck_id: str,
    commander_oracle_ids: Iterable[str],
    library_oracle_ids: Iterable[str],
) -> PlayerState:
    """Create a fresh player state at turn 0 (pre-mulligan).

    Library is the full 99-card list (commander goes to command zone, not
    library). Hand starts empty; mulligan resolution happens before turn 1.
    """
    library_tuple = tuple(library_oracle_ids)
    cmdr_tuple = tuple(commander_oracle_ids)
    return PlayerState(
        player_id=player_id,
        deck_id=deck_id,
        life=STARTING_LIFE,
        library_oracle_ids=library_tuple,
        hand_oracle_ids=(),
        battlefield_oracle_ids=(),
        graveyard_oracle_ids=(),
        exile_oracle_ids=(),
        command_zone_oracle_ids=cmdr_tuple,
        mana_available=0,
        commander_tax=0,
        commander_damage_dealt=(),
        is_alive=True,
    )


def make_initial_game_state(
    *,
    players: Iterable[PlayerState],
    seed: int,
) -> GameState:
    """Create a fresh GameState at turn 0 / phase=untap / priority=player 0."""
    players_tuple = tuple(players)
    if len(players_tuple) != PLAYER_COUNT:
        raise ValueError(
            f"GameState requires exactly {PLAYER_COUNT} players (got {len(players_tuple)})"
        )
    return GameState(
        version=GAME_STATE_V1_VERSION,
        turn=0,
        phase=PHASE_UNTAP,
        priority_holder=0,
        active_player=0,
        players=players_tuple,
        seed=seed,
        history_summary=(),
    )


def state_hash(state: GameState) -> str:
    """Stable SHA-256 hash of a game state for determinism checks."""
    payload = {
        "version": state.version,
        "turn": state.turn,
        "phase": state.phase,
        "priority_holder": state.priority_holder,
        "active_player": state.active_player,
        "seed": state.seed,
        "history_summary": list(state.history_summary),
        "players": [
            {
                "player_id": p.player_id,
                "deck_id": p.deck_id,
                "life": p.life,
                "library_count": len(p.library_oracle_ids),
                "hand_oracle_ids": sorted(p.hand_oracle_ids),
                "battlefield_oracle_ids": sorted(p.battlefield_oracle_ids),
                "graveyard_oracle_ids": sorted(p.graveyard_oracle_ids),
                "exile_oracle_ids": sorted(p.exile_oracle_ids),
                "command_zone_oracle_ids": sorted(p.command_zone_oracle_ids),
                "mana_available": p.mana_available,
                "commander_tax": p.commander_tax,
                "commander_damage_dealt": [list(t) for t in sorted(p.commander_damage_dealt)],
                "is_alive": p.is_alive,
            }
            for p in state.players
        ],
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def alive_player_count(state: GameState) -> int:
    return sum(1 for p in state.players if p.is_alive)


def winner(state: GameState) -> Optional[int]:
    """Return the player_id of the sole survivor, or None if game in progress."""
    alive = [p for p in state.players if p.is_alive]
    if len(alive) == 1:
        return alive[0].player_id
    return None


def kill_player(state: GameState, player_id: int) -> GameState:
    """Return a new GameState with player_id marked is_alive=False."""
    new_players = tuple(
        replace(p, is_alive=False) if p.player_id == player_id else p
        for p in state.players
    )
    return replace(state, players=new_players)


def update_player_state(
    state: GameState,
    player_id: int,
    **kwargs: Any,
) -> GameState:
    """Return a new GameState with one player's fields updated."""
    new_players = tuple(
        replace(p, **kwargs) if p.player_id == player_id else p
        for p in state.players
    )
    return replace(state, players=new_players)
