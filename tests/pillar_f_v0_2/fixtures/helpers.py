"""Shared fixture helpers — kept light so each fixture is a focused
assertion on a substrate behavior."""
from __future__ import annotations

from typing import List, Optional

from api.engine.pillar_f.v0_2.state import (
    Card, GameState, PlayerState, PlayerZones,
)


def empty_4p_game(*, life: int = 40) -> GameState:
    """4-player game with no cards. Active player = 0."""
    gs = GameState()
    for pid in range(4):
        gs.players.append(PlayerState(player_id=pid, name=f"P{pid}",
                                      life_total=life, zones=PlayerZones()))
    gs.active_player = 0
    return gs


def add_creature(
    gs: GameState, name: str, *, owner: int = 0, controller: Optional[int] = None,
    power: int = 2, toughness: int = 2, keywords: Optional[List[str]] = None,
    summoning_sick: bool = False, tapped: bool = False,
    is_commander: bool = False, oracle_id: Optional[str] = None,
    counters: Optional[dict] = None,
) -> Card:
    if controller is None:
        controller = owner
    c = Card(
        name=name, owner=owner, controller=controller,
        type_line=("Legendary Creature — Hero" if is_commander
                   else "Creature — Hero"),
        power=str(power), toughness=str(toughness),
        keywords=keywords or [],
        oracle_id=oracle_id or f"oracle-{name.lower().replace(' ', '-')}",
        summoning_sick=summoning_sick, tapped=tapped,
        counters=dict(counters or {}),
    )
    gs.add_card(c)
    gs.players[owner].zones.battlefield.append(c.card_id)
    if is_commander:
        gs.commander_card_ids[owner] = c.card_id
        gs.players[owner].zones.command.append(c.card_id)  # also tracked
    return c


def add_artifact(gs: GameState, name: str, *, owner: int = 0) -> Card:
    c = Card(name=name, owner=owner, controller=owner, type_line="Artifact")
    gs.add_card(c)
    gs.players[owner].zones.battlefield.append(c.card_id)
    return c


def add_enchantment(gs: GameState, name: str, *, owner: int = 0,
                    keywords: Optional[List[str]] = None) -> Card:
    c = Card(name=name, owner=owner, controller=owner,
             type_line="Enchantment", keywords=keywords or [])
    gs.add_card(c)
    gs.players[owner].zones.battlefield.append(c.card_id)
    return c


def add_planeswalker(gs: GameState, name: str, *, owner: int = 0,
                     loyalty: int = 3) -> Card:
    c = Card(name=name, owner=owner, controller=owner,
             type_line="Legendary Planeswalker — Hero",
             loyalty=str(loyalty))
    c.counters["loyalty"] = loyalty
    gs.add_card(c)
    gs.players[owner].zones.battlefield.append(c.card_id)
    return c


def add_library_cards(gs: GameState, player_id: int, n: int) -> None:
    """Add N filler cards to player_id's library."""
    for i in range(n):
        c = Card(name=f"Lib_{player_id}_{i}", owner=player_id)
        gs.add_card(c)
        gs.players[player_id].zones.library.append(c.card_id)


def add_graveyard_card(gs: GameState, player_id: int, *,
                      name: str, type_line: str = "Instant") -> Card:
    c = Card(name=name, owner=player_id, type_line=type_line)
    gs.add_card(c)
    gs.players[player_id].zones.graveyard.append(c.card_id)
    return c
