"""Per-player state aggregating zones, life, mana pool, and per-turn
counters per scoping doc section (a)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

from api.engine.pillar_f.v0_2.state.zones import PlayerZones


@dataclass
class ManaPool:
    """Per-player mana pool with per-color buckets + colorless + any-color
    proxy. EMPTIES at end-of-step/phase per CR 106.4 — the engine's turn
    machine clears it at appropriate boundaries."""
    W: int = 0
    U: int = 0
    B: int = 0
    R: int = 0
    G: int = 0
    C: int = 0   # colorless
    # snow mana isn't modeled separately — treated as C+source-tag on
    # the producing land. Generic-cost activations search for any color
    # first, then snow-tagged C; iter-10 collapses snow into C.

    def empty(self) -> None:
        self.W = self.U = self.B = self.R = self.G = self.C = 0

    def total(self) -> int:
        return self.W + self.U + self.B + self.R + self.G + self.C

    def to_dict(self) -> Dict[str, int]:
        return {"W": self.W, "U": self.U, "B": self.B,
                "R": self.R, "G": self.G, "C": self.C}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ManaPool":
        return cls(W=int(d.get("W", 0)), U=int(d.get("U", 0)),
                   B=int(d.get("B", 0)), R=int(d.get("R", 0)),
                   G=int(d.get("G", 0)), C=int(d.get("C", 0)))


@dataclass
class PlayerState:
    """All state owned by a single player."""
    player_id: int = 0
    name: str = ""
    life_total: int = 40
    zones: PlayerZones = field(default_factory=PlayerZones)
    mana_pool: ManaPool = field(default_factory=ManaPool)
    commander_damage_taken_from: Dict[str, int] = field(default_factory=dict)
    # ^ key = commander oracle_id (or card_id if commander is a token); int = damage marked
    lands_played_this_turn: int = 0
    cards_drawn_this_turn: int = 0
    spells_cast_this_turn: int = 0
    priority_passed_this_round: bool = False
    has_lost: bool = False         # SBA marks; loop terminates when only 1 player remains
    has_drawn_from_empty_library: bool = False  # SBA at draw step
    # Politics tracking (used by sub-mega-task B's LLM policy in iter 11+;
    # iter 10 just plumbs the slot).
    politics_state: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "player_id": self.player_id,
            "name": self.name,
            "life_total": self.life_total,
            "zones": self.zones.to_dict(),
            "mana_pool": self.mana_pool.to_dict(),
            "commander_damage_taken_from": dict(self.commander_damage_taken_from),
            "lands_played_this_turn": self.lands_played_this_turn,
            "cards_drawn_this_turn": self.cards_drawn_this_turn,
            "spells_cast_this_turn": self.spells_cast_this_turn,
            "priority_passed_this_round": self.priority_passed_this_round,
            "has_lost": self.has_lost,
            "has_drawn_from_empty_library": self.has_drawn_from_empty_library,
            "politics_state": dict(self.politics_state),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "PlayerState":
        return cls(
            player_id=int(d.get("player_id", 0)),
            name=d.get("name", ""),
            life_total=int(d.get("life_total", 40)),
            zones=PlayerZones.from_dict(d.get("zones") or {}),
            mana_pool=ManaPool.from_dict(d.get("mana_pool") or {}),
            commander_damage_taken_from=dict(d.get("commander_damage_taken_from") or {}),
            lands_played_this_turn=int(d.get("lands_played_this_turn", 0)),
            cards_drawn_this_turn=int(d.get("cards_drawn_this_turn", 0)),
            spells_cast_this_turn=int(d.get("spells_cast_this_turn", 0)),
            priority_passed_this_round=bool(d.get("priority_passed_this_round", False)),
            has_lost=bool(d.get("has_lost", False)),
            has_drawn_from_empty_library=bool(d.get("has_drawn_from_empty_library", False)),
            politics_state=dict(d.get("politics_state") or {}),
        )
