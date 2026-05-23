"""Card object model.

Each card carries (per scoping doc section a):
  - identity: name, oracle_id, mana_cost, cmc, type_line, subtypes,
    oracle_text, power, toughness, loyalty, colors, color_identity,
    keywords, owner (baked from deck list)
  - mutable in-game state: face_down, tapped, summoning_sick,
    damage_marked, counters, attached_to, attached_by, controller,
    card_id (unique per game instance)

The `card_id` is a runtime UUID; the same Sol Ring in 4 different
players' decks has 4 distinct card_ids. `oracle_id` is the Scryfall
oracle UUID; cards with the same oracle_id ARE the same card by
identity (e.g., 4 different printings of Lightning Bolt share an
oracle_id).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
import uuid


def _new_card_id() -> str:
    """Generate a unique runtime card_id. UUID4 hex prefix for brevity."""
    return uuid.uuid4().hex[:12]


@dataclass
class Card:
    """Mutable in-game card instance. The identity fields (name, oracle_id,
    mana_cost, etc.) come from the deck list at game-start; the mutable
    fields (tapped, damage_marked, counters, etc.) track the card's
    current battlefield/zone state.
    """
    # Identity (from deck list / Scryfall — immutable per instance).
    name: str = ""
    oracle_id: str = ""
    mana_cost: str = ""               # e.g. "{1}{B}{B}"
    cmc: float = 0.0
    type_line: str = ""               # e.g. "Legendary Creature — Vampire Knight"
    subtypes: List[str] = field(default_factory=list)
    oracle_text: str = ""
    power: Optional[str] = None       # str so "*" works (Tarmogoyf)
    toughness: Optional[str] = None
    loyalty: Optional[str] = None     # planeswalker base loyalty
    colors: List[str] = field(default_factory=list)             # WUBRG
    color_identity: List[str] = field(default_factory=list)
    keywords: List[str] = field(default_factory=list)           # ["flying", "trample", ...]
    owner: int = 0                    # player_id
    card_id: str = field(default_factory=_new_card_id)

    # In-game mutable state.
    controller: int = 0               # defaults to owner; changes via control-changing effects
    face_down: bool = False
    tapped: bool = False
    summoning_sick: bool = False      # cleared at controller's untap
    damage_marked: int = 0            # cleared at cleanup step
    counters: Dict[str, int] = field(default_factory=dict)      # e.g. {"+1/+1": 3, "loyalty": 4}
    attached_to: Optional[str] = None # card_id of permanent this is attached to (Aura/Equipment)
    attached_by: List[str] = field(default_factory=list)        # card_ids attached TO this permanent

    def __post_init__(self) -> None:
        # Controller defaults to owner at game start.
        if self.controller == 0 and self.owner != 0:
            self.controller = self.owner

    def is_creature(self) -> bool:
        tl = (self.type_line or "").lower()
        return "creature" in tl

    def is_land(self) -> bool:
        return "land" in (self.type_line or "").lower()

    def is_planeswalker(self) -> bool:
        return "planeswalker" in (self.type_line or "").lower()

    def is_legendary(self) -> bool:
        return "legendary" in (self.type_line or "").lower()

    def has_keyword(self, kw: str) -> bool:
        return any(k.lower() == kw.lower() for k in self.keywords)

    def power_int(self) -> int:
        """Best-effort int conversion of `power`. '*' returns 0 for SBA
        purposes (creatures with * P/T may have specific CDA rules
        handled in layer 7b). 0/None returns 0."""
        try:
            return int(self.power) if self.power is not None else 0
        except (TypeError, ValueError):
            return 0

    def toughness_int(self) -> int:
        try:
            return int(self.toughness) if self.toughness is not None else 0
        except (TypeError, ValueError):
            return 0

    def to_dict(self) -> Dict[str, Any]:
        """Serializable dict for JSON round-trip. Lists/dicts are copied
        to detach from instance mutation."""
        return {
            "card_id": self.card_id,
            "name": self.name,
            "oracle_id": self.oracle_id,
            "mana_cost": self.mana_cost,
            "cmc": self.cmc,
            "type_line": self.type_line,
            "subtypes": list(self.subtypes),
            "oracle_text": self.oracle_text,
            "power": self.power,
            "toughness": self.toughness,
            "loyalty": self.loyalty,
            "colors": list(self.colors),
            "color_identity": list(self.color_identity),
            "keywords": list(self.keywords),
            "owner": self.owner,
            "controller": self.controller,
            "face_down": self.face_down,
            "tapped": self.tapped,
            "summoning_sick": self.summoning_sick,
            "damage_marked": self.damage_marked,
            "counters": dict(self.counters),
            "attached_to": self.attached_to,
            "attached_by": list(self.attached_by),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Card":
        return cls(
            card_id=d.get("card_id") or _new_card_id(),
            name=d.get("name", ""),
            oracle_id=d.get("oracle_id", ""),
            mana_cost=d.get("mana_cost", ""),
            cmc=float(d.get("cmc", 0.0) or 0.0),
            type_line=d.get("type_line", ""),
            subtypes=list(d.get("subtypes") or []),
            oracle_text=d.get("oracle_text", ""),
            power=d.get("power"),
            toughness=d.get("toughness"),
            loyalty=d.get("loyalty"),
            colors=list(d.get("colors") or []),
            color_identity=list(d.get("color_identity") or []),
            keywords=list(d.get("keywords") or []),
            owner=int(d.get("owner", 0)),
            controller=int(d.get("controller", d.get("owner", 0))),
            face_down=bool(d.get("face_down", False)),
            tapped=bool(d.get("tapped", False)),
            summoning_sick=bool(d.get("summoning_sick", False)),
            damage_marked=int(d.get("damage_marked", 0)),
            counters=dict(d.get("counters") or {}),
            attached_to=d.get("attached_to"),
            attached_by=list(d.get("attached_by") or []),
        )

    def to_opaque(self) -> Dict[str, Any]:
        """Hidden-information view: returns a minimal representation
        for face-down or in-opponent-hand cards. Just card_id + opaque
        marker so the game can track WHERE the card is without revealing
        identity."""
        return {"card_id": self.card_id, "opaque": True}
