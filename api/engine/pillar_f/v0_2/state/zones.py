"""Per-player zones.

Each player has 7 zones: hand, library, battlefield, graveyard, exile,
command, stack_membership (the last is a derived view of stack entries
this player controls).

Zones are ordered lists of card_ids (NOT Card objects — Card objects
live in `GameState.cards_by_id` and are referenced by id from every
zone). This avoids duplication + makes serialization simpler.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class PlayerZones:
    """Per-player zone ownership. All zone members are card_ids."""
    hand: List[str] = field(default_factory=list)
    library: List[str] = field(default_factory=list)      # index 0 = top
    battlefield: List[str] = field(default_factory=list)
    graveyard: List[str] = field(default_factory=list)    # index 0 = bottom
    exile: List[str] = field(default_factory=list)
    command: List[str] = field(default_factory=list)      # commander + face-up command-zone

    def all_card_ids(self) -> List[str]:
        return (self.hand + self.library + self.battlefield
                + self.graveyard + self.exile + self.command)

    def find_zone(self, card_id: str) -> Optional[str]:
        """Returns the zone-name string holding this card, or None."""
        for zone in ("hand", "library", "battlefield", "graveyard",
                     "exile", "command"):
            if card_id in getattr(self, zone):
                return zone
        return None

    def remove_card(self, card_id: str) -> Optional[str]:
        """Remove card_id from whichever zone holds it. Returns the
        zone-name string (or None if not found)."""
        for zone in ("hand", "library", "battlefield", "graveyard",
                     "exile", "command"):
            lst = getattr(self, zone)
            if card_id in lst:
                lst.remove(card_id)
                return zone
        return None

    def add_card(self, card_id: str, zone: str, *, to_top: bool = False) -> None:
        """Add card_id to the named zone. For library, to_top=True
        places at index 0; else at the bottom (end of list). For
        graveyard, cards go on top (end) by default."""
        if zone not in ("hand", "library", "battlefield", "graveyard",
                        "exile", "command"):
            raise ValueError(f"unknown zone {zone!r}")
        lst = getattr(self, zone)
        if zone == "library" and to_top:
            lst.insert(0, card_id)
        else:
            lst.append(card_id)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "hand": list(self.hand),
            "library": list(self.library),
            "battlefield": list(self.battlefield),
            "graveyard": list(self.graveyard),
            "exile": list(self.exile),
            "command": list(self.command),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "PlayerZones":
        return cls(
            hand=list(d.get("hand") or []),
            library=list(d.get("library") or []),
            battlefield=list(d.get("battlefield") or []),
            graveyard=list(d.get("graveyard") or []),
            exile=list(d.get("exile") or []),
            command=list(d.get("command") or []),
        )
