"""Per-permanent characteristics view.

A `Characteristics` snapshot is the AT-THIS-MOMENT view of a permanent's
type/color/keywords/P-T after all continuous effects have been applied
in CR 613 layer order. It's a derived view, not the canonical card —
the canonical Card has PRINTED values; Characteristics has CURRENT values.

The layer engine in layer_engine.py computes one snapshot per
battlefield permanent per `apply_continuous_effects` call. Iter-10
recomputes the whole table on demand (no caching yet). Sub-mega-task B's
LLM perspective view will use these snapshots to know what each
permanent actually does right now.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set


@dataclass
class Characteristics:
    """Current characteristics of a permanent. Derived from printed
    Card values + applied continuous effects."""
    card_id: str = ""
    name: str = ""
    type_line: str = ""
    subtypes: List[str] = field(default_factory=list)
    supertypes: List[str] = field(default_factory=list)   # ["Legendary"]
    types: List[str] = field(default_factory=list)         # ["Creature", "Artifact"]
    colors: List[str] = field(default_factory=list)
    color_identity: List[str] = field(default_factory=list)
    keywords: List[str] = field(default_factory=list)
    abilities: List[str] = field(default_factory=list)     # text snippets / ability ids
    power: int = 0
    toughness: int = 0
    loyalty: int = 0
    controller: int = 0
    is_copy_of_card_id: Optional[str] = None  # set by layer-1 copy effects

    def has_keyword(self, kw: str) -> bool:
        return any(k.lower() == kw.lower() for k in self.keywords)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "card_id": self.card_id,
            "name": self.name,
            "type_line": self.type_line,
            "subtypes": list(self.subtypes),
            "supertypes": list(self.supertypes),
            "types": list(self.types),
            "colors": list(self.colors),
            "color_identity": list(self.color_identity),
            "keywords": list(self.keywords),
            "abilities": list(self.abilities),
            "power": self.power,
            "toughness": self.toughness,
            "loyalty": self.loyalty,
            "controller": self.controller,
            "is_copy_of_card_id": self.is_copy_of_card_id,
        }
