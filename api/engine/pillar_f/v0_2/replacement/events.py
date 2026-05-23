"""Event taxonomy for the replacement-effect engine.

Each event is a dataclass that replacement effects can pattern-match
on (type + optional predicate keys). The event also carries enough
context to apply the event when it does run.

Iter 10 supplies the 9 event types listed in the scoping doc Phase 4:
DrawEvent, DamageEvent, EnterBattlefieldEvent, DieEvent,
LifeChangeEvent, CounterAddEvent, CounterRemoveEvent, DiscardEvent,
MillEvent.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class Event:
    """Base event dataclass. Subclasses set `event_type` for pattern
    matching. Subclasses are dataclasses so replacement_fn can mutate
    fields to "replace" an event (e.g., would-deal-damage 3 → replaced
    to deal 0)."""
    event_type: str = ""
    replaced: bool = False              # set True when a replacement consumed it
    prevent: bool = False               # set True to fully prevent (skip apply)


@dataclass
class DrawEvent(Event):
    event_type: str = "DrawEvent"
    player_id: int = 0
    count: int = 1


@dataclass
class DamageEvent(Event):
    event_type: str = "DamageEvent"
    source_card_id: Optional[str] = None
    source_controller: int = 0
    # target_kind: "creature" | "player" | "planeswalker"
    target_kind: str = "player"
    target_id: Any = None              # card_id (creature/pw) or player_id
    amount: int = 0
    is_combat: bool = False
    is_first_strike: bool = False


@dataclass
class EnterBattlefieldEvent(Event):
    event_type: str = "EnterBattlefieldEvent"
    card_id: str = ""
    controller: int = 0
    tapped_on_etb: bool = False         # set True by replacement (ETB tapped)
    from_zone: str = ""                  # "hand" | "graveyard" | "library" | "exile"
    counters_on_etb: Dict[str, int] = field(default_factory=dict)


@dataclass
class DieEvent(Event):
    event_type: str = "DieEvent"
    card_id: str = ""
    controller: int = 0
    cause: str = "damage"               # "damage" | "destroy" | "sacrifice" | "sba"
    instead_zone: Optional[str] = None  # if non-None, replacement sent it elsewhere (e.g., "exile")


@dataclass
class LifeChangeEvent(Event):
    event_type: str = "LifeChangeEvent"
    player_id: int = 0
    delta: int = 0                      # negative = loss


@dataclass
class CounterAddEvent(Event):
    event_type: str = "CounterAddEvent"
    target_card_id: str = ""
    counter_type: str = ""              # e.g. "+1/+1", "loyalty"
    count: int = 1


@dataclass
class CounterRemoveEvent(Event):
    event_type: str = "CounterRemoveEvent"
    target_card_id: str = ""
    counter_type: str = ""
    count: int = 1


@dataclass
class DiscardEvent(Event):
    event_type: str = "DiscardEvent"
    player_id: int = 0
    card_id: str = ""


@dataclass
class MillEvent(Event):
    event_type: str = "MillEvent"
    player_id: int = 0
    count: int = 1


# Convenience map for pattern matching.
EVENT_TYPES = {
    "DrawEvent", "DamageEvent", "EnterBattlefieldEvent",
    "DieEvent", "LifeChangeEvent", "CounterAddEvent",
    "CounterRemoveEvent", "DiscardEvent", "MillEvent",
}
