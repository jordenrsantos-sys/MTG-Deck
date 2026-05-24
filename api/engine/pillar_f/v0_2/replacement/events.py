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


# ============================================================
# v14 additions — substrate extension event types.
# ============================================================


@dataclass
class TokenCreateEvent(Event):
    """Fires when a card or ability creates a token. Caller registers
    the token spec; the substrate emit_token_create helper handles
    battlefield insertion + downstream EnterBattlefieldEvent
    propagation + replacement effects (Doubling Season-style
    multipliers).

    `count` is the requested number of tokens; replacement effects
    (e.g., Doubling Season) may modify it before emission resolves.
    """
    event_type: str = "TokenCreateEvent"
    creator_card_id: Optional[str] = None
    controller_id: int = 0
    token_name: str = ""
    token_power: Optional[str] = None
    token_toughness: Optional[str] = None
    token_types: List[str] = field(default_factory=list)
    token_subtypes: List[str] = field(default_factory=list)
    token_colors: List[str] = field(default_factory=list)
    token_keywords: List[str] = field(default_factory=list)
    count: int = 1


@dataclass
class LibrarySearchEvent(Event):
    """Fires when a card or ability searches a player's library
    (tutors, fetch lands, Demonic Tutor). Unlocks interactions with
    Aven Mindcensor, Stranglehold, Opposition Agent via replacement
    effects. iter-12+ tutor cards wire emission as they're added.

    `search_predicate`: e.g. "any" | "basic_land" | "creature" |
    "instant_or_sorcery". Free-form string for iter-11 simplicity;
    iter-12+ may formalize.
    """
    event_type: str = "LibrarySearchEvent"
    searcher_id: int = 0
    target_player_id: int = 0       # whose library is being searched
    search_predicate: str = ""
    reveal: bool = False
    shuffle_after: bool = True


@dataclass
class CombatDamageDealtEvent(Event):
    """Fires after combat damage has been DEALT (post-damage-mark,
    pre-SBA-cascade). Replaces v11's shim in
    cards/triggered/framework.py which is now a thin re-export of
    this substrate dataclass -- listeners are unchanged.

    `target_kind`: "creature" | "player" | "planeswalker".
    `amount`: damage actually dealt (post-prevention/replacement).
    `is_first_strike`: True if dealt during the first-strike pass.

    Field names match the v11 shim for drop-in compatibility.
    """
    event_type: str = "CombatDamageDealtEvent"
    source_card_id: str = ""
    source_controller: int = 0
    target_kind: str = "player"
    target_id: Any = None           # card_id (creature/pw) or player_id (int)
    amount: int = 0
    is_first_strike: bool = False


# Convenience map for pattern matching.
EVENT_TYPES = {
    "DrawEvent", "DamageEvent", "EnterBattlefieldEvent",
    "DieEvent", "LifeChangeEvent", "CounterAddEvent",
    "CounterRemoveEvent", "DiscardEvent", "MillEvent",
    # v14 additions:
    "TokenCreateEvent", "LibrarySearchEvent", "CombatDamageDealtEvent",
}
