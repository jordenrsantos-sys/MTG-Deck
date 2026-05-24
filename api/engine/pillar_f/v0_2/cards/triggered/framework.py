"""Triggered-ability framework: per-card event-trigger registry.

The substrate ships:
  - `register_step_trigger(step, trigger_dict)` — fires at the start of
    a named step (upkeep, draw, etc.) by step-machine integration.
  - `enqueue_triggers(state, triggers, source_event)` — adds triggers
    to the delayed-trigger queue.
  - `drain_triggers_to_stack(state)` — moves the queue to the stack
    in APNAP order.

What this module adds:
  - A per-card EVENT-trigger registry keyed by (event_type, card_name).
    Events are the v11 EVENT_TYPES the substrate emits (DrawEvent,
    DieEvent, CounterAddEvent, etc.) PLUS v11-defined extras like
    SpellCastEvent and CombatDamageDealtEvent that the substrate
    doesn't ship but the v11 layer adds as a thin shim.

  - `fire_event_triggers(state, event)` — dispatcher: walks every
    card on the battlefield, looks up matching triggers, enqueues
    the trigger dicts.

  - Upkeep / step triggers integrate with the substrate's
    register_step_trigger directly when the card enters play (the
    builder attaches; LTB cleanup detaches).

Per-card modules:
  1. Define a resolver: `def blood_artist_resolve(state, entry): ...`.
     Register it: `register_resolver("trig_blood_artist", ...)`.
  2. Define a trigger builder: `def blood_artist_die_trigger(state,
     event): -> List[trigger_dict]`.
     Register it: `register_event_trigger("DieEvent", "Blood Artist",
     blood_artist_die_trigger)`.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

from api.engine.pillar_f.v0_2.replacement import Event
from api.engine.pillar_f.v0_2.stack import enqueue_triggers
from api.engine.pillar_f.v0_2.state import GameState


# Trigger-build fn: (state, event) → list of trigger dicts. Trigger
# dict shape matches the substrate's enqueue_triggers contract.
EventTriggerFn = Callable[
    [GameState, Event],
    List[Dict[str, Any]],
]


# Registry keyed by (event_type, card_name). The dispatcher walks the
# battlefield checking each card for matching registrations.
_EVENT_TRIGGER_REGISTRY: Dict[Tuple[str, str], EventTriggerFn] = {}


def register_event_trigger(event_type: str, card_name: str,
                            fn: EventTriggerFn) -> None:
    _EVENT_TRIGGER_REGISTRY[(event_type, card_name)] = fn


def get_event_trigger(event_type: str,
                      card_name: str) -> Optional[EventTriggerFn]:
    return _EVENT_TRIGGER_REGISTRY.get((event_type, card_name))


def fire_event_triggers(state: GameState, event: Event) -> int:
    """Walk all battlefield permanents; for each card with a matching
    (event_type, card_name) registration, invoke its trigger fn +
    enqueue the resulting trigger dicts.

    Returns the total number of trigger dicts enqueued. APNAP ordering
    is handled by the substrate's drain_triggers_to_stack."""
    enqueued = 0
    for ps in state.players:
        if ps.has_lost:
            continue
        for cid in ps.zones.battlefield:
            card = state.get_card(cid)
            if card is None:
                continue
            fn = _EVENT_TRIGGER_REGISTRY.get(
                (event.event_type, card.name),
            )
            if fn is None:
                continue
            try:
                triggers = fn(state, event) or []
            except Exception:
                continue
            if not triggers:
                continue
            enqueue_triggers(state, triggers, source_event={
                "type": event.event_type,
                "source_card_id": cid,
            })
            enqueued += len(triggers)
    return enqueued


# ============================================================
# Per-card upkeep/step trigger registry (the substrate's
# register_step_trigger is a global queue; this layer indexes by card
# so LTB can remove cleanly.)
# ============================================================


_STEP_TRIGGER_REGISTRY: Dict[str, List[Dict[str, Any]]] = {}


def register_step_trigger_for_card(card_name: str,
                                    trigger_dict: Dict[str, Any]) -> None:
    """Register a step trigger keyed by source card name. Used by ETB
    attach to install the trigger into the substrate's step_trigger
    list, and by LTB to remove it cleanly later."""
    _STEP_TRIGGER_REGISTRY.setdefault(card_name, []).append(trigger_dict)


def get_step_triggers_for_card(card_name: str) -> List[Dict[str, Any]]:
    return list(_STEP_TRIGGER_REGISTRY.get(card_name, []))


# ============================================================
# v11 SHIM EVENTS that the substrate doesn't ship but per-card triggers
# want to subscribe to. These are not part of the substrate's event
# taxonomy; the v11 layer emits them when the relevant action happens.
# ============================================================


from dataclasses import dataclass


@dataclass
class SpellCastEvent(Event):
    """Fired when a player casts a spell. v11-defined shim; substrate
    doesn't emit this in iter-10 (cast pipeline is iter-12+ scope).
    Tests that exercise spell-cast-triggered abilities (Rhystic Study,
    Beast Whisperer, etc.) manually fire this event."""
    event_type: str = "SpellCastEvent"
    caster_player_id: int = 0
    spell_card_id: str = ""
    spell_card_name: str = ""
    spell_types: List[str] = None  # ["Creature", "Instant", "Sorcery"]
    spell_subtypes: List[str] = None
    spell_colors: List[str] = None
    spell_mana_cost: str = ""

    def __post_init__(self):
        if self.spell_types is None:
            self.spell_types = []
        if self.spell_subtypes is None:
            self.spell_subtypes = []
        if self.spell_colors is None:
            self.spell_colors = []


@dataclass
class CombatDamageDealtEvent(Event):
    """Fired when combat damage is dealt. Used by Ragavan, Professional
    Face-Breaker, etc. The substrate's combat module dispatches damage
    in iter-10 but doesn't emit this event — v11 tests fire it
    manually until the cast/combat pipeline is wired."""
    event_type: str = "CombatDamageDealtEvent"
    source_card_id: str = ""
    source_controller: int = 0
    target_kind: str = "player"      # "player" | "creature" | "planeswalker"
    target_id: Any = None
    amount: int = 0


@dataclass
class AttackDeclaredEvent(Event):
    """Fired during the declare-attackers step for each declared attacker.
    Used by attack-trigger cards (Sword of the Animist, Edgar Markov,
    Sanctum Seeker, etc.)."""
    event_type: str = "AttackDeclaredEvent"
    attacker_card_id: str = ""
    attacker_controller: int = 0
    defending_player: int = 0


@dataclass
class LandTappedForManaEvent(Event):
    """Fired when a land taps to produce mana. Used by Wild Growth,
    Crypt Ghast (doubling Swamp mana), etc."""
    event_type: str = "LandTappedForManaEvent"
    land_card_id: str = ""
    land_controller: int = 0
    mana_produced_color: str = ""   # "G", "B", etc.
