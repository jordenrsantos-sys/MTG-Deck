"""Replacement-effect engine.

Per CR 614: replacement effects modify or replace events BEFORE they
happen. A replacement effect is registered against a source card with
an event-type pattern + optional predicate filters. When the engine
processes an event, it walks all matching replacement effects in
controller-determined order (CR 616 — affected event's controller
chooses order when multiple apply).

Self-replacement: each replacement effect applies AT MOST ONCE per
event (CR 614.5). The engine tracks already-applied effect_ids per
event instance.

Iter-10 contract: replacement_fn(event, state, source_card_id) →
mutates event in place. Set `event.replaced = True` if a replacement
was applied (CR considers ETB-tapped replacements to "consume" the
ETB event for purposes of further matching).

The function is keyed by name in `_REPLACEMENT_FN_REGISTRY` —
ReplacementEffect.replacement_fn_name stores the name, which the
engine resolves at apply time.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Set

from api.engine.pillar_f.v0_2.state import (
    GameState, ReplacementEffect,
)
from api.engine.pillar_f.v0_2.replacement.events import Event


REPLACEMENT_VERSION = "pillar_f_v0_2_replacement_v1"

# replacement_fn signature: (event, state, source_card_id) → None
# Mutates event in place. Set event.replaced=True if it modified the event.
ReplacementFn = Callable[[Event, GameState, Optional[str]], None]

_REPLACEMENT_FN_REGISTRY: Dict[str, ReplacementFn] = {}


def register_replacement_fn(name: str, fn: ReplacementFn) -> None:
    _REPLACEMENT_FN_REGISTRY[name] = fn


def get_replacement_fn(name: str) -> Optional[ReplacementFn]:
    return _REPLACEMENT_FN_REGISTRY.get(name)


def _pattern_matches(pattern: Dict[str, Any], event: Event) -> bool:
    """Check whether `pattern` matches `event`. Pattern keys:
      - "type": event type string (REQUIRED). Compared to event.event_type.
      - Other keys: attribute names on the event. Value can be:
          * scalar → equality check
          * list → membership check (event.attr in list)
          * dict with "in" → membership check on a list value
    """
    if pattern.get("type") != event.event_type:
        return False
    for key, expected in pattern.items():
        if key == "type":
            continue
        if not hasattr(event, key):
            return False
        actual = getattr(event, key)
        if isinstance(expected, list):
            if actual not in expected:
                return False
        elif isinstance(expected, dict):
            if "in" in expected and actual not in expected["in"]:
                return False
            if "ne" in expected and actual == expected["ne"]:
                return False
        else:
            if actual != expected:
                return False
    return True


def apply_replacements(
    state: GameState,
    event: Event,
    *,
    affected_controller: Optional[int] = None,
) -> Event:
    """Walk all registered replacement effects matching `event` and
    apply them in controller-determined order. Each effect applies at
    most once per event (self-replacement rule).

    `affected_controller` is the player whose event is affected — used
    to resolve ordering when multiple replacements match (CR 616).
    Default = state.active_player.

    Returns the (possibly modified) event. Event.replaced=True signals
    a replacement applied; event.prevent=True signals full prevention.
    """
    if affected_controller is None:
        affected_controller = state.active_player

    applied_ids: Set[str] = set()
    safety = 50  # prevents infinite loops on poorly-defined replacement chains
    while safety > 0:
        safety -= 1
        # Find all matching, not-yet-applied replacements.
        candidates: List[ReplacementEffect] = []
        for r in state.replacement_effects:
            if r.effect_id in applied_ids:
                continue
            if _pattern_matches(r.event_pattern, event):
                candidates.append(r)
        if not candidates:
            break
        # CR 616 ordering: affected_controller chooses among candidates.
        # Iter-10: deterministic — pick the candidate whose controller
        # matches affected_controller first, else by source_card_id
        # alphabetical for stability. Sub-mega-task B can override.
        candidates.sort(key=lambda r: (
            0 if r.controller == affected_controller else 1,
            r.source_card_id or "",
        ))
        chosen = candidates[0]
        fn = get_replacement_fn(chosen.replacement_fn_name)
        if fn is None:
            applied_ids.add(chosen.effect_id)
            continue
        try:
            fn(event, state, chosen.source_card_id)
        except Exception:
            pass
        applied_ids.add(chosen.effect_id)
        if event.prevent:
            break
    return event


# ============================================================
# Built-in replacement functions (iter-10 minimal set for fixtures)
# ============================================================


def _replace_fog_prevent_combat_damage(
    event: Event, state: GameState, source_card_id: Optional[str],
) -> None:
    """Fog stub: prevent all combat damage from being dealt this turn.
    Pattern: type=DamageEvent, is_combat=True."""
    if event.event_type == "DamageEvent" and getattr(event, "is_combat", False):
        event.amount = 0
        event.prevent = True
        event.replaced = True


def _replace_rip_die_to_exile(
    event: Event, state: GameState, source_card_id: Optional[str],
) -> None:
    """Rest in Peace stub: when a creature would die, exile it instead.
    Pattern: type=DieEvent."""
    if event.event_type == "DieEvent":
        event.instead_zone = "exile"
        event.replaced = True


def _replace_etb_tapped(
    event: Event, state: GameState, source_card_id: Optional[str],
) -> None:
    """Stub: target card ETBs tapped. Pattern: type=EnterBattlefieldEvent."""
    if event.event_type == "EnterBattlefieldEvent":
        event.tapped_on_etb = True
        event.replaced = True


def _replace_doubling_season_counters(
    event: Event, state: GameState, source_card_id: Optional[str],
) -> None:
    """Doubling Season stub: if a player's permanent would have a +1/+1
    or loyalty counter put on it, it gets twice that many. Pattern:
    type=CounterAddEvent, counter_type in {+1/+1, loyalty}."""
    if event.event_type == "CounterAddEvent":
        if event.counter_type in ("+1/+1", "loyalty"):
            event.count *= 2
            event.replaced = True


def _replace_leyline_of_void_to_exile(
    event: Event, state: GameState, source_card_id: Optional[str],
) -> None:
    """Leyline of the Void stub: opponent's creatures going to graveyard
    get exiled instead. Pattern: type=DieEvent (controller != source.controller)."""
    if event.event_type == "DieEvent":
        # Simplification: exile if the dying card's controller != Leyline's
        # controller. Iter-10 hardcodes; iter 11+ wires the full pattern.
        leyline_controller = None
        for r in state.replacement_effects:
            if r.source_card_id == source_card_id:
                leyline_controller = r.controller
                break
        if leyline_controller is not None and event.controller != leyline_controller:
            event.instead_zone = "exile"
            event.replaced = True


# Register built-ins.
register_replacement_fn("fog_prevent_combat_damage", _replace_fog_prevent_combat_damage)
register_replacement_fn("rest_in_peace_die_to_exile", _replace_rip_die_to_exile)
register_replacement_fn("etb_tapped", _replace_etb_tapped)
register_replacement_fn("doubling_season_counters", _replace_doubling_season_counters)
register_replacement_fn("leyline_of_void_to_exile", _replace_leyline_of_void_to_exile)
