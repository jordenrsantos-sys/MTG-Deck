"""Replacement-effect framework: per-card builders for the substrate's
replacement-effect engine.

The substrate's `register_replacement_fn(name, fn)` API takes a
replacement_fn keyed by name. Per-card modules register the
replacement_fn at import time + a builder that, on ETB, constructs
a ReplacementEffect referencing the fn-name and appends it to
state.replacement_effects.

The substrate's `apply_replacements(state, event)` walks all matching
ReplacementEffects + applies the registered fn. The self-replacement
rule (each fn applies at most once per event — CR 614.5) is enforced
by the substrate.

Iter-10 substrate ships 5 built-in replacement_fns:
  - fog_prevent_combat_damage
  - rest_in_peace_die_to_exile
  - etb_tapped
  - doubling_season_counters
  - leyline_of_void_to_exile

This module REUSES substrate built-ins where possible (Doubling Season's
counter side, Rest in Peace's "die → exile" generalization) and only
registers NEW replacement_fns for cards the substrate doesn't already
cover.
"""
from __future__ import annotations

from typing import Any, Callable, Optional

from api.engine.pillar_f.v0_2.state import (
    GameState, ReplacementEffect,
)


# Builder signature mirrors the continuous framework: invoked on ETB,
# returns a ReplacementEffect (or list) for the caller to append to
# state.replacement_effects.
ReplacementBuilder = Callable[
    [GameState, str, int],
    Any,  # ReplacementEffect or List[ReplacementEffect]
]


_REPLACEMENT_BUILDERS: dict = {}


def register_replacement_builder(card_name: str,
                                  builder: ReplacementBuilder) -> None:
    _REPLACEMENT_BUILDERS[card_name] = builder


def get_replacement_builder(card_name: str) -> Optional[ReplacementBuilder]:
    return _REPLACEMENT_BUILDERS.get(card_name)


def attach_replacements_for(state: GameState, card_id: str) -> int:
    """On ETB: look up the source card's replacement builder, run it,
    append the resulting ReplacementEffect(s) to state.replacement_effects.
    Returns the number attached.
    """
    card = state.get_card(card_id)
    if card is None:
        return 0
    builder = _REPLACEMENT_BUILDERS.get(card.name)
    if builder is None:
        return 0
    try:
        result = builder(state, card_id, card.controller)
    except Exception:
        return 0
    if result is None:
        return 0
    if isinstance(result, ReplacementEffect):
        state.replacement_effects.append(result)
        return 1
    if isinstance(result, list):
        added = 0
        for r in result:
            if isinstance(r, ReplacementEffect):
                state.replacement_effects.append(r)
                added += 1
        return added
    return 0


def detach_replacements_for(state: GameState, card_id: str) -> int:
    """On LTB: remove all ReplacementEffect entries with source_card_id
    == card_id. Returns the number removed."""
    before = len(state.replacement_effects)
    state.replacement_effects = [
        r for r in state.replacement_effects if r.source_card_id != card_id
    ]
    return before - len(state.replacement_effects)


def all_registered_card_names() -> list:
    return sorted(_REPLACEMENT_BUILDERS.keys())
