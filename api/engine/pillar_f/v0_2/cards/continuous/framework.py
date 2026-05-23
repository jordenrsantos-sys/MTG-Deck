"""Continuous-effect framework: per-card helpers for the substrate's
layer engine + a parallel registry for static effects that the layer
engine doesn't model (cost reducers, attack taxes, action restrictions).

Two registries:
  1. LAYERED — wraps the substrate's register_layer_effect. Per-card
     modules call `attach_layer_effect_on_etb(card_name, builder)` to
     register a builder that, when the card enters the battlefield,
     constructs a ContinuousEffect + appends it to
     `state.continuous_effects` (the substrate's layer engine then
     picks it up the next time `apply_continuous_effects` runs).

  2. STATIC_MODIFIERS — for effects that aren't expressible as one of
     the 7 layers but ARE static abilities affecting cast-cost,
     attack-cost, action-permissions, etc. (Medallions, Propaganda,
     Grand Abolisher, Drannith Magistrate, Exploration, Azusa). The
     substrate's cast/attack/play-land machinery doesn't currently
     consult these at iter-10 — the registry is a queryable data
     structure that iter-11+ (LLM policy) AND iter-12+ (cast pipeline)
     can read to compute eligible actions + their adjusted costs.

The static-modifier registry is the v11 escape hatch for the cards
that the substrate's existing layer-effect API can't capture. It's a
data structure, not a behavior override — so no substrate change is
needed. The substrate's `ContinuousEffect` schema is unchanged.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from api.engine.pillar_f.v0_2.state import ContinuousEffect, GameState


# ============================================================
# 1. LAYERED effects — registry of builders run on card ETB
# ============================================================

# Signature: (state, card_id, controller) → ContinuousEffect (or list)
# The builder is invoked when the source card enters the battlefield;
# it constructs the ContinuousEffect(s) + the caller appends them to
# state.continuous_effects so the substrate's layer engine picks them
# up in `apply_continuous_effects`.
ContinuousEffectBuilder = Callable[
    [GameState, str, int],
    Any,  # ContinuousEffect or List[ContinuousEffect]
]


_LAYERED_BUILDERS: Dict[str, ContinuousEffectBuilder] = {}


def register_continuous_effect(card_name: str,
                                builder: ContinuousEffectBuilder) -> None:
    """Register the layered-effect builder for a card. When the card
    enters the battlefield, the caller invokes
    `attach_layered_effects_for(state, card)` to get the
    ContinuousEffect(s) + append them to state.continuous_effects.

    Last-registration-wins per name (consistent with the other v11
    registries)."""
    _LAYERED_BUILDERS[card_name] = builder


def get_continuous_effect_builder(
    card_name: str,
) -> Optional[ContinuousEffectBuilder]:
    return _LAYERED_BUILDERS.get(card_name)


def attach_layered_effects_for(state: GameState, card_id: str) -> int:
    """Per ETB: look up the source card's continuous-effect builder, run
    it, and append the resulting ContinuousEffect(s) to
    state.continuous_effects. Returns the number of effects attached.
    """
    card = state.get_card(card_id)
    if card is None:
        return 0
    builder = _LAYERED_BUILDERS.get(card.name)
    if builder is None:
        return 0
    try:
        result = builder(state, card_id, card.controller)
    except Exception:
        return 0
    if result is None:
        return 0
    if isinstance(result, ContinuousEffect):
        state.continuous_effects.append(result)
        return 1
    if isinstance(result, list):
        added = 0
        for ce in result:
            if isinstance(ce, ContinuousEffect):
                state.continuous_effects.append(ce)
                added += 1
        return added
    return 0


def detach_layered_effects_for(state: GameState, card_id: str) -> int:
    """When the source card leaves the battlefield, remove every
    ContinuousEffect with source_card_id == card_id. Returns the
    number of effects removed. Caller is responsible for invoking this
    on bounce/destroy/exile."""
    before = len(state.continuous_effects)
    state.continuous_effects = [
        ce for ce in state.continuous_effects
        if ce.source_card_id != card_id
    ]
    return before - len(state.continuous_effects)


# ============================================================
# 2. STATIC MODIFIERS — non-layered static effects
# ============================================================


@dataclass
class StaticModifier:
    """A static ability that doesn't fit one of the 7 layers but is
    queryable as data. Iter-11+ (LLM policy + cast pipeline) reads this
    registry when computing what actions are eligible + their costs."""
    card_name: str
    effect_key: str            # short id: "cost_reduction", "attack_tax",
                               # "spell_restriction", "additional_land_drops"
    # Effect-specific fields. Iter-10 uses a free-form dict so cards
    # can express their specific modifier shape without forcing a new
    # dataclass per effect type.
    params: Dict[str, Any] = field(default_factory=dict)
    description: str = ""


_STATIC_MODIFIER_REGISTRY: Dict[str, List[StaticModifier]] = {}


def register_static_modifier(mod: StaticModifier) -> None:
    """Register a non-layered static effect. Multiple modifiers per
    card name are allowed (a card might both reduce cast cost AND
    grant a static keyword)."""
    _STATIC_MODIFIER_REGISTRY.setdefault(mod.card_name, []).append(mod)


def get_static_modifiers(card_name: str) -> List[StaticModifier]:
    return list(_STATIC_MODIFIER_REGISTRY.get(card_name, []))


def all_static_modifier_card_names() -> List[str]:
    return sorted(_STATIC_MODIFIER_REGISTRY.keys())


def query_active_static_modifiers(
    state: GameState, effect_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return active static modifiers currently in play (i.e., from
    cards on a player's battlefield). Each entry includes the
    controller + modifier metadata so consumers can apply per-player
    semantics ("opponents can't" vs. "you can").

    `effect_key` filter: pass "cost_reduction" to get only cost
    reducers, etc.
    """
    out: List[Dict[str, Any]] = []
    for ps in state.players:
        if ps.has_lost:
            continue
        for cid in ps.zones.battlefield:
            card = state.get_card(cid)
            if card is None:
                continue
            mods = _STATIC_MODIFIER_REGISTRY.get(card.name, [])
            for m in mods:
                if effect_key is not None and m.effect_key != effect_key:
                    continue
                out.append({
                    "source_card_id": cid,
                    "controller": ps.player_id,
                    "card_name": card.name,
                    "effect_key": m.effect_key,
                    "params": dict(m.params),
                    "description": m.description,
                })
    return out


def clear_static_modifier_registry() -> None:
    """Test-only helper."""
    _STATIC_MODIFIER_REGISTRY.clear()
