"""Phase 4 — Layer-4 type-adding lands.

Urborg, Tomb of Yawgmoth: "Each land is a Swamp in addition to its
other land types."

Yavimaya, Cradle of Growth: "Each land is a Forest in addition to its
other land types."

These are layer-4 effects (type/subtype changes). The substrate's
layer engine ships an `add_subtype` effect_fn that takes a target_pattern
with `{add_subtype: str, all_lands: True}`. Iter-10's `_select_targets`
in layer_engine.py doesn't yet support an `all_lands` pattern key —
it supports `all_creatures`, `all_creatures_controller`, etc. Rather
than add an `all_lands` selector to the substrate (would be a
substrate change → halt), we register a layer-4 effect that uses the
substrate's existing `effect_fn` slot with a custom apply function
defined locally in this module.

The substrate exposes `register_layer_effect(name, fn)`; we register
a "land_type_adder" effect whose apply scans all battlefield permanents
+ adds the subtype to lands. This is fully within the substrate's
existing API — we're adding a NEW NAMED effect to the registry, not
modifying any existing substrate behavior.
"""
from __future__ import annotations

from typing import Any, Dict

from api.engine.pillar_f.v0_2.cards.continuous.framework import (
    register_continuous_effect,
)
from api.engine.pillar_f.v0_2.layers import register_layer_effect
from api.engine.pillar_f.v0_2.layers.layer_engine import (
    parse_type_line, reassemble_type_line,
)
from api.engine.pillar_f.v0_2.state import (
    ContinuousEffect, GameState,
)


# ============================================================
# Substrate layer-effect function: add a subtype to every land.
# ============================================================


def _layer4_add_subtype_to_all_lands(
    state: GameState, effect: "ContinuousEffect", table: Dict[str, Any],
) -> None:
    """Walk every characteristic record in the table; if the permanent
    is a land, add the subtype from `effect.target_pattern["add_subtype"]`
    (idempotent — won't add twice).

    The Characteristics record already has `subtypes` + `types`; we
    check `"Land" in chars.types` to identify lands."""
    subtype = effect.target_pattern.get("add_subtype")
    if not subtype:
        return
    for chars in table.values():
        if "Land" not in chars.types:
            continue
        if subtype in chars.subtypes:
            continue
        chars.subtypes.append(subtype)
        chars.type_line = reassemble_type_line(
            chars.supertypes, chars.types, chars.subtypes,
        )


register_layer_effect("layer4_add_subtype_to_all_lands",
                     _layer4_add_subtype_to_all_lands)


# ============================================================
# Per-card builders.
# ============================================================


def _build_urborg_effect(state: GameState, card_id: str,
                          controller: int) -> ContinuousEffect:
    return ContinuousEffect(
        effect_id=f"urborg_{card_id}",
        source_card_id=card_id, controller=controller,
        layer=4, sublayer=None,
        effect_fn_name="layer4_add_subtype_to_all_lands",
        target_pattern={"add_subtype": "Swamp"},
        description="Urborg: each land is a Swamp",
    )


def _build_yavimaya_effect(state: GameState, card_id: str,
                            controller: int) -> ContinuousEffect:
    return ContinuousEffect(
        effect_id=f"yavimaya_{card_id}",
        source_card_id=card_id, controller=controller,
        layer=4, sublayer=None,
        effect_fn_name="layer4_add_subtype_to_all_lands",
        target_pattern={"add_subtype": "Forest"},
        description="Yavimaya: each land is a Forest",
    )


register_continuous_effect("Urborg, Tomb of Yawgmoth", _build_urborg_effect)
register_continuous_effect("Yavimaya, Cradle of Growth", _build_yavimaya_effect)
