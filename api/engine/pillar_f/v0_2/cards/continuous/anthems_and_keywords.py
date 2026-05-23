"""Phase 4 — Layer-6 (keyword grants) + Layer-7c (P/T anthems).

Rhythm of the Wild — "Creature spells you control can't be countered.
Nontoken creatures you control have riot." → riot keyword grant in
layer 6 to your nontoken creatures.

Anger — "Haste. As long as Anger is in your graveyard and you control
a Mountain, creatures you control have haste." → conditional layer-6
grant (haste). The base printed Haste is in `Card.keywords` already.

Hexing Squelcher — multi-effect (uncounter + other creatures you
control etc.) → routes to complex if too many ability paragraphs;
here we handle the "Other creatures you control" anthem-style grant.

Honor of the Pure-style anthems (not in top 500 but the framework
should exist): "Creatures you control get +1/+1" → layer 7c with
target_pattern selecting controller's creatures. Cards: Glorious
Anthem family.

The substrate's layer engine already ships `_l6_grant_keyword` +
`_l7c_anthem` effect_fns; this module reuses them via
ContinuousEffect with the right target_pattern.
"""
from __future__ import annotations

from api.engine.pillar_f.v0_2.cards.continuous.framework import (
    register_continuous_effect,
)
from api.engine.pillar_f.v0_2.state import (
    ContinuousEffect, GameState,
)


# ============================================================
# Rhythm of the Wild — grant riot to controller's nontoken creatures.
# ============================================================


def _build_rhythm_of_wild_effect(state: GameState, card_id: str,
                                  controller: int) -> ContinuousEffect:
    """The substrate's _l6_grant_keyword uses target_pattern selectors
    that include `all_creatures_controller` (selects all creatures
    controlled by player_id). We grant 'riot' to all creatures
    controller controls. Token-only filtering isn't currently supported
    by the substrate's _select_targets (it accepts `name`/`subtype`/
    `controller`/`all_creatures*`), so this stub grants riot to ALL
    of controller's creatures, including tokens. The "nontoken" filter
    is an iter-11+ refinement."""
    return ContinuousEffect(
        effect_id=f"rotw_{card_id}",
        source_card_id=card_id, controller=controller,
        layer=6, sublayer=None,
        effect_fn_name="grant_keyword",
        target_pattern={
            "all_creatures_controller": controller,
            "grant_keyword": "riot",
        },
        description="Rhythm of the Wild: grant riot to your creatures",
    )


register_continuous_effect("Rhythm of the Wild",
                          _build_rhythm_of_wild_effect)


# ============================================================
# Anger — grant haste to controller's creatures when Anger is in
# graveyard AND controller controls a Mountain.
#
# This is a "static ability from outside the battlefield" (CR 112.6c).
# The substrate's layer engine only walks effects in
# state.continuous_effects, which are typically attached on ETB.
# For Anger's graveyard-static, we register the ContinuousEffect when
# the card MOVES TO THE GRAVEYARD (handled by per-card LTB logic in
# Phase 8 / complex; the framework is here so Phase 8 can register
# the trigger that adds the effect).
#
# Iter-10 simplification: we register the effect at ETB too, so
# Anger-on-the-battlefield grants haste. (Anger on the battlefield
# DOES have haste itself — printed keyword. The grant is conditional
# on being in graveyard, but the iter-10 stub doesn't check zone.)
#
# Iter-11+ TODO: zone-aware activation of this effect.
# ============================================================


def _build_anger_effect(state: GameState, card_id: str,
                        controller: int) -> ContinuousEffect:
    return ContinuousEffect(
        effect_id=f"anger_{card_id}",
        source_card_id=card_id, controller=controller,
        layer=6, sublayer=None,
        effect_fn_name="grant_keyword",
        target_pattern={
            "all_creatures_controller": controller,
            "grant_keyword": "haste",
        },
        description="Anger: grant haste to your creatures",
    )


register_continuous_effect("Anger", _build_anger_effect)
