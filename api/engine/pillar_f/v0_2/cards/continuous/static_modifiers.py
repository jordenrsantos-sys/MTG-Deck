"""Phase 4 — Non-layered static modifiers.

Cards in the top-500 continuous bucket whose effects aren't expressible
as one of the substrate's 7 layers. Each card registers a
`StaticModifier` entry. The registry is a queryable data structure —
the substrate's cast/attack/play-land machinery doesn't currently
consult it (iter-10 cost-payment is caller-controlled), but iter-11+
(LLM policy + cast pipeline) reads it to compute eligible actions
+ adjusted costs.

Coverage:
  cost_reduction:
    - 5 Medallions (Jet, Sapphire, Ruby, Pearl, Emerald)
    - Foundry Inspector (artifact spells {1} less)
    - Etherium Sculptor (artifact spells {1} less)
    - Goblin Anarchomancer (red or green spells {1} less)

  attack_tax:
    - Propaganda + Ghostly Prison (attackers pay {2} per creature
      attacking you, controller paying)

  spell_restriction:
    - Grand Abolisher (during your turn, opponents can't cast spells
      or activate non-mana abilities of artifacts/creatures/enchantments)
    - Drannith Magistrate (opponents can't cast spells from anywhere
      other than hand)
    - Hexing Squelcher partial (spells you control can't be countered;
      also has multi-effect components → Phase 8)

  additional_land_drops:
    - Exploration (+1 land drop per turn)
    - Azusa, Lost but Seeking (+2 land drops per turn)
"""
from __future__ import annotations

from api.engine.pillar_f.v0_2.cards.continuous.framework import (
    StaticModifier, register_static_modifier,
)


# ============================================================
# Cost reducers
# ============================================================


_COLOR_MEDALLIONS = (
    # (card_name, color_letter, oracle_id_color)
    ("Jet Medallion", "B"),
    ("Sapphire Medallion", "U"),
    ("Ruby Medallion", "R"),
    ("Pearl Medallion", "W"),
    ("Emerald Medallion", "G"),
)

for name, color in _COLOR_MEDALLIONS:
    register_static_modifier(StaticModifier(
        card_name=name, effect_key="cost_reduction",
        params={
            "reduction": 1,                # spells cost {1} less
            "applies_to": "your_spells",
            "color_filter": color,         # spell must include this color
            "minimum_cost_after": 0,       # never below 0
        },
        description=f"{name}: your {color} spells cost {{1}} less",
    ))


# Type-based cost reducers.
_TYPE_REDUCERS = (
    ("Foundry Inspector", "Artifact", 1, "your_spells", None),
    ("Etherium Sculptor", "Artifact", 1, "your_spells", None),
    # Goblin Anarchomancer = any spell that's red OR green (not both
    # required) → params["color_filter"] is a list-of-OR.
    ("Goblin Anarchomancer", None, 1, "your_spells", ["R", "G"]),
)

for name, type_filter, reduction, scope, multi_color in _TYPE_REDUCERS:
    params = {
        "reduction": reduction,
        "applies_to": scope,
        "minimum_cost_after": 0,
    }
    if type_filter:
        params["type_filter"] = type_filter
    if multi_color:
        params["color_filter_any"] = multi_color
    register_static_modifier(StaticModifier(
        card_name=name, effect_key="cost_reduction",
        params=params,
        description=f"{name}: cost reduction (-{reduction})",
    ))


# ============================================================
# Attack taxes
# ============================================================


_ATTACK_TAX = (
    ("Propaganda", 2),
    ("Ghostly Prison", 2),
    # Sphere of Safety, No Mercy etc. — only the 2 in top-500.
)

for name, tax in _ATTACK_TAX:
    register_static_modifier(StaticModifier(
        card_name=name, effect_key="attack_tax",
        params={
            "tax_mana": tax,             # {tax} per attacking creature
            "applies_to_attackers_of": "controller",  # protect controller
            "per_attacker": True,
        },
        description=f"{name}: attackers pay {{{tax}}} per attacking creature",
    ))


# ============================================================
# Spell / action restrictions
# ============================================================


register_static_modifier(StaticModifier(
    card_name="Grand Abolisher",
    effect_key="spell_restriction",
    params={
        "applies_to": "opponents",
        "during": "controllers_turn",
        "restrict": ["cast_spells", "activate_nonmana_abilities"],
        "ability_source_filter": ["artifact", "creature", "enchantment"],
    },
    description="Grand Abolisher: opponents silent during your turn",
))

register_static_modifier(StaticModifier(
    card_name="Drannith Magistrate",
    effect_key="spell_restriction",
    params={
        "applies_to": "opponents",
        "during": "always",
        "restrict": ["cast_from_non_hand_zones"],
    },
    description="Drannith Magistrate: opponents can only cast from hand",
))


# Hexing Squelcher partial (the "spells you control can't be countered"
# side; remaining sides routed to Phase 8 / complex).
register_static_modifier(StaticModifier(
    card_name="Hexing Squelcher",
    effect_key="uncounterable",
    params={
        "applies_to": "your_spells",
    },
    description="Hexing Squelcher: your spells can't be countered (partial)",
))


# ============================================================
# Additional-land-drop permissions
# ============================================================


register_static_modifier(StaticModifier(
    card_name="Exploration",
    effect_key="additional_land_drops",
    params={"additional_drops_per_turn": 1, "applies_to": "controller"},
    description="Exploration: +1 land drop per turn",
))

register_static_modifier(StaticModifier(
    card_name="Azusa, Lost but Seeking",
    effect_key="additional_land_drops",
    params={"additional_drops_per_turn": 2, "applies_to": "controller"},
    description="Azusa: +2 land drops per turn",
))
