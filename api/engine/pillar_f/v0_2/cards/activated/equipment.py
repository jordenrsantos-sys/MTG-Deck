"""Phase 3 — Equipment equip-costs.

Lightning Greaves ({0}: Attach), Swiftfoot Boots ({1}: Attach),
Whispersilk Cloak ({3}: Attach), Mithril Coat ({0}: ETB attach handled
in etb/lands_and_enchantments.py; equip cost wired here too).

Each equipment registers the equip-cost activated ability via the
framework's `equip_resolver()` factory. The shroud-restriction on
Lightning Greaves' equipped creature (and on Swiftfoot Boots' hexproof
side) lives in the continuous bucket (Phase 4) since it's a static
"equipped creature has shroud" effect.
"""
from __future__ import annotations

from api.engine.pillar_f.v0_2.cards.activated.framework import (
    ActivatedAbilityMeta, register_activated_ability, equip_resolver,
)
from api.engine.pillar_f.v0_2.stack import register_resolver


_EQUIPMENT = (
    # (name, equip_cost_mana)
    ("Lightning Greaves", "{0}"),
    ("Swiftfoot Boots", "{1}"),
    ("Whispersilk Cloak", "{3}"),
    ("Mithril Coat", "{0}"),  # Mithril Coat's equip is "Equip legendary creature {1}"
                              # but its ETB attach is {0}. Iter-10 stub: use {0}.
    ("Commander's Plate", "{3}"),
    ("Blackblade Reforged", "{7}"),
    ("Skullclamp", "{1}"),  # Skullclamp's equip cost — death trigger lives in complex/
)

for name, cost in _EQUIPMENT:
    resolver_key = "act_equip_" + name.lower().replace(" ", "_").replace("'", "")
    register_resolver(resolver_key, equip_resolver())
    register_activated_ability(ActivatedAbilityMeta(
        card_name=name, ability_key="equip",
        resolver_name=resolver_key,
        cost_mana=cost, sorcery_speed=True,  # equip is sorcery-speed per CR 702.6
        description=f"Equip {cost}",
    ))
