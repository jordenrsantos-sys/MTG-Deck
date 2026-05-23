"""Phase 3 — Fetchlands.

The 10 Onslaught/Zendikar fetchlands + 5 "rainbow-fetch" lands +
5 "search any basic" lands (Evolving Wilds, Terramorphic Expanse,
Prismatic Vista, Fabled Passage, Myriad Landscape).

Each fetchland's activated ability:
  {T}, Pay 1 life, Sacrifice this land: Search your library for a
  [type] card, put it onto the battlefield, then shuffle.

Caller pays the activation costs (tap, pay-life, sacrifice = move
fetch to graveyard) BEFORE pushing the activation to the stack. The
resolver then performs the library search + move-to-battlefield.

The framework's `fetchland_resolver(allowed_subtypes)` factory
generates the per-card resolver. Each fetchland registers with its
acceptable land types.
"""
from __future__ import annotations

from typing import Tuple

from api.engine.pillar_f.v0_2.cards.activated.framework import (
    ActivatedAbilityMeta, register_activated_ability, fetchland_resolver,
)
from api.engine.pillar_f.v0_2.stack import register_resolver


# (card_name, allowed_subtypes, pay_life)
_FETCHLANDS: Tuple[Tuple[str, Tuple[str, ...], int], ...] = (
    # Onslaught + Zendikar 10-fetch cycle (pay 1 life).
    ("Polluted Delta", ("Island", "Swamp"), 1),
    ("Bloodstained Mire", ("Swamp", "Mountain"), 1),
    ("Flooded Strand", ("Plains", "Island"), 1),
    ("Marsh Flats", ("Plains", "Swamp"), 1),
    ("Windswept Heath", ("Forest", "Plains"), 1),
    ("Wooded Foothills", ("Mountain", "Forest"), 1),
    ("Verdant Catacombs", ("Swamp", "Forest"), 1),
    ("Misty Rainforest", ("Forest", "Island"), 1),
    ("Scalding Tarn", ("Island", "Mountain"), 1),
    ("Arid Mesa", ("Mountain", "Plains"), 1),
    # Generic basic-land fetches (no life cost).
    ("Evolving Wilds",
     ("Plains", "Island", "Swamp", "Mountain", "Forest"), 0),
    ("Terramorphic Expanse",
     ("Plains", "Island", "Swamp", "Mountain", "Forest"), 0),
    ("Fabled Passage",
     ("Plains", "Island", "Swamp", "Mountain", "Forest"), 0),
    ("Prismatic Vista",
     ("Plains", "Island", "Swamp", "Mountain", "Forest"), 1),
)


for card_name, subs, life in _FETCHLANDS:
    resolver_key = "act_fetch_" + card_name.lower().replace(" ", "_").replace("'", "")
    register_resolver(resolver_key, fetchland_resolver(subs, basic_only=True))
    register_activated_ability(ActivatedAbilityMeta(
        card_name=card_name, ability_key="fetch",
        resolver_name=resolver_key,
        cost_tap=True, cost_pay_life=life,
        cost_sacrifice_self=True,
        description=f"{{T}},{life} life,Sac: Fetch from {sorted(subs)}",
    ))
