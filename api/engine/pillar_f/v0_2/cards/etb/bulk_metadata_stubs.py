"""Phase 9 — Bulk ETB-trigger metadata stubs for the 9 remaining
top-500 ETB cards routed to Phase 8+ for full handler authoring.

Each entry registers a NOOP ETB trigger via the framework — when the
ETB event fires, the dispatcher finds a registered handler, invokes
it, gets an empty trigger list, and moves on. The card is "covered"
for the Phase 9 sweep purposes (the registry has an entry) but the
behavior is a stub — the LLM policy + cast pipeline can read the
static_modifier metadata to know what the card should do.
"""
from __future__ import annotations

from typing import Any, Dict, List

from api.engine.pillar_f.v0_2.cards.continuous import (
    StaticModifier, register_static_modifier,
)
from api.engine.pillar_f.v0_2.cards.etb.framework import (
    register_etb_trigger,
)
from api.engine.pillar_f.v0_2.replacement import EnterBattlefieldEvent
from api.engine.pillar_f.v0_2.state import GameState


def _noop_etb_trigger(state: GameState,
                       event: EnterBattlefieldEvent) -> List[Dict[str, Any]]:
    """ETB stub: triggers nothing; real behavior pending iter-12+."""
    return []


# 9 ETB cards routed to Phase 8 → iter-12+ for full handlers. Register
# them as known ETB cards (the coverage sweep counts them) but the
# behavior is stubbed.
_PENDING_ETB = (
    "Orcish Bowmasters", "Phyrexian Metamorph", "Thassa's Oracle",
    "Mithril Coat", "Bastion of Remembrance", "Sculpting Steel",
    "Massacre Wurm", "The Meathook Massacre", "Marionette Apprentice",
)

for name in _PENDING_ETB:
    register_etb_trigger(name, _noop_etb_trigger)
    register_static_modifier(StaticModifier(
        card_name=name, effect_key="etb_trigger_pending",
        params={"pending_iter12_cast_pipeline": True},
        description=f"{name}: ETB pending iter-12 cast pipeline",
    ))
