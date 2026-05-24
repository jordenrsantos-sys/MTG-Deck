"""Replacement effects + state-based actions. Phase 4 of mega-task v9.

v14 (2026-05-24) added 3 substrate event types: TokenCreateEvent,
LibrarySearchEvent, CombatDamageDealtEvent. The last is a promotion
of v11's `cards/triggered/framework.py` shim to the substrate so
combat code can emit it directly; the v11 shim re-exports this
dataclass for backwards compat.
"""
from api.engine.pillar_f.v0_2.replacement.events import (
    Event, DrawEvent, DamageEvent, EnterBattlefieldEvent,
    DieEvent, LifeChangeEvent, CounterAddEvent, CounterRemoveEvent,
    DiscardEvent, MillEvent, EVENT_TYPES,
    # v14 additions:
    TokenCreateEvent, LibrarySearchEvent, CombatDamageDealtEvent,
)
from api.engine.pillar_f.v0_2.replacement.replacement import (
    REPLACEMENT_VERSION, ReplacementFn,
    register_replacement_fn, get_replacement_fn,
    apply_replacements,
)
from api.engine.pillar_f.v0_2.replacement.sba import (
    SBA_VERSION, COMMANDER_DAMAGE_LETHAL,
    check_state_based_actions, run_sba_loop,
)

__all__ = [
    # events
    "Event", "DrawEvent", "DamageEvent", "EnterBattlefieldEvent",
    "DieEvent", "LifeChangeEvent", "CounterAddEvent", "CounterRemoveEvent",
    "DiscardEvent", "MillEvent", "EVENT_TYPES",
    # v14 events
    "TokenCreateEvent", "LibrarySearchEvent", "CombatDamageDealtEvent",
    # replacement engine
    "REPLACEMENT_VERSION", "ReplacementFn",
    "register_replacement_fn", "get_replacement_fn",
    "apply_replacements",
    # state-based actions
    "SBA_VERSION", "COMMANDER_DAMAGE_LETHAL",
    "check_state_based_actions", "run_sba_loop",
]
