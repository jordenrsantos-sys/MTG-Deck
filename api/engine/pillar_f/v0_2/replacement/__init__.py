"""Replacement effects + state-based actions. Phase 4 of mega-task v9."""
from api.engine.pillar_f.v0_2.replacement.events import (
    Event, DrawEvent, DamageEvent, EnterBattlefieldEvent,
    DieEvent, LifeChangeEvent, CounterAddEvent, CounterRemoveEvent,
    DiscardEvent, MillEvent, EVENT_TYPES,
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
    # replacement engine
    "REPLACEMENT_VERSION", "ReplacementFn",
    "register_replacement_fn", "get_replacement_fn",
    "apply_replacements",
    # state-based actions
    "SBA_VERSION", "COMMANDER_DAMAGE_LETHAL",
    "check_state_based_actions", "run_sba_loop",
]
