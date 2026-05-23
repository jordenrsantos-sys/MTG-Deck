"""Phase 2 — Enter-the-battlefield triggers.

Cards whose headline behavior is an ETB triggered ability — the trigger
puts a triggered-ability StackEntry on the stack at the moment the
permanent enters the battlefield. Registration uses a thin wrapper
around the substrate's `register_resolver` API: each card's ETB hooks a
named resolver, and the per-card module also exposes a
`build_etb_trigger(...)` helper that the test fixtures and the
higher-level "cast and resolve" plumbing use to enqueue the trigger.

Iter-10 substrate exposes `enqueue_triggers(state, [{...}])` +
`drain_triggers_to_stack(state)`; each ETB handler in this package
follows the same trigger-dict shape: `{"source_card_id": ..., "controller":
..., "resolver": "etb_<card_slug>", "payment": {...}, "description":
"<Card name> ETB"}`. The resolver fn is registered via the substrate's
`register_resolver`.
"""
# Framework first — the per-card modules depend on it.
from api.engine.pillar_f.v0_2.cards.etb import framework  # noqa: F401
from api.engine.pillar_f.v0_2.cards.etb.framework import (
    register_etb_trigger, get_etb_trigger, fire_etb_triggers,
    build_etb_trigger_dict,
)

# Per-card modules — importing triggers their registration calls.
from api.engine.pillar_f.v0_2.cards.etb import creatures  # noqa: F401
from api.engine.pillar_f.v0_2.cards.etb import lands_and_enchantments  # noqa: F401

__all__ = [
    "register_etb_trigger", "get_etb_trigger", "fire_etb_triggers",
    "build_etb_trigger_dict",
]
