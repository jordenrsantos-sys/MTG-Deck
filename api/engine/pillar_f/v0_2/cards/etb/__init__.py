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
# Phase 2 modules wire in as cards are added. Empty initially —
# populated by the Phase 2 commit.
