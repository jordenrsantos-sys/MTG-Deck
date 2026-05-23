"""ETB-trigger framework: per-card ETB ability registration + dispatcher.

The iter-10 substrate provides:
  - `EnterBattlefieldEvent` — the event type fired when a card moves
    to the battlefield (Phase 4 / `replacement/events.py`).
  - `enqueue_triggers(state, triggers, source_event)` — adds trigger
    dicts to `state.delayed_triggers_pending` (Phase 2 / `stack/stack.py`).
  - `drain_triggers_to_stack(state)` — moves pending → stack in APNAP
    order (CR 603.3).
  - `register_resolver(name, fn)` — registers the resolve-fn for an
    ability when it resolves off the stack.

What's NOT in the substrate (and what this module adds):
  - A registry mapping CARD NAME → ETB-trigger-fn(state, etb_event) →
    `List[trigger_dict]`. The substrate doesn't ship per-card matching
    because that's the per-card oracle work (v11's scope).
  - A `fire_etb_triggers(state, etb_event)` helper that scans the
    registry for matching cards on the battlefield + enqueues their
    triggers via the substrate's `enqueue_triggers`.

Per-card modules:
  - Define a resolver function: `def reclamation_sage_resolve(state,
    entry): ...` and register it: `register_resolver(
    "etb_reclamation_sage", reclamation_sage_resolve)`.
  - Define an ETB-trigger function: `def reclamation_sage_etb_trigger(
    state, etb_event): return [{"source_card_id": etb_event.card_id,
    "controller": etb_event.controller, "resolver":
    "etb_reclamation_sage", "targets": [...], "payment": {...},
    "description": "Reclamation Sage ETB"}]` and register the trigger
    via `register_etb_trigger("Reclamation Sage",
    reclamation_sage_etb_trigger)`.

The dispatcher fires once per ETB event: the substrate caller (in
v11+, the per-card "cast and put onto battlefield" code; for now the
fixture suite calls it explicitly) constructs an EnterBattlefieldEvent
and calls `fire_etb_triggers(state, etb_event)` after the card has
actually been moved onto the battlefield.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from api.engine.pillar_f.v0_2.replacement import EnterBattlefieldEvent
from api.engine.pillar_f.v0_2.stack import enqueue_triggers
from api.engine.pillar_f.v0_2.state import GameState


# ETB trigger function signature: (state, etb_event) → list of trigger
# dicts. Each trigger dict matches the `enqueue_triggers` contract:
#   {"source_card_id": str, "controller": int, "resolver": str,
#    "targets": [...], "payment": {...}, "description": str}
ETBTriggerFn = Callable[
    [GameState, EnterBattlefieldEvent],
    List[Dict[str, Any]],
]


_ETB_TRIGGER_REGISTRY: Dict[str, ETBTriggerFn] = {}


def register_etb_trigger(card_name: str, fn: ETBTriggerFn) -> None:
    """Register an ETB-trigger function keyed by card name. When a card
    of this name enters the battlefield, `fire_etb_triggers` looks up
    the function + invokes it to get the trigger payload.

    Idempotent: calling twice with the same name silently overwrites
    (last-registration-wins). Test setups that rebuild state between
    runs benefit from the overwrite semantics; production registration
    happens once at import time.
    """
    _ETB_TRIGGER_REGISTRY[card_name] = fn


def get_etb_trigger(card_name: str) -> Optional[ETBTriggerFn]:
    return _ETB_TRIGGER_REGISTRY.get(card_name)


def clear_etb_registry() -> None:
    """Test-only helper: empty the ETB-trigger registry. Used by test
    setups that need a clean state without per-card pollution."""
    _ETB_TRIGGER_REGISTRY.clear()


def fire_etb_triggers(state: GameState, etb_event: EnterBattlefieldEvent) -> int:
    """Match the ETB event's card against the registry. If a trigger is
    registered for the card's name, invoke its trigger fn to build the
    trigger payload + enqueue via the substrate's `enqueue_triggers`.

    Returns the number of triggers enqueued (0 = no matching registration).

    Multiple cards entering simultaneously (e.g., a mass-token-creation
    effect) should fire ETB triggers per-card by calling this helper
    once per entering permanent in APNAP order — the substrate's
    `drain_triggers_to_stack` then enforces APNAP ordering when moving
    the queued triggers onto the stack.
    """
    card = state.get_card(etb_event.card_id)
    if card is None:
        return 0
    fn = _ETB_TRIGGER_REGISTRY.get(card.name)
    if fn is None:
        return 0
    try:
        triggers = fn(state, etb_event) or []
    except Exception:
        # Iter-10 convention: silently skip per-card handler errors
        # (mirrors the layer engine's error tolerance). Phase 9's
        # coverage sweep counts these as fall-through events.
        return 0
    if not triggers:
        return 0
    enqueue_triggers(
        state, triggers,
        source_event={
            "type": "EnterBattlefieldEvent",
            "card_id": etb_event.card_id,
            "controller": etb_event.controller,
            "from_zone": etb_event.from_zone,
        },
    )
    return len(triggers)


# Convenience constructor: turn a "this card enters the battlefield"
# event into the boilerplate trigger dict skeleton. Per-card modules
# can use this to avoid repeating the dict layout.
def build_etb_trigger_dict(
    etb_event: EnterBattlefieldEvent,
    *,
    resolver: str,
    targets: Optional[List[Any]] = None,
    payment: Optional[Dict[str, Any]] = None,
    description: str = "",
) -> Dict[str, Any]:
    return {
        "source_card_id": etb_event.card_id,
        "controller": etb_event.controller,
        "resolver": resolver,
        "targets": list(targets or []),
        "payment": dict(payment or {}),
        "description": description,
    }
