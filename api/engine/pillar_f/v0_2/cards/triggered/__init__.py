"""Phase 6 — Triggered abilities (non-ETB, non-LTB).

Whenever-creature-attacks (Sanctum Seeker, Edgar Markov), upkeep
triggers (Phyrexian Arena, Necropotence), end-of-turn triggers,
opponent-casts triggers (Mindbreak Trap-style, Notion Thief), draw
triggers (Rhystic Study), and similar.

Per-card modules register the trigger via a thin event-stream check
(the substrate exposes `register_step_trigger(step, trigger)` for
phase/step triggers; per-event triggers like "whenever a creature
attacks" hook into the combat phase's `declare_attackers` callback
surface).

APNAP ordering: when multi-card events fire multiple triggers (e.g.,
5 vampires attack while Edgar Markov is in play + a Cordial Vampire is
in play → 5 attack triggers, 5 +1/+1 distributions, 1 attack-counter
trigger per Cordial), the substrate's `drain_triggers_to_stack` enqueues
them in APNAP order. Per-card handlers should not assume ordering
within same-controller batches — those are the controller's choice.
"""
from api.engine.pillar_f.v0_2.cards.triggered import framework  # noqa: F401
from api.engine.pillar_f.v0_2.cards.triggered.framework import (
    EventTriggerFn, register_event_trigger, get_event_trigger,
    fire_event_triggers, register_step_trigger_for_card,
    get_step_triggers_for_card,
    SpellCastEvent, CombatDamageDealtEvent,
    AttackDeclaredEvent, LandTappedForManaEvent,
)

# Per-card modules.
from api.engine.pillar_f.v0_2.cards.triggered import death_triggers  # noqa: F401
from api.engine.pillar_f.v0_2.cards.triggered import spell_cast_triggers  # noqa: F401
from api.engine.pillar_f.v0_2.cards.triggered import upkeep_triggers  # noqa: F401

__all__ = [
    "EventTriggerFn", "register_event_trigger", "get_event_trigger",
    "fire_event_triggers", "register_step_trigger_for_card",
    "get_step_triggers_for_card",
    "SpellCastEvent", "CombatDamageDealtEvent",
    "AttackDeclaredEvent", "LandTappedForManaEvent",
]
