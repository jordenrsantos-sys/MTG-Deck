"""Phase 5 — Replacement effects.

Doubling Season (token + counter doubling), Rest in Peace (graveyard →
exile), Leyline of the Void, Anafenza (opponents' creatures can't go to
graveyards), Hardened Scales (+1/+1 counter +1), Torpor Orb (ETB
triggers don't fire — modeled as a replacement of the trigger event).

Per-card modules register a `replacement_fn` via the substrate's
`register_replacement_fn(name, fn)` API. The on-battlefield wiring (the
ReplacementEffect entry in `state.replacement_effects`) is set up when
the card enters the battlefield.

Self-replacement rule (CR 614.5): each replacement effect applies AT
MOST ONCE per event instance — the substrate's `apply_replacements`
helper tracks per-event applied_ids to enforce this. Per-card handlers
should be idempotent on the event object (calling them twice with the
same event shouldn't double-apply).
"""
from api.engine.pillar_f.v0_2.cards.replacement import framework  # noqa: F401
from api.engine.pillar_f.v0_2.cards.replacement.framework import (
    ReplacementBuilder, register_replacement_builder,
    get_replacement_builder, attach_replacements_for,
    detach_replacements_for, all_registered_card_names,
)

# Per-card module — wires substrate replacement_fns + builders.
from api.engine.pillar_f.v0_2.cards.replacement import cards  # noqa: F401

__all__ = [
    "ReplacementBuilder", "register_replacement_builder",
    "get_replacement_builder", "attach_replacements_for",
    "detach_replacements_for", "all_registered_card_names",
]
