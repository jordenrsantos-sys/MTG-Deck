"""Phase 7 — Instants + sorceries (one-shot spell resolution).

Counterspell, Path to Exile, Cyclonic Rift, Swords to Plowshares, Doom
Blade, Mana Drain, Demonic Tutor, Ad Nauseam, Thassa's Oracle (ETB on
a creature but acts as a spell-style finisher — its spell-resolve win
check is registered here for clarity), and similar one-shot effects.

Registration model: each spell registers a `spell_resolve_fn` via the
substrate's `register_resolver(name, fn)` API. The casting side
(target legality, cost payment, alternative-cost flags) is caller-
controlled in iter-10; per-card resolvers assume targets + payment
have already been validated.
"""
from api.engine.pillar_f.v0_2.cards.spell import framework  # noqa: F401
from api.engine.pillar_f.v0_2.cards.spell.framework import (
    SpellResolveFn, register_spell, get_spell_resolver_key,
    all_registered_spell_names, build_spell_payload,
)

# Per-card modules.
from api.engine.pillar_f.v0_2.cards.spell import removal  # noqa: F401
from api.engine.pillar_f.v0_2.cards.spell import counterspells  # noqa: F401
from api.engine.pillar_f.v0_2.cards.spell import ramp_and_draw  # noqa: F401
from api.engine.pillar_f.v0_2.cards.spell import mass_removal_and_protection  # noqa: F401
from api.engine.pillar_f.v0_2.cards.spell import bulk_stubs  # noqa: F401

__all__ = [
    "SpellResolveFn", "register_spell", "get_spell_resolver_key",
    "all_registered_spell_names", "build_spell_payload",
]
