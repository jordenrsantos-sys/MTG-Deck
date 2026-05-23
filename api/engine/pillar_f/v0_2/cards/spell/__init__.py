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
# Phase 7 modules wire in as cards are added.
