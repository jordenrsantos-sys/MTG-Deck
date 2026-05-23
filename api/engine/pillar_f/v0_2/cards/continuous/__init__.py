"""Phase 4 — Continuous / static effects (layer 6 + layer 7).

Anthems (Honor of the Pure, Glorious Anthem, Crusade), lords
(Captivating Vampire, Lord of the Undead — also layer 6 + 7c), keyword
grants (Cathars' Crusade), ability strippers (Humility, Linvala),
characteristic-defining abilities (Tarmogoyf, Mortivore).

Per-card modules register a `ContinuousEffect` via the substrate's
`register_layer_effect(name, fn)` API and attach the effect to
`state.continuous_effects` at the moment the source card enters the
battlefield. The substrate's `apply_continuous_effects(state)` walks
layers 1→7 in canonical order; per-card effects only need to specify
the layer + sublayer + target predicate + apply function.

Layer mapping reminder (CR 613):
  4  — type/subtype changes
  5  — color changes
  6  — abilities (add/remove keywords, Humility-style ability stripping)
  7a — base P/T setting
  7b — CDA (Tarmogoyf-style state-driven P/T)
  7c — P/T modifications (anthems +1/+1)
  7d — P/T switches (Inverter of Truth)
"""
# Phase 4 modules wire in as cards are added.
