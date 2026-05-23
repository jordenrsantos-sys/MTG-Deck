"""Phase 1 — Mana dorks (1-drop creatures that tap for one mana).

Llanowar Elves, Elvish Mystic, Birds of Paradise, Noble Hierarch,
Avacyn's Pilgrim. Each registers a `{T}: Add {<color>}` activated
ability. Birds of Paradise and Noble Hierarch produce any color (any-
color stub uses the generic "add to pool of caller's choice" pattern;
iter-10 collapses to colorless C since the choice plumbing is iter-11+
scope per the v9 ship).
"""
from __future__ import annotations

from typing import Dict

from api.engine.pillar_f.v0_2.cards.simple.basic_lands import _add_mana_resolver
from api.engine.pillar_f.v0_2.stack import register_resolver


# Reuse the basic-land resolver closures for single-color dorks: Llanowar
# Elves and Elvish Mystic just produce {G}, identical behavior to a
# Forest's tap-mana. The resolver is keyed by ability rather than by
# card so the same resolver serves multiple cards.
register_resolver("mana_dork_tap_W", _add_mana_resolver("W"))
register_resolver("mana_dork_tap_U", _add_mana_resolver("U"))
register_resolver("mana_dork_tap_B", _add_mana_resolver("B"))
register_resolver("mana_dork_tap_R", _add_mana_resolver("R"))
register_resolver("mana_dork_tap_G", _add_mana_resolver("G"))


# Any-color dorks (Birds of Paradise, Noble Hierarch's exalted side,
# Bloom Tender). Iter-10 stub: the caller picks via payment["color"];
# if not specified, default to C colorless. Iter-11+ will plumb the
# LLM-driven color choice.
def _any_color_tap_resolver(state, entry):
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    pool = state.players[pid].mana_pool
    color = entry.payment.get("color", "C")
    if color not in ("W", "U", "B", "R", "G", "C"):
        color = "C"
    setattr(pool, color, getattr(pool, color, 0) + 1)
    return None


register_resolver("mana_dork_tap_any", _any_color_tap_resolver)


# Mapping by oracle name → resolver key. Used by the per-card test
# helpers + the eventual "cast and resolve" plumbing.
MANA_DORK_RESOLVERS: Dict[str, str] = {
    "Llanowar Elves": "mana_dork_tap_G",
    "Elvish Mystic": "mana_dork_tap_G",
    "Fyndhorn Elves": "mana_dork_tap_G",
    "Arbor Elf": "mana_dork_tap_G",
    "Birds of Paradise": "mana_dork_tap_any",
    "Noble Hierarch": "mana_dork_tap_any",
    "Bloom Tender": "mana_dork_tap_any",
    "Avacyn's Pilgrim": "mana_dork_tap_W",
}
