"""Phase 1 — Vanilla creatures with no oracle text (or only printed
keyword abilities like flying/trample/vigilance/lifelink).

These cards need NO per-card handler registration. Their printed
keywords sit in `Card.keywords` and the combat module + layer engine
already honor them. This module is intentionally near-empty — it lists
the vanilla creatures we've identified in the top-500 so the coverage
sweep in Phase 9 can confirm them.

If a "vanilla" card in this list later turns out to have a non-trivial
ability (e.g., a re-read of its oracle text reveals something the
heuristic missed), move it out of this list and into the appropriate
phase's module.
"""
from __future__ import annotations

from typing import List


# Vanilla creatures + plain keyword creatures from the top 500. The set
# is layered as "names known to need no handler beyond default permanent
# behavior + printed keywords".
VANILLA_CREATURES: List[str] = [
    # Add specific names as Phase 1 widens. The top-500 categorization
    # script lists ~30 "simple" creatures; these need no oracle wiring.
    # Listed here so the Phase 9 coverage sweep can verify them.
]
