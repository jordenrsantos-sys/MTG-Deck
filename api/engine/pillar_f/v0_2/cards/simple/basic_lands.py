"""Phase 1 — Basic lands (Plains, Island, Swamp, Mountain, Forest, Wastes
+ snow basics).

Each basic land registers a `{T}: Add {<color>}` activated ability via
the substrate's `register_resolver` API. The resolver simply adds one
mana of the corresponding color to the controller's mana pool.

Iter-10 substrate plumbing: cost-payment (tap the land) is the caller's
responsibility per Phase 3 of the v9 ship. The resolver here is the
EFFECT side only — when the tap-mana ability resolves, this is what
runs.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from api.engine.pillar_f.v0_2.state import GameState, StackEntry
from api.engine.pillar_f.v0_2.stack import register_resolver


def _add_mana_resolver(color: str):
    """Build a closure that adds one mana of `color` to the controller's
    pool when the basic-land tap ability resolves. Color is a single
    letter: W/U/B/R/G/C (colorless)."""
    def fn(state: GameState, entry: StackEntry) -> Optional[List[Dict[str, Any]]]:
        pid = entry.controller
        if not (0 <= pid < len(state.players)):
            return None
        pool = state.players[pid].mana_pool
        # ManaPool exposes per-color int fields W/U/B/R/G/C directly
        # (no .add helper in iter-10 — that's iter-11+ scope per the
        # v9 ship report).
        current = getattr(pool, color, 0)
        setattr(pool, color, current + 1)
        return None
    return fn


# Resolver naming: `basic_tap_<color>` is a stable key; the per-land
# stack entry references it via payment["resolver"].
register_resolver("basic_tap_W", _add_mana_resolver("W"))
register_resolver("basic_tap_U", _add_mana_resolver("U"))
register_resolver("basic_tap_B", _add_mana_resolver("B"))
register_resolver("basic_tap_R", _add_mana_resolver("R"))
register_resolver("basic_tap_G", _add_mana_resolver("G"))
register_resolver("basic_tap_C", _add_mana_resolver("C"))


# Mapping that the higher-level "cast and resolve" plumbing uses to
# look up the resolver name for a given basic land by oracle name.
BASIC_LAND_RESOLVERS: Dict[str, str] = {
    "Plains": "basic_tap_W",
    "Island": "basic_tap_U",
    "Swamp": "basic_tap_B",
    "Mountain": "basic_tap_R",
    "Forest": "basic_tap_G",
    "Wastes": "basic_tap_C",
    "Snow-Covered Plains": "basic_tap_W",
    "Snow-Covered Island": "basic_tap_U",
    "Snow-Covered Swamp": "basic_tap_B",
    "Snow-Covered Mountain": "basic_tap_R",
    "Snow-Covered Forest": "basic_tap_G",
}
