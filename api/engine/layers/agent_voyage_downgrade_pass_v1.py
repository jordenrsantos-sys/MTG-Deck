"""
agent_voyage_downgrade_pass_v1 — Mega-task v4 Phase 10.

For each anchor card, query Voyage for semantic neighbors, filter to
those with `cmc < anchor.cmc` (cheaper to cast) AND color identity
subset, and surface as "cheaper alternatives" suggestions in the
build response. Used for B4/B5 cEDH builds and storm/combo/tempo
themes where mana cost is a load-bearing constraint.

Public API:
  - `find_cheaper_alternatives(anchor_name, anchor_cmc, color_identity,
                                k=10) -> list[dict]`
  - `should_run_downgrade_pass(bracket, theme_profile) -> bool`

Returns suggestions; does NOT auto-swap. User reviews and decides.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


VOYAGE_DOWNGRADE_PASS_VERSION = "agent_voyage_downgrade_pass_v1.0"

# Themes where cheap-to-cast alternatives are particularly valuable.
# Combo + storm + tempo + ninja all care about CMC-per-mechanic for
# tempo / threat density / kill turn timing.
_DOWNGRADE_RELEVANT_THEMES = {
    "combo", "storm", "storm_combo", "ninja_tempo", "voltron",
    "reanimator",
}


def should_run_downgrade_pass(
    bracket: str,
    theme_profile: Optional[Dict[str, Any]] = None,
) -> bool:
    """Heuristic gate: run the downgrade pass for high-bracket builds or
    when the theme_profile signals a CMC-conscious archetype.

    Returns True for:
      - bracket B4 or B5
      - any theme_profile slot weight > 0.2 in a downgrade-relevant theme
    """
    if bracket in ("B4", "B5"):
        return True
    if isinstance(theme_profile, dict):
        for slot in ("primary", "secondary", "tertiary"):
            entry = theme_profile.get(slot)
            if isinstance(entry, dict):
                theme = (entry.get("theme") or "").strip().lower()
                try:
                    w = float(entry.get("weight") or 0.0)
                except (TypeError, ValueError):
                    w = 0.0
                if theme in _DOWNGRADE_RELEVANT_THEMES and w > 0.2:
                    return True
    return False


def find_cheaper_alternatives(
    anchor_name: str,
    anchor_cmc: Optional[float],
    color_identity: Optional[List[str]] = None,
    k: int = 10,
) -> List[Dict[str, Any]]:
    """Query Voyage for semantic neighbors of `anchor_name`, filter to
    those with cmc strictly less than `anchor_cmc` AND color identity
    subset.

    Returns: list of `{name, cmc, color_identity, similarity, savings}`
    sorted by descending similarity. `savings = anchor_cmc - candidate.cmc`.
    Empty list when:
      - anchor_cmc is None / 0 (nothing to downgrade)
      - Voyage isn't available
      - no qualifying alternatives exist
    """
    if not anchor_name or not anchor_cmc or anchor_cmc <= 0:
        return []

    try:
        from api.engine.layers.agent_semantic_retrieval_v1 import (
            is_available, query_neighbors,
        )
    except ImportError:
        return []
    if not is_available():
        return []

    candidates = query_neighbors(
        anchor_name, k=max(k * 3, 30),
        color_identity_filter=color_identity,
    ) or []

    out: List[Dict[str, Any]] = []
    for c in candidates:
        cmc = c.get("cmc")
        if cmc is None or float(cmc) >= float(anchor_cmc):
            continue
        out.append({
            "name": c.get("name", ""),
            "cmc": float(cmc),
            "color_identity": c.get("color_identity") or [],
            "similarity": float(c.get("similarity") or 0.0),
            "savings": float(anchor_cmc) - float(cmc),
        })
        if len(out) >= k:
            break

    out.sort(key=lambda d: (-d["similarity"], d["cmc"]))
    return out


def run_downgrade_pass_for_deck(
    anchor_names: List[str],
    deck_cards_with_cmc: Dict[str, float],
    color_identity: Optional[List[str]] = None,
    k_per_anchor: int = 5,
) -> List[Dict[str, Any]]:
    """Run the downgrade pass for a list of anchor cards (typically
    must-includes + key staples). Returns `[{anchor, alternatives: [...]}]`
    for caller to surface as build-response suggestions.
    """
    results: List[Dict[str, Any]] = []
    for anchor in anchor_names:
        cmc = deck_cards_with_cmc.get(anchor)
        if cmc is None:
            continue
        alternatives = find_cheaper_alternatives(
            anchor, cmc, color_identity, k=k_per_anchor,
        )
        if alternatives:
            results.append({"anchor": anchor, "anchor_cmc": cmc,
                            "alternatives": alternatives})
    return results
