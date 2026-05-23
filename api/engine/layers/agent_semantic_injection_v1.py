"""agent_semantic_injection_v1 — Iter 7 mega-task v6 Phase 2 (BLOCKING).

Closes iter 6 success criterion #6 (voyage_semantic_avg = 1.4-2.0 vs
target ≥3). Replaces the failed score-boost (iter 5) + explicit prompt
requirement (iter 5/6) approaches.

Why this layer exists
---------------------
Per `feedback_pool_score_does_not_drive_llm_picking` (cowork memory),
the LLM picks from prompt content based on its reasoning, NOT pool
ranking. Reordering the prompt with score boosts didn't lift selection
rate. Adding "MUST SELECT 3 SEMANTIC NEIGHBORS" to the prompt didn't
either when the upstream pool sometimes had 0 semantic neighbors. The
ONLY mechanism that GUARANTEES outcomes is a deterministic post-hoc
layer that operates AFTER the LLM has picked.

Integration: runs at the end of the C2.1/C2.2 + validate_swap chain in
``agent_build_deck_v1.compute_agent_build_deck_v1``, BEFORE the D2
rationale rewrite. D2 then writes rationales for the post-injection
deck composition.

Public API
----------
``inject_semantic_picks(deck, anchor_cards, color_identity, *,
n_target, forbidden_set=None, query_neighbors=None) -> (modified_deck,
swap_log)``

The injected cards get ``source: semantic_injection`` (countable in the
iter 7 sweep metric); displaced cards are recorded in the swap_log so
downstream phases + the build response can surface what changed.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple


SEMANTIC_INJECTION_VERSION = "agent_semantic_injection_v1.0"

SOURCE_TAG = "semantic_injection"

# Bracket-aware injection targets. B5 cEDH gets the strongest injection
# (semantic neighbors of tempo/combo anchors land more tech). B2 casual
# stays gentle (the user picked a casual bracket; don't trample intent).
_DEFAULT_N_TARGETS: Dict[str, int] = {
    "B1": 2,
    "B2": 2,
    "B3": 3,
    "B4": 3,
    "B5": 4,
}

# Voyage query top-k per anchor — wider net so color-identity filtering
# still leaves us with enough candidates after de-duplication.
_NEIGHBORS_PER_ANCHOR = 30

# Substrings on a card's `source` field that mark it as a "low-priority
# C2.2 wild discovery pick" (eligible for swap-out). C2.1 picks, mana
# base, must-includes, commander, and archetype staples are PROTECTED
# (we don't swap user-intent / structural cards).
_SWAPPABLE_SOURCE_SUBSTRINGS: Tuple[str, ...] = (
    "C2_2_wild_combo_discovery_added",
    "wild_combo_discovery",
    # NOTE: do NOT include "creative_outlier" here on its own — those are
    # also often anchors. We rely on the anchor list passed in to filter
    # what we swap OUT.
)

# Sources we must never swap out, regardless of other tags on the card.
# Composite sources are split on "|" so any one of these substrings is
# treated as protected.
_PROTECTED_SOURCE_SUBSTRINGS: Tuple[str, ...] = (
    "user_intent",
    "mana_base",
    "C2_1_candidate_critic",
    "archetype_staple",
)


def resolve_n_target(bracket: str) -> int:
    """Bracket → target semantic-injection count (kickoff Phase 2 spec)."""
    if not bracket:
        return _DEFAULT_N_TARGETS["B3"]
    return _DEFAULT_N_TARGETS.get(bracket.upper(), _DEFAULT_N_TARGETS["B3"])


def _is_semantic_card(card: Dict[str, Any]) -> bool:
    """True if the card was already counted as a semantic-neighbor pick.

    Matches both the iter-6 ``from_semantic_neighbor`` upstream tag and
    this module's own ``semantic_injection`` tag. Iter 7 sweep counter
    can use the same predicate for the metric.
    """
    src = (card.get("source") or "")
    if SOURCE_TAG in src:
        return True
    if "from_semantic_neighbor" in src:
        return True
    if "semantic_neighbor" in src:
        return True
    return False


def _is_protected_card(card: Dict[str, Any], anchor_names: Set[str]) -> bool:
    """A card we must NEVER swap out."""
    name = (card.get("card_name") or "").strip().lower()
    if name in anchor_names:
        return True
    src = (card.get("source") or "")
    for tag in _PROTECTED_SOURCE_SUBSTRINGS:
        if tag in src:
            return True
    return False


def _is_swappable_wild_pick(card: Dict[str, Any], anchor_names: Set[str]) -> bool:
    """A card we are allowed to swap OUT to make room for a semantic neighbor.

    Strict: must be a C2.2 wild-discovery pick AND not on the protected
    list AND not in the anchor set.
    """
    if _is_protected_card(card, anchor_names):
        return False
    src = (card.get("source") or "")
    return any(tag in src for tag in _SWAPPABLE_SOURCE_SUBSTRINGS)


def _is_basic_land(card: Dict[str, Any]) -> bool:
    name = (card.get("card_name") or "").strip()
    return name in {
        "Plains", "Island", "Swamp", "Mountain", "Forest", "Wastes",
        "Snow-Covered Plains", "Snow-Covered Island", "Snow-Covered Swamp",
        "Snow-Covered Mountain", "Snow-Covered Forest", "Snow-Covered Wastes",
    }


def inject_semantic_picks(
    deck: List[Dict[str, Any]],
    anchor_cards: Sequence[str],
    color_identity: Sequence[str],
    *,
    n_target: int,
    forbidden_set: Optional[Iterable[str]] = None,
    query_neighbors: Optional[Callable[..., List[Dict[str, Any]]]] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Post-hoc semantic-neighbor injection.

    Args:
        deck: current deck (list of {card_name, reason, source, ...}).
        anchor_cards: card names to query Voyage for (commander +
            must-includes + creative outliers from C2.1/C2.2).
        color_identity: e.g. ["B", "R", "W"] for Edgar Markov.
        n_target: target count of semantic-injection cards in the final
            deck. Cards already tagged ``from_semantic_neighbor`` count
            toward this target.
        forbidden_set: card names that must not be added (e.g. iter-3
            anchor guard's forbidden cards).
        query_neighbors: callable equivalent to
            ``agent_semantic_retrieval_v1.query_neighbors``. Defaults to
            the real implementation when ``None`` (mock-friendly for
            tests / offline operation).

    Returns:
        ``(modified_deck, swap_log)``. ``swap_log`` is a list of dicts:
        ``{"removed": "<old card>", "added": "<new card>",
        "anchor": "<source anchor>", "similarity": <float>}``.

    Graceful fallback: if no neighbors are available (Voyage offline,
    empty index, all neighbors already in deck), returns the unmodified
    deck and an empty swap log. Never raises.
    """
    if query_neighbors is None:
        try:
            from api.engine.layers.agent_semantic_retrieval_v1 import (
                query_neighbors as _real_query,
            )

            query_neighbors = _real_query
        except Exception:
            return list(deck), []

    if not deck or n_target <= 0:
        return list(deck), []

    forbidden_names: Set[str] = {
        (n or "").strip().lower() for n in (forbidden_set or []) if n
    }
    anchor_names: Set[str] = {
        (n or "").strip().lower() for n in anchor_cards if n
    }

    # Count semantic cards already in the deck.
    existing_semantic = sum(1 for c in deck if _is_semantic_card(c))
    need = max(0, n_target - existing_semantic)
    if need == 0:
        return list(deck), []

    # In-deck card names — never inject a duplicate.
    in_deck = {(c.get("card_name") or "").strip().lower() for c in deck}

    # Collect candidate neighbors from each anchor.
    seen_candidates: Set[str] = set()
    candidates: List[Dict[str, Any]] = []
    for anchor in anchor_cards:
        if not anchor:
            continue
        try:
            neighbors = query_neighbors(
                anchor,
                k=_NEIGHBORS_PER_ANCHOR,
                color_identity_filter=list(color_identity) if color_identity else None,
            )
        except Exception:
            neighbors = []
        for nb in neighbors:
            name = (nb.get("name") or "").strip()
            key = name.lower()
            if not name or key in seen_candidates:
                continue
            if key in in_deck or key in anchor_names or key in forbidden_names:
                continue
            seen_candidates.add(key)
            candidates.append({
                "name": name,
                "similarity": float(nb.get("similarity") or 0.0),
                "anchor": anchor,
            })

    if not candidates:
        return list(deck), []

    # Highest-similarity candidates first.
    candidates.sort(key=lambda c: -c["similarity"])

    # Identify swap-out targets: ordered worst-priority first (current
    # heuristic = highest deck index among swappable wild picks, since
    # later iter-3 phase additions append to the end).
    swappable_indices: List[int] = [
        i for i, c in enumerate(deck) if _is_swappable_wild_pick(c, anchor_names)
    ]
    # Don't swap basic lands even if somehow tagged otherwise.
    swappable_indices = [i for i in swappable_indices if not _is_basic_land(deck[i])]
    swappable_indices.sort(reverse=True)

    if not swappable_indices:
        return list(deck), []

    new_deck = list(deck)
    swap_log: List[Dict[str, Any]] = []
    candidate_iter = iter(candidates)

    for _ in range(min(need, len(swappable_indices))):
        try:
            cand = next(candidate_iter)
        except StopIteration:
            break
        swap_idx = swappable_indices.pop(0)
        removed_card = new_deck[swap_idx]
        new_deck[swap_idx] = {
            "card_name": cand["name"],
            "reason": (
                f"[slot=semantic_injection] Voyage neighbor of "
                f"{cand['anchor']} (similarity={cand['similarity']:.3f}); "
                f"displaces low-priority C2.2 wild pick "
                f"\"{(removed_card.get('card_name') or '').strip()}\"."
            ),
            "source": SOURCE_TAG,
        }
        swap_log.append({
            "removed": (removed_card.get("card_name") or "").strip(),
            "added": cand["name"],
            "anchor": cand["anchor"],
            "similarity": cand["similarity"],
        })

    return new_deck, swap_log
