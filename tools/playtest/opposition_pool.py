"""Opposition deck pool for Phase 5b offline playtest framework (stage 5b.2).

Bracket-deterministic opposition selection from the 26-deck Phase 5a corpus.
Per OQ-2 resolution: corpus-only initially; per OQ-7 resolution: 5a.3.2
TappedOut/cEDH-database expansion permanently deferred. Per OQ-5 resolution:
fixed 4-player Commander pod (1 test deck + 3 opposition).

Calibration boundary discipline: this module is calibration-only. It reads
the existing `external_decks_v1.json` corpus pack (which is already
`calibration_only:true` in the manifest) but never writes to runtime packs.
The runtime engine never imports this module.
"""
from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

OPPOSITION_POOL_V1_VERSION = "opposition_pool_v1"

EXTERNAL_DECKS_PACK_DEFAULT = (
    Path(__file__).resolve().parents[2]
    / "api" / "engine" / "data" / "calibration" / "external_decks_v1.json"
)

POD_SIZE_DEFAULT = 3
BRACKET_ORDER = ("B1", "B2", "B3", "B4", "B5")


def load_corpus(corpus_path: Optional[Path] = None) -> Dict[str, Any]:
    """Load the external_decks_v1 corpus pack."""
    path = corpus_path if corpus_path is not None else EXTERNAL_DECKS_PACK_DEFAULT
    return json.loads(path.read_text(encoding="utf-8"))


def _bracket_priority_order(target: str) -> List[str]:
    """Return brackets in fallback priority for opposition selection.

    Primary: target bracket. Fallback ladder spreads outward to nearest
    neighbors (closer brackets preferred over distant ones).
    """
    if target not in BRACKET_ORDER:
        return list(BRACKET_ORDER)
    target_idx = BRACKET_ORDER.index(target)
    out = [target]
    for offset in range(1, len(BRACKET_ORDER)):
        # Prefer one step up first, then one step down (deterministic order).
        if target_idx + offset < len(BRACKET_ORDER):
            out.append(BRACKET_ORDER[target_idx + offset])
        if target_idx - offset >= 0:
            out.append(BRACKET_ORDER[target_idx - offset])
    return out


def _bracket_for_deck(deck: Dict[str, Any]) -> Optional[str]:
    """Resolve a deck's bracket via engine_assigned then self_reported."""
    bracket = deck.get("engine_assigned_bracket")
    if isinstance(bracket, str) and bracket in BRACKET_ORDER:
        return bracket
    self_reported = deck.get("self_reported_bracket")
    if isinstance(self_reported, str) and self_reported in BRACKET_ORDER:
        return self_reported
    return None


def select_opposition(
    *,
    target_bracket: str,
    exclude_deck_id: str,
    corpus: Optional[Dict[str, Any]] = None,
    seed: int = 0,
    pod_size: int = POD_SIZE_DEFAULT,
) -> Dict[str, Any]:
    """Select bracket-deterministic opposition for a 4-player Commander pod.

    Returns a dict with:
      - opposition_deck_ids: list of pod_size deck IDs (sorted-deterministic)
      - target_bracket: the requested target
      - exclude_deck_id: the test deck excluded from selection
      - fallback_used: True iff some opposition came from a non-target bracket
      - fallback_buckets: [(bracket, count), ...] showing where decks came from
      - collision_detected: True iff target bracket has fewer than pod_size+1
        decks (i.e., test deck IS one of the candidates)
      - seed: the seed used for deterministic shuffling

    Per safety #7 (B5 pod-sim collision): when target_bracket has fewer than
    pod_size+1 distinct decks (e.g., B5 has only 3 decks total in the 26-deck
    corpus), opposition pool falls back to nearest-neighbor brackets.
    `collision_detected=True` flags this to the caller for downstream
    analysis.
    """
    if corpus is None:
        corpus = load_corpus()
    decks = corpus.get("decks", [])
    if not isinstance(decks, list):
        decks = []

    by_bracket: Dict[str, List[str]] = {b: [] for b in BRACKET_ORDER}
    target_bracket_total = 0
    for d in decks:
        if not isinstance(d, dict):
            continue
        deck_id = d.get("deck_id")
        if not isinstance(deck_id, str):
            continue
        bracket = _bracket_for_deck(d)
        if bracket is None:
            continue
        if bracket == target_bracket:
            target_bracket_total += 1
        if deck_id == exclude_deck_id:
            continue
        by_bracket[bracket].append(deck_id)

    collision_detected = target_bracket_total <= pod_size

    rng = random.Random(seed)
    selected: List[str] = []
    fallback_buckets: List[Tuple[str, int]] = []

    for bracket in _bracket_priority_order(target_bracket):
        if len(selected) >= pod_size:
            break
        candidates = sorted(by_bracket.get(bracket, []))
        if not candidates:
            continue
        rng.shuffle(candidates)
        take = min(pod_size - len(selected), len(candidates))
        if take > 0:
            selected.extend(candidates[:take])
            fallback_buckets.append((bracket, take))

    fallback_used = (
        len(fallback_buckets) > 1
        or (len(fallback_buckets) == 1 and fallback_buckets[0][0] != target_bracket)
    )

    return {
        "version": OPPOSITION_POOL_V1_VERSION,
        "target_bracket": target_bracket,
        "exclude_deck_id": exclude_deck_id,
        "opposition_deck_ids": selected,
        "fallback_used": fallback_used,
        "fallback_buckets": [list(b) for b in fallback_buckets],
        "collision_detected": collision_detected,
        "seed": seed,
    }


def deck_count_by_bracket(corpus: Optional[Dict[str, Any]] = None) -> Dict[str, int]:
    """Return per-bracket deck counts for the corpus (engine-assigned-aware)."""
    if corpus is None:
        corpus = load_corpus()
    decks = corpus.get("decks", []) or []
    counts: Dict[str, int] = {b: 0 for b in BRACKET_ORDER}
    for d in decks:
        if not isinstance(d, dict):
            continue
        bracket = _bracket_for_deck(d)
        if bracket in counts:
            counts[bracket] += 1
    return counts
