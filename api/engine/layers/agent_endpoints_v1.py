"""
agent_endpoints_v1 — Pillar A.5 convenience endpoints layer.

Composes the four foundation Pillar A endpoints (analyze, candidate_pool,
strength_check) plus corpus lookups into AI-ergonomic shapes:

  - /agent/context_bundle_v1 — one-round-trip composite for agent kickoff.
  - /commander/archetype_brief_v1 — given a commander, return common
    archetypes from the corpus + theme distribution + staple cards.
  - /theme/top_cards_v1 — given a theme, return cards heavily tagged
    with that theme's primitives.
  - /corpus/similar_decks_v1 — surface the corpus deck list itself
    by similarity to a partial deck (alias of strength_check's nearest_neighbors
    but with the full corpus decklists embedded for AI consumption).

Architectural rules served:
  - 1.2 Speed budget: <1000ms for context_bundle (composes 3-4 inner
    calls); <300ms warm for the others.
  - 1.1 Creativity envelope: no top-N narrowing on any output.
"""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional


AGENT_BUNDLE_VERSION = "agent_context_bundle_v1.0"
ARCHETYPE_BRIEF_VERSION = "archetype_brief_v1.0"
THEME_TOP_CARDS_VERSION = "theme_top_cards_v1.0"
CORPUS_SIMILAR_VERSION = "corpus_similar_decks_v1.0"


# ============================================================
# /agent/context_bundle_v1
# ============================================================


def compute_agent_context_bundle_v1(
    *,
    db_snapshot_id: str,
    commander: Optional[str],
    raw_decklist_text: str,
    intent: Optional[str] = None,
    include: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Compose analyze + candidate_pool + strength_check + reference_decks
    into a single response. AI agents call this once at kickoff and get
    everything needed for a build/improve session.

    Budget: <1000ms warm (sum of inner calls + small overhead).
    """
    from api.engine.layers.deck_analyze_v1 import compute_deck_analyze_v1
    from api.engine.layers.deck_candidate_pool_v1 import compute_candidate_pool_v1
    from api.engine.layers.deck_strength_check_v1 import compute_deck_strength_check_v1

    inc = set(include or ["analyze", "candidate_pool", "strength_check", "reference_decks"])
    warnings: List[Dict[str, str]] = []
    bundle: Dict[str, Any] = {
        "version": AGENT_BUNDLE_VERSION,
        "db_snapshot_id": db_snapshot_id,
        "commander": commander,
        "intent": intent,
        "analyze": None,
        "candidate_pool": None,
        "strength_check": None,
        "reference_decks": None,
        "warnings": warnings,
    }

    if "analyze" in inc:
        try:
            bundle["analyze"] = compute_deck_analyze_v1(
                db_snapshot_id=db_snapshot_id,
                commander=commander,
                raw_decklist_text=raw_decklist_text,
                include_debug=False,
            )
        except Exception as exc:
            warnings.append({"code": "BUNDLE_ANALYZE_FAILED", "message": str(exc)})

    if "candidate_pool" in inc:
        try:
            bundle["candidate_pool"] = compute_candidate_pool_v1(
                db_snapshot_id=db_snapshot_id,
                commander=commander,
                raw_decklist_text=raw_decklist_text,
                intent=intent,
            )
        except Exception as exc:
            warnings.append({"code": "BUNDLE_CANDIDATE_POOL_FAILED", "message": str(exc)})

    if "strength_check" in inc:
        try:
            bundle["strength_check"] = compute_deck_strength_check_v1(
                db_snapshot_id=db_snapshot_id,
                commander=commander,
                raw_decklist_text=raw_decklist_text,
            )
        except Exception as exc:
            warnings.append({"code": "BUNDLE_STRENGTH_CHECK_FAILED", "message": str(exc)})

    if "reference_decks" in inc:
        try:
            bundle["reference_decks"] = compute_corpus_similar_decks_v1(
                db_snapshot_id=db_snapshot_id,
                commander=commander,
                raw_decklist_text=raw_decklist_text,
                k=5,
                include_decklists=True,
            )
        except Exception as exc:
            warnings.append({"code": "BUNDLE_REFERENCE_DECKS_FAILED", "message": str(exc)})

    return bundle


# ============================================================
# /commander/archetype_brief_v1
# ============================================================


def compute_archetype_brief_v1(
    *,
    db_snapshot_id: str,
    commander: str,
) -> Dict[str, Any]:
    """Return common archetypes + theme distribution + staple cards for a commander.

    v1.0 derives from the corpus. When corpus has few entries for the commander,
    the response will reflect that with `corpus_deck_count` and warnings.

    Budget: <300ms warm.
    """
    # Import as module so we read live _CORPUS_RAW (the lazy-load mutates it
    # post-import; `from X import _CORPUS_RAW` would bind the empty dict).
    from api.engine.layers import deck_strength_check_v1 as _sc
    warnings: List[Dict[str, str]] = []

    _sc._load_corpus()
    # Filter corpus to entries with matching commander
    matching = [
        e for e in _sc._CORPUS_RAW.get("decks", [])
        if isinstance(e, dict) and e.get("commander", "").lower() == (commander or "").lower()
    ]

    if not matching:
        warnings.append({
            "code": "NO_CORPUS_ENTRIES_FOR_COMMANDER",
            "message": f"Corpus has 0 entries for {commander}. Returning derivable shell only.",
        })

    # Aggregate archetypes
    archetype_counter: Counter = Counter()
    bracket_counter: Counter = Counter()
    card_usage: Counter = Counter()
    total_decks = len(matching)

    # NOTE: previous revisions called `_sc._ensure_vectors(db_snapshot_id)` here,
    # binding the result to a `vectors` variable that was never read by any
    # subsequent code in this function. The aggregation below iterates
    # `matching` directly from `_CORPUS_RAW`. The vectorize pass is a side
    # effect (it populates `_CORPUS_VECTORS` for downstream strength_check
    # callers) but executing it eagerly here cost ~10 minutes per cold call
    # against the full 13K-deck corpus + 30K-card snapshot — which made
    # Pillar D Phase F's validation sweep unable to run end-to-end. The
    # vectorize cache is still warmed on demand by deck_strength_check_v1
    # itself when its cosine-similarity computation needs it, so removing
    # the eager call here is observably equivalent to "archetype_brief is
    # the same; first strength_check is slower; subsequent calls hit cache."

    for entry in matching:
        ar = entry.get("archetype") or "Unknown"
        br = entry.get("bracket") or "Unknown"
        archetype_counter[ar] += 1
        bracket_counter[br] += 1
        # Count unique-cards-per-deck so usage_pct is fraction of decks
        # including the card, not total copies (a deck with 35 Mountains
        # should contribute Mountain=1 toward staple usage, not 35).
        seen_in_deck: set = set()
        for card in entry.get("decklist", []) or []:
            if isinstance(card, str) and card.strip():
                norm = card.strip()
                if norm not in seen_in_deck:
                    seen_in_deck.add(norm)
                    card_usage[norm] += 1

    # Commander oracle id (try)
    commander_oracle_id: Optional[str] = None
    try:
        from engine.db import find_card_by_name as _find
        card = _find(db_snapshot_id, commander)
        if isinstance(card, dict):
            commander_oracle_id = card.get("oracle_id")
    except Exception:
        pass

    # Color identity
    color_identity: List[str] = []
    if commander_oracle_id:
        try:
            from engine.db import find_card_by_name as _find2
            card2 = _find2(db_snapshot_id, commander)
            ci = (card2 or {}).get("color_identity")
            if isinstance(ci, list):
                color_identity = sorted(set(c.upper() for c in ci if isinstance(c, str)))
            elif isinstance(ci, str):
                try:
                    parsed = json.loads(ci)
                    if isinstance(parsed, list):
                        color_identity = sorted(set(c.upper() for c in parsed if isinstance(c, str)))
                except Exception:
                    pass
        except Exception:
            pass

    archetypes_out = [
        {
            "name": name,
            "frequency": round(count / max(1, total_decks), 4),
            "deck_count": count,
        }
        for name, count in archetype_counter.most_common()
    ]
    bracket_distribution = {br: cnt / max(1, total_decks) for br, cnt in bracket_counter.items()}
    staple_cards = [
        {
            "name": name,
            "usage_pct": round(count / max(1, total_decks), 4),
        }
        for name, count in card_usage.most_common(30)
    ]

    return {
        "version": ARCHETYPE_BRIEF_VERSION,
        "commander": commander,
        "commander_oracle_id": commander_oracle_id,
        "color_identity": color_identity,
        "corpus_deck_count": total_decks,
        "common_archetypes": archetypes_out,
        "bracket_distribution": bracket_distribution,
        "staple_cards": staple_cards,
        "warnings": warnings,
    }


# ============================================================
# /theme/top_cards_v1
# ============================================================


def compute_theme_top_cards_v1(
    *,
    db_snapshot_id: str,
    theme_id: str,
    color_identity: Optional[List[str]] = None,
    limit: int = 50,
) -> Dict[str, Any]:
    """Cards heavily tagged for a given theme.

    For typal themes (TYPAL_<TRIBE>): filter by subtype matching.
    For main themes: use the theme's score_formula referenced primitives,
    intersected with available card_tags primitives.

    Budget: <300ms warm.
    """
    from api.engine.layers.card_search_v1 import search_cards_v1
    from api.engine.layers.deck_theme_classifier_v1 import _THEMES, _TYPAL_THEMES
    warnings: List[Dict[str, str]] = []

    # Parse: "TYPAL_GOBLINS:Goblin" or "TYPAL_GOBLINS" or "THEME_CONTROL"
    base_id = theme_id.split(":", 1)[0] if isinstance(theme_id, str) else ""
    subtype = theme_id.split(":", 1)[1] if isinstance(theme_id, str) and ":" in theme_id else None

    filters: Dict[str, Any] = {}
    if color_identity:
        filters["color_identity_subset_of"] = color_identity

    # Look up theme definition
    theme_def: Optional[Dict[str, Any]] = None
    for t in _THEMES:
        if t.get("theme_id") == base_id:
            theme_def = t
            break
    if theme_def is None:
        for t in _TYPAL_THEMES:
            if t.get("typal_id") == base_id:
                theme_def = t
                if not subtype:
                    subtype = t.get("subtype")
                break

    if theme_def is None:
        warnings.append({"code": "THEME_NOT_FOUND", "message": f"Theme '{theme_id}' not in taxonomy."})
        return {
            "version": THEME_TOP_CARDS_VERSION,
            "theme_id": theme_id,
            "results": [],
            "warnings": warnings,
        }

    # For typal themes, filter by subtype
    if subtype and not subtype.startswith("<"):
        filters["subtypes_any"] = [subtype]

    # Extract identifiers from score_formula (UPPERCASE tokens). These may be
    # bridge signals (TRIBAL_PAYOFFS, etc.) which don't exist in card_tags
    # primitive vocabulary. Expand bridge signals to their composite primitives
    # via the v1.5 bridge file before applying as a search filter.
    import re
    score_text = theme_def.get("score_formula", "") or theme_def.get("required_signals", "")
    tokens = set(re.findall(r"\b[A-Z][A-Z0-9_]+\b", str(score_text)))
    tokens -= {"AND", "OR", "SCORE", "TYPE_DENSITY"}  # TYPE_DENSITY is subtype-count, not a primitive

    # Resolve bridge-signal aliases to their composite primitives
    expanded_primitives: set = set()
    try:
        from api.engine.layers.deck_theme_classifier_v1 import _SIGNAL_COMPOSITES
        for tok in tokens:
            composites = _SIGNAL_COMPOSITES.get(tok)
            if composites:
                # tok is a signal — expand to its composites
                for p in composites:
                    if isinstance(p, str) and p:
                        expanded_primitives.add(p)
            else:
                # tok is a direct primitive
                expanded_primitives.add(tok)
    except Exception:
        expanded_primitives = set(tokens)

    if expanded_primitives:
        filters["primitives_any"] = sorted(list(expanded_primitives))

    try:
        search = search_cards_v1(
            db_snapshot_id=db_snapshot_id,
            filters=filters,
            limit=limit,
            offset=0,
            sort_by="cmc",
            include=["primitives", "type_line", "cmc", "color_identity"],
        )
    except Exception as exc:
        warnings.append({"code": "SEARCH_FAILED", "message": str(exc)})
        search = {"results": []}

    results = []
    for r in search.get("results", []):
        prims = r.get("primitives") or []
        # "Score" by count of expanded primitives present (catches both
        # direct primitives and bridge-signal composites)
        score = sum(1 for t in expanded_primitives if t in prims)
        results.append({
            "oracle_id": r.get("oracle_id"),
            "name": r.get("name"),
            "type_line": r.get("type_line"),
            "cmc": r.get("cmc"),
            "primitives": prims,
            "theme_signal_count": score,
        })
    # Sort by theme_signal_count desc (this is theme-relevance, not "best card"
    # — still respects creativity envelope since AI ranks among returned set)
    results.sort(key=lambda r: (-r["theme_signal_count"], r.get("name") or ""))

    return {
        "version": THEME_TOP_CARDS_VERSION,
        "theme_id": theme_id,
        "subtype": subtype,
        "primitives_used_for_match": sorted(list(tokens)),
        "matched_count": search.get("matched_count", len(results)),
        "returned_count": len(results),
        "results": results,
        "warnings": warnings,
    }


# ============================================================
# /corpus/similar_decks_v1
# ============================================================


def compute_corpus_similar_decks_v1(
    *,
    db_snapshot_id: str,
    commander: Optional[str],
    raw_decklist_text: str,
    k: int = 5,
    include_decklists: bool = False,
) -> Dict[str, Any]:
    """Return the k corpus decks most similar to the given partial deck.

    Optionally embeds the full decklist of each (use sparingly — large
    payload). AI uses this to read prior-art decks directly.

    Budget: <500ms warm. Reuses strength_check's vectorization.
    """
    from api.engine.layers import deck_strength_check_v1 as _sc
    warnings: List[Dict[str, str]] = []

    sc = _sc.compute_deck_strength_check_v1(
        db_snapshot_id=db_snapshot_id,
        commander=commander,
        raw_decklist_text=raw_decklist_text,
        k_nearest=k,
    )
    neighbors = sc.get("measurement_a", {}).get("nearest_neighbors", []) or []

    if include_decklists:
        _sc._load_corpus()
        decklist_by_id: Dict[str, List[str]] = {}
        for entry in _sc._CORPUS_RAW.get("decks", []) or []:
            cid = entry.get("corpus_id")
            if isinstance(cid, str):
                decklist_by_id[cid] = entry.get("decklist", []) or []
        for n in neighbors:
            cid = n.get("corpus_id")
            if cid in decklist_by_id:
                n["decklist"] = decklist_by_id[cid]

    return {
        "version": CORPUS_SIMILAR_VERSION,
        "k_returned": len(neighbors),
        "decks": neighbors,
        "warnings": warnings,
    }
