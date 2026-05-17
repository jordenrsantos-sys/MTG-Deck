"""
deck_candidate_pool_v1 — Pillar A.3 AI-facing candidate-pool endpoint layer.

Given a deck + optional intent, returns ≥100 candidate cards clustered by
which axis they'd shore up. Each candidate annotated with primitives,
themes-matched, synergy-with-existing-cards count, and bracket effect.

NEVER returns top-N. Returns wide clustered pools so the AI can pick from
within each cluster by its own creative judgment (DESIGN_DECISIONS rule 1.1).

Architectural rules served:
  - 1.1 Creativity envelope: clusters, not top-N. AI ranks.
  - 1.2 Speed budget: <500ms warm. Reuses card_search_v1 + analyze layers.
  - 1.3 Strength oracle: rationale references heuristic axis targets in v1.0;
    will reference corpus medians once Pillar A.4 ships.
"""
from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Optional, Tuple


CANDIDATE_POOL_VERSION = "candidate_pool_v1.0"

# v1.0 heuristic axis targets — informed by typical commander deck composition.
# Corpus-derived medians replace these in v1.1 once /deck/strength_check_v1
# (Pillar A.4) ships. Format: (axis_name, primitive_or_primitives, target_count,
# rationale_template, search_limit).
_AXIS_TARGETS: List[Tuple[str, List[str], int, str, int]] = [
    ("RAMP", ["MANA_ROCK", "MANA_RAMP_LAND_SEARCH", "MANA_RAMP_CREATURE_DORK"], 10,
     "Most commander decks want ~10 ramp cards by turn 4-6.", 25),
    ("CARD_DRAW", ["CARD_DRAW_BURST", "CARD_DRAW_REPEATABLE", "DRAW_REPLACEMENT"], 10,
     "Card advantage engine; ~10 is the commander norm for sustainable draw.", 25),
    ("REMOVAL_SINGLE", ["TARGETED_REMOVAL_CREATURE"], 6,
     "Single-target removal for problem permanents.", 20),
    ("REMOVAL_WIPE", ["BOARDWIPE_CREATURES"], 3,
     "Mass board interaction to reset go-wide opponents.", 15),
    ("PROTECTION", ["COUNTERSPELL_PROTECTION", "CANT_BE_COUNTERED"], 3,
     "Protect key threats / counter their interaction.", 15),
    ("TUTORS", ["TUTOR_ANY", "TUTOR_CREATURE", "TUTOR_LAND"], 3,
     "Consistency layer — bracket-gated; B1/B2 stay low, B4/B5 lean heavy.", 15),
]


def compute_candidate_pool_v1(
    *,
    db_snapshot_id: str,
    commander: Optional[str],
    raw_decklist_text: str,
    intent: Optional[str] = None,
    extra_filters: Optional[Dict[str, Any]] = None,
    min_pool_size: int = 100,
    max_pool_size: int = 250,
) -> Dict[str, Any]:
    """Build the clustered candidate pool. Returns response shape per
    ENGINE_API_GUIDE.md /deck/candidate_pool_v1. Never raises."""
    from api.engine.layers.deck_analyze_v1 import compute_deck_analyze_v1
    from api.engine.layers.card_search_v1 import search_cards_v1

    warnings: List[Dict[str, str]] = []

    # ---- Step 1: analyze the deck to establish baseline + color identity ----
    try:
        analyze_result = compute_deck_analyze_v1(
            db_snapshot_id=db_snapshot_id,
            commander=commander,
            raw_decklist_text=raw_decklist_text,
            include_debug=False,
        )
    except Exception as exc:
        warnings.append({"code": "ANALYZE_FAILED", "message": f"{exc.__class__.__name__}: {exc}"})
        analyze_result = {}

    primitive_density: Dict[str, int] = analyze_result.get("primitive_density", {}) or {}
    color_identity: List[str] = analyze_result.get("color_identity", []) or []
    deck_themes: List[Dict[str, Any]] = analyze_result.get("deck_themes_v1", []) or []
    subtype_density: Dict[str, int] = analyze_result.get("subtype_density", {}) or {}
    deck_primitives_set: set = set(primitive_density.keys())

    # ---- Step 2: determine which axes need shoring up ----
    axis_plan: List[Dict[str, Any]] = []
    for axis_name, prims, target, rationale_template, search_limit in _AXIS_TARGETS:
        current = sum(int(primitive_density.get(p, 0)) for p in prims)
        gap = max(0, target - current)
        axis_plan.append({
            "axis_name": axis_name,
            "primitives": prims,
            "target": target,
            "current": current,
            "gap": gap,
            "rationale_template": rationale_template,
            "search_limit": search_limit,
        })

    # Always include a THEME axis for each classified theme
    for theme in deck_themes[:3]:  # cap at top 3 themes to stay within speed budget
        theme_id = theme.get("theme_id")
        if not isinstance(theme_id, str) or not theme_id:
            continue
        # For typal themes, the cluster surfaces more cards of that subtype
        subtype = theme.get("subtype")
        if isinstance(subtype, str) and subtype:
            axis_plan.append({
                "axis_name": f"THEME:{theme_id}",
                "primitives": [],
                "subtype_filter": subtype,
                "target": 30,
                "current": int(subtype_density.get(subtype, 0)),
                "gap": max(0, 30 - int(subtype_density.get(subtype, 0))),
                "rationale_template": f"Deepen the {subtype} tribal theme already active in this deck.",
                "search_limit": 30,
            })

    # ---- Step 3: per-axis search ----
    clusters: List[Dict[str, Any]] = []
    accumulated = 0
    for plan in axis_plan:
        if accumulated >= max_pool_size:
            break
        if plan.get("gap", 0) <= 0:
            # No gap on this axis — skip search but include in summary
            clusters.append({
                "axis": plan["axis_name"],
                "current": plan["current"],
                "target": plan["target"],
                "gap": 0,
                "rationale": f"{plan['axis_name']}: already at target ({plan['current']}/{plan['target']}).",
                "candidates": [],
            })
            continue

        filters: Dict[str, Any] = {
            "color_identity_subset_of": color_identity if color_identity else None,
        }
        if plan.get("primitives"):
            filters["primitives_any"] = plan["primitives"]
        if plan.get("subtype_filter"):
            filters["subtypes_any"] = [plan["subtype_filter"]]
        if extra_filters:
            # Merge caller-provided filters on top (e.g. budget cap, format leg.)
            for k, v in extra_filters.items():
                if v is not None:
                    filters[k] = v

        try:
            search = search_cards_v1(
                db_snapshot_id=db_snapshot_id,
                filters=filters,
                limit=plan["search_limit"],
                offset=0,
                sort_by="cmc",
                include=["primitives", "type_line", "cmc", "color_identity"],
            )
        except Exception as exc:
            warnings.append({"code": "SEARCH_FAILED_AXIS", "message": f"{plan['axis_name']}: {exc}"})
            search = {"results": [], "matched_count": 0}

        # Annotate each candidate with synergy_count_with_existing
        results = search.get("results") or []
        annotated: List[Dict[str, Any]] = []
        for r in results:
            cprims = set(r.get("primitives") or [])
            synergy = len(cprims & deck_primitives_set)
            annotated.append({
                "oracle_id": r.get("oracle_id"),
                "name": r.get("name"),
                "type_line": r.get("type_line"),
                "cmc": r.get("cmc"),
                "color_identity": r.get("color_identity"),
                "primitives": r.get("primitives") or [],
                "synergy_count_with_existing": synergy,
                "bracket_effect": "neutral",  # v1.0 stub — Pillar A.4 wires bracket-tier deltas
                "corpus_usage_pct": None,     # v1.0 stub — Pillar A.4 corpus ships this
            })

        clusters.append({
            "axis": plan["axis_name"],
            "current": plan["current"],
            "target": plan["target"],
            "gap": plan["gap"],
            "matched_count": search.get("matched_count", len(results)),
            "rationale": (
                f"{plan['axis_name']}: deck has {plan['current']}; v1.0 heuristic target is "
                f"{plan['target']} (gap {plan['gap']}). {plan['rationale_template']}"
            ),
            "candidates": annotated,
        })
        accumulated += len(annotated)

    # ---- Step 4: assemble response ----
    return {
        "version": CANDIDATE_POOL_VERSION,
        "db_snapshot_id": db_snapshot_id,
        "commander": commander,
        "intent": intent,
        "color_identity": color_identity,
        "deck_themes_summary": [t.get("theme_id") for t in deck_themes if isinstance(t, dict)],
        "pool_size": accumulated,
        "clusters": clusters,
        "warnings": warnings,
    }
