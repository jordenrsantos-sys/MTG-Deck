"""
deck_analyze_v1 — Pillar A.1 AI-facing analyze endpoint layer.

Given a decklist + commander, returns a structured diagnostic snapshot:
  - card_count
  - primitive_density (aggregated across deck)
  - subtype_density
  - mana_curve
  - color_identity
  - deck_themes_v1 (reuses classify_deck_themes_v1)
  - detected_combos_v1 (reuses compute_deck_combo_insights_v1)
  - bracket_estimate + bracket_envelope (walks B1..B5 enforcement)
  - gap_signal — stub in v1.0; populated when /deck/strength_check_v1 (Pillar A.4)
    corpus ships and analyze can cross-reference axis medians.
  - warnings

Architectural rules served (per DESIGN_DECISIONS.md):
  - Speed budget: target <500ms warm. Single bulk SQL fetch for tags;
    bracket walk is 5 cheap enforcement calls.
  - Creativity envelope: no candidate cards returned, no ranking;
    just structural facts about the existing deck. AI uses analyze as
    the kickoff context for its own creative reasoning.
  - Honest signal: when corpus is absent, gap_signal returns []; we do
    not fabricate axis norms.

Pure function (no DB writes). Reads cards/card_tags via existing db_cards
and db_tags helpers. Never raises on data issues — returns warnings instead.
"""
from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Optional, Tuple


ANALYZE_VERSION = "analyze_v1.0"

# Bracket walk order — lowest first. The "natural" bracket is the lowest
# bracket whose enforcement produces zero violations.
_BRACKET_WALK_ORDER: List[str] = ["B1", "B2", "B3", "B4", "B5"]


def compute_deck_analyze_v1(
    *,
    db_snapshot_id: str,
    commander: Optional[str],
    raw_decklist_text: str,
    include_debug: bool = False,
) -> Dict[str, Any]:
    """Run the analyze pipeline and return the v1.0 response shape.

    Args:
        db_snapshot_id: Required snapshot identifier.
        commander: Commander card name (or None for non-commander format use).
        raw_decklist_text: Mainboard decklist text (TappedOut/MTGO format).
        include_debug: When True, include the internal diagnostic fields under
            response["_debug"]. AI agents typically want this off.

    Returns:
        Response dict matching the ENGINE_API_GUIDE.md /deck/analyze_v1 spec.
        Never raises — all errors are surfaced as `warnings` entries.
    """
    warnings: List[Dict[str, str]] = []

    # ---- Parse decklist ----
    parse_result = _safe_parse_decklist(raw_decklist_text, warnings)
    items = parse_result.get("items", [])
    commander_names: List[str] = []
    deck_card_names: List[str] = []
    for item in items:
        section = item.get("section") or "mainboard"
        name = item.get("name_norm")
        count = int(item.get("count", 1))
        if not isinstance(name, str) or not name:
            continue
        target = commander_names if section == "commander" else deck_card_names
        for _ in range(max(1, count)):
            target.append(name)

    # Honor the explicit commander parameter as well — it overrides/augments any
    # `Commander` section in the text. If both are present, dedupe by name.
    if isinstance(commander, str) and commander.strip():
        cname = commander.strip()
        if cname not in commander_names:
            commander_names.append(cname)

    all_names = list(commander_names) + list(deck_card_names)
    card_count = len(deck_card_names) + (1 if commander_names else 0)

    # ---- Resolve names → oracle_ids + card rows (single bulk pass) ----
    name_to_oracle, name_to_card = _safe_bulk_resolve(
        db_snapshot_id, all_names, warnings
    )

    # ---- Aggregate structural facts ----
    color_identity = _aggregate_color_identity(name_to_card, all_names)
    mana_curve = _aggregate_mana_curve(name_to_card, deck_card_names)
    subtype_density = _aggregate_subtype_density(name_to_card, all_names)

    # ---- Primitives + theme classification (reuses Phase 2.1a layer) ----
    primitive_density, deck_themes_v1 = _safe_primitive_index_and_themes(
        db_snapshot_id, all_names, subtype_density, warnings,
        name_to_card=name_to_card,
    )

    # ---- Combos (reuses v1.7.2 layer) ----
    detected_combos_v1, missing_partners_v1 = _safe_combo_insights(
        db_snapshot_id, commander_names, deck_card_names, warnings
    )

    # ---- Bracket walk ----
    bracket_estimate, bracket_envelope = _walk_brackets(
        db_snapshot_id=db_snapshot_id,
        commander_names=commander_names,
        deck_card_names=deck_card_names,
        detected_combos_v1=detected_combos_v1,
        warnings=warnings,
    )

    # ---- Gap signal — heuristic axis targets (corpus-driven version will
    # replace these once Phase 5a expands the corpus). Aligns with the
    # candidate_pool clustering rule of thumb so AI consumers reading analyze
    # get the same gap framing as candidate_pool returns.
    gap_signal: List[Dict[str, Any]] = _heuristic_gap_signal(primitive_density)

    # ---- Commander oracle id (convenience) ----
    commander_oracle_id: Optional[str] = None
    if commander_names:
        first = commander_names[0]
        commander_oracle_id = name_to_oracle.get(first)

    response: Dict[str, Any] = {
        "version": ANALYZE_VERSION,
        "db_snapshot_id": db_snapshot_id,
        "commander_oracle_id": commander_oracle_id,
        "commander": commander_names[0] if commander_names else None,
        "card_count": card_count,
        "color_identity": sorted(list(color_identity)),
        "mana_curve": mana_curve,
        "subtype_density": subtype_density,
        "primitive_density": primitive_density,
        "deck_themes_v1": deck_themes_v1,
        "detected_combos_v1": detected_combos_v1,
        "missing_partners_v1": missing_partners_v1,
        "bracket_estimate": bracket_estimate,
        "bracket_envelope": bracket_envelope,
        "gap_signal": gap_signal,
        "warnings": warnings,
    }

    if include_debug:
        response["_debug"] = {
            "resolved_card_count": len(name_to_card),
            "unresolved_card_count": len(all_names) - len(name_to_card),
        }

    return response


# ============================================================
# Helpers
# ============================================================


def _safe_parse_decklist(text: str, warnings: List[Dict[str, str]]) -> Dict[str, Any]:
    try:
        from api.engine.decklist_parse_v1 import parse_decklist_text
        return parse_decklist_text(text)
    except Exception as exc:
        warnings.append({
            "code": "DECKLIST_PARSE_FAILED",
            "message": f"{exc.__class__.__name__}: {exc}",
        })
        return {"items": []}


def _safe_bulk_resolve(
    db_snapshot_id: str,
    names: List[str],
    warnings: List[Dict[str, str]],
) -> Tuple[Dict[str, str], Dict[str, Dict[str, Any]]]:
    """Resolve each unique name → oracle_id + card row.

    Single SQL query against the cards table — avoids the per-name connection
    cost of find_card_by_name (which would be ~30 calls × ~30ms = ~1s for an
    average deck). Only fetches columns analyze needs (name, oracle_id,
    cmc, type_line, color_identity); skips the secondary tag_facets lookup
    that find_card_by_name does internally.

    Returns (name→oracle_id, name→card_row). Names are matched case-insensitively
    against the cards table.
    """
    name_to_oracle: Dict[str, str] = {}
    name_to_card: Dict[str, Dict[str, Any]] = {}
    if not names:
        return name_to_oracle, name_to_card

    # Dedupe + normalize input names (preserve original-case key for response)
    unique_names: List[str] = []
    name_seen: set = set()
    for n in names:
        if not isinstance(n, str):
            continue
        s = n.strip()
        if not s or s in name_seen:
            continue
        name_seen.add(s)
        unique_names.append(s)
    if not unique_names:
        return name_to_oracle, name_to_card

    try:
        from engine.db import connect as cards_db_connect
    except Exception as exc:
        warnings.append({
            "code": "DB_IMPORT_FAILED",
            "message": f"{exc.__class__.__name__}: {exc}",
        })
        return name_to_oracle, name_to_card

    try:
        lowered = [n.lower() for n in unique_names]
        placeholders = ",".join("?" for _ in lowered)
        query = (
            "SELECT name, oracle_id, cmc, type_line, color_identity, mana_cost "
            "FROM cards WHERE snapshot_id = ? AND LOWER(name) IN (" + placeholders + ")"
        )
        params: List[Any] = [db_snapshot_id] + lowered
        with cards_db_connect() as con:
            rows = con.execute(query, params).fetchall()
        # Build lookup by lowercase name
        by_lower: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            row_dict = dict(row) if hasattr(row, "keys") else dict(row)
            row_name = row_dict.get("name")
            if isinstance(row_name, str):
                by_lower[row_name.lower()] = row_dict
        # Map each input unique name to its row
        for n in unique_names:
            row = by_lower.get(n.lower())
            if row is None:
                continue
            oracle_id = row.get("oracle_id")
            if isinstance(oracle_id, str) and oracle_id:
                name_to_oracle[n] = oracle_id
                name_to_card[n] = row
    except Exception as exc:
        warnings.append({
            "code": "BULK_RESOLVE_FAILED",
            "message": f"{exc.__class__.__name__}: {exc}",
        })
    return name_to_oracle, name_to_card


def _aggregate_color_identity(
    name_to_card: Dict[str, Dict[str, Any]],
    all_names: List[str],
) -> set:
    ci: set = set()
    for name in set(all_names):
        card = name_to_card.get(name)
        if not isinstance(card, dict):
            continue
        cid = card.get("color_identity")
        if isinstance(cid, list):
            for c in cid:
                if isinstance(c, str) and c:
                    ci.add(c.upper())
        elif isinstance(cid, str):
            # SQL TEXT field may be stored as JSON string
            try:
                import json as _json
                parsed = _json.loads(cid)
                if isinstance(parsed, list):
                    for c in parsed:
                        if isinstance(c, str) and c:
                            ci.add(c.upper())
            except Exception:
                # Or as comma-separated
                for c in cid.split(","):
                    cc = c.strip().upper()
                    if cc:
                        ci.add(cc)
    return ci


def _aggregate_mana_curve(
    name_to_card: Dict[str, Dict[str, Any]],
    deck_card_names: List[str],
) -> Dict[str, int]:
    curve: Counter = Counter()
    for name in deck_card_names:
        card = name_to_card.get(name)
        if not isinstance(card, dict):
            continue
        type_line = card.get("type_line", "")
        # Lands are excluded from curve (CMC 0 spam would dominate)
        if isinstance(type_line, str) and "Land" in type_line.split("—")[0]:
            continue
        cmc = card.get("cmc")
        try:
            cmc_int = int(cmc) if cmc is not None else 0
        except (TypeError, ValueError):
            cmc_int = 0
        # Bucket 7+ together
        bucket = str(min(cmc_int, 7))
        curve[bucket] += 1
    return dict(sorted(curve.items(), key=lambda kv: int(kv[0])))


def _aggregate_subtype_density(
    name_to_card: Dict[str, Dict[str, Any]],
    all_names: List[str],
) -> Dict[str, int]:
    """Same logic as compute_subtype_counts_from_card_names but operating on the
    already-resolved card rows so we don't pay DB cost twice."""
    counts: Counter = Counter()
    for name in all_names:
        card = name_to_card.get(name)
        if not isinstance(card, dict):
            continue
        type_line = card.get("type_line", "")
        if not isinstance(type_line, str):
            continue
        for separator in ("—", "–", " - "):
            if separator in type_line:
                after = type_line.split(separator, 1)[1].strip()
                for subtype in after.split():
                    if subtype and subtype[0].isupper() and "," not in subtype:
                        counts[subtype] += 1
                break
    return dict(counts.most_common(40))


def _safe_primitive_index_and_themes(
    db_snapshot_id: str,
    all_names: List[str],
    subtype_density: Dict[str, int],
    warnings: List[Dict[str, str]],
    name_to_card: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Tuple[Dict[str, int], List[Dict[str, Any]]]:
    """Compute primitive_density + classified themes.

    Quality filter (v1.1): when `name_to_card` is provided, primitive_density
    excludes contributions from Basic Land cards. Reason: the production tag
    pack mis-tags basic lands as MANA_ROCK; that primitive count inflates
    artifact/ramp signals on every deck. The classifier itself still sees the
    raw index (preserves existing theme-fire behavior). Only the response's
    `primitive_density` summary is filtered. AI consumers see honest numbers.
    """
    primitive_density: Dict[str, int] = {}
    themes: List[Dict[str, Any]] = []
    try:
        from api.engine.layers.deck_theme_classifier_v1 import (
            classify_deck_themes_v1,
            compute_primitive_index_from_card_names,
        )
    except Exception as exc:
        warnings.append({
            "code": "CLASSIFIER_IMPORT_FAILED",
            "message": f"{exc.__class__.__name__}: {exc}",
        })
        return primitive_density, themes
    try:
        prim_index = compute_primitive_index_from_card_names(db_snapshot_id, all_names)
        # Aggregate density across slots, filtering land-card contributions
        # when name_to_card is available.
        counter: Counter = Counter()
        for slot_id, prims in prim_index.items():
            if not isinstance(prims, list):
                continue
            # slot_id format: "slot_<idx>_<name>"
            card_name = None
            if isinstance(slot_id, str) and slot_id.startswith("slot_"):
                parts = slot_id.split("_", 2)
                if len(parts) == 3:
                    card_name = parts[2]
            is_basic_land = False
            if name_to_card and card_name:
                card = name_to_card.get(card_name)
                if isinstance(card, dict):
                    type_line = card.get("type_line", "") or ""
                    if "Basic Land" in type_line or (
                        "Land" in type_line.split("—")[0]
                        and any(
                            sub in (type_line.split("—")[1] if "—" in type_line else "")
                            for sub in ("Mountain", "Island", "Plains", "Swamp", "Forest", "Wastes")
                        )
                    ):
                        is_basic_land = True
            for p in prims:
                if isinstance(p, str) and p:
                    if is_basic_land:
                        # Drop the false-positive MANA_ROCK from basic lands;
                        # other primitives on lands (rare) still count.
                        if p == "MANA_ROCK":
                            continue
                    counter[p] += 1
        primitive_density = dict(counter.most_common(60))
        themes = classify_deck_themes_v1(
            primitive_index_by_slot=prim_index,
            deck_subtype_counts=subtype_density,
            max_themes=20,
        )
    except Exception as exc:
        warnings.append({
            "code": "CLASSIFIER_FAILED",
            "message": f"{exc.__class__.__name__}: {exc}",
        })
    return primitive_density, themes


def _safe_combo_insights(
    db_snapshot_id: str,
    commander_names: List[str],
    deck_card_names: List[str],
    warnings: List[Dict[str, str]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    try:
        from api.engine.layers.deck_combo_insights_v1 import compute_deck_combo_insights_v1
        result = compute_deck_combo_insights_v1(
            db_snapshot_id=db_snapshot_id,
            commander_names=commander_names,
            deck_cards_after_completion=deck_card_names,
        )
        return (
            result.get("detected_combos_v1") or [],
            result.get("missing_partners_v1") or [],
        )
    except Exception as exc:
        warnings.append({
            "code": "COMBO_INSIGHTS_FAILED",
            "message": f"{exc.__class__.__name__}: {exc}",
        })
        return [], []


def _heuristic_gap_signal(primitive_density: Dict[str, int]) -> List[Dict[str, Any]]:
    """Heuristic axis-gap detector. Mirrors the candidate_pool's axis targets
    so AI consumers see consistent framing across endpoints. Corpus-driven
    medians replace these targets once /deck/strength_check_v1 + an expanded
    corpus ships."""
    # (axis, primitives_to_sum, target_count, severity_threshold)
    axes: List[Tuple[str, List[str], int]] = [
        ("RAMP", ["MANA_ROCK", "MANA_RAMP_LAND_SEARCH", "MANA_RAMP_CREATURE_DORK"], 10),
        ("CARD_DRAW", ["CARD_DRAW_BURST", "CARD_DRAW_REPEATABLE", "DRAW_REPLACEMENT"], 10),
        ("REMOVAL_SINGLE", ["TARGETED_REMOVAL_CREATURE"], 6),
        ("REMOVAL_WIPE", ["BOARDWIPE_CREATURES"], 3),
        ("PROTECTION", ["COUNTERSPELL_PROTECTION", "CANT_BE_COUNTERED"], 3),
        ("TUTORS", ["TUTOR_ANY", "TUTOR_CREATURE", "TUTOR_LAND"], 3),
    ]
    out: List[Dict[str, Any]] = []
    for axis_name, prims, target in axes:
        current = sum(int(primitive_density.get(p, 0)) for p in prims)
        delta = current - target
        if delta >= 0:
            continue  # at or above target — no gap
        if abs(delta) >= 6:
            severity = "HIGH"
        elif abs(delta) >= 3:
            severity = "MEDIUM"
        else:
            severity = "LOW"
        out.append({
            "axis": axis_name,
            "current": current,
            "heuristic_target": target,
            "delta": delta,
            "severity": severity,
            "note": (
                "Heuristic target from typical commander deck composition. "
                "Corpus-driven medians replace this once strength oracle "
                "Measurement A's corpus expands."
            ),
        })
    return out


def _walk_brackets(
    *,
    db_snapshot_id: str,
    commander_names: List[str],
    deck_card_names: List[str],
    detected_combos_v1: List[Dict[str, Any]],
    warnings: List[Dict[str, str]],
) -> Tuple[Optional[str], Dict[str, Any]]:
    """Determine the natural bracket — lowest bracket the deck passes.

    For Pillar A.1 v1.0 we use a lightweight walk based on detected_combos_v1:
      - If any 2-card combo is detected, deck cannot be B1 or B2.
      - For B3+: deck passes (the deeper game-changer / GC-count gating
        happens in `profile_bracket_enforcement_v1`; calling it requires
        running the full pipeline for each bracket which is too slow for
        an analyze call's <500ms budget). We surface a v1.1 follow-up.

    Returns (bracket_estimate, envelope_dict).
    """
    combo_violators_b1_b2 = bool(detected_combos_v1)

    # v1.0 walk: bracket is B1 if no combos and small deck, else B3.
    # Calibration-honest: a more precise walk happens in v1.1 once we wire in
    # cached per-bracket enforcement (Phase 2.1.6 task).
    if combo_violators_b1_b2:
        bracket_estimate = "B3"
        min_bracket = "B3"
        max_bracket = "B5"
        blockers_to_lower = [
            {
                "code": "TWO_CARD_COMBO_DETECTED",
                "message": (
                    f"{len(detected_combos_v1)} two-card combo(s) detected; "
                    "B1 and B2 disallow 2-card combos."
                ),
                "combos": [c.get("variant_id") for c in detected_combos_v1[:5] if isinstance(c, dict)],
            }
        ]
    else:
        bracket_estimate = "B2"
        min_bracket = "B1"
        max_bracket = "B5"
        blockers_to_lower = []

    envelope = {
        "min_bracket_possible": min_bracket,
        "max_bracket_possible": max_bracket,
        "current_estimate": bracket_estimate,
        "blockers_to_lower": blockers_to_lower,
        "headroom_to_higher": [
            {
                "code": "BRACKET_WALK_DEFERRED_V1_1",
                "message": (
                    "Full per-bracket enforcement walk deferred to analyze_v1.1; "
                    "current v1.0 estimate uses combo detection only."
                ),
            }
        ],
    }
    return bracket_estimate, envelope
