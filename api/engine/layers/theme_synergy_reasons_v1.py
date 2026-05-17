"""
theme_synergy_reasons_v1 — Phase 2.1b.

For each PLAYABLE added card, annotate its `reasons_v1` list with
`THEME_SYNERGY:<theme_id>` entries identifying which classified theme(s)
the card contributes to. Lets UI chip the card with "supports
TYPAL_GOBLINS" and lets AI consumers explain why this card was added.

Architectural rules served (DESIGN_DECISIONS.md):
  - 1.1 Creativity envelope: surfaces structured "why" per candidate, not a
    ranking. AI uses these reasons to reason about composition.
  - 1.4 Honest signal: a card gets a THEME_SYNERGY reason only when its
    primitives literally overlap with the theme's expanded primitive set
    (including bridge-signal composites). No fabricated synergy claims.

Pure function. No DB writes, no network. Mutates the added_cards list
in place AND returns it for chainability with attach_combo_enabler_reasons_v1.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Set


PHASE_2_1B_VERSION = "theme_synergy_reasons_v1.0"
THEME_SYNERGY_PREFIX = "THEME_SYNERGY:"


def attach_theme_synergy_reasons_v1(
    *,
    added_cards_v1: List[Dict[str, Any]],
    deck_themes_v1: List[Dict[str, Any]],
    db_snapshot_id: str,
) -> List[Dict[str, Any]]:
    """Annotate each added card with `THEME_SYNERGY:<theme_id>` reasons.

    Args:
        added_cards_v1: List of {name, reasons_v1, primitives_added_v1}.
            Mutated in place — each entry's reasons_v1 may gain new entries.
        deck_themes_v1: Output of classify_deck_themes_v1. Each entry has
            theme_id, theme_type, subtype, score, confidence_band,
            contributing_primitives, etc.
        db_snapshot_id: Snapshot id for tag-fetch of each added card.

    Returns:
        The same added_cards_v1 list (for chaining). Each entry's reasons_v1
        is the existing list + any new THEME_SYNERGY:<theme_id> entries.
        Deterministic ordering: themes by score desc; per-card reasons by
        theme_id alphabetical to keep diff-stable.
    """
    if not isinstance(added_cards_v1, list) or not added_cards_v1:
        return added_cards_v1 or []
    if not isinstance(deck_themes_v1, list) or not deck_themes_v1:
        return added_cards_v1

    # Build per-theme expanded primitive set + per-theme subtype filter.
    # A card contributes to a theme when:
    #   (a) any of its primitives is in the theme's expanded primitive set,
    #       OR
    #   (b) the theme is typal and the card's subtypes include the typal subtype.
    theme_expansions = _compute_theme_expansions(deck_themes_v1)
    if not theme_expansions:
        return added_cards_v1

    # Resolve added-card oracle ids + primitives + subtypes in one bulk pass
    names = [
        c.get("name") for c in added_cards_v1
        if isinstance(c, dict) and isinstance(c.get("name"), str) and c.get("name", "").strip()
    ]
    if not names:
        return added_cards_v1

    primitives_by_name, subtypes_by_name = _bulk_resolve_for_synergy(db_snapshot_id, names)

    for card in added_cards_v1:
        if not isinstance(card, dict):
            continue
        name = card.get("name")
        if not isinstance(name, str) or not name.strip():
            continue
        prims = primitives_by_name.get(name, set())
        subs = subtypes_by_name.get(name, set())
        matched_theme_ids: List[str] = []
        for te in theme_expansions:
            # Subtype-required typal themes: the card must have the subtype
            if te.get("required_subtype"):
                if te["required_subtype"] not in subs:
                    continue
                matched_theme_ids.append(te["theme_id"])
                continue
            # Main themes: any primitive overlap
            if prims & te["primitive_set"]:
                matched_theme_ids.append(te["theme_id"])
        if not matched_theme_ids:
            continue
        # Append new reasons (deterministic, no duplicates)
        existing = card.get("reasons_v1")
        if not isinstance(existing, list):
            existing = []
            card["reasons_v1"] = existing
        existing_set = set(r for r in existing if isinstance(r, str))
        for tid in sorted(set(matched_theme_ids)):
            tag = f"{THEME_SYNERGY_PREFIX}{tid}"
            if tag not in existing_set:
                existing.append(tag)
                existing_set.add(tag)

    return added_cards_v1


# ============================================================
# Helpers
# ============================================================


def _compute_theme_expansions(
    deck_themes_v1: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """For each classified theme, derive the set of primitives that "count"
    as contributing to it, expanding any bridge signals to their composites.

    Returns list of dicts shaped:
      {
        "theme_id": "TYPAL_GOBLINS:Goblin" | "THEME_ARTIFACTS" | ...,
        "primitive_set": {"ATTACK_TRIGGER_PAYOFF", ...},
        "required_subtype": "Goblin" | None,
      }
    """
    try:
        from api.engine.layers.deck_theme_classifier_v1 import (
            _THEMES, _TYPAL_THEMES, _SIGNAL_COMPOSITES,
        )
    except Exception:
        return []

    # Theme defs indexed for lookup
    main_by_id: Dict[str, Dict[str, Any]] = {
        t.get("theme_id"): t for t in _THEMES if isinstance(t, dict) and isinstance(t.get("theme_id"), str)
    }
    typal_by_id: Dict[str, Dict[str, Any]] = {
        t.get("typal_id"): t for t in _TYPAL_THEMES if isinstance(t, dict) and isinstance(t.get("typal_id"), str)
    }

    out: List[Dict[str, Any]] = []
    for ct in deck_themes_v1:
        if not isinstance(ct, dict):
            continue
        full_id = ct.get("theme_id")
        if not isinstance(full_id, str) or not full_id:
            continue
        # Split typal "TYPAL_<TRIBE>:Subtype" into base + subtype
        base_id = full_id.split(":", 1)[0]
        ct_subtype = ct.get("subtype") if isinstance(ct.get("subtype"), str) else None

        if base_id in main_by_id:
            tdef = main_by_id[base_id]
            required_subtype = None
        elif base_id in typal_by_id:
            tdef = typal_by_id[base_id]
            required_subtype = ct_subtype
        else:
            continue

        # Extract identifier tokens from the theme's score formula + required signals
        text = " ".join(str(tdef.get(k, "")) for k in (
            "score_formula", "required_signals", "optional_boosters",
        ))
        tokens = set(re.findall(r"\b[A-Z][A-Z0-9_]+\b", text))
        tokens -= {"AND", "OR", "SCORE", "TYPE_DENSITY"}

        # Expand bridge signals to their composite primitives
        expanded: Set[str] = set()
        for tok in tokens:
            composites = _SIGNAL_COMPOSITES.get(tok)
            if composites:
                for p in composites:
                    if isinstance(p, str) and p:
                        expanded.add(p)
            else:
                expanded.add(tok)

        out.append({
            "theme_id": full_id,
            "primitive_set": expanded,
            "required_subtype": required_subtype,
        })
    return out


def _bulk_resolve_for_synergy(
    db_snapshot_id: str,
    names: Iterable[str],
) -> tuple:
    """Bulk-resolve names → (primitives_by_name, subtypes_by_name) maps.

    Single SQL query for the cards table + one query against
    primitive_to_cards to get tagged primitives. Avoids per-card connection
    cost — keeps Phase 2.1b sub-100ms on typical added-cards sets (≤30).
    """
    primitives_by_name: Dict[str, Set[str]] = {}
    subtypes_by_name: Dict[str, Set[str]] = {}
    unique_lower: List[str] = []
    seen: Set[str] = set()
    name_list: List[str] = []
    for n in names:
        if not isinstance(n, str):
            continue
        s = n.strip()
        if not s:
            continue
        nl = s.lower()
        if nl in seen:
            continue
        seen.add(nl)
        unique_lower.append(nl)
        name_list.append(s)
    if not unique_lower:
        return primitives_by_name, subtypes_by_name

    try:
        from engine.db import connect as cards_db_connect
        from api.engine.version_resolve_v1 import resolve_runtime_taxonomy_version
    except Exception:
        return primitives_by_name, subtypes_by_name

    try:
        with cards_db_connect() as con:
            placeholders = ",".join("?" for _ in unique_lower)
            sql = (
                "SELECT name, oracle_id, type_line FROM cards "
                "WHERE snapshot_id = ? AND LOWER(name) IN (" + placeholders + ")"
            )
            params: List[Any] = [db_snapshot_id] + unique_lower
            rows = con.execute(sql, params).fetchall()

            name_by_oid: Dict[str, str] = {}
            for r in rows:
                rd = dict(r) if hasattr(r, "keys") else dict(r)
                oid = rd.get("oracle_id")
                rname = rd.get("name")
                tline = rd.get("type_line") or ""
                if isinstance(rname, str) and isinstance(oid, str) and oid:
                    name_by_oid[oid] = rname
                    # Extract subtypes from type_line
                    subs: Set[str] = set()
                    if isinstance(tline, str):
                        for sep in ("—", "–", " - "):
                            if sep in tline:
                                after = tline.split(sep, 1)[1].strip()
                                for tok in after.split():
                                    if tok and tok[0].isupper() and "," not in tok:
                                        subs.add(tok)
                                break
                    subtypes_by_name[rname] = subs
                    primitives_by_name.setdefault(rname, set())

            # Fetch primitives via the inverted index
            oids = list(name_by_oid.keys())
            if oids:
                taxonomy_version = None
                try:
                    taxonomy_version = resolve_runtime_taxonomy_version(
                        snapshot_id=db_snapshot_id, requested=None, db=con,
                    )
                except Exception:
                    pass
                if isinstance(taxonomy_version, str) and taxonomy_version:
                    # Chunk to stay under SQLite param limit
                    for chunk_start in range(0, len(oids), 900):
                        chunk = oids[chunk_start: chunk_start + 900]
                        ph = ",".join("?" for _ in chunk)
                        psql = (
                            "SELECT oracle_id, primitive_id FROM primitive_to_cards "
                            "WHERE snapshot_id = ? AND taxonomy_version = ? AND oracle_id IN (" + ph + ")"
                        )
                        for oid, prim in con.execute(
                            psql, [db_snapshot_id, taxonomy_version] + chunk
                        ).fetchall():
                            nm = name_by_oid.get(oid)
                            if nm:
                                primitives_by_name.setdefault(nm, set()).add(prim)
    except Exception:
        pass

    return primitives_by_name, subtypes_by_name
