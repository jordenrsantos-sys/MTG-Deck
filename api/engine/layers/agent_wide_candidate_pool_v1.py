"""
agent_wide_candidate_pool_v1 — Pillar D iteration 2 Phase C2.2.

Builds a BROADER candidate pool (300-500 cards) than the standard Phase
B pool, intended for the wild-combo-discovery LLM call. Phase B narrows
by theme/staple frequency; Phase C2.2 needs a wider net so the LLM can
find non-obvious synergies the corpus alone misses.

Filters applied:
  1. Snapshot match.
  2. Color identity ⊆ commander's CI.
  3. Type filter: drop basic lands, drop tokens (cards with empty
     mana_cost AND no type that should be tokenless — practical filter
     to keep the pool to playable cards).
  4. Theme adjacency: prefer cards whose primitives overlap with any
     of the requested theme primitives. Cards with no overlap are
     still INCLUDED but ranked below those with overlap.
  5. Recent-set boost: NOT IMPLEMENTED in iteration 2. The cards table
     has no released_at column; pulling release dates from cards_raw
     would require parsing JSON for every row, which kills the pool
     build time. Iteration 3 should denormalize released_at into the
     cards table or maintain a separate priors table.

Output: list of card dicts with full oracle_text + primitives so the
Phase C2.2 LLM call can reason semantically about interactions the
corpus-frequency-driven Phase B pool misses.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Sequence


WIDE_POOL_VERSION = "agent_wide_candidate_pool_v1.0"
DEFAULT_POOL_SIZE = 400
MAX_POOL_SIZE = 600


def compute_agent_wide_candidate_pool_v1(
    *,
    db_snapshot_id: str,
    commander: str,
    color_identity: Sequence[str],
    theme_primitives: Optional[Sequence[str]] = None,
    pool_size: int = DEFAULT_POOL_SIZE,
    exclude_names: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Return a wide candidate pool for Phase C2.2 (wild combo discovery).

    Args:
        db_snapshot_id: Snapshot to read from.
        commander: Commander name (used only for the warning trail; the
            filter is on color identity, not the commander itself).
        color_identity: e.g. ["B", "R", "W"] — pool restricts to cards
            whose color_identity is a subset.
        theme_primitives: List of primitive tags from the requested
            themes. Cards with primitive overlap rank higher.
        pool_size: Target pool size (300-500 recommended).
        exclude_names: Card names already in the deck — excluded from
            the pool to keep the LLM's choices novel.

    Returns:
        {
          "version": str,
          "candidates": List[Dict],   # name, type_line, cmc, primitives,
                                      # color_identity, oracle_text, score
          "color_identity": List[str],
          "filter_summary": Dict,
          "warnings": List[Dict],
        }
    """
    from engine import db as eng_db

    warnings: List[Dict[str, str]] = []
    ci_set = {c.upper() for c in (color_identity or []) if isinstance(c, str)}
    theme_prim_set = {p for p in (theme_primitives or []) if isinstance(p, str) and p}
    exclude_lower = {(n or "").strip().lower() for n in (exclude_names or [])}

    pool_size = max(50, min(MAX_POOL_SIZE, pool_size or DEFAULT_POOL_SIZE))

    try:
        with eng_db.connect() as con:
            # Pull every card in the snapshot once; filter + rank in
            # Python because the corpus is ~30K cards which is small
            # enough to iterate and the filter logic mixes SQL-hostile
            # operations (JSON parsing, set membership).
            rows = con.execute(
                """
                SELECT name, type_line, cmc, color_identity, primitives_json,
                       oracle_text, mana_cost
                FROM cards
                WHERE snapshot_id = ?
                """,
                (db_snapshot_id,),
            ).fetchall()
    except Exception as exc:
        warnings.append({
            "code": "WIDE_POOL_DB_FAILED",
            "message": f"{exc.__class__.__name__}: {exc}",
        })
        return {
            "version": WIDE_POOL_VERSION,
            "candidates": [],
            "color_identity": sorted(ci_set),
            "filter_summary": {"total_rows": 0, "after_ci": 0, "after_type": 0,
                               "after_exclude": 0, "with_theme_overlap": 0,
                               "without_theme_overlap": 0, "returned": 0},
            "warnings": warnings,
        }

    total_rows = len(rows)
    candidates_with_theme: List[Dict[str, Any]] = []
    candidates_without_theme: List[Dict[str, Any]] = []
    after_ci = 0
    after_type = 0
    after_exclude = 0

    for row in rows:
        name = row["name"]
        if not name:
            continue
        # Color identity filter.
        ci_raw = row["color_identity"] or ""
        ci_list = _parse_color_identity(ci_raw)
        if ci_list and not set(ci_list).issubset(ci_set or {"W", "U", "B", "R", "G"}):
            # Empty ci_set (colorless commander) accepts only colorless cards
            # — i.e. ci_list must be empty too.
            if not ci_set:
                continue
            continue
        if not ci_set and ci_list:
            # Colorless commander, colored card — reject.
            continue
        after_ci += 1

        # Type filter — drop basic lands (they're never interesting
        # combo pieces) and obvious non-playable cards.
        type_line = (row["type_line"] or "").strip()
        type_lower = type_line.lower()
        if "basic land" in type_lower:
            continue
        # Skip tokens (per Scryfall convention they have no mana_cost AND
        # type_line contains "Token").
        if "token" in type_lower:
            continue
        after_type += 1

        # Exclusion (cards already in deck).
        if name.strip().lower() in exclude_lower:
            continue
        after_exclude += 1

        # Parse primitives + oracle_text.
        prim_list: List[str] = []
        prim_raw = row["primitives_json"]
        if prim_raw:
            try:
                parsed = json.loads(prim_raw)
                if isinstance(parsed, list):
                    prim_list = [str(p) for p in parsed if isinstance(p, str)]
            except Exception:
                pass

        primitives_set = set(prim_list)
        theme_overlap = len(primitives_set & theme_prim_set) if theme_prim_set else 0
        score = float(theme_overlap) * 10.0

        oracle_text = (row["oracle_text"] or "").strip()
        # Truncate oracle text to keep token budget reasonable. Most
        # mechanics fit in the first ~200 chars; longer rules (saga,
        # MDFC) get cut off — that's an iteration-3 problem.
        if oracle_text and len(oracle_text) > 220:
            oracle_text = oracle_text[:217] + "..."

        cand = {
            "name": name,
            "type_line": type_line,
            "cmc": row["cmc"],
            "primitives": prim_list,
            "color_identity": ci_list,
            "oracle_text": oracle_text,
            "score": score,
            "has_theme_overlap": theme_overlap > 0,
            "theme_overlap_count": theme_overlap,
        }
        if theme_overlap > 0:
            candidates_with_theme.append(cand)
        else:
            candidates_without_theme.append(cand)

    # Rank: theme-overlap cards first (descending overlap count + name
    # tiebreak for determinism), then non-overlap (alphabetical for
    # stability, since we don't have a corpus-prior here).
    candidates_with_theme.sort(key=lambda c: (-c["theme_overlap_count"], c["name"]))
    candidates_without_theme.sort(key=lambda c: c["name"])

    # 70/30 split: 70% theme-overlap, 30% non-overlap to ensure the LLM
    # sees some "weird" cards that aren't pre-filtered for theme.
    theme_count = min(len(candidates_with_theme), int(pool_size * 0.7))
    non_theme_count = pool_size - theme_count
    non_theme_count = min(len(candidates_without_theme), non_theme_count)
    candidates = candidates_with_theme[:theme_count] + candidates_without_theme[:non_theme_count]

    return {
        "version": WIDE_POOL_VERSION,
        "candidates": candidates,
        "color_identity": sorted(ci_set),
        "filter_summary": {
            "total_rows": total_rows,
            "after_ci": after_ci,
            "after_type": after_type,
            "after_exclude": after_exclude,
            "with_theme_overlap": len(candidates_with_theme),
            "without_theme_overlap": len(candidates_without_theme),
            "returned": len(candidates),
        },
        "warnings": warnings,
    }


def _parse_color_identity(raw: Any) -> List[str]:
    """Normalize color_identity from the DB into a sorted uppercase list.
    Mirrors _normalize_color_identity in agent_build_deck_v1 — duplicated
    here to keep this module standalone (no cross-layer import)."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return sorted({c.upper() for c in raw if isinstance(c, str) and c})
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        if s.startswith("["):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return sorted({c.upper() for c in parsed if isinstance(c, str) and c})
            except Exception:
                pass
        return sorted({c.strip().upper() for c in s.split(",") if c.strip()})
    return []
