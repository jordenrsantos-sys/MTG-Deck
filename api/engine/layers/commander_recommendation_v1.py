"""Commander recommendation layer (Phase 2 — PATH C reduced ambition).

Recommends top_n commander candidates given a seed primitive set + seed color
identity. Output shape per Phase 2 PATH C spec:

  {
    "version": "commander_recommendation_v1",
    "status": "OK" | "INSUFFICIENT_SIGNAL",
    "top_n": int,
    "considered_count": int,
    "candidates": [
      {
        "name": str,
        "color_identity": List[str],
        "edge_score": float,
        "primitives_overlap": float,
        "oracle_id": str,
      },
      ...
    ],
  }

Path C reduced-ambition deliberate scope:
- DROPPED: theme_match + signature_completion + commander_role bracket-tier
  filter (per Phase 2 Path A halt: commander_contracts_v1_9 had only 4
  contracts; Path B halt: commander_role_system_v1 missing + themes_v1_5 not
  per-card).
- KEPT: color_identity superset filter with exact-match bonus, primitives
  overlap (Jaccard-like) against seed primitives, GAME_CHANGERS_SET density
  for power_marker signal.
- Phase 2.1 enrichment work deferred: theme_match (requires per-card theme-
  fit scorer); signature_completion (requires sig-vocab→commander mapping);
  bracket-tier filter (requires curated commander_role_system_v1 pack).

Edge formula (weights from `commander_recommendation_weights_v1.json`):
  edge_score = 0.6 × primitives_overlap
             + 0.25 × color_efficiency
             + 0.15 × power_marker_density

Determinism: same (seed_primitive_ids, seed_color_identity, snapshot_id, top_n,
weights) inputs -> bytewise-identical output. Tie-break by oracle_id alphabetical.

Calibration boundary discipline: this is RUNTIME engine code. Reads ONLY:
- `mtg.sqlite` cards table (read-only via existing `cards_db_connect()`)
- `commander_recommendation_weights_v1.json` runtime pack
- `GAME_CHANGERS_SET` runtime constant
Never reads calibration packs (external_decks_v1 / pilot_personalities_v1 /
playtest_changelog_v1).
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from api.engine.constants import GAME_CHANGERS_SET
from api.engine.db_cards import cards_db_connect

COMMANDER_RECOMMENDATION_V1_VERSION = "commander_recommendation_v1"

DEFAULT_TOP_N = 10

DEFAULT_WEIGHTS_PACK_PATH = (
    Path(__file__).resolve().parents[1]
    / "data" / "recommendations" / "commander_recommendation_weights_v1.json"
)

STATUS_OK = "OK"
STATUS_INSUFFICIENT_SIGNAL = "INSUFFICIENT_SIGNAL"

# Legality predicate via type_line OR oracle_text "can be your commander".
# Compiled as a SQL WHERE clause (cards table columns verified at stage 0
# discovery: snapshot_id, oracle_id, name, type_line, oracle_text, color_
# identity, primitives_json all present).
_COMMANDER_LEGALITY_WHERE = (
    "(type_line LIKE '%Legendary%Creature%' "
    "OR oracle_text LIKE '%can be your commander%')"
)


def _load_weights(weights_path: Optional[Path] = None) -> Dict[str, float]:
    path = weights_path if weights_path is not None else DEFAULT_WEIGHTS_PACK_PATH
    raw = json.loads(path.read_text(encoding="utf-8"))
    weights = raw.get("weights") or {}
    return {
        "primitives_overlap": float(weights.get("primitives_overlap", 0.6)),
        "color_efficiency": float(weights.get("color_efficiency", 0.25)),
        "power_marker_density": float(weights.get("power_marker_density", 0.15)),
    }


def _parse_color_identity(raw: Optional[str]) -> List[str]:
    """Parse the cards.color_identity column (JSON-string list like '["G"]')."""
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [c for c in parsed if isinstance(c, str)]


def _parse_primitives(raw: Optional[str]) -> List[str]:
    """Parse the cards.primitives_json column (JSON-string list like '["CARD_DRAW"]')."""
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [p for p in parsed if isinstance(p, str)]


def _color_identity_compatible(
    candidate_ci: List[str], seed_ci: List[str],
) -> bool:
    """A candidate is compatible iff seed_ci is a SUBSET of candidate_ci.

    (Per Commander rules: a card is legal in a deck iff every color in its
    cost is in the commander's color identity. The commander's CI must be a
    SUPERSET of the seed's combined colors.)
    """
    return set(seed_ci).issubset(set(candidate_ci))


def _color_efficiency(
    candidate_ci: List[str], seed_ci: List[str],
) -> float:
    """1.0 for exact match; decay for each extra color the candidate has.

    Exact match: 1.0
    Each extra color in candidate beyond seed: -0.1 (floor at 0.0)
    """
    if not _color_identity_compatible(candidate_ci, seed_ci):
        return 0.0
    extra = len(set(candidate_ci) - set(seed_ci))
    return max(0.0, 1.0 - 0.1 * extra)


def _primitives_overlap(
    candidate_primitives: List[str], seed_primitives: List[str],
) -> float:
    """Jaccard overlap |A ∩ B| / |A ∪ B| (returns 0.0 when both empty)."""
    a = set(candidate_primitives)
    b = set(seed_primitives)
    if not a and not b:
        return 0.0
    union = a | b
    if not union:
        return 0.0
    return len(a & b) / len(union)


def _power_marker_density(candidate_name: str) -> float:
    """1.0 if the candidate is in GAME_CHANGERS_SET; 0.0 otherwise."""
    return 1.0 if candidate_name in GAME_CHANGERS_SET else 0.0


def _edge_score(
    *,
    primitives_overlap: float,
    color_efficiency: float,
    power_marker_density: float,
    weights: Dict[str, float],
) -> float:
    return (
        weights["primitives_overlap"] * primitives_overlap
        + weights["color_efficiency"] * color_efficiency
        + weights["power_marker_density"] * power_marker_density
    )


def _enumerate_commander_candidates(
    *,
    snapshot_id: str,
    conn=None,
) -> List[Dict[str, Any]]:
    """Enumerate all commander-legal cards for the given snapshot.

    Uses an existing public helper (`cards_db_connect()` from `db_cards.py`)
    plus a SELECT against existing cards table columns. Per Path C safety
    #5 (relaxed): SELECT queries against existing tables are PERMITTED;
    only new helper modules under engine.db are forbidden.
    """
    own_conn = conn is None
    if own_conn:
        conn = cards_db_connect()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT name, oracle_id, color_identity, primitives_json "
                "FROM cards WHERE snapshot_id = ? AND " + _COMMANDER_LEGALITY_WHERE,
                (snapshot_id,),
            )
            rows = cur.fetchall()
        except sqlite3.OperationalError:
            # cards table missing or schema mismatch — treat as empty candidate
            # set rather than crashing the build pipeline. Hermetic test DBs
            # (per `tests/conftest.py`) intentionally lack the cards table.
            rows = []
    finally:
        if own_conn:
            conn.close()

    out: List[Dict[str, Any]] = []
    for name, oid, ci_raw, prim_raw in rows:
        if not isinstance(name, str) or not isinstance(oid, str):
            continue
        out.append({
            "name": name,
            "oracle_id": oid,
            "color_identity": _parse_color_identity(ci_raw),
            "primitives": _parse_primitives(prim_raw),
        })
    return out


def _empty_envelope(
    *,
    top_n: int,
    considered_count: int,
    status: str,
) -> Dict[str, Any]:
    return {
        "version": COMMANDER_RECOMMENDATION_V1_VERSION,
        "status": status,
        "top_n": top_n,
        "considered_count": considered_count,
        "candidates": [],
    }


def run_commander_recommendation_v1(
    seed_primitive_ids: List[str],
    seed_color_identity: List[str],
    snapshot_id: str,
    *,
    top_n: int = DEFAULT_TOP_N,
    weights: Optional[Dict[str, float]] = None,
    candidates_override: Optional[List[Dict[str, Any]]] = None,
    weights_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Recommend top_n commanders given seed primitives + color identity.

    `candidates_override` is for tests: bypasses the DB enumeration step.
    `weights_path` is for tests: lets the test inject a custom weights pack.
    """
    seed_primitive_ids_clean = [
        p for p in (seed_primitive_ids or []) if isinstance(p, str)
    ]
    seed_color_identity_clean = [
        c for c in (seed_color_identity or []) if isinstance(c, str)
    ]

    if not seed_primitive_ids_clean and not seed_color_identity_clean:
        return _empty_envelope(
            top_n=top_n, considered_count=0, status=STATUS_INSUFFICIENT_SIGNAL,
        )

    weights_resolved = weights if weights is not None else _load_weights(weights_path)

    if candidates_override is not None:
        candidates = list(candidates_override)
    else:
        candidates = _enumerate_commander_candidates(snapshot_id=snapshot_id)

    considered_count = len(candidates)

    scored: List[Tuple[float, str, Dict[str, Any]]] = []
    for c in candidates:
        ci = c.get("color_identity") or []
        if not _color_identity_compatible(ci, seed_color_identity_clean):
            continue
        prims = c.get("primitives") or []
        po = _primitives_overlap(prims, seed_primitive_ids_clean)
        ce = _color_efficiency(ci, seed_color_identity_clean)
        pmd = _power_marker_density(c.get("name") or "")
        edge = _edge_score(
            primitives_overlap=po, color_efficiency=ce,
            power_marker_density=pmd, weights=weights_resolved,
        )
        oid = c.get("oracle_id") or ""
        scored.append((edge, oid, {
            "name": c.get("name") or "",
            "color_identity": ci,
            "edge_score": edge,
            "primitives_overlap": po,
            "oracle_id": oid,
        }))

    # Sort by (-edge_score, oracle_id) — alphabetical tie-break per spec.
    scored.sort(key=lambda t: (-t[0], t[1]))

    top = [entry for _, _, entry in scored[:top_n]]

    status = STATUS_OK if scored else STATUS_INSUFFICIENT_SIGNAL
    return {
        "version": COMMANDER_RECOMMENDATION_V1_VERSION,
        "status": status,
        "top_n": top_n,
        "considered_count": considered_count,
        "candidates": top,
    }
