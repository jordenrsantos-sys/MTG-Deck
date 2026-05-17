"""v1.7 Stage 4 — Bracket-aware proactive game-changer recommendations.

Reads the chosen bracket's GC cap from
`api/engine/data/brackets/gc_limits_v1.json` (via the existing
`resolve_gc_limits` helper) and the curated GC card list
(`api.engine.constants.GAME_CHANGERS_SET`, sourced from
`data/game_changers/gc_v0_userlist_2025-11-20.json`). Both data
files are READ-ONLY HARD safeties.

When the bracket allows GCs and the deck has fewer than the cap,
this layer emits proactive `BRACKET_AWARE_GC` recommendations into
the Power Tune `recommended_swaps_v1` array. Each recommendation
is a NEW swap row pairing a deck card (the suggested cut, chosen
heuristically from the existing cut-candidate ranking) with a GC
card filtered for the commander's color identity.

The reason is encoded as the tagged string
    "BRACKET_AWARE_GC:<json>"
with payload `{recommended_gc_oracle_id, recommended_gc_name,
current_deck_gc_count, bracket_max_gc}`, mirroring the
COMBO_ENABLER pattern from Stage 2 so it flows through api/main.py's
strict `reasons_v1: List[str]` Pydantic filter (HARD BYTE-IDENTICAL).

Scaling targets per bracket (when current_deck_gc_count == 0):
    B1, B2 → 0       (max=0, GCs disallowed)
    B3     → 2       (max=3, room for ≥1)
    B4     → 3       (max=5, room for ≥2)
    B5     → 3       (max=null/unlimited, room for ≥3)
    other  → max(1, min(3, max - current))

Production wiring note: this stage ships the layer as a public
function + convenience wrapper. The FastAPI Power Tune route
(api/main.py) is on the v1.7 HARD-safety BYTE-IDENTICAL list and is
NOT modified here. A future stage will wire the wrapper into
api/main.py when the api/main.py freeze lifts.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Set, Tuple

from api.engine.bracket_gc_limits import resolve_gc_limits
from api.engine.constants import GAME_CHANGERS_SET
from api.engine.db_cards import lookup_cards_by_oracle_ids
from engine.db import find_card_by_name


BRACKET_AWARE_RECOMMENDATIONS_V1_VERSION = "bracket_aware_recommendations_v1"
BRACKET_AWARE_GC_REASON_PREFIX = "BRACKET_AWARE_GC:"

_PER_BRACKET_TARGET: Dict[str, int] = {
    "B1": 0,
    "B2": 0,
    "B3": 2,
    "B4": 3,
    "B5": 3,
}


def _normalize_color_identity(value: Any) -> Set[str]:
    if value is None:
        return set()
    if isinstance(value, (list, tuple, set)):
        out: Set[str] = set()
        for c in value:
            if isinstance(c, str):
                t = c.strip().upper()
                if t in {"W", "U", "B", "R", "G"}:
                    out.add(t)
        return out
    if isinstance(value, str):
        token = value.strip()
        if token.startswith("[") and token.endswith("]"):
            try:
                parsed = json.loads(token)
                if isinstance(parsed, list):
                    return _normalize_color_identity(parsed)
            except (json.JSONDecodeError, TypeError):
                pass
        out = set()
        for c in token:
            if c.upper() in {"W", "U", "B", "R", "G"}:
                out.add(c.upper())
        return out
    return set()


def _color_identity_legal(card_ci: Set[str], commander_ci: Set[str]) -> bool:
    return card_ci.issubset(commander_ci)


def _resolve_commander_ci(snapshot_id: str, commander_name: str) -> Optional[Set[str]]:
    card = find_card_by_name(snapshot_id, commander_name)
    if not isinstance(card, dict):
        return None
    return _normalize_color_identity(card.get("color_identity"))


def _count_existing_gcs_in_deck(deck_cards: List[str], commander_names: List[str]) -> int:
    gc_set = GAME_CHANGERS_SET if isinstance(GAME_CHANGERS_SET, (set, frozenset)) else set()
    count = 0
    for n in (commander_names or []):
        if isinstance(n, str) and n in gc_set:
            count += 1
    for n in (deck_cards or []):
        if isinstance(n, str) and n in gc_set:
            count += 1
    return count


def _resolve_target_count(
    bracket_id: str,
    *,
    gc_max: Optional[int],
    current_count: int,
) -> int:
    if gc_max is not None:
        room = max(0, int(gc_max) - int(current_count))
        if room <= 0:
            return 0
    else:
        # unlimited (B5)-style cap: floor at 3 per spec.
        room = 999

    per_bracket = _PER_BRACKET_TARGET.get(bracket_id)
    if per_bracket is not None:
        return min(per_bracket, room)
    return max(1, min(3, room)) if room > 0 else 0


def _resolve_legal_gc_candidates(
    *,
    snapshot_id: str,
    commander_ci: Set[str],
    deck_card_set: Set[str],
    commander_set: Set[str],
) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    excluded = deck_card_set | commander_set
    for name in sorted(GAME_CHANGERS_SET):
        if name in excluded:
            continue
        card = find_card_by_name(snapshot_id, name)
        if not isinstance(card, dict):
            continue
        oracle_id = card.get("oracle_id")
        card_ci = _normalize_color_identity(card.get("color_identity"))
        if not isinstance(oracle_id, str) or oracle_id == "":
            continue
        if not _color_identity_legal(card_ci, commander_ci):
            continue
        candidates.append({"oracle_id": oracle_id, "name": name})
    return candidates


def _encode_bracket_aware_gc_reason(
    *,
    recommended_gc_oracle_id: str,
    recommended_gc_name: str,
    current_deck_gc_count: int,
    bracket_max_gc: Optional[int],
) -> str:
    payload = {
        "recommended_gc_oracle_id": recommended_gc_oracle_id,
        "recommended_gc_name": recommended_gc_name,
        "current_deck_gc_count": int(current_deck_gc_count),
        "bracket_max_gc": int(bracket_max_gc) if bracket_max_gc is not None else None,
    }
    return BRACKET_AWARE_GC_REASON_PREFIX + json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def _pick_suggested_cut(
    *,
    existing_swaps: List[Dict[str, Any]],
    deck_cards: List[str],
) -> str:
    # Prefer a cut already nominated by Power Tune's own evaluation —
    # gives the user a coherent "X out / Y in" pairing. Fall back to
    # the first non-empty deck card if no prior swap rows exist.
    for swap in existing_swaps:
        if not isinstance(swap, dict):
            continue
        cut_name = swap.get("cut_name")
        if isinstance(cut_name, str) and cut_name.strip() != "":
            return cut_name.strip()
    for n in deck_cards:
        if isinstance(n, str) and n.strip() != "":
            return n.strip()
    return ""


def attach_bracket_aware_gc_recommendations_v1(
    payload: Dict[str, Any],
    *,
    canonical_deck_input: Any,
    db_snapshot_id: str,
    bracket_id: str,
) -> Dict[str, Any]:
    """Post-process a Power Tune `payload` by appending BRACKET_AWARE_GC
    recommendations to `recommended_swaps_v1` when the bracket allows
    GCs and the deck has room. Returns a NEW payload dict; the input
    is not mutated.

    Safe-by-default: returns the input payload (deep copied at the
    `recommended_swaps_v1` level) when any precondition fails — no
    snapshot, no commander, no legal GC candidates, max=0 bracket,
    etc.
    """
    if not isinstance(payload, dict):
        return payload

    # Defensive copy of the layer this function mutates.
    out = dict(payload)
    existing_swaps_raw = out.get("recommended_swaps_v1") if isinstance(out.get("recommended_swaps_v1"), list) else []
    existing_swaps: List[Dict[str, Any]] = [s for s in existing_swaps_raw if isinstance(s, dict)]
    out["recommended_swaps_v1"] = list(existing_swaps)

    snapshot_id = db_snapshot_id if isinstance(db_snapshot_id, str) and db_snapshot_id.strip() else ""
    if snapshot_id == "":
        return out

    bracket_token = bracket_id.strip() if isinstance(bracket_id, str) else ""
    if bracket_token == "":
        return out

    canonical = canonical_deck_input if isinstance(canonical_deck_input, dict) else {}
    commander_name = canonical.get("commander")
    if not isinstance(commander_name, str) or commander_name.strip() == "":
        return out
    commander_name = commander_name.strip()

    deck_cards_raw = canonical.get("cards") if isinstance(canonical.get("cards"), list) else []
    deck_cards: List[str] = [c.strip() for c in deck_cards_raw if isinstance(c, str) and c.strip() != ""]

    gc_min, gc_max, _version, _unknown = resolve_gc_limits(bracket_token)

    current_gc_count = _count_existing_gcs_in_deck(deck_cards, [commander_name])

    target = _resolve_target_count(bracket_token, gc_max=gc_max, current_count=current_gc_count)
    if target <= 0:
        return out

    commander_ci = _resolve_commander_ci(snapshot_id, commander_name)
    if commander_ci is None:
        return out

    deck_card_set = set(deck_cards)
    commander_set = {commander_name}

    legal_gcs = _resolve_legal_gc_candidates(
        snapshot_id=snapshot_id,
        commander_ci=commander_ci,
        deck_card_set=deck_card_set,
        commander_set=commander_set,
    )

    # De-dupe against any GCs already proposed by Power Tune's
    # regular pool — if Rhystic Study is already in the swaps via
    # ADD_PRIMITIVE_COVERAGE, don't re-propose it with a duplicate
    # BRACKET_AWARE_GC row (but DO annotate the existing row's
    # reasons_v1 with the bracket-aware code so the UI surfaces
    # the bracket context).
    existing_add_names: Set[str] = set()
    for s in existing_swaps:
        n = s.get("add_name")
        if isinstance(n, str) and n.strip() != "":
            existing_add_names.add(n.strip())

    appended = 0
    annotated_existing = 0
    new_rows: List[Dict[str, Any]] = []
    annotated_swaps: List[Dict[str, Any]] = list(existing_swaps)

    for cand in legal_gcs:
        if appended + annotated_existing >= target:
            break
        gc_name = cand["name"]
        gc_oracle = cand["oracle_id"]
        reason_str = _encode_bracket_aware_gc_reason(
            recommended_gc_oracle_id=gc_oracle,
            recommended_gc_name=gc_name,
            current_deck_gc_count=current_gc_count,
            bracket_max_gc=gc_max,
        )

        if gc_name in existing_add_names:
            # Annotate the matching existing swap row in-place.
            for i, s in enumerate(annotated_swaps):
                if s.get("add_name") == gc_name:
                    reasons = list(s.get("reasons_v1") or [])
                    if reason_str not in reasons:
                        reasons.append(reason_str)
                        new_s = dict(s)
                        new_s["reasons_v1"] = sorted(set(reasons))
                        annotated_swaps[i] = new_s
                        annotated_existing += 1
                    break
            continue

        suggested_cut = _pick_suggested_cut(
            existing_swaps=annotated_swaps,
            deck_cards=deck_cards,
        )
        if suggested_cut == "":
            continue
        new_rows.append({
            "cut_name": suggested_cut,
            "add_name": gc_name,
            "cut_oracle_id": "",
            "add_oracle_id": gc_oracle,
            "reasons_v1": [reason_str],
            "delta_summary_v1": {
                "total_score_delta_v1": 0.0,
                "coherence_delta_v1": 0.0,
                "primitive_coverage_delta_v1": 0,
                "missing_required_count_delta_v1": 0,
                "gc_compliance_preserved_v1": True,
            },
        })
        appended += 1

    out["recommended_swaps_v1"] = annotated_swaps + new_rows
    return out


def run_deck_tune_with_bracket_aware_recommendations_v1(
    *,
    canonical_deck_input: Any,
    baseline_build_result: Any,
    db_snapshot_id: str,
    bracket_id: str,
    profile_id: str,
    mulligan_model_id: str,
    max_swaps: int,
    collect_dev_metrics: bool = False,
) -> Dict[str, Any]:
    """Convenience wrapper: run Power Tune then attach bracket-aware GC
    recommendations. Future stage will wire this into api/main.py when
    the api/main.py BYTE-IDENTICAL freeze lifts.
    """
    # Local import to avoid module-import cycles and to keep the
    # post-processor decoupled from Power Tune's internals.
    from api.engine.deck_tune_engine_v1 import run_deck_tune_engine_v1

    payload = run_deck_tune_engine_v1(
        canonical_deck_input=canonical_deck_input,
        baseline_build_result=baseline_build_result,
        db_snapshot_id=db_snapshot_id,
        bracket_id=bracket_id,
        profile_id=profile_id,
        mulligan_model_id=mulligan_model_id,
        max_swaps=max_swaps,
        collect_dev_metrics=collect_dev_metrics,
    )
    return attach_bracket_aware_gc_recommendations_v1(
        payload,
        canonical_deck_input=canonical_deck_input,
        db_snapshot_id=db_snapshot_id,
        bracket_id=bracket_id,
    )
