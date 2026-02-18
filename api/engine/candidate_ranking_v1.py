from typing import Any, Dict, List

_BUCKET_ORDER_V1 = ("ramp", "draw", "interaction", "protection", "wincon")


def _normalize_primitives(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    out: List[str] = []
    for item in value:
        if isinstance(item, str):
            clean = item.strip()
            if clean != "":
                out.append(clean)
    return out


def _primitive_matches_bucket(primitive: str, bucket: str) -> bool:
    upper = primitive.upper()
    if bucket == "ramp":
        return ("RAMP" in upper) or ("MANA_FIX" in upper) or ("TREASURE" in upper)
    if bucket == "draw":
        return ("DRAW" in upper) or ("ADVANTAGE" in upper) or ("LOOT" in upper)
    if bucket == "interaction":
        return ("REMOVAL" in upper) or ("COUNTER" in upper) or ("BOARD_WIPE" in upper) or ("DISRUPT" in upper)
    if bucket == "protection":
        return ("PROTECTION" in upper) or ("HEXPROOF" in upper) or ("WARD" in upper) or ("INDESTRUCT" in upper)
    if bucket == "wincon":
        return (
            ("WIN" in upper)
            or ("FINISH" in upper)
            or ("TOKEN_PRODUCTION" in upper)
            or ("COMBAT" in upper)
            or ("DAMAGE" in upper)
            or ("STORM" in upper)
        )
    return False


def _to_str_set(value: Any) -> set[str]:
    if not isinstance(value, list):
        return set()
    out: set[str] = set()
    for item in value:
        if isinstance(item, str) and item.strip() != "":
            out.add(item)
    return out


def _extract_missing_targets(missing_targets: Dict[str, Any]) -> tuple[set[str], List[str]]:
    missing_targets_obj = missing_targets if isinstance(missing_targets, dict) else {}
    missing_primitives = _to_str_set(missing_targets_obj.get("missing_primitives"))

    missing_by_bucket = missing_targets_obj.get("missing_by_bucket")
    missing_by_bucket = missing_by_bucket if isinstance(missing_by_bucket, dict) else {}
    missing_buckets: List[str] = []
    for bucket in _BUCKET_ORDER_V1:
        missing_value = missing_by_bucket.get(bucket)
        if isinstance(missing_value, int) and missing_value > 0:
            missing_buckets.append(bucket)

    return missing_primitives, missing_buckets


def _gc_penalty(is_game_changer: bool, gc_remaining: int | None) -> int:
    if not is_game_changer:
        return 0
    if not isinstance(gc_remaining, int):
        return 0
    if gc_remaining <= 0:
        return 2
    if gc_remaining == 1:
        return 1
    return 0


def rank_candidates_v1(
    candidates: list[dict],
    deck_state: dict,
    hypothesis: dict | None,
    missing_targets: dict,
    gc_remaining: int | None,
) -> list[dict]:
    """Return candidates sorted deterministically by strategic value."""

    candidate_rows = candidates if isinstance(candidates, list) else []
    deck_state_obj = deck_state if isinstance(deck_state, dict) else {}
    hypothesis_obj = hypothesis if isinstance(hypothesis, dict) else {}

    commander_primitives_set = _to_str_set(deck_state_obj.get("commander_primitives"))
    anchor_primitives_set = _to_str_set(deck_state_obj.get("anchor_primitives"))
    hypothesis_primitives_set = _to_str_set(hypothesis_obj.get("core_primitives"))

    missing_primitives_set, missing_buckets = _extract_missing_targets(missing_targets)

    ranked_rows: List[Dict[str, Any]] = []
    for candidate in candidate_rows:
        if not isinstance(candidate, dict):
            continue
        name = candidate.get("name")
        if not isinstance(name, str) or name == "":
            continue

        primitives_set = set(_normalize_primitives(candidate.get("primitives")))

        covers_missing_primitives_count = len([
            primitive
            for primitive in missing_primitives_set
            if primitive in primitives_set
        ])

        covered_missing_buckets = [
            bucket
            for bucket in missing_buckets
            if any(_primitive_matches_bucket(primitive, bucket) for primitive in primitives_set)
        ]
        covers_missing_targets_count = len(covered_missing_buckets)

        anchor_overlap_score = len([p for p in primitives_set if p in anchor_primitives_set])
        commander_alignment_score = len([p for p in primitives_set if p in commander_primitives_set])
        hypothesis_alignment_score = len([p for p in primitives_set if p in hypothesis_primitives_set])

        role_compression_score = covers_missing_targets_count
        if covers_missing_primitives_count > 0 and covers_missing_targets_count > 0:
            role_compression_score += 1

        gc_penalty = _gc_penalty(
            is_game_changer=bool(candidate.get("is_game_changer")),
            gc_remaining=gc_remaining,
        )

        rank_key = (
            -int(role_compression_score),
            -int(covers_missing_targets_count),
            -int(covers_missing_primitives_count),
            -int(hypothesis_alignment_score),
            -int(commander_alignment_score),
            -int(anchor_overlap_score),
            int(gc_penalty),
            name,
        )

        row_out = dict(candidate)
        row_out["ranking_signals_v1"] = {
            "role_compression_score": int(role_compression_score),
            "covers_missing_targets_count": int(covers_missing_targets_count),
            "covers_missing_primitives_count": int(covers_missing_primitives_count),
            "hypothesis_alignment_score": int(hypothesis_alignment_score),
            "commander_alignment_score": int(commander_alignment_score),
            "anchor_overlap_score": int(anchor_overlap_score),
            "gc_penalty": int(gc_penalty),
        }
        row_out["_rank_key_v1"] = rank_key
        ranked_rows.append(row_out)

    ranked_rows.sort(key=lambda row: row.get("_rank_key_v1"))
    for row in ranked_rows:
        row.pop("_rank_key_v1", None)

    return ranked_rows
