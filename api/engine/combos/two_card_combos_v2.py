from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

from api.engine.two_card_combos import TWO_CARD_COMBOS_V1_VERSION, load_two_card_combos_v1


TWO_CARD_COMBOS_V2_VERSION = "two_card_combos_v2"
TWO_CARD_COMBOS_MATCH_MAX = 25

_TWO_CARD_COMBOS_V2_FILE = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "combos"
    / "two_card_combos_v2.json"
)


def _runtime_error(code: str, detail: str) -> RuntimeError:
    return RuntimeError(f"{code}: {detail}")


def _runtime_error_code(exc: RuntimeError) -> str:
    message = str(exc)
    if ":" in message:
        return message.split(":", 1)[0].strip()
    return message.strip()


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _canonical_card_key_optional(value: Any) -> str | None:
    if isinstance(value, dict):
        for field_name in ("oracle_id", "name"):
            field_value = value.get(field_name)
            if isinstance(field_value, str):
                token = " ".join(field_value.strip().split()).lower()
                if token != "":
                    return token
        return None

    if not isinstance(value, str):
        return None
    token = " ".join(value.strip().split()).lower()
    if token == "":
        return None
    return token


def _canonical_card_key(value: Any, *, field_path: str) -> str:
    if not isinstance(value, str):
        raise _runtime_error("TWO_CARD_COMBOS_V2_INVALID", f"{field_path} must be a string")

    token = " ".join(value.strip().split()).lower()
    if token == "":
        raise _runtime_error("TWO_CARD_COMBOS_V2_INVALID", f"{field_path} must be a non-empty string")
    return token


def _normalize_variant_ids(raw: Any, *, field_path: str) -> List[str]:
    if not isinstance(raw, list):
        raise _runtime_error("TWO_CARD_COMBOS_V2_INVALID", f"{field_path} must be a list")

    variant_ids = sorted(
        {
            token
            for token in (_nonempty_str(item) for item in raw)
            if token is not None
        }
    )
    if len(variant_ids) == 0:
        raise _runtime_error("TWO_CARD_COMBOS_V2_INVALID", f"{field_path} must include at least one variant id")
    return variant_ids


def _normalize_pair(raw: Any, *, index: int) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise _runtime_error("TWO_CARD_COMBOS_V2_INVALID", f"pairs[{index}] must be an object")

    a_key = _canonical_card_key(raw.get("a"), field_path=f"pairs[{index}].a")
    b_key = _canonical_card_key(raw.get("b"), field_path=f"pairs[{index}].b")

    if a_key == b_key:
        raise _runtime_error("TWO_CARD_COMBOS_V2_INVALID", f"pairs[{index}] must reference two distinct card keys")

    if not a_key < b_key:
        raise _runtime_error("TWO_CARD_COMBOS_V2_INVALID", f"pairs[{index}] must keep canonical ordering a < b")

    variant_ids = _normalize_variant_ids(raw.get("variant_ids"), field_path=f"pairs[{index}].variant_ids")

    return {
        "a": a_key,
        "b": b_key,
        "variant_ids": variant_ids,
    }


def load_two_card_combos_v2() -> Dict[str, Any]:
    if not _TWO_CARD_COMBOS_V2_FILE.is_file():
        raise _runtime_error("TWO_CARD_COMBOS_V2_MISSING", str(_TWO_CARD_COMBOS_V2_FILE))

    try:
        parsed = json.loads(_TWO_CARD_COMBOS_V2_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        raise _runtime_error("TWO_CARD_COMBOS_V2_INVALID_JSON", str(_TWO_CARD_COMBOS_V2_FILE)) from exc

    if not isinstance(parsed, dict):
        raise _runtime_error("TWO_CARD_COMBOS_V2_INVALID", "root must be an object")

    version = _nonempty_str(parsed.get("version"))
    if version != TWO_CARD_COMBOS_V2_VERSION:
        raise _runtime_error("TWO_CARD_COMBOS_V2_INVALID", f"version must equal '{TWO_CARD_COMBOS_V2_VERSION}'")

    pairs_raw = parsed.get("pairs")
    if not isinstance(pairs_raw, list):
        raise _runtime_error("TWO_CARD_COMBOS_V2_INVALID", "pairs must be a list")

    normalized_pairs: List[Dict[str, Any]] = []
    seen_pairs: set[Tuple[str, str]] = set()

    for index, pair_raw in enumerate(pairs_raw):
        pair = _normalize_pair(pair_raw, index=index)
        pair_key = (pair["a"], pair["b"])
        if pair_key in seen_pairs:
            raise _runtime_error("TWO_CARD_COMBOS_V2_INVALID", f"duplicate pair {pair_key}")
        seen_pairs.add(pair_key)
        normalized_pairs.append(pair)

    normalized_pairs_sorted = sorted(normalized_pairs, key=lambda row: (str(row.get("a") or ""), str(row.get("b") or "")))

    return {
        "version": TWO_CARD_COMBOS_V2_VERSION,
        "pairs": normalized_pairs_sorted,
    }


def derive_two_card_combos_v2_from_variants(variants_payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(variants_payload, dict):
        raise _runtime_error("TWO_CARD_COMBOS_V2_INVALID", "variants payload must be an object")

    variants_raw = variants_payload.get("variants")
    if not isinstance(variants_raw, list):
        raise _runtime_error("TWO_CARD_COMBOS_V2_INVALID", "variants payload must include variants list")

    grouped_variant_ids: Dict[Tuple[str, str], set[str]] = {}

    for index, variant_raw in enumerate(variants_raw):
        if not isinstance(variant_raw, dict):
            raise _runtime_error("TWO_CARD_COMBOS_V2_INVALID", f"variants[{index}] must be an object")

        variant_id = _nonempty_str(variant_raw.get("variant_id"))
        if variant_id is None:
            raise _runtime_error("TWO_CARD_COMBOS_V2_INVALID", f"variants[{index}].variant_id must be a non-empty string")

        cards_raw = variant_raw.get("cards")
        if not isinstance(cards_raw, list):
            raise _runtime_error("TWO_CARD_COMBOS_V2_INVALID", f"variants[{index}].cards must be a list")

        cards = sorted(
            {
                _canonical_card_key(item, field_path=f"variants[{index}].cards[]")
                for item in cards_raw
            }
        )

        if len(cards) != 2:
            continue

        pair_key = (cards[0], cards[1])
        grouped_variant_ids.setdefault(pair_key, set()).add(variant_id)

    pairs = [
        {
            "a": pair_key[0],
            "b": pair_key[1],
            "variant_ids": sorted(variant_ids),
        }
        for pair_key, variant_ids in sorted(grouped_variant_ids.items(), key=lambda item: item[0])
    ]

    return {
        "version": TWO_CARD_COMBOS_V2_VERSION,
        "pairs": pairs,
    }


def _normalize_pairs_from_v1(v1_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    pairs_raw = v1_payload.get("pairs")
    if not isinstance(pairs_raw, list):
        raise _runtime_error("TWO_CARD_COMBOS_V2_INVALID", "legacy v1 payload pairs must be a list")

    grouped_variant_ids: Dict[Tuple[str, str], set[str]] = {}
    for index, pair_raw in enumerate(pairs_raw):
        if not isinstance(pair_raw, dict):
            raise _runtime_error("TWO_CARD_COMBOS_V2_INVALID", f"legacy pairs[{index}] must be an object")

        a_key = _canonical_card_key(pair_raw.get("a"), field_path=f"legacy pairs[{index}].a")
        b_key = _canonical_card_key(pair_raw.get("b"), field_path=f"legacy pairs[{index}].b")

        if a_key == b_key:
            raise _runtime_error("TWO_CARD_COMBOS_V2_INVALID", f"legacy pairs[{index}] must reference two distinct card keys")

        pair_key = tuple(sorted((a_key, b_key)))
        pair_id = _nonempty_str(pair_raw.get("id")) or f"LEGACY_PAIR_{index + 1:06d}"
        grouped_variant_ids.setdefault(pair_key, set()).add(pair_id)

    return [
        {
            "a": pair_key[0],
            "b": pair_key[1],
            "variant_ids": sorted(variant_ids),
        }
        for pair_key, variant_ids in sorted(grouped_variant_ids.items(), key=lambda item: item[0])
    ]


def load_two_card_combos_prefer_v2() -> Dict[str, Any]:
    try:
        return load_two_card_combos_v2()
    except RuntimeError as exc:
        if _runtime_error_code(exc) != "TWO_CARD_COMBOS_V2_MISSING":
            raise

    legacy_payload = load_two_card_combos_v1()
    return {
        "version": TWO_CARD_COMBOS_V1_VERSION,
        "pairs": _normalize_pairs_from_v1(legacy_payload),
    }


def detect_two_card_combos(deck_card_keys: Any, *, max_matches: int = TWO_CARD_COMBOS_MATCH_MAX) -> Dict[str, Any]:
    bounded_max = max_matches if isinstance(max_matches, int) and not isinstance(max_matches, bool) else TWO_CARD_COMBOS_MATCH_MAX
    if bounded_max < 0:
        bounded_max = 0

    try:
        combos_payload = load_two_card_combos_prefer_v2()
    except RuntimeError as exc:
        return {
            "version": None,
            "supported": False,
            "count": None,
            "matches": [],
            "error_code": _runtime_error_code(exc),
        }

    pairs = combos_payload.get("pairs") if isinstance(combos_payload.get("pairs"), list) else []

    if isinstance(deck_card_keys, dict):
        deck_key_candidates = list(deck_card_keys.values())
    elif isinstance(deck_card_keys, list):
        deck_key_candidates = list(deck_card_keys)
    else:
        deck_key_candidates = []

    deck_keys = sorted(
        {
            token
            for token in (_canonical_card_key_optional(item) for item in deck_key_candidates)
            if token is not None
        }
    )

    deck_key_set = set(deck_keys)

    total_count = 0
    matches_top: List[Dict[str, Any]] = []

    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        a_key = _nonempty_str(pair.get("a"))
        b_key = _nonempty_str(pair.get("b"))
        if a_key is None or b_key is None:
            continue
        variant_ids_raw = pair.get("variant_ids")
        variant_ids = (
            sorted(
                {
                    token
                    for token in (_nonempty_str(item) for item in variant_ids_raw)
                    if token is not None
                }
            )
            if isinstance(variant_ids_raw, list)
            else []
        )

        if a_key in deck_key_set and b_key in deck_key_set:
            total_count += 1
            if len(matches_top) < bounded_max:
                match_row: Dict[str, Any] = {
                    "a": a_key,
                    "b": b_key,
                }
                if len(variant_ids) > 0:
                    match_row["variant_ids"] = variant_ids
                matches_top.append(match_row)

    return {
        "version": _nonempty_str(combos_payload.get("version")),
        "supported": True,
        "count": int(total_count),
        "matches": matches_top,
    }
