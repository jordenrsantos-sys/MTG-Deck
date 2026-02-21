from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from api.engine.curated_pack_manifest_v1 import resolve_pack_file_path

TWO_CARD_COMBOS_V1_VERSION = "two_card_combos_v1"

_TWO_CARD_COMBOS_FILE: Path | None = None
_EXPECTED_MODE = "pairs_only"


def _runtime_error(code: str, detail: str) -> RuntimeError:
    return RuntimeError(f"{code}: {detail}")


def _resolve_two_card_combos_v1_file() -> Path:
    if isinstance(_TWO_CARD_COMBOS_FILE, Path):
        return _TWO_CARD_COMBOS_FILE

    try:
        return resolve_pack_file_path(
            pack_id="two_card_combos_v1",
            pack_version=TWO_CARD_COMBOS_V1_VERSION,
        )
    except RuntimeError as exc:
        code = str(exc).split(":", 1)[0].strip()
        if code in {
            "CURATED_PACK_MANIFEST_V1_MISSING",
            "CURATED_PACK_MANIFEST_V1_INVALID_JSON",
            "CURATED_PACK_MANIFEST_V1_INVALID",
            "CURATED_PACK_MANIFEST_V1_DUPLICATE_ENTRY",
            "CURATED_PACK_MANIFEST_V1_PACK_NOT_FOUND",
            "CURATED_PACK_MANIFEST_V1_FILE_MISSING",
        }:
            raise _runtime_error("TWO_CARD_COMBOS_V1_MISSING", str(exc)) from exc
        raise


def _canonical_card_key(value: Any) -> str | None:
    if not isinstance(value, str):
        return None

    token = " ".join(value.strip().split()).lower()
    if token == "":
        return None
    return token


def _normalize_pair(raw: Any, *, index: int) -> Dict[str, str]:
    if not isinstance(raw, dict):
        raise _runtime_error("TWO_CARD_COMBOS_V1_INVALID", f"pairs[{index}] must be an object")

    pair_id_raw = raw.get("id")
    if not isinstance(pair_id_raw, str) or pair_id_raw.strip() == "":
        raise _runtime_error("TWO_CARD_COMBOS_V1_INVALID", f"pairs[{index}].id must be a non-empty string")
    pair_id = pair_id_raw.strip()

    a_key = _canonical_card_key(raw.get("a"))
    if a_key is None:
        raise _runtime_error("TWO_CARD_COMBOS_V1_INVALID", f"pairs[{index}].a must be a non-empty string")

    b_key = _canonical_card_key(raw.get("b"))
    if b_key is None:
        raise _runtime_error("TWO_CARD_COMBOS_V1_INVALID", f"pairs[{index}].b must be a non-empty string")

    if a_key == b_key:
        raise _runtime_error("TWO_CARD_COMBOS_V1_INVALID", f"pairs[{index}] must reference two distinct card keys")

    normalized: Dict[str, str] = {
        "id": pair_id,
        "a": a_key,
        "b": b_key,
    }

    if "label" in raw:
        label_raw = raw.get("label")
        if not isinstance(label_raw, str) or label_raw.strip() == "":
            raise _runtime_error("TWO_CARD_COMBOS_V1_INVALID", f"pairs[{index}].label must be a non-empty string")
        normalized["label"] = label_raw.strip()

    return normalized


def load_two_card_combos_v1() -> dict:
    combos_file = _resolve_two_card_combos_v1_file()
    if not combos_file.is_file():
        raise _runtime_error("TWO_CARD_COMBOS_V1_MISSING", str(combos_file))

    try:
        parsed = json.loads(combos_file.read_text(encoding="utf-8"))
    except Exception as exc:
        raise _runtime_error("TWO_CARD_COMBOS_V1_INVALID_JSON", str(combos_file)) from exc

    if not isinstance(parsed, dict):
        raise _runtime_error("TWO_CARD_COMBOS_V1_INVALID", "root must be an object")

    version_raw = parsed.get("version")
    if not isinstance(version_raw, str) or version_raw.strip() != TWO_CARD_COMBOS_V1_VERSION:
        raise _runtime_error(
            "TWO_CARD_COMBOS_V1_INVALID",
            f"version must equal '{TWO_CARD_COMBOS_V1_VERSION}'",
        )

    mode_raw = parsed.get("mode")
    if not isinstance(mode_raw, str) or mode_raw.strip() != _EXPECTED_MODE:
        raise _runtime_error("TWO_CARD_COMBOS_V1_INVALID", f"mode must equal '{_EXPECTED_MODE}'")

    pairs_raw = parsed.get("pairs")
    if not isinstance(pairs_raw, list):
        raise _runtime_error("TWO_CARD_COMBOS_V1_INVALID", "pairs must be a list")

    normalized_pairs: List[Dict[str, str]] = []
    seen_ids: set[str] = set()
    seen_pair_keys: set[tuple[str, str]] = set()

    for index, pair_raw in enumerate(pairs_raw):
        pair = _normalize_pair(pair_raw, index=index)

        pair_id = pair["id"]
        if pair_id in seen_ids:
            raise _runtime_error("TWO_CARD_COMBOS_V1_INVALID", f"duplicate pair id '{pair_id}'")
        seen_ids.add(pair_id)

        pair_key = tuple(sorted((pair["a"], pair["b"])))
        if pair_key in seen_pair_keys:
            raise _runtime_error("TWO_CARD_COMBOS_V1_INVALID", f"duplicate combo pair {pair_key}")
        seen_pair_keys.add(pair_key)

        normalized_pairs.append(pair)

    normalized_pairs_sorted = sorted(
        normalized_pairs,
        key=lambda pair: (pair.get("a", ""), pair.get("b", ""), pair.get("id", "")),
    )

    return {
        "version": TWO_CARD_COMBOS_V1_VERSION,
        "mode": _EXPECTED_MODE,
        "pairs": normalized_pairs_sorted,
    }


def detect_two_card_combos(deck_card_keys: list[str]) -> dict:
    payload = load_two_card_combos_v1()
    pairs = payload.get("pairs") if isinstance(payload.get("pairs"), list) else []

    deck_keys_clean: List[str] = []
    if isinstance(deck_card_keys, list):
        for key in deck_card_keys:
            normalized_key = _canonical_card_key(key)
            if normalized_key is not None:
                deck_keys_clean.append(normalized_key)

    deck_key_set = set(sorted(set(deck_keys_clean)))

    matches: List[Dict[str, str]] = []
    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        pair_id = pair.get("id")
        a_key = pair.get("a")
        b_key = pair.get("b")
        if not isinstance(pair_id, str) or not isinstance(a_key, str) or not isinstance(b_key, str):
            continue

        if a_key in deck_key_set and b_key in deck_key_set:
            matches.append(
                {
                    "id": pair_id,
                    "a": a_key,
                    "b": b_key,
                }
            )

    matches_sorted = sorted(matches, key=lambda pair: (pair.get("id", ""), pair.get("a", ""), pair.get("b", "")))

    return {
        "version": TWO_CARD_COMBOS_V1_VERSION,
        "count": len(matches_sorted),
        "matches": matches_sorted,
    }
