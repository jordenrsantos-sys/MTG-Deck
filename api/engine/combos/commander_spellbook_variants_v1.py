from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from api.engine.curated_pack_manifest_v1 import resolve_pack_file_path


SPELLBOOK_VARIANTS_V1_VERSION = "commander_spellbook_variants_v1"
_SPELLBOOK_VARIANTS_V1_PACK_ID = "commander_spellbook_variants_v1"

_SPELLBOOK_VARIANTS_V1_FILE: Path | None = None


def _runtime_error(code: str, detail: str) -> RuntimeError:
    return RuntimeError(f"{code}: {detail}")


def _resolve_spellbook_variants_v1_file() -> Path:
    if isinstance(_SPELLBOOK_VARIANTS_V1_FILE, Path):
        return _SPELLBOOK_VARIANTS_V1_FILE

    try:
        return resolve_pack_file_path(
            pack_id=_SPELLBOOK_VARIANTS_V1_PACK_ID,
            pack_version=SPELLBOOK_VARIANTS_V1_VERSION,
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
            raise _runtime_error("SPELLBOOK_VARIANTS_V1_MISSING", str(exc)) from exc
        raise


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _canonical_card_key(value: Any, *, field_path: str) -> str:
    if not isinstance(value, str):
        raise _runtime_error("SPELLBOOK_VARIANTS_V1_INVALID", f"{field_path} must be a string")

    token = " ".join(value.strip().split()).lower()
    if token == "":
        raise _runtime_error("SPELLBOOK_VARIANTS_V1_INVALID", f"{field_path} must be a non-empty string")
    return token


def _normalize_cards(raw: Any, *, field_path: str) -> List[str]:
    if not isinstance(raw, list):
        raise _runtime_error("SPELLBOOK_VARIANTS_V1_INVALID", f"{field_path} must be a list")

    cards = sorted(
        {
            _canonical_card_key(item, field_path=f"{field_path}[]")
            for item in raw
        }
    )
    if len(cards) < 2:
        raise _runtime_error(
            "SPELLBOOK_VARIANTS_V1_INVALID",
            f"{field_path} must include at least two card keys",
        )
    return cards


def _normalize_tags(raw: Any, *, field_path: str) -> List[str] | None:
    if raw is None:
        return None
    if not isinstance(raw, list):
        raise _runtime_error("SPELLBOOK_VARIANTS_V1_INVALID", f"{field_path} must be a list")

    tags = sorted(
        {
            token
            for token in (_nonempty_str(item) for item in raw)
            if token is not None
        }
    )
    return tags if len(tags) > 0 else None


def _normalize_variant(raw: Any, *, index: int) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise _runtime_error("SPELLBOOK_VARIANTS_V1_INVALID", f"variants[{index}] must be an object")

    variant_id = _nonempty_str(raw.get("variant_id"))
    if variant_id is None:
        raise _runtime_error(
            "SPELLBOOK_VARIANTS_V1_INVALID",
            f"variants[{index}].variant_id must be a non-empty string",
        )

    normalized: Dict[str, Any] = {
        "variant_id": variant_id,
        "cards": _normalize_cards(raw.get("cards"), field_path=f"variants[{index}].cards"),
    }

    result_label = _nonempty_str(raw.get("result"))
    if result_label is not None:
        normalized["result"] = result_label

    tags = _normalize_tags(raw.get("tags"), field_path=f"variants[{index}].tags")
    if tags is not None:
        normalized["tags"] = tags

    return normalized


def load_commander_spellbook_variants_v1() -> Dict[str, Any]:
    variants_file = _resolve_spellbook_variants_v1_file()
    if not variants_file.is_file():
        raise _runtime_error("SPELLBOOK_VARIANTS_V1_MISSING", str(variants_file))

    try:
        parsed = json.loads(variants_file.read_text(encoding="utf-8"))
    except Exception as exc:
        raise _runtime_error("SPELLBOOK_VARIANTS_V1_INVALID_JSON", str(variants_file)) from exc

    if not isinstance(parsed, dict):
        raise _runtime_error("SPELLBOOK_VARIANTS_V1_INVALID", "root must be an object")

    version = _nonempty_str(parsed.get("version"))
    if version != SPELLBOOK_VARIANTS_V1_VERSION:
        raise _runtime_error(
            "SPELLBOOK_VARIANTS_V1_INVALID",
            f"version must equal '{SPELLBOOK_VARIANTS_V1_VERSION}'",
        )

    source = _nonempty_str(parsed.get("source"))
    if source is None:
        raise _runtime_error("SPELLBOOK_VARIANTS_V1_INVALID", "source must be a non-empty string")

    generated_from = _nonempty_str(parsed.get("generated_from"))
    if generated_from is None:
        raise _runtime_error("SPELLBOOK_VARIANTS_V1_INVALID", "generated_from must be a non-empty string")

    variants_raw = parsed.get("variants")
    if not isinstance(variants_raw, list):
        raise _runtime_error("SPELLBOOK_VARIANTS_V1_INVALID", "variants must be a list")

    normalized_variants: List[Dict[str, Any]] = []
    seen_variant_ids: set[str] = set()
    for index, variant_raw in enumerate(variants_raw):
        normalized_variant = _normalize_variant(variant_raw, index=index)
        variant_id = normalized_variant["variant_id"]
        if variant_id in seen_variant_ids:
            raise _runtime_error("SPELLBOOK_VARIANTS_V1_INVALID", f"duplicate variant_id '{variant_id}'")
        seen_variant_ids.add(variant_id)
        normalized_variants.append(normalized_variant)

    normalized_variants_sorted = sorted(normalized_variants, key=lambda row: str(row.get("variant_id") or ""))

    return {
        "version": SPELLBOOK_VARIANTS_V1_VERSION,
        "source": source,
        "generated_from": generated_from,
        "variants": normalized_variants_sorted,
    }
