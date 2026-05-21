"""
combo_registry_merger_v1 — Mega-task v4 Phase 12.

Loads BOTH `combo_brackets_v1.json` (Spellbook canonical) AND
`combo_brackets_v1_external_sources.json` (additive curated +
extracted from other sources) into a single in-memory registry.

Merge policy: Spellbook entries take precedence on bracket conflicts.
When the SAME card-pair appears in both sources with different
bracket classifications, the merged record annotates the conflict and
uses Spellbook's classification.

Public API:
  - `load_merged_registry() -> Dict[str, Any]`
  - `MERGED_VARIANT_COUNT` — read after `load_merged_registry()` once
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


COMBO_REGISTRY_MERGER_VERSION = "combo_registry_merger_v1.0"

CANONICAL_REGISTRY_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "combos" / "combo_brackets_v1.json"
)
ADDITIVE_REGISTRY_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "combos"
    / "combo_brackets_v1_set_appended.json"
)
EXTERNAL_SOURCES_REGISTRY_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "combos"
    / "combo_brackets_v1_external_sources.json"
)


def _normalize_pair_key(card_names: List[str]) -> Tuple[str, ...]:
    """Use sorted lowercase names as the pair-identity key. Order
    doesn't matter for combo-pair identity."""
    return tuple(sorted(n.strip().lower() for n in card_names if n))


def _load_json(path: Path) -> Optional[Any]:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _iter_variants_from_canonical(data: Any):
    """Yield variant dicts from the Spellbook canonical registry."""
    if isinstance(data, list):
        for v in data:
            if isinstance(v, dict):
                yield ("spellbook", v)
    elif isinstance(data, dict):
        if "by_variant_id" in data and isinstance(data["by_variant_id"], dict):
            for v in data["by_variant_id"].values():
                if isinstance(v, dict):
                    yield ("spellbook", v)
        elif "variants" in data and isinstance(data["variants"], list):
            for v in data["variants"]:
                if isinstance(v, dict):
                    yield ("spellbook", v)


def _iter_variants_from_external(data: Any):
    """Yield variant dicts from the external-sources registry."""
    if isinstance(data, dict) and isinstance(data.get("discovered"), list):
        for v in data["discovered"]:
            if isinstance(v, dict):
                src = v.get("source") or "external"
                yield (src, v)


def load_merged_registry() -> Dict[str, Any]:
    """Load + merge the canonical Spellbook registry + the additive
    external-sources registry.

    Returns:
      {
        merged_variants: list of merged variant dicts,
        canonical_count: int,
        external_count: int,
        merged_count: int,
        bracket_conflicts: list of {pair: [...], spellbook_brackets: [...], external_brackets: [...], source: str},
      }

    On a (case-insensitive sorted) pair-key conflict between sources,
    the Spellbook entry's `brackets_allowed` wins; the external entry's
    classification is logged in `bracket_conflicts` for downstream
    audit.
    """
    canonical = _load_json(CANONICAL_REGISTRY_PATH)
    external = _load_json(EXTERNAL_SOURCES_REGISTRY_PATH)

    merged_by_pair: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    bracket_conflicts: List[Dict[str, Any]] = []
    canonical_count = 0
    external_count = 0

    for src, variant in _iter_variants_from_canonical(canonical):
        canonical_count += 1
        names = variant.get("card_names") or variant.get("cards") or []
        if not names:
            continue
        key = _normalize_pair_key(names)
        if key not in merged_by_pair:
            merged_by_pair[key] = {
                **variant, "source": src,
            }

    for src, variant in _iter_variants_from_external(external):
        external_count += 1
        names = variant.get("card_names") or []
        if not names:
            continue
        key = _normalize_pair_key(names)
        if key in merged_by_pair:
            existing = merged_by_pair[key]
            sb_brackets = existing.get("brackets_allowed") or []
            ex_brackets = variant.get("brackets_allowed") or []
            if sorted(sb_brackets) != sorted(ex_brackets):
                bracket_conflicts.append({
                    "pair": list(key),
                    "spellbook_brackets": sb_brackets,
                    "external_brackets": ex_brackets,
                    "external_source": src,
                })
            # Spellbook wins on bracket; external still contributes its
            # outcome/category if Spellbook didn't have one.
            if not existing.get("description") and variant.get("outcome"):
                existing["description"] = variant["outcome"]
        else:
            merged_by_pair[key] = {**variant, "source": src}

    return {
        "merged_variants": list(merged_by_pair.values()),
        "canonical_count": canonical_count,
        "external_count": external_count,
        "merged_count": len(merged_by_pair),
        "bracket_conflicts": bracket_conflicts,
        "version": COMBO_REGISTRY_MERGER_VERSION,
    }


def load_combo_assembly_names_merged() -> Set[str]:
    """Return the set of card names appearing in any combo variant
    from BOTH the canonical and external-sources registries. Used by
    Pillar C extractor as the `combo-assembly` tag source.
    """
    merged = load_merged_registry()
    names: Set[str] = set()
    for v in merged["merged_variants"]:
        for c in v.get("card_names") or []:
            if isinstance(c, str):
                names.add(c.strip().lower())
    return names
