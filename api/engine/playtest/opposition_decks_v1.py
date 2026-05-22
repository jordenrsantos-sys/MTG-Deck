"""
opposition_decks_v1 — Phase 5b.5. Curated opposition deck registry.

A small loader + filter API over the JSON registry at
`api/engine/data/playtest/opposition_decks_v1.json`. The calibration suite
(5b.3) consumes this — given a role_tag like "B2-precon-elven-empire" it
returns the corresponding corpus deck so calibration matchups are
reproducible across module changes.

Why a registry (vs picking corpus_ids inline in the calibration suite):
  - role_tag is a stable handle. Corpus IDs are generated and may shift
    if the corpus is re-ingested with a new timestamp.
  - The registry is the single place to curate the "canonical" B1-B5
    opponents. Swap a deck → update the registry → all calibration code
    that references the role_tag picks up the new entry.
  - Tests pin: every entry resolves to a real corpus deck (no stale
    references), role_tags unique within bracket, B2 anchors include all
    7 Wizards precons confirmed in corpus.

Pure data + lookup. No DB. No network.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional


REGISTRY_VERSION = "opposition_decks_v1"

_REGISTRY_PATH = (
    Path(__file__).resolve().parents[2]
    / "engine" / "data" / "playtest" / "opposition_decks_v1.json"
)

_cache: Optional[Dict[str, Any]] = None


def load_registry(*, force_reload: bool = False) -> Dict[str, Any]:
    """Load the JSON registry. Cached after first call; pass force_reload=True
    to bypass the cache (useful in tests that mutate the file)."""
    global _cache
    if _cache is not None and not force_reload:
        return _cache
    with open(_REGISTRY_PATH, "r", encoding="utf-8") as f:
        _cache = json.load(f)
    return _cache


def get_by_role_tag(role_tag: str) -> Optional[Dict[str, Any]]:
    """Return the registry entry whose role_tag matches, or None."""
    if not isinstance(role_tag, str):
        return None
    target = role_tag.strip()
    if target == "":
        return None
    reg = load_registry()
    for entry in reg.get("entries", []):
        if entry.get("role_tag") == target:
            return entry
    return None


def filter_by_bracket(bracket: str) -> List[Dict[str, Any]]:
    """Return all entries for the given bracket (B1-B5). Empty list if none."""
    if not isinstance(bracket, str):
        return []
    target = bracket.strip().upper()
    if target not in {"B1", "B2", "B3", "B4", "B5"}:
        return []
    reg = load_registry()
    return [e for e in reg.get("entries", []) if e.get("bracket") == target]


def all_role_tags() -> List[str]:
    """Return the list of all role_tags, in registry order."""
    reg = load_registry()
    return [e["role_tag"] for e in reg.get("entries", []) if "role_tag" in e]


def registry_summary() -> Dict[str, Any]:
    """Compact summary for endpoint payloads: counts per bracket, total.

    Mega-task v5 Phase 11: also surfaces per-bracket per-tier counts so the
    UI / graduated playtest can see which (bracket, tier) cells have
    opponents populated.
    """
    reg = load_registry()
    entries = reg.get("entries", [])
    by_bracket: Dict[str, int] = {}
    by_bracket_tier: Dict[str, Dict[str, int]] = {}
    for e in entries:
        b = e.get("bracket", "?")
        t = e.get("opposition_tier", 1)
        by_bracket[b] = by_bracket.get(b, 0) + 1
        bt = by_bracket_tier.setdefault(b, {})
        key = f"tier{t}"
        bt[key] = bt.get(key, 0) + 1
    return {
        "version": reg.get("version", REGISTRY_VERSION),
        "schema_version": reg.get("schema_version"),
        "total_entries": len(entries),
        "per_bracket_count": by_bracket,
        "per_bracket_tier_count": by_bracket_tier,
    }


def filter_by_tier(tier: int) -> List[Dict[str, Any]]:
    """Mega-task v5 Phase 11: filter entries by opposition_tier.

    Entries that pre-date the tier schema and don't carry the field default
    to tier 1 (mid-tier) for backwards-compat.
    """
    if not isinstance(tier, int):
        return []
    reg = load_registry()
    return [
        e for e in reg.get("entries", [])
        if int(e.get("opposition_tier", 1)) == tier
    ]


def filter_by_bracket_and_tier(bracket: str, tier: int) -> List[Dict[str, Any]]:
    """Mega-task v5 Phase 11: filter entries by both bracket and tier.

    Returns the entries for, e.g., (B3, tier=0) — the precon-equivalents
    for bracket 3. Empty list if no entries exist for that (bracket, tier).
    """
    if not isinstance(bracket, str) or not isinstance(tier, int):
        return []
    target = bracket.strip().upper()
    if target not in {"B1", "B2", "B3", "B4", "B5"}:
        return []
    reg = load_registry()
    return [
        e for e in reg.get("entries", [])
        if e.get("bracket") == target
        and int(e.get("opposition_tier", 1)) == tier
    ]
