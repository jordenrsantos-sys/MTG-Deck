"""
primitive_extractor_v2 — Pillar C ontology v1 extractor (iter 5 Phase 3).

Loads `ontology_v1.md` (81 tags across 7 dimensions, includes the
rules_modifiers dimension added in iter 5) and applies all regex
patterns to a card's text. Cards where regex extraction returns <2
tags can be routed to the LLM-extractor supplement
(`primitive_extractor_llm_v1`) for additional tagging.

Backwards-compat: re-exports `load_combo_assembly_names` and the
`ParsedTag` dataclass from v1 for callers that just want the existing
shape. The `extract_primitives` function in v1 still works; v2 adds:

  - `load_ontology_v1()` — default-loads `ontology_v1.md`
  - `extract_primitives_v2(...)` — same signature as v1's
    `extract_primitives` but uses v1 ontology by default

The Phase 3 backfill walks the cards table, runs v2 extraction, and
writes results to `cards.primitives_v1_json` (the column shipped in v2
Phase 5 of mega-task v2).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Set

from api.engine.extractors.primitive_extractor_v1 import (
    ParsedTag,
    extract_primitives as _extract_primitives_v0,
    load_combo_assembly_names,
    load_ontology as _load_ontology_v0,
)


PRIMITIVE_EXTRACTOR_V2_VERSION = "primitive_extractor_v2.0"

ONTOLOGY_V1_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "primitives" / "ontology_v1.md"
)


def load_ontology_v1() -> Dict[str, ParsedTag]:
    """Load the v1 ontology (default) — 81 tags / 7 dimensions."""
    return _load_ontology_v0(ontology_path=ONTOLOGY_V1_PATH)


def extract_primitives_v2(
    oracle_text: str,
    type_line: str = "",
    mana_cost: str = "",
    card_name: Optional[str] = None,
    ontology: Optional[Dict[str, ParsedTag]] = None,
    combo_assembly_set: Optional[Set[str]] = None,
) -> Set[str]:
    """Apply v1 ontology regex extraction to a card. Same signature as
    v1's `extract_primitives` but defaults to the v1 ontology.

    Returns: set of v1 tag IDs.
    """
    if ontology is None:
        ontology = load_ontology_v1()
    return _extract_primitives_v0(
        oracle_text=oracle_text,
        type_line=type_line,
        mana_cost=mana_cost,
        card_name=card_name,
        ontology=ontology,
        combo_assembly_set=combo_assembly_set,
    )
