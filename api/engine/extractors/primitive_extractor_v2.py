"""
primitive_extractor_v2 — Pillar C ontology extractor.

Mega-task v6 Phase 3: switched the default ontology from v1 (81 tags
across 7 dimensions) to v2 (93 tags across 8 dimensions, adds
`counters_and_proliferate` with 12 tags). Closes iter 6 success
criterion #7 by replacing the v5 Phase 7 `anthem-effect` proxy with
real counter / proliferate primitives.

Backwards-compat preserved:
  - `load_ontology_v1()` still loads `ontology_v1.md` exactly as before
  - `extract_primitives_v2()` now defaults to v2 (was v1); pass
    `ontology=load_ontology_v1()` explicitly for the v1 behavior
  - all v1 tag IDs remain valid; v2 only ADDS new tags

The Phase 3 backfill walks the cards table, runs v2 extraction, and
writes results to `cards.primitives_v1_json` (column name unchanged
for SQL compatibility).
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


PRIMITIVE_EXTRACTOR_V2_VERSION = "primitive_extractor_v2.1_counters_and_proliferate"

ONTOLOGY_V1_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "primitives" / "ontology_v1.md"
)
ONTOLOGY_V2_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "primitives" / "ontology_v2.md"
)


def load_ontology_v1() -> Dict[str, ParsedTag]:
    """Load the v1 ontology — 81 tags / 7 dimensions."""
    return _load_ontology_v0(ontology_path=ONTOLOGY_V1_PATH)


def load_ontology_v2() -> Dict[str, ParsedTag]:
    """Load the v2 ontology — 93 tags / 8 dimensions (adds
    counters_and_proliferate)."""
    return _load_ontology_v0(ontology_path=ONTOLOGY_V2_PATH)


def extract_primitives_v2(
    oracle_text: str,
    type_line: str = "",
    mana_cost: str = "",
    card_name: Optional[str] = None,
    ontology: Optional[Dict[str, ParsedTag]] = None,
    combo_assembly_set: Optional[Set[str]] = None,
) -> Set[str]:
    """Apply v2 ontology regex extraction to a card. Same signature as
    v1's `extract_primitives` but defaults to the v2 ontology (was v1
    before mega-task v6 Phase 3).

    Returns: set of v2 tag IDs.
    """
    if ontology is None:
        ontology = load_ontology_v2()
    return _extract_primitives_v0(
        oracle_text=oracle_text,
        type_line=type_line,
        mana_cost=mana_cost,
        card_name=card_name,
        ontology=ontology,
        combo_assembly_set=combo_assembly_set,
    )
