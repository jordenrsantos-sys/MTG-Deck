"""v1.7.2 — Shared combo data loader (public helpers).

Stage 0 halt-decision (option a, per v1.7.2 spec): combo_enabler_reasons_v1's
loader/normalizer helpers all carry leading underscores marking them
module-private. Reusing them from a NEW layer requires either (a) extracting
public versions into a shared module, or (b) duplicating the logic.

This module ships option (a): NEW shared module with PUBLIC helpers
consumed by `deck_combo_insights_v1` (the v1.7.2 engine layer). The
combo_enabler_reasons_v1 module is left BYTE-IDENTICAL — its private
helpers stay verbatim; a future stage may refactor it to import from
here, retiring the duplication. Tech-debt logged in v1.7.2 Stage 1
autonomous_repair_log.

Data files (READ-ONLY HARD safety, BYTE-IDENTICAL):
  - repo/api/engine/data/combos/two_card_combos_v2.json          (4547 pairs)
  - repo/api/engine/data/combos/commander_spellbook_combo_outcomes_v1.json
    (4527 variant_id → outcome records)
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, FrozenSet, Iterable, List, Optional, Set

from api.engine.db_cards import lookup_cards_by_oracle_ids
from engine.db import find_card_by_name


_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "combos"
TWO_CARD_COMBOS_V2_PATH = _DATA_DIR / "two_card_combos_v2.json"
COMBO_OUTCOMES_V1_PATH = _DATA_DIR / "commander_spellbook_combo_outcomes_v1.json"


def normalize_oracle_id(value: Any) -> Optional[str]:
    """Lowercase + strip an oracle_id value; return None on bad input."""
    if not isinstance(value, str):
        return None
    token = value.strip().lower()
    if token == "":
        return None
    return token


def normalize_card_name(value: Any) -> Optional[str]:
    """Collapse internal whitespace, strip outer whitespace; return None
    on empty / non-string."""
    if not isinstance(value, str):
        return None
    token = " ".join(value.split()).strip()
    if token == "":
        return None
    return token


def load_two_card_pair_index(path: Path = TWO_CARD_COMBOS_V2_PATH) -> Dict[FrozenSet[str], List[str]]:
    """Load v2 pairs into an index keyed by `frozenset({oracle_a, oracle_b})`
    → sorted list of variant_ids. Skips malformed rows defensively."""
    parsed = json.loads(path.read_text(encoding="utf-8"))
    pairs = parsed.get("pairs") if isinstance(parsed, dict) else None
    if not isinstance(pairs, list):
        return {}

    index: Dict[FrozenSet[str], List[str]] = {}
    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        a = normalize_oracle_id(pair.get("a"))
        b = normalize_oracle_id(pair.get("b"))
        if a is None or b is None or a == b:
            continue
        variant_ids_raw = pair.get("variant_ids")
        if not isinstance(variant_ids_raw, list):
            continue
        variants = sorted({
            v.strip() for v in variant_ids_raw
            if isinstance(v, str) and v.strip() != ""
        })
        if not variants:
            continue
        key = frozenset({a, b})
        existing = index.get(key)
        if existing is None:
            index[key] = list(variants)
        else:
            existing.extend(v for v in variants if v not in existing)
            existing.sort()
    return index


def load_outcomes(path: Path = COMBO_OUTCOMES_V1_PATH) -> Dict[str, Dict[str, Any]]:
    """Load outcome pack into a dict keyed by variant_id → `{label,
    partner_oracle_ids}`. Missing/blank labels are skipped."""
    parsed = json.loads(path.read_text(encoding="utf-8"))
    outcomes_raw = parsed.get("outcomes") if isinstance(parsed, dict) else None
    if not isinstance(outcomes_raw, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for vid, rec in outcomes_raw.items():
        if not isinstance(vid, str) or not isinstance(rec, dict):
            continue
        label = rec.get("label")
        if not isinstance(label, str) or label.strip() == "":
            continue
        partner_oids_raw = rec.get("partner_oracle_ids")
        partner_oids: List[str] = []
        if isinstance(partner_oids_raw, list):
            for oid in partner_oids_raw:
                norm = normalize_oracle_id(oid)
                if norm is not None and norm not in partner_oids:
                    partner_oids.append(norm)
        out[vid] = {"label": label.strip(), "partner_oracle_ids": partner_oids}
    return out


def resolve_name_to_oracle_id(snapshot_id: str, name: str) -> Optional[str]:
    """Look up a single card by name via the existing engine.db helper;
    return its oracle_id (normalized) or None."""
    card = find_card_by_name(snapshot_id, name)
    if not isinstance(card, dict):
        return None
    return normalize_oracle_id(card.get("oracle_id"))


def resolve_oracle_ids_to_names(
    snapshot_id: str, oracle_ids: Iterable[str]
) -> Dict[str, str]:
    """Batch UUID→name lookup via the existing `lookup_cards_by_oracle_ids`
    helper. Keys are normalized lowercase oracle_ids; missing rows are
    simply absent from the returned map."""
    target: Set[str] = {oid for oid in oracle_ids if isinstance(oid, str) and oid}
    if not target:
        return {}
    raw = lookup_cards_by_oracle_ids(
        conn=None,
        snapshot_id=snapshot_id,
        oracle_ids=target,
        requested_fields=["oracle_id", "name"],
    )
    out: Dict[str, str] = {}
    for oracle_id, row in raw.items():
        if not isinstance(row, dict):
            continue
        name = row.get("name")
        if isinstance(name, str) and name.strip() != "":
            norm = normalize_oracle_id(oracle_id)
            if norm is not None:
                out[norm] = name.strip()
    return out
