"""v1.7 Stage 2 — combo-aware Complete-Deck reasoning.

Post-processes `added_cards_v1` rows produced by `run_deck_complete_engine_v1`
and annotates each proposed-add with a `COMBO_ENABLER` reason whenever the
proposed card forms a known 2-card combo with an existing deck card (or
the commander).

Frozen data inputs (loaded once at module import):
  - repo/api/engine/data/combos/two_card_combos_v2.json        (4547 pairs)
  - repo/api/engine/data/combos/commander_spellbook_combo_outcomes_v1.json
    (4527 variant_id → outcome records, Stage 1.5 SHA-pinned data pack)

Layer output: each matched proposed-add receives a tagged-string reason of
the form `"COMBO_ENABLER:<json>"` appended to its `reasons_v1` array. The
JSON payload carries `{partner_card_oracle_id, partner_card_name,
combo_outcome_label}`. The tagged-string form lets the reason survive the
strict `reasons_v1: List[str]` filter in `api/main.py` (which is on the
v1.7 hard-safety BYTE-IDENTICAL list and cannot be modified).

UI discrimination: the v1.7 Phase-2 UI scans each row's `reasons_v1` for
entries beginning with `COMBO_ENABLER:`, parses the JSON payload, and
renders a distinct chip variant (see `lib/justificationLabels.ts` +
`components/.../AddedCardsPanel.tsx`).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, FrozenSet, Iterable, List, Optional, Set, Tuple

from api.engine.db_cards import lookup_cards_by_oracle_ids
from engine.db import find_card_by_name


COMBO_ENABLER_REASONS_V1_VERSION = "combo_enabler_reasons_v1"
COMBO_ENABLER_REASON_PREFIX = "COMBO_ENABLER:"

_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "combos"
_TWO_CARD_COMBOS_V2_PATH = _DATA_DIR / "two_card_combos_v2.json"
_COMBO_OUTCOMES_V1_PATH = _DATA_DIR / "commander_spellbook_combo_outcomes_v1.json"


def _normalize_oracle_id(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    token = value.strip().lower()
    if token == "":
        return None
    return token


def _normalize_card_name(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    token = " ".join(value.split()).strip()
    if token == "":
        return None
    return token


def _load_two_card_pair_index(path: Path) -> Dict[FrozenSet[str], List[str]]:
    """Read v2 pairs → index keyed by frozenset of normalized oracle_ids.

    Value is the list of variant_ids (sorted) attached to the pair.
    """
    parsed = json.loads(path.read_text(encoding="utf-8"))
    pairs = parsed.get("pairs") if isinstance(parsed, dict) else None
    if not isinstance(pairs, list):
        return {}

    index: Dict[FrozenSet[str], List[str]] = {}
    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        a = _normalize_oracle_id(pair.get("a"))
        b = _normalize_oracle_id(pair.get("b"))
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


def _load_outcomes(path: Path) -> Dict[str, Dict[str, Any]]:
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
                norm = _normalize_oracle_id(oid)
                if norm is not None and norm not in partner_oids:
                    partner_oids.append(norm)
        out[vid] = {"label": label.strip(), "partner_oracle_ids": partner_oids}
    return out


_PAIR_INDEX: Dict[FrozenSet[str], List[str]] = _load_two_card_pair_index(_TWO_CARD_COMBOS_V2_PATH)
_OUTCOMES: Dict[str, Dict[str, Any]] = _load_outcomes(_COMBO_OUTCOMES_V1_PATH)


def _resolve_name_to_oracle_id(snapshot_id: str, name: str) -> Optional[str]:
    card = find_card_by_name(snapshot_id, name)
    if not isinstance(card, dict):
        return None
    return _normalize_oracle_id(card.get("oracle_id"))


def _resolve_oracle_ids_to_names(
    snapshot_id: str, oracle_ids: Iterable[str]
) -> Dict[str, str]:
    """Batch UUID→name via db_cards.lookup_cards_by_oracle_ids.

    Keys + values normalized to lowercase oracle_id / unstripped name.
    Missing oracle_ids are simply absent from the returned dict.
    """
    target = {oid for oid in oracle_ids if isinstance(oid, str) and oid}
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
            norm = _normalize_oracle_id(oracle_id)
            if norm is not None:
                out[norm] = name.strip()
    return out


def _encode_combo_enabler_reason(
    *,
    partner_oracle_id: str,
    partner_card_name: str,
    combo_outcome_label: str,
) -> str:
    payload = {
        "partner_card_oracle_id": partner_oracle_id,
        "partner_card_name": partner_card_name,
        "combo_outcome_label": combo_outcome_label,
    }
    return COMBO_ENABLER_REASON_PREFIX + json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def attach_combo_enabler_reasons_v1(
    *,
    db_snapshot_id: str,
    commander_names: List[str],
    deck_cards: List[str],
    added_cards_v1: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Annotate `added_cards_v1` rows with COMBO_ENABLER reasons in place.

    Returns a new list of row dicts (each row also new). Original input is
    not mutated. Rows that don't form a combo pass through unchanged.

    Safe to call when any/all of the inputs are missing — returns the rows
    untouched (deep-copied) if any precondition fails (no snapshot id, no
    pair index, no outcomes, etc.).
    """
    if not isinstance(added_cards_v1, list) or not added_cards_v1:
        return list(added_cards_v1) if isinstance(added_cards_v1, list) else []

    output_rows: List[Dict[str, Any]] = []
    for row in added_cards_v1:
        if not isinstance(row, dict):
            continue
        existing_reasons = row.get("reasons_v1") if isinstance(row.get("reasons_v1"), list) else []
        output_rows.append({
            "name": row.get("name"),
            "reasons_v1": list(existing_reasons),
            "primitives_added_v1": list(row.get("primitives_added_v1") or [])
                if isinstance(row.get("primitives_added_v1"), list) else [],
        })

    snapshot_id = db_snapshot_id if isinstance(db_snapshot_id, str) and db_snapshot_id.strip() else ""
    if not snapshot_id or not _PAIR_INDEX or not _OUTCOMES:
        return output_rows

    deck_name_inputs: List[str] = []
    if isinstance(commander_names, list):
        for n in commander_names:
            norm = _normalize_card_name(n)
            if norm is not None:
                deck_name_inputs.append(norm)
    if isinstance(deck_cards, list):
        for n in deck_cards:
            norm = _normalize_card_name(n)
            if norm is not None:
                deck_name_inputs.append(norm)

    deck_oracle_ids: Set[str] = set()
    for name in deck_name_inputs:
        oid = _resolve_name_to_oracle_id(snapshot_id, name)
        if oid is not None:
            deck_oracle_ids.add(oid)
    if not deck_oracle_ids:
        return output_rows

    added_oracle_ids: List[Tuple[int, str]] = []
    for idx, row in enumerate(output_rows):
        name = _normalize_card_name(row.get("name"))
        if name is None:
            continue
        oid = _resolve_name_to_oracle_id(snapshot_id, name)
        if oid is None:
            continue
        added_oracle_ids.append((idx, oid))
    if not added_oracle_ids:
        return output_rows

    matches_by_row: Dict[int, List[Tuple[str, str]]] = {}
    partner_oid_pool: Set[str] = set()
    for idx, added_oid in added_oracle_ids:
        for deck_oid in deck_oracle_ids:
            if deck_oid == added_oid:
                continue
            key = frozenset({added_oid, deck_oid})
            variant_ids = _PAIR_INDEX.get(key)
            if not variant_ids:
                continue
            matched_vid: Optional[str] = None
            for vid in variant_ids:
                if vid in _OUTCOMES:
                    matched_vid = vid
                    break
            if matched_vid is None:
                continue
            matches_by_row.setdefault(idx, []).append((deck_oid, matched_vid))
            partner_oid_pool.add(deck_oid)

    if not matches_by_row:
        return output_rows

    partner_name_map = _resolve_oracle_ids_to_names(snapshot_id, partner_oid_pool)

    for idx, matches in matches_by_row.items():
        row = output_rows[idx]
        existing = row["reasons_v1"]
        seen: Set[str] = {r for r in existing if isinstance(r, str)}
        new_entries: List[str] = []
        for partner_oid, variant_id in sorted(matches, key=lambda t: (t[0], t[1])):
            partner_name = partner_name_map.get(partner_oid)
            if partner_name is None:
                continue
            outcome = _OUTCOMES.get(variant_id)
            if outcome is None:
                continue
            label = outcome.get("label")
            if not isinstance(label, str) or label.strip() == "":
                continue
            encoded = _encode_combo_enabler_reason(
                partner_oracle_id=partner_oid,
                partner_card_name=partner_name,
                combo_outcome_label=label,
            )
            if encoded in seen:
                continue
            seen.add(encoded)
            new_entries.append(encoded)
        if new_entries:
            row["reasons_v1"] = sorted(set(existing) | set(new_entries))

    return output_rows
