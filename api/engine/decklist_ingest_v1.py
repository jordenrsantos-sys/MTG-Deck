from __future__ import annotations

from typing import Any, Dict, List

from api.engine.db_cards import lookup_cards_by_oracle_ids
from api.engine.decklist_parse_v1 import parse_decklist_text, normalize_decklist_name
from api.engine.decklist_resolve_v1 import resolve_parsed_decklist
from api.engine.utils import sha256_hex, stable_json_dumps


DECKLIST_INGEST_VERSION = "decklist_ingest_v1"
REQUEST_HASH_V1_VERSION = "request_hash_v1"


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _normalize_format(value: Any) -> str:
    token = _nonempty_str(value)
    if token is None:
        return "commander"
    return token.casefold()


def _safe_positive_int(value: Any, *, default: int = 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        return int(default)
    if int(value) < 1:
        return int(default)
    return int(value)


def _safe_line_no(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        return 0
    if int(value) < 0:
        return 0
    return int(value)


def _normalize_name_overrides_v1(value: Any) -> List[Dict[str, str]]:
    rows = value if isinstance(value, list) else []
    normalized_rows: List[Dict[str, str]] = []

    for row in rows:
        if not isinstance(row, dict):
            continue

        name_raw = _nonempty_str(row.get("name_raw"))
        if name_raw is None:
            continue

        resolved_oracle_id = _nonempty_str(row.get("resolved_oracle_id"))
        resolved_name = _nonempty_str(row.get("resolved_name"))
        if resolved_oracle_id is None and resolved_name is None:
            continue

        normalized_entry: Dict[str, str] = {
            "name_raw": name_raw,
        }
        if resolved_oracle_id is not None:
            normalized_entry["resolved_oracle_id"] = resolved_oracle_id
        elif resolved_name is not None:
            normalized_entry["resolved_name"] = resolved_name

        normalized_rows.append(normalized_entry)

    normalized_rows = sorted(
        normalized_rows,
        key=lambda row: (
            normalize_decklist_name(row.get("name_raw")) or "",
            str(row.get("name_raw") or "").casefold(),
            str(row.get("name_raw") or ""),
            str(row.get("resolved_oracle_id") or ""),
            str(row.get("resolved_name") or "").casefold(),
            str(row.get("resolved_name") or ""),
        ),
    )

    dedup_by_name_norm: set[str] = set()
    deduped_rows: List[Dict[str, str]] = []
    for row in normalized_rows:
        name_norm = normalize_decklist_name(row.get("name_raw"))
        if name_norm is None or name_norm in dedup_by_name_norm:
            continue
        dedup_by_name_norm.add(name_norm)
        deduped_rows.append(dict(row))

    return deduped_rows


def build_canonical_deck_input_v1(
    *,
    db_snapshot_id: Any,
    profile_id: Any,
    bracket_id: Any,
    format: Any,
    commander: Any,
    cards: Any,
    engine_patches_v0: Any,
    name_overrides_v1: Any = None,
) -> Dict[str, Any]:
    format_token = _normalize_format(format)
    commander_token = _nonempty_str(commander) or ""
    cards_list = cards if isinstance(cards, list) else []
    patches_list = engine_patches_v0 if isinstance(engine_patches_v0, list) else []
    normalized_overrides = _normalize_name_overrides_v1(name_overrides_v1)

    return {
        "db_snapshot_id": _nonempty_str(db_snapshot_id) or "",
        "profile_id": _nonempty_str(profile_id) or "",
        "bracket_id": _nonempty_str(bracket_id) or "",
        "format": format_token,
        "commander": commander_token,
        "cards": [name for name in cards_list if isinstance(name, str)],
        "engine_patches_v0": [row for row in patches_list if isinstance(row, dict)],
        "name_overrides_v1": normalized_overrides,
    }


def compute_request_hash_v1(canonical_deck_input: Any) -> str:
    payload = canonical_deck_input if isinstance(canonical_deck_input, dict) else {}
    normalized = build_canonical_deck_input_v1(
        db_snapshot_id=payload.get("db_snapshot_id"),
        profile_id=payload.get("profile_id"),
        bracket_id=payload.get("bracket_id"),
        format=payload.get("format"),
        commander=payload.get("commander"),
        cards=payload.get("cards"),
        engine_patches_v0=payload.get("engine_patches_v0"),
        name_overrides_v1=payload.get("name_overrides_v1"),
    )
    return sha256_hex(stable_json_dumps(normalized))


def _is_basic_land(*, card_name: str, type_line: str) -> bool:
    type_token = type_line.casefold()
    if "basic" in type_token and "land" in type_token:
        return True

    name_token = card_name.casefold()
    if name_token.startswith("snow-covered ") and "land" in type_token:
        return True

    return False


def _compute_violations_v1(
    *,
    format: str,
    db_snapshot_id: str,
    resolved_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if format != "commander":
        return []

    grouped: Dict[str, Dict[str, Any]] = {}
    for row in resolved_rows:
        if not isinstance(row, dict):
            continue

        card_name = _nonempty_str(row.get("name"))
        if card_name is None:
            continue

        oracle_id = _nonempty_str(row.get("oracle_id")) or ""
        key = oracle_id if oracle_id != "" else f"name::{card_name.casefold()}"

        entry = grouped.get(key)
        if entry is None:
            entry = {
                "card_name": card_name,
                "oracle_id": oracle_id,
                "count": 0,
                "line_nos": set(),
            }
            grouped[key] = entry

        entry["count"] = int(entry.get("count", 0)) + _safe_positive_int(row.get("count"), default=1)
        entry["line_nos"].add(_safe_line_no(row.get("source_line_no")))

    oracle_ids = {
        entry.get("oracle_id")
        for entry in grouped.values()
        if isinstance(entry.get("oracle_id"), str) and entry.get("oracle_id") != ""
    }
    cards_by_oracle = lookup_cards_by_oracle_ids(
        conn=None,
        snapshot_id=db_snapshot_id,
        oracle_ids=oracle_ids,
        requested_fields=["oracle_id", "name", "type_line"],
    )

    violations: List[Dict[str, Any]] = []
    sorted_entries = sorted(
        grouped.values(),
        key=lambda item: (
            str(item.get("card_name") or "").casefold(),
            str(item.get("card_name") or ""),
            str(item.get("oracle_id") or ""),
        ),
    )
    for entry in sorted_entries:
        card_name = _nonempty_str(entry.get("card_name"))
        if card_name is None:
            continue

        count = _safe_positive_int(entry.get("count"), default=1)
        if count <= 1:
            continue

        oracle_id = _nonempty_str(entry.get("oracle_id")) or ""
        card_lookup = cards_by_oracle.get(oracle_id) if oracle_id != "" else None
        type_line = _nonempty_str((card_lookup or {}).get("type_line")) or ""

        if _is_basic_land(card_name=card_name, type_line=type_line):
            continue

        line_no_values = entry.get("line_nos") if isinstance(entry.get("line_nos"), set) else set()
        line_nos = sorted(
            {
                int(value)
                for value in line_no_values
                if isinstance(value, int) and not isinstance(value, bool) and int(value) >= 0
            }
        )
        violations.append(
            {
                "code": "COMMANDER_DUPLICATE_NONBASIC",
                "card_name": card_name,
                "count": int(count),
                "line_nos": line_nos,
                "message": "Commander duplicates are only allowed for basic lands (including snow-covered basics).",
            }
        )

    return violations


def _expand_card_names(resolved_rows: List[Dict[str, Any]]) -> List[str]:
    expanded: List[str] = []
    for row in resolved_rows:
        if not isinstance(row, dict):
            continue

        name = _nonempty_str(row.get("name"))
        count = row.get("count")
        if name is None or not isinstance(count, int) or isinstance(count, bool) or count < 1:
            continue

        for _ in range(count):
            expanded.append(name)

    return expanded


def _resolve_single_commander(name: str, db_snapshot_id: str) -> Dict[str, Any]:
    parsed_payload = {
        "version": "decklist_parse_v1",
        "items": [
            {
                "count": 1,
                "name_raw": name,
                "name_norm": normalize_decklist_name(name),
                "line_no": 0,
                "section": "commander",
            }
        ],
    }
    return resolve_parsed_decklist(parsed_payload, db_snapshot_id)


def ingest_decklist(
    raw_text: str,
    db_snapshot_id: str,
    format: str = "commander",
    commander_name_override: str | None = None,
    name_overrides_v1: Any = None,
) -> Dict[str, Any]:
    parsed = parse_decklist_text(raw_text)
    resolution = resolve_parsed_decklist(
        parsed,
        db_snapshot_id,
        name_overrides_v1=name_overrides_v1,
    )

    parsed_items = parsed.get("items") if isinstance(parsed.get("items"), list) else []
    resolved_rows = resolution.get("resolved_cards") if isinstance(resolution.get("resolved_cards"), list) else []
    unknowns = [entry for entry in (resolution.get("unknowns") if isinstance(resolution.get("unknowns"), list) else []) if isinstance(entry, dict)]

    commander_name: str | None = None
    commander_oracle_id: str | None = None
    commander_line_no: int | None = None

    override_name = _nonempty_str(commander_name_override)
    if override_name is not None:
        commander_resolution = _resolve_single_commander(override_name, db_snapshot_id)
        commander_resolved_rows = (
            commander_resolution.get("resolved_cards")
            if isinstance(commander_resolution.get("resolved_cards"), list)
            else []
        )
        commander_unknowns = (
            commander_resolution.get("unknowns")
            if isinstance(commander_resolution.get("unknowns"), list)
            else []
        )

        if len(commander_resolved_rows) == 1 and isinstance(commander_resolved_rows[0], dict):
            commander_name = _nonempty_str(commander_resolved_rows[0].get("name"))
            commander_oracle_id = _nonempty_str(commander_resolved_rows[0].get("oracle_id"))

        for unknown_row in commander_unknowns:
            if isinstance(unknown_row, dict):
                unknowns.append(unknown_row)
    else:
        commander_items = [
            item
            for item in parsed_items
            if isinstance(item, dict)
            and _nonempty_str(item.get("section")) == "commander"
        ]

        if len(commander_items) == 1 and isinstance(commander_items[0], dict):
            commander_line_no_raw = commander_items[0].get("line_no")
            commander_line_no = commander_line_no_raw if isinstance(commander_line_no_raw, int) and commander_line_no_raw >= 0 else 0

            commander_resolved = [
                row
                for row in resolved_rows
                if isinstance(row, dict)
                and isinstance(row.get("source_line_no"), int)
                and row.get("source_line_no") == commander_line_no
            ]
            if len(commander_resolved) == 1 and isinstance(commander_resolved[0], dict):
                commander_name = _nonempty_str(commander_resolved[0].get("name"))
                commander_oracle_id = _nonempty_str(commander_resolved[0].get("oracle_id"))
        else:
            unknowns.append(
                {
                    "name_raw": "",
                    "name_norm": "",
                    "count": 1,
                    "line_no": 0,
                    "reason_code": "COMMANDER_MISSING",
                    "candidates": [],
                }
            )

    resolved_non_commander_rows = [
        row
        for row in resolved_rows
        if isinstance(row, dict)
        and not (
            commander_line_no is not None
            and isinstance(row.get("source_line_no"), int)
            and row.get("source_line_no") == commander_line_no
        )
    ]

    expanded_cards = _expand_card_names(resolved_non_commander_rows)

    unknowns_sorted = sorted(
        unknowns,
        key=lambda row: (
            int(row.get("line_no")) if isinstance(row.get("line_no"), int) else 0,
            str(row.get("reason_code") or ""),
            str(row.get("name_norm") or ""),
            str(row.get("name_raw") or ""),
        ),
    )

    format_token = _normalize_format(format)
    violations_v1 = _compute_violations_v1(
        format=format_token,
        db_snapshot_id=db_snapshot_id,
        resolved_rows=resolved_non_commander_rows,
    )

    canonical_deck_input = {
        "format": format_token,
        "commander": commander_name or "",
        "cards": expanded_cards,
    }

    return {
        "version": DECKLIST_INGEST_VERSION,
        "status": "UNKNOWN_PRESENT" if len(unknowns_sorted) > 0 else "OK",
        "db_snapshot_id": db_snapshot_id,
        "format": format_token,
        "canonical_deck_input": canonical_deck_input,
        "unknowns": unknowns_sorted,
        "violations_v1": violations_v1,
        "parse_version": parsed.get("version"),
        "resolve_version": resolution.get("version"),
        "ingest_version": DECKLIST_INGEST_VERSION,
        "parsed": parsed,
        "resolution": resolution,
        "commander_oracle_id": commander_oracle_id,
    }
