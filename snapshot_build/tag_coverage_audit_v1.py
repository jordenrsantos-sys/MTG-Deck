from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List

from engine.db import resolve_db_path
from engine.determinism import stable_json_dumps


VERSION = "tag_coverage_audit_v1"

_TYPE_BUCKETS: tuple[str, ...] = (
    "creature",
    "instant",
    "sorcery",
    "artifact",
    "enchantment",
    "planeswalker",
    "land",
)

_PLAY_COUNT_COLUMNS: tuple[str, ...] = (
    "play_count",
    "playcount",
    "deck_count",
    "usage_count",
    "popularity",
    "edhrec_rank",
)


def _nonempty_str(value: Any) -> str:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return ""


def _coerce_float(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _round6(value: float) -> float:
    return float(f"{float(value):.6f}")


def _parse_json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if not isinstance(value, str):
        return []
    try:
        parsed = json.loads(value)
    except Exception:
        return []
    return parsed if isinstance(parsed, list) else []


def _resolve_db_path_from_cli(db_path: Any) -> Path:
    token = _nonempty_str(db_path)
    if token == "":
        return resolve_db_path()

    candidate = Path(token).expanduser()
    if not candidate.is_absolute():
        candidate = (Path.cwd() / candidate).resolve()

    if not candidate.is_file():
        raise RuntimeError(f"Database file not found: {candidate}")
    return candidate


def _cards_table_columns(con: sqlite3.Connection) -> List[str]:
    rows = con.execute("PRAGMA table_info(cards)").fetchall()
    out: List[str] = []
    for row in rows:
        row_dict = dict(row) if isinstance(row, sqlite3.Row) else {}
        col_name = _nonempty_str(row_dict.get("name") if row_dict else row[1] if len(row) > 1 else "")
        if col_name == "":
            continue
        out.append(col_name)
    return out


def _resolve_taxonomy_version(
    con: sqlite3.Connection,
    *,
    snapshot_id: str,
    taxonomy_version: str | None,
) -> str:
    token = _nonempty_str(taxonomy_version)
    if token != "":
        return token

    try:
        row = con.execute(
            """
            SELECT taxonomy_version
            FROM card_tags
            WHERE snapshot_id = ?
            GROUP BY taxonomy_version
            ORDER BY taxonomy_version DESC
            LIMIT 1
            """,
            (snapshot_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        row = None

    if row is None:
        return ""

    row_dict = dict(row) if isinstance(row, sqlite3.Row) else {}
    value = _nonempty_str(row_dict.get("taxonomy_version") if row_dict else row[0] if len(row) > 0 else "")
    return value


def _normalize_primitives(raw: Any) -> List[str]:
    values = _parse_json_list(raw)
    out = sorted(
        {
            token
            for token in (_nonempty_str(value) for value in values)
            if token != ""
        }
    )
    return out


def _normalize_color_identity_bucket(raw: Any) -> str:
    values: List[Any]
    parsed_json_list = False

    if isinstance(raw, list):
        values = raw
        parsed_json_list = True
    elif isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = None
        if isinstance(parsed, list):
            values = parsed
            parsed_json_list = True
        else:
            values = []
    else:
        values = []

    if len(values) == 0:
        if parsed_json_list:
            return "COLORLESS"
        text = _nonempty_str(raw)
        if text != "":
            return text
        return "COLORLESS"

    tokens = sorted(
        {
            token
            for token in (_nonempty_str(value).upper() for value in values)
            if token in {"W", "U", "B", "R", "G"}
        }
    )
    if len(tokens) == 0:
        return "COLORLESS"
    return "".join(tokens)


def _coverage_entry(card: Dict[str, Any], *, has_any_primitives: bool, play_count_field: str = "") -> Dict[str, Any]:
    cmc_value = _coerce_float(card.get("cmc"))
    entry: Dict[str, Any] = {
        "oracle_id": _nonempty_str(card.get("oracle_id")),
        "name": _nonempty_str(card.get("name")),
        "cmc": cmc_value,
        "type_line": _nonempty_str(card.get("type_line")),
        "color_identity_bucket": _normalize_color_identity_bucket(card.get("color_identity")),
        "has_any_primitives": bool(has_any_primitives),
    }

    if play_count_field != "":
        entry["play_count_field"] = play_count_field
        entry["play_count_value"] = _coerce_float(card.get(play_count_field))

    return entry


def _resolve_play_count_field(cards_columns: Iterable[str], missing_cards: List[Dict[str, Any]]) -> str:
    columns = set(cards_columns)
    for field_name in _PLAY_COUNT_COLUMNS:
        if field_name not in columns:
            continue
        for card in missing_cards:
            if _coerce_float(card.get(field_name)) is not None:
                return field_name
    return ""


def _build_top_missing_by_play_count(missing_cards: List[Dict[str, Any]], *, play_count_field: str) -> List[Dict[str, Any]]:
    if play_count_field == "":
        return []

    ascending = "rank" in play_count_field.lower()

    def sort_key(card: Dict[str, Any]) -> tuple[bool, float, str, str]:
        score = _coerce_float(card.get(play_count_field))
        score_missing = score is None
        score_value = float(score or 0.0)
        if not ascending:
            score_value = -score_value
        return (
            score_missing,
            score_value,
            _nonempty_str(card.get("oracle_id")),
            _nonempty_str(card.get("name")),
        )

    ordered = sorted(missing_cards, key=sort_key)
    return [_coverage_entry(card, has_any_primitives=False, play_count_field=play_count_field) for card in ordered[:200]]


def _build_top_missing_by_cmc(missing_cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def sort_key(card: Dict[str, Any]) -> tuple[float, str, str]:
        cmc_value = _coerce_float(card.get("cmc"))
        cmc_key = float(cmc_value) if cmc_value is not None else 9999.0
        return (
            cmc_key,
            _nonempty_str(card.get("oracle_id")),
            _nonempty_str(card.get("name")),
        )

    ordered = sorted(missing_cards, key=sort_key)
    return [_coverage_entry(card, has_any_primitives=False) for card in ordered[:200]]


def _build_top_missing_by_color_identity(missing_cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for card in missing_cards:
        bucket = _normalize_color_identity_bucket(card.get("color_identity"))
        buckets.setdefault(bucket, []).append(card)

    bucket_order = sorted(
        buckets.keys(),
        key=lambda bucket: (-len(buckets[bucket]), bucket),
    )

    remaining = 200
    output: List[Dict[str, Any]] = []
    for bucket in bucket_order:
        if remaining <= 0:
            break

        cards_for_bucket = sorted(
            buckets[bucket],
            key=lambda card: (
                _nonempty_str(card.get("oracle_id")),
                _nonempty_str(card.get("name")),
            ),
        )
        selected = cards_for_bucket[:remaining]
        remaining -= len(selected)

        output.append(
            {
                "color_identity_bucket": bucket,
                "card_count": int(len(cards_for_bucket)),
                "cards": [
                    _coverage_entry(card, has_any_primitives=False)
                    for card in selected
                ],
            }
        )

    return output


def _build_top_missing_deterministic_sample(missing_cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ordered = sorted(
        missing_cards,
        key=lambda card: (
            _nonempty_str(card.get("oracle_id")),
            _nonempty_str(card.get("name")),
        ),
    )
    return [_coverage_entry(card, has_any_primitives=False) for card in ordered[:200]]


def _build_type_bucket_coverage(
    cards: List[Dict[str, Any]],
    *,
    has_any_by_oracle: Dict[str, bool],
) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    for bucket in _TYPE_BUCKETS:
        total = 0
        tagged = 0

        for card in cards:
            type_line = _nonempty_str(card.get("type_line")).lower()
            if bucket not in type_line:
                continue
            total += 1
            oracle_id = _nonempty_str(card.get("oracle_id"))
            if bool(has_any_by_oracle.get(oracle_id)):
                tagged += 1

        pct_tagged = _round6((float(tagged) / float(total)) if total > 0 else 0.0)
        output.append(
            {
                "type_bucket": bucket,
                "total_cards": int(total),
                "tagged_cards": int(tagged),
                "untagged_cards": int(max(total - tagged, 0)),
                "pct_tagged": pct_tagged,
            }
        )

    return output


def build_tag_coverage_audit_v1(
    *,
    snapshot_id: str,
    db_path: Any = None,
    taxonomy_version: str | None = None,
) -> Dict[str, Any]:
    snapshot_id_clean = _nonempty_str(snapshot_id)
    if snapshot_id_clean == "":
        raise ValueError("snapshot_id is required")

    db_path_resolved = _resolve_db_path_from_cli(db_path)

    con = sqlite3.connect(str(db_path_resolved))
    con.row_factory = sqlite3.Row
    try:
        cards_columns = _cards_table_columns(con)
        cards_rows = con.execute(
            """
            SELECT *
            FROM cards
            WHERE snapshot_id = ?
            ORDER BY
              CASE WHEN oracle_id IS NULL THEN 1 ELSE 0 END ASC,
              oracle_id ASC,
              name ASC
            """,
            (snapshot_id_clean,),
        ).fetchall()

        cards = [dict(row) if isinstance(row, sqlite3.Row) else {} for row in cards_rows]
        if len(cards) == 0:
            raise ValueError(f"snapshot_id not found in cards table: {snapshot_id_clean}")

        taxonomy_version_clean = _resolve_taxonomy_version(
            con,
            snapshot_id=snapshot_id_clean,
            taxonomy_version=taxonomy_version,
        )

        tags_by_oracle: Dict[str, List[str]] = {}
        if taxonomy_version_clean != "":
            try:
                tag_rows = con.execute(
                    """
                    SELECT oracle_id, primitive_ids_json
                    FROM card_tags
                    WHERE snapshot_id = ?
                      AND taxonomy_version = ?
                    ORDER BY oracle_id ASC
                    """,
                    (snapshot_id_clean, taxonomy_version_clean),
                ).fetchall()
            except sqlite3.OperationalError:
                tag_rows = []

            for row in tag_rows:
                row_dict = dict(row) if isinstance(row, sqlite3.Row) else {}
                oracle_id = _nonempty_str(row_dict.get("oracle_id") if row_dict else row[0] if len(row) > 0 else "")
                if oracle_id == "":
                    continue
                primitive_ids = _normalize_primitives(
                    row_dict.get("primitive_ids_json") if row_dict else row[1] if len(row) > 1 else []
                )
                tags_by_oracle[oracle_id] = primitive_ids
    finally:
        con.close()

    has_any_by_oracle: Dict[str, bool] = {}
    primitive_counts: Dict[str, int] = {}
    missing_cards: List[Dict[str, Any]] = []

    for card in cards:
        oracle_id = _nonempty_str(card.get("oracle_id"))
        if oracle_id == "":
            continue

        primitive_ids = tags_by_oracle.get(oracle_id, [])
        has_any = len(primitive_ids) > 0
        has_any_by_oracle[oracle_id] = has_any

        if has_any:
            for primitive_id in primitive_ids:
                primitive_counts[primitive_id] = int(primitive_counts.get(primitive_id, 0)) + 1
        else:
            missing_cards.append(card)

    total_cards = int(len(has_any_by_oracle))
    cards_with_any = int(sum(1 for value in has_any_by_oracle.values() if value))
    cards_with_zero = int(max(total_cards - cards_with_any, 0))
    pct_with_any = _round6((float(cards_with_any) / float(total_cards)) if total_cards > 0 else 0.0)

    primitive_distribution = [
        {
            "primitive_id": primitive_id,
            "card_count": int(card_count),
        }
        for primitive_id, card_count in sorted(
            primitive_counts.items(),
            key=lambda item: (-int(item[1]), str(item[0])),
        )
    ]

    play_count_field = _resolve_play_count_field(cards_columns, missing_cards)
    top_missing_by_play_count = _build_top_missing_by_play_count(
        missing_cards,
        play_count_field=play_count_field,
    )

    has_cmc = "cmc" in set(cards_columns)
    has_color_identity = "color_identity" in set(cards_columns)

    top_missing_by_cmc: List[Dict[str, Any]] = []
    top_missing_by_color_identity: List[Dict[str, Any]] = []
    top_missing_sample: List[Dict[str, Any]] = []

    if play_count_field != "":
        missing_strategy = "play_count"
    elif has_cmc and has_color_identity:
        missing_strategy = "cmc_and_color_identity"
        top_missing_by_cmc = _build_top_missing_by_cmc(missing_cards)
        top_missing_by_color_identity = _build_top_missing_by_color_identity(missing_cards)
    else:
        missing_strategy = "oracle_id_fallback"
        top_missing_sample = _build_top_missing_deterministic_sample(missing_cards)

    type_bucket_coverage = _build_type_bucket_coverage(cards, has_any_by_oracle=has_any_by_oracle)

    dfc_total = 0
    dfc_tagged = 0
    for card in cards:
        name = _nonempty_str(card.get("name"))
        if " // " not in name:
            continue
        dfc_total += 1
        oracle_id = _nonempty_str(card.get("oracle_id"))
        if bool(has_any_by_oracle.get(oracle_id)):
            dfc_tagged += 1

    dfc_untagged = int(max(dfc_total - dfc_tagged, 0))
    pct_dfc_with_any = _round6((float(dfc_tagged) / float(dfc_total)) if dfc_total > 0 else 0.0)

    return {
        "version": VERSION,
        "snapshot_id": snapshot_id_clean,
        "taxonomy_version": taxonomy_version_clean if taxonomy_version_clean != "" else None,
        "total_cards": int(total_cards),
        "cards_with_any_primitives": int(cards_with_any),
        "cards_with_zero_primitives": int(cards_with_zero),
        "pct_with_any_primitives": pct_with_any,
        "primitive_distribution": primitive_distribution,
        "top_missing": {
            "strategy": missing_strategy,
            "play_count_field": play_count_field if play_count_field != "" else None,
            "top_200_missing_by_play_count": top_missing_by_play_count,
            "top_200_missing_by_cmc": top_missing_by_cmc,
            "top_200_missing_by_color_identity": top_missing_by_color_identity,
            "top_200_missing_deterministic_sample": top_missing_sample,
        },
        "type_bucket_coverage": type_bucket_coverage,
        "dfc_coverage_sanity": {
            "dfc_cards_total": int(dfc_total),
            "dfc_cards_tagged": int(dfc_tagged),
            "dfc_cards_untagged": int(dfc_untagged),
            "pct_dfc_with_any_primitives": pct_dfc_with_any,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Deterministic tag coverage audit for one snapshot")
    parser.add_argument("--snapshot_id", required=True, help="Snapshot id from cards.snapshot_id")
    parser.add_argument("--db_path", default=None, help="Optional sqlite path (defaults to MTG_ENGINE_DB_PATH or repo default)")
    parser.add_argument("--taxonomy_version", default=None, help="Optional taxonomy version override")
    parser.add_argument("--out", default=None, help="Optional output JSON file path")
    args = parser.parse_args()

    report = build_tag_coverage_audit_v1(
        snapshot_id=args.snapshot_id,
        db_path=args.db_path,
        taxonomy_version=args.taxonomy_version,
    )

    serialized = stable_json_dumps(report)

    out_path = _nonempty_str(args.out)
    if out_path != "":
        target = Path(out_path)
        if not target.is_absolute():
            target = (Path.cwd() / target).resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(serialized, encoding="utf-8")

    print(serialized)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
