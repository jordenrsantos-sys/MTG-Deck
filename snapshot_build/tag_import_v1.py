from __future__ import annotations

import argparse
import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set

from engine.db import resolve_db_path
from engine.determinism import stable_json_dumps
from snapshot_build.tag_snapshot import compile_snapshot_tags
from taxonomy.loader import load


VERSION = "tag_import_v1"


def _nonempty_str(value: Any) -> str:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return ""


def _clean_unique_strings(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []
    return sorted(
        {
            token
            for token in (_nonempty_str(value) for value in values)
            if token != ""
        }
    )


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


def _load_import_pack(pack_path: Path) -> Dict[str, Any]:
    payload = json.loads(pack_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Import pack must be a JSON object")

    version = _nonempty_str(payload.get("version"))
    if version == "":
        raise ValueError("Import pack missing non-empty 'version'")

    entries_raw = payload.get("entries") if isinstance(payload.get("entries"), list) else []

    merged: Dict[str, Dict[str, Any]] = {}
    for row in entries_raw:
        if not isinstance(row, dict):
            continue

        oracle_id = _nonempty_str(row.get("oracle_id"))
        if oracle_id == "":
            continue

        card_name = _nonempty_str(row.get("name"))
        primitive_ids = _clean_unique_strings(row.get("primitive_ids"))
        if len(primitive_ids) == 0:
            continue

        bucket = merged.setdefault(
            oracle_id,
            {
                "oracle_id": oracle_id,
                "name": card_name,
                "primitive_ids": set(),
            },
        )
        if _nonempty_str(bucket.get("name")) == "" and card_name != "":
            bucket["name"] = card_name

        primitive_set = bucket.get("primitive_ids") if isinstance(bucket.get("primitive_ids"), set) else set()
        primitive_set.update(primitive_ids)
        bucket["primitive_ids"] = primitive_set

    normalized: List[Dict[str, Any]] = []
    for oracle_id in sorted(merged.keys()):
        row = merged[oracle_id]
        primitive_set = row.get("primitive_ids") if isinstance(row.get("primitive_ids"), set) else set()
        normalized.append(
            {
                "oracle_id": oracle_id,
                "name": _nonempty_str(row.get("name")),
                "primitive_ids": sorted({token for token in primitive_set if isinstance(token, str) and token != ""}),
            }
        )

    return {
        "version": version,
        "entries": normalized,
    }


def _load_allowed_primitive_ids(taxonomy_pack: Any) -> Set[str]:
    allowed: Set[str] = set()

    rules = taxonomy_pack.rulespec_rules if hasattr(taxonomy_pack, "rulespec_rules") else []
    for rule in rules if isinstance(rules, (list, tuple)) else []:
        if not isinstance(rule, dict):
            continue
        primitive_id = _nonempty_str(
            rule.get("primitive_id")
            or rule.get("primitive")
            or rule.get("tag")
            or rule.get("primitive_tag")
        )
        if primitive_id != "":
            allowed.add(primitive_id)

    other_sheets = taxonomy_pack.other_sheets if hasattr(taxonomy_pack, "other_sheets") else {}
    primitives_sheet = other_sheets.get("primitives.json") if isinstance(other_sheets, dict) else None
    if isinstance(primitives_sheet, list):
        for row in primitives_sheet:
            if isinstance(row, dict):
                primitive_id = _nonempty_str(
                    row.get("primitive_id")
                    or row.get("id")
                    or row.get("tag_id")
                    or row.get("name")
                )
            elif isinstance(row, str):
                primitive_id = _nonempty_str(row)
            else:
                primitive_id = ""
            if primitive_id != "":
                allowed.add(primitive_id)

    return allowed


def _chunked(items: List[str], size: int) -> Iterable[List[str]]:
    chunk_size = max(int(size), 1)
    for start in range(0, len(items), chunk_size):
        yield items[start : start + chunk_size]


def _load_cards_and_primitives(
    *,
    db_path: Path,
    snapshot_id: str,
    taxonomy_version: str,
    oracle_ids: List[str],
) -> Dict[str, Dict[str, Any]]:
    if len(oracle_ids) == 0:
        return {}

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        cards_by_oracle: Dict[str, Dict[str, Any]] = {}

        for oracle_chunk in _chunked(oracle_ids, 500):
            placeholders = ",".join(["?"] * len(oracle_chunk))

            card_rows = con.execute(
                f"""
                SELECT oracle_id, name
                FROM cards
                WHERE snapshot_id = ?
                  AND oracle_id IN ({placeholders})
                ORDER BY oracle_id ASC
                """,
                (snapshot_id, *oracle_chunk),
            ).fetchall()
            for row in card_rows:
                row_dict = dict(row) if isinstance(row, sqlite3.Row) else {}
                oracle_id = _nonempty_str(row_dict.get("oracle_id") if row_dict else row[0] if len(row) > 0 else "")
                if oracle_id == "":
                    continue
                cards_by_oracle.setdefault(
                    oracle_id,
                    {
                        "oracle_id": oracle_id,
                        "name": _nonempty_str(row_dict.get("name") if row_dict else row[1] if len(row) > 1 else ""),
                        "existing_primitives": set(),
                    },
                )

            try:
                tag_rows = con.execute(
                    f"""
                    SELECT oracle_id, primitive_ids_json
                    FROM card_tags
                    WHERE snapshot_id = ?
                      AND taxonomy_version = ?
                      AND oracle_id IN ({placeholders})
                    ORDER BY oracle_id ASC
                    """,
                    (snapshot_id, taxonomy_version, *oracle_chunk),
                ).fetchall()
            except sqlite3.OperationalError:
                tag_rows = []

            for row in tag_rows:
                row_dict = dict(row) if isinstance(row, sqlite3.Row) else {}
                oracle_id = _nonempty_str(row_dict.get("oracle_id") if row_dict else row[0] if len(row) > 0 else "")
                if oracle_id == "":
                    continue
                bucket = cards_by_oracle.setdefault(
                    oracle_id,
                    {
                        "oracle_id": oracle_id,
                        "name": "",
                        "existing_primitives": set(),
                    },
                )
                primitive_raw = row_dict.get("primitive_ids_json") if row_dict else row[1] if len(row) > 1 else []
                primitive_ids = _clean_unique_strings(json.loads(primitive_raw) if isinstance(primitive_raw, str) else primitive_raw)
                existing = bucket.get("existing_primitives") if isinstance(bucket.get("existing_primitives"), set) else set()
                existing.update(primitive_ids)
                bucket["existing_primitives"] = existing

        return {
            oracle_id: {
                "oracle_id": oracle_id,
                "name": _nonempty_str(row.get("name")),
                "existing_primitives": set(
                    token
                    for token in (row.get("existing_primitives") if isinstance(row.get("existing_primitives"), set) else set())
                    if isinstance(token, str) and token != ""
                ),
            }
            for oracle_id, row in cards_by_oracle.items()
        }
    finally:
        con.close()


@contextmanager
def _temporary_db_env(db_path: Path):
    old_value = os.getenv("MTG_ENGINE_DB_PATH")
    os.environ["MTG_ENGINE_DB_PATH"] = str(db_path)
    try:
        yield
    finally:
        if old_value is None:
            os.environ.pop("MTG_ENGINE_DB_PATH", None)
        else:
            os.environ["MTG_ENGINE_DB_PATH"] = old_value


def run_tag_import_v1(
    *,
    snapshot_id: str,
    taxonomy_pack_folder: str,
    import_pack_path: str,
    db_path: Any = None,
    build_indices: bool = False,
    dry_run: bool = False,
) -> Dict[str, Any]:
    snapshot_id_clean = _nonempty_str(snapshot_id)
    if snapshot_id_clean == "":
        raise ValueError("snapshot_id is required")

    taxonomy_pack_folder_clean = _nonempty_str(taxonomy_pack_folder)
    if taxonomy_pack_folder_clean == "":
        raise ValueError("taxonomy_pack_folder is required")

    import_pack_path_clean = _nonempty_str(import_pack_path)
    if import_pack_path_clean == "":
        raise ValueError("import_pack_path is required")

    db_path_resolved = _resolve_db_path_from_cli(db_path)

    taxonomy_pack = load(taxonomy_pack_folder_clean)
    taxonomy_version = _nonempty_str(getattr(taxonomy_pack, "taxonomy_version", ""))
    if taxonomy_version == "":
        raise ValueError("taxonomy pack missing taxonomy_version")

    import_pack_obj = _load_import_pack(Path(import_pack_path_clean))
    entries = import_pack_obj["entries"]
    allowed_primitives = _load_allowed_primitive_ids(taxonomy_pack)

    requested_oracle_ids = sorted(
        {
            _nonempty_str(row.get("oracle_id"))
            for row in entries
            if isinstance(row, dict)
        }
    )
    requested_oracle_ids = [oracle_id for oracle_id in requested_oracle_ids if oracle_id != ""]

    cards_by_oracle = _load_cards_and_primitives(
        db_path=db_path_resolved,
        snapshot_id=snapshot_id_clean,
        taxonomy_version=taxonomy_version,
        oracle_ids=requested_oracle_ids,
    )

    patch_rows: List[Dict[str, Any]] = []
    invalid_primitive_ids: Set[str] = set()
    missing_oracle_ids: List[str] = []
    skipped_existing_count = 0

    for row in entries:
        if not isinstance(row, dict):
            continue
        oracle_id = _nonempty_str(row.get("oracle_id"))
        if oracle_id == "":
            continue

        card_row = cards_by_oracle.get(oracle_id)
        if card_row is None:
            missing_oracle_ids.append(oracle_id)
            continue

        existing_primitives = card_row.get("existing_primitives") if isinstance(card_row.get("existing_primitives"), set) else set()
        primitive_ids = _clean_unique_strings(row.get("primitive_ids"))

        for primitive_id in primitive_ids:
            if primitive_id not in allowed_primitives:
                invalid_primitive_ids.add(primitive_id)
                continue
            if primitive_id in existing_primitives:
                skipped_existing_count += 1
                continue
            patch_rows.append(
                {
                    "op": "primitive_add",
                    "oracle_id": oracle_id,
                    "primitive_id": primitive_id,
                    "patch_pack_version": import_pack_obj["version"],
                }
            )

    patch_rows = sorted(
        {
            (
                _nonempty_str(row.get("oracle_id")),
                _nonempty_str(row.get("primitive_id")),
            )
            for row in patch_rows
            if isinstance(row, dict)
        },
        key=lambda item: (item[0], item[1]),
    )

    patch_rows_final = [
        {
            "op": "primitive_add",
            "oracle_id": oracle_id,
            "primitive_id": primitive_id,
            "patch_pack_version": import_pack_obj["version"],
        }
        for oracle_id, primitive_id in patch_rows
    ]

    summary: Dict[str, Any] = {
        "version": VERSION,
        "snapshot_id": snapshot_id_clean,
        "taxonomy_version": taxonomy_version,
        "import_pack_version": import_pack_obj["version"],
        "entries_total": int(len(entries)),
        "requested_oracle_ids": int(len(requested_oracle_ids)),
        "missing_oracle_ids": sorted(set(missing_oracle_ids)),
        "invalid_primitive_ids": sorted(invalid_primitive_ids),
        "skipped_existing_count": int(skipped_existing_count),
        "patch_rows_planned": int(len(patch_rows_final)),
        "patch_rows_preview_top10": patch_rows_final[:10],
        "dry_run": bool(dry_run),
    }

    if dry_run:
        return summary

    with _temporary_db_env(db_path_resolved):
        compile_summary = compile_snapshot_tags(
            snapshot_id=snapshot_id_clean,
            taxonomy_pack_folder=taxonomy_pack_folder_clean,
            patch_rows=patch_rows_final,
            build_indices=bool(build_indices),
        )

    summary["compile_summary"] = compile_summary
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Deterministic offline tag import pipeline v1")
    parser.add_argument("--snapshot_id", required=True, help="Snapshot id from cards.snapshot_id")
    parser.add_argument("--taxonomy_pack", required=True, help="Path to taxonomy pack folder")
    parser.add_argument("--import_pack", required=True, help="Path to tag import pack JSON")
    parser.add_argument("--db_path", default=None, help="Optional sqlite path (defaults to MTG_ENGINE_DB_PATH or repo default)")
    parser.add_argument("--build_indices", action="store_true", help="Build runtime indices after compile")
    parser.add_argument("--dry_run", action="store_true", help="Do not persist, only report planned patch rows")
    args = parser.parse_args()

    summary = run_tag_import_v1(
        snapshot_id=args.snapshot_id,
        taxonomy_pack_folder=args.taxonomy_pack,
        import_pack_path=args.import_pack,
        db_path=args.db_path,
        build_indices=bool(args.build_indices),
        dry_run=bool(args.dry_run),
    )
    print(stable_json_dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
