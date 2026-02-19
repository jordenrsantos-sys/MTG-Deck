from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Tuple
import json
import sqlite3


@dataclass(frozen=True)
class PatchAppliedRow:
    oracle_id: str
    snapshot_id: str
    taxonomy_version: str
    patch_pack_version: str
    patch_json: str
    created_at: str


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_patches_applied_table(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS patches_applied (
          id INTEGER PRIMARY KEY,
          oracle_id TEXT,
          snapshot_id TEXT,
          taxonomy_version TEXT,
          patch_pack_version TEXT,
          patch_json TEXT,
          created_at TEXT
        )
        """
    )


def record_patch_rows(con: sqlite3.Connection, rows: Iterable[PatchAppliedRow]) -> int:
    prepared: List[Tuple[str, str, str, str, str, str]] = []
    for row in rows:
        prepared.append(
            (
                row.oracle_id,
                row.snapshot_id,
                row.taxonomy_version,
                row.patch_pack_version,
                row.patch_json,
                row.created_at,
            )
        )

    if not prepared:
        return 0

    con.executemany(
        """
        INSERT INTO patches_applied (
          oracle_id,
          snapshot_id,
          taxonomy_version,
          patch_pack_version,
          patch_json,
          created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        prepared,
    )
    return len(prepared)


def apply_patch_overrides(
    compiled_rows: List[Dict[str, Any]],
    patch_rows: List[Dict[str, Any]] | None,
) -> Tuple[List[Dict[str, Any]], List[PatchAppliedRow]]:
    """
    Deterministic patch application hook for compile-time taxonomy rows.

    v1 behavior is conservative and additive only:
    - If no patch rows provided, return rows unchanged.
    - If patch rows are provided, only support explicit primitive_add operations.

    Unsupported patch rows are ignored to keep compiler stable in closed-world mode.
    """

    if not patch_rows:
        return compiled_rows, []

    by_oracle: Dict[str, Dict[str, Any]] = {
        str(row.get("oracle_id")): row
        for row in compiled_rows
        if isinstance(row.get("oracle_id"), str) and str(row.get("oracle_id")) != ""
    }

    applied: List[PatchAppliedRow] = []
    for patch in patch_rows:
        if not isinstance(patch, dict):
            continue
        op = str(patch.get("op") or "").strip().lower()
        oracle_id = str(patch.get("oracle_id") or "").strip()
        if op != "primitive_add" or oracle_id == "":
            continue

        primitive_id = str(patch.get("primitive_id") or "").strip()
        if primitive_id == "":
            continue

        card_row = by_oracle.get(oracle_id)
        if card_row is None:
            continue

        primitive_ids = card_row.get("primitive_ids") if isinstance(card_row.get("primitive_ids"), list) else []
        if primitive_id not in primitive_ids:
            primitive_ids.append(primitive_id)
            primitive_ids_sorted = sorted(set([pid for pid in primitive_ids if isinstance(pid, str) and pid != ""]))
            card_row["primitive_ids"] = primitive_ids_sorted

            evidence = card_row.get("evidence") if isinstance(card_row.get("evidence"), list) else []
            evidence.append(
                {
                    "rule_id": "PATCH_PRIMITIVE_ADD",
                    "field": "patch",
                    "span": [0, 0],
                    "snippet": primitive_id,
                }
            )
            card_row["evidence"] = sorted(
                evidence,
                key=lambda item: (
                    str(item.get("rule_id") or ""),
                    str(item.get("field") or ""),
                    str(item.get("snippet") or ""),
                ),
            )

        # metadata fields are added downstream by caller
        applied.append(
            PatchAppliedRow(
                oracle_id=oracle_id,
                snapshot_id="",
                taxonomy_version="",
                patch_pack_version=str(patch.get("patch_pack_version") or ""),
                patch_json=json.dumps(patch, separators=(",", ":"), sort_keys=True, ensure_ascii=False),
                created_at=utc_now_iso(),
            )
        )

    return compiled_rows, applied
