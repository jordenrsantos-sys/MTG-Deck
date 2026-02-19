from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
import sys
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.engine.version_resolve_v1 import resolve_runtime_ruleset_version, resolve_runtime_taxonomy_version
from engine.db_tags import ensure_tag_tables


def _decode_json_dict(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str):
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _decode_json_str_list(raw: Any) -> List[str]:
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, str)]
    if not isinstance(raw, str):
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [item for item in parsed if isinstance(item, str)]


def main() -> int:
    ap = argparse.ArgumentParser(description="Inspect compiled snapshot tags and runtime indices")
    ap.add_argument("--db", required=True, help="Path to mtg.sqlite")
    ap.add_argument("--snapshot_id", required=True, help="Snapshot id")
    ap.add_argument("--taxonomy_version", required=True, help="Taxonomy version")
    ap.add_argument("--ruleset_version", default=None, help="Optional ruleset version override")
    ap.add_argument("--commander_oracle_id", default=None, help="Optional commander oracle_id for focused facets check")
    args = ap.parse_args()

    db_path = str(Path(args.db))
    snapshot_id = str(args.snapshot_id)

    with sqlite3.connect(db_path) as con:
        con.row_factory = sqlite3.Row
        ensure_tag_tables(con)

        taxonomy_version = resolve_runtime_taxonomy_version(
            snapshot_id=snapshot_id,
            requested=args.taxonomy_version,
            db=con,
        )
        ruleset_version = resolve_runtime_ruleset_version(
            snapshot_id=snapshot_id,
            taxonomy_version=taxonomy_version,
            requested=args.ruleset_version,
            db=con,
        )

        card_tags_rows = 0
        facets_nonempty_rows = 0
        commander_rows = 0
        commander_facets_nonempty_rows = 0

        if isinstance(taxonomy_version, str) and taxonomy_version != "" and isinstance(ruleset_version, str) and ruleset_version != "":
            card_tags_rows = int(
                (
                    con.execute(
                        """
                        SELECT COUNT(1)
                        FROM card_tags
                        WHERE snapshot_id = ?
                          AND taxonomy_version = ?
                          AND ruleset_version = ?
                        """,
                        (snapshot_id, taxonomy_version, ruleset_version),
                    ).fetchone()
                    or [0]
                )[0]
            )

            facets_nonempty_rows = int(
                (
                    con.execute(
                        """
                        SELECT COUNT(1)
                        FROM card_tags
                        WHERE snapshot_id = ?
                          AND taxonomy_version = ?
                          AND ruleset_version = ?
                          AND facets_json IS NOT NULL
                          AND TRIM(facets_json) NOT IN ('', '{}', 'null')
                        """,
                        (snapshot_id, taxonomy_version, ruleset_version),
                    ).fetchone()
                    or [0]
                )[0]
            )

            if isinstance(args.commander_oracle_id, str) and args.commander_oracle_id.strip() != "":
                row = con.execute(
                    """
                    SELECT facets_json
                    FROM card_tags
                    WHERE snapshot_id = ?
                      AND taxonomy_version = ?
                      AND ruleset_version = ?
                      AND oracle_id = ?
                    LIMIT 1
                    """,
                    (snapshot_id, taxonomy_version, ruleset_version, args.commander_oracle_id.strip()),
                ).fetchone()
                if row is not None:
                    commander_rows = 1
                    commander_facets_nonempty_rows = 1 if len(_decode_json_dict(row[0])) > 0 else 0

        primitive_to_cards_table_exists = (
            con.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='primitive_to_cards' LIMIT 1"
            ).fetchone()
            is not None
        )
        primitive_to_cards_rows = 0
        if primitive_to_cards_table_exists and isinstance(taxonomy_version, str) and taxonomy_version != "":
            primitive_to_cards_rows = int(
                (
                    con.execute(
                        "SELECT COUNT(1) FROM primitive_to_cards WHERE snapshot_id = ? AND taxonomy_version = ?",
                        (snapshot_id, taxonomy_version),
                    ).fetchone()
                    or [0]
                )[0]
            )

        sample_rows = []
        if isinstance(taxonomy_version, str) and taxonomy_version != "" and isinstance(ruleset_version, str) and ruleset_version != "":
            raw_rows = con.execute(
                """
                SELECT oracle_id, primitive_ids_json, facets_json
                FROM card_tags
                WHERE snapshot_id = ?
                  AND taxonomy_version = ?
                  AND ruleset_version = ?
                ORDER BY oracle_id ASC
                LIMIT 5
                """,
                (snapshot_id, taxonomy_version, ruleset_version),
            ).fetchall()
            for row in raw_rows:
                sample_rows.append(
                    {
                        "oracle_id": row[0],
                        "primitive_ids": _decode_json_str_list(row[1]),
                        "facets": _decode_json_dict(row[2]),
                    }
                )

    facets_nonempty_rate_overall = (float(facets_nonempty_rows) / float(card_tags_rows)) if card_tags_rows > 0 else 0.0
    facets_nonempty_rate_commander = (
        (float(commander_facets_nonempty_rows) / float(commander_rows)) if commander_rows > 0 else None
    )

    report = {
        "snapshot_id": snapshot_id,
        "taxonomy_version_detected": taxonomy_version,
        "ruleset_version_detected": ruleset_version,
        "counts": {
            "card_tags_rows_snapshot_taxonomy_ruleset": card_tags_rows,
            "primitive_to_cards_rows_snapshot_taxonomy": primitive_to_cards_rows,
            "commander_rows_snapshot_taxonomy_ruleset": commander_rows,
            "commander_facets_nonempty_rows": commander_facets_nonempty_rows,
        },
        "rates": {
            "facets_nonempty_rate_overall": round(facets_nonempty_rate_overall, 6),
            "facets_nonempty_rate_commander": (
                round(facets_nonempty_rate_commander, 6) if isinstance(facets_nonempty_rate_commander, float) else None
            ),
        },
        "sample_rows": sample_rows,
    }

    print(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
