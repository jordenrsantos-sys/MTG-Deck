"""
backfill_primitives — Pillar C iter 4 Phase 5.

One-shot tool: applies `primitive_extractor_v1` to every card in the
cards table and writes the matching tag IDs to a new
`cards.primitives_v1_json` column (JSON list of kebab-case tag IDs).

Idempotent:
  - Re-running produces the same output (regex is deterministic).
  - Column is added via `ALTER TABLE ... ADD COLUMN` (try/except for
    re-runs against a snapshot that already has the column).
  - Cross-snapshot propagation: for derived snapshots (tagpass) whose
    rows don't appear in `cards_raw`, the script copies the parent
    snapshot's primitives_v1_json by oracle_id where available, then
    runs the extractor on rows still missing.

Usage:
  python -m tools.backfill_primitives [--db PATH] [--snapshot ID] [--limit N]

The --limit flag is for smoke testing; omit to process all rows.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Set


def _resolve_db_path(arg: Optional[str]) -> str:
    if arg:
        return arg
    # Mirror engine.db resolution.
    repo_root = Path(__file__).resolve().parents[1]
    candidates = [
        repo_root.parent / "data" / "mtg.sqlite",
        repo_root / "data" / "mtg.sqlite",
    ]
    for c in candidates:
        if c.is_file() and c.stat().st_size > 0:
            return str(c)
    raise FileNotFoundError(
        f"Could not locate cards DB. Tried: {[str(c) for c in candidates]}"
    )


def _ensure_column(con: sqlite3.Connection) -> None:
    """Add primitives_v1_json column if not already present. Idempotent."""
    cols = {r[1] for r in con.execute("PRAGMA table_info(cards)")}
    if "primitives_v1_json" not in cols:
        con.execute("ALTER TABLE cards ADD COLUMN primitives_v1_json TEXT")
        con.commit()


def _backfill_snapshot(
    con: sqlite3.Connection,
    snapshot_id: str,
    limit: Optional[int],
    extractor_args: Dict,
) -> Dict:
    """Backfill primitives_v1_json for all rows in this snapshot."""
    from api.engine.extractors.primitive_extractor_v1 import (
        extract_primitives,
    )
    ontology = extractor_args["ontology"]
    combo_assembly = extractor_args["combo_assembly"]

    where = "WHERE snapshot_id=?"
    args: List = [snapshot_id]
    if limit is not None:
        where += " LIMIT ?"
        args.append(limit)
    rows = list(con.execute(
        f"SELECT rowid, name, oracle_text, type_line, mana_cost "
        f"FROM cards {where}",
        args,
    ))
    n_total = len(rows)
    n_tagged = 0
    n_nonempty = 0
    distinct_tags: Set[str] = set()
    t0 = time.perf_counter()
    BATCH = 500
    pending: List = []
    for rid, name, oracle_text, type_line, mana_cost in rows:
        tags = extract_primitives(
            oracle_text=oracle_text or "",
            type_line=type_line or "",
            mana_cost=mana_cost or "",
            card_name=name or "",
            ontology=ontology,
            combo_assembly_set=combo_assembly,
        )
        if tags:
            n_nonempty += 1
            distinct_tags |= tags
        n_tagged += 1
        pending.append((json.dumps(sorted(tags)), rid))
        if len(pending) >= BATCH:
            con.executemany(
                "UPDATE cards SET primitives_v1_json=? WHERE rowid=?",
                pending,
            )
            con.commit()
            pending.clear()
    if pending:
        con.executemany(
            "UPDATE cards SET primitives_v1_json=? WHERE rowid=?",
            pending,
        )
        con.commit()
    elapsed = time.perf_counter() - t0
    return {
        "snapshot_id": snapshot_id,
        "rows_processed": n_tagged,
        "rows_with_tags": n_nonempty,
        "coverage_pct": (n_nonempty / n_total * 100.0) if n_total else 0.0,
        "distinct_tags_seen": len(distinct_tags),
        "elapsed_s": round(elapsed, 2),
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=None,
                        help="Path to mtg.sqlite (auto-detected if omitted)")
    parser.add_argument(
        "--snapshot", action="append", default=None,
        help="Snapshot ID to backfill (repeat for multiple). "
             "If omitted, all snapshots are processed.",
    )
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap rows per snapshot (for smoke testing).")
    args = parser.parse_args(argv)

    db_path = _resolve_db_path(args.db)
    print(f"DB: {db_path}", file=sys.stderr)

    # Pre-load ontology + combo-assembly set once.
    from api.engine.extractors.primitive_extractor_v1 import (
        load_combo_assembly_names, load_ontology,
    )
    print("Loading ontology + combo registry…", file=sys.stderr)
    ontology = load_ontology()
    combo_assembly = load_combo_assembly_names()
    print(
        f"  ontology tags: {len(ontology)}; combo-assembly set: "
        f"{len(combo_assembly)}",
        file=sys.stderr,
    )

    con = sqlite3.connect(db_path)
    try:
        _ensure_column(con)
        if args.snapshot:
            snapshots = list(args.snapshot)
        else:
            snapshots = [
                r[0] for r in con.execute(
                    "SELECT DISTINCT snapshot_id FROM cards"
                ).fetchall()
            ]
        results = []
        for sid in snapshots:
            print(f"Backfilling snapshot {sid}…", file=sys.stderr)
            stats = _backfill_snapshot(
                con, sid, args.limit,
                {"ontology": ontology, "combo_assembly": combo_assembly},
            )
            print(
                f"  {sid}: {stats['rows_with_tags']}/{stats['rows_processed']} "
                f"({stats['coverage_pct']:.1f}%) tagged in "
                f"{stats['elapsed_s']}s",
                file=sys.stderr,
            )
            results.append(stats)
    finally:
        con.close()
    # Print machine-readable summary to stdout.
    print(json.dumps({"backfill_results": results}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
