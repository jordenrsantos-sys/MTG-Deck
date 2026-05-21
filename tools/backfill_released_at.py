"""Iter 3 Phase 5: add `released_at` column to the cards table and backfill
from `cards_raw.json`.

Idempotent: re-running the script leaves the schema and data unchanged.

The backfill computes `min(released_at)` across all printings of each
oracle_id within each snapshot (cards is keyed on snapshot_id +
oracle_id; cards_raw is keyed on snapshot_id + scryfall_id with
oracle_id available). The "earliest release" is what matters for a
"recent set" filter — a reprint in a recent set doesn't make the card
itself recent.

Usage:
    python tools/backfill_released_at.py [--db PATH] [--dry-run]

Default `--db` resolves via the same code path as the engine (env var
MTG_ENGINE_DB_PATH or the canonical fallback).
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Dict, Iterator, Optional, Tuple


def column_exists(con: sqlite3.Connection, table: str, column: str) -> bool:
    cur = con.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cur)


def ensure_column(con: sqlite3.Connection, dry_run: bool = False) -> bool:
    """Add the `released_at TEXT` column to cards if missing. Returns
    True if a change was made, False if column already existed."""
    if column_exists(con, "cards", "released_at"):
        return False
    if dry_run:
        return True
    con.execute("ALTER TABLE cards ADD COLUMN released_at TEXT")
    con.commit()
    return True


def _iter_card_release_dates(con: sqlite3.Connection) -> Iterator[Tuple[str, str, str]]:
    """Yield (snapshot_id, oracle_id, earliest_release_iso) tuples from
    cards_raw. Cards without a valid released_at in their JSON are
    skipped; cards with multiple printings yield the earliest by
    iso-date string comparison (which is correct for ISO YYYY-MM-DD)."""
    cur = con.execute(
        "SELECT snapshot_id, oracle_id, json FROM cards_raw WHERE oracle_id IS NOT NULL"
    )
    earliest: Dict[Tuple[str, str], str] = {}
    for snap, oid, j in cur:
        try:
            data = json.loads(j)
        except (TypeError, json.JSONDecodeError):
            continue
        rd = data.get("released_at")
        if not isinstance(rd, str) or len(rd) < 10:
            continue
        key = (snap, oid)
        existing = earliest.get(key)
        if existing is None or rd < existing:
            earliest[key] = rd
    for (snap, oid), rd in earliest.items():
        yield snap, oid, rd


def propagate_across_snapshots(con: sqlite3.Connection, dry_run: bool = False) -> int:
    """After the initial backfill from cards_raw, some snapshots may
    still have NULL released_at — typically derived snapshots that
    inherit cards from a parent snapshot without re-ingesting
    cards_raw (e.g. tagpass snapshots). Propagate the earliest
    released_at across snapshots by oracle_id.

    Returns the count of cards rows updated.
    """
    if dry_run:
        # Estimate by counting NULL rows that have a populated sibling.
        return con.execute(
            "SELECT COUNT(*) FROM cards c "
            "WHERE c.released_at IS NULL "
            "  AND EXISTS (SELECT 1 FROM cards s "
            "              WHERE s.oracle_id = c.oracle_id "
            "                AND s.released_at IS NOT NULL)"
        ).fetchone()[0]
    # Build a map oracle_id -> earliest released_at across ALL snapshots,
    # then update cards rows that are NULL.
    cur = con.execute(
        "SELECT oracle_id, MIN(released_at) "
        "FROM cards WHERE released_at IS NOT NULL "
        "GROUP BY oracle_id"
    )
    earliest = {row[0]: row[1] for row in cur if row[0] and row[1]}
    updated = 0
    BATCH = 1000
    pending = []
    for oid, rd in earliest.items():
        pending.append((rd, oid))
        if len(pending) >= BATCH:
            cur = con.executemany(
                "UPDATE cards SET released_at = ? "
                "WHERE oracle_id = ? AND released_at IS NULL",
                pending,
            )
            updated += cur.rowcount or 0
            con.commit()
            pending = []
    if pending:
        cur = con.executemany(
            "UPDATE cards SET released_at = ? "
            "WHERE oracle_id = ? AND released_at IS NULL",
            pending,
        )
        updated += cur.rowcount or 0
        con.commit()
    return updated


def backfill(con: sqlite3.Connection, dry_run: bool = False) -> Dict[str, int]:
    """Backfill `cards.released_at` by joining with cards_raw on
    (snapshot_id, oracle_id). Returns counts: total seen, updated."""
    if not column_exists(con, "cards", "released_at"):
        raise RuntimeError(
            "cards.released_at column does not exist. Run with "
            "ensure_column first (the CLI does this automatically)."
        )

    seen = 0
    updated = 0
    BATCH = 1000
    pending = []
    for snap, oid, rd in _iter_card_release_dates(con):
        seen += 1
        pending.append((rd, snap, oid))
        if len(pending) >= BATCH:
            if not dry_run:
                con.executemany(
                    "UPDATE cards SET released_at = ? "
                    "WHERE snapshot_id = ? AND oracle_id = ? "
                    "  AND (released_at IS NULL OR released_at > ?)",
                    [(rd, s, o, rd) for rd, s, o in pending],
                )
                con.commit()
            updated += con.total_changes
            pending = []
    if pending:
        if not dry_run:
            con.executemany(
                "UPDATE cards SET released_at = ? "
                "WHERE snapshot_id = ? AND oracle_id = ? "
                "  AND (released_at IS NULL OR released_at > ?)",
                [(rd, s, o, rd) for rd, s, o in pending],
            )
            con.commit()
        updated += con.total_changes
    return {"seen": seen, "updated": updated}


def _resolve_db_path(arg: Optional[str]) -> Path:
    if arg:
        return Path(arg)
    # Mirror engine.db.resolve_db_path() — env var first, fallback to
    # the project's canonical mtg.sqlite location.
    import os
    env = os.environ.get("MTG_ENGINE_DB_PATH")
    if env:
        return Path(env)
    return Path(r"E:\MTG Root\mtg-engine\data\mtg.sqlite")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=None, help="path to mtg.sqlite")
    parser.add_argument("--dry-run", action="store_true",
                        help="print what would change without writing")
    args = parser.parse_args()

    path = _resolve_db_path(args.db)
    if not path.is_file():
        print(f"ERROR: db file not found: {path}", file=sys.stderr)
        return 2

    con = sqlite3.connect(str(path))
    try:
        added = ensure_column(con, dry_run=args.dry_run)
        print(f"Column add: {'would add (dry-run)' if args.dry_run and added else ('added' if added else 'already exists')}")
        if args.dry_run and not column_exists(con, "cards", "released_at"):
            # In dry-run mode we can't backfill without the column.
            print("Dry-run: column missing; skipping backfill counts.")
            return 0
        result = backfill(con, dry_run=args.dry_run)
        print(f"Backfill {'dry-run ' if args.dry_run else ''}seen={result['seen']} updated={result['updated']}")
        # Step 2: propagate across snapshots (cards_raw doesn't always
        # have all snapshots; derived snapshots like tagpass inherit
        # cards rows without re-ingesting cards_raw).
        propagated = propagate_across_snapshots(con, dry_run=args.dry_run)
        print(f"Cross-snapshot propagation: {'would update' if args.dry_run else 'updated'} {propagated} rows")
        # Quick sanity: how many cards now have released_at populated?
        count = con.execute(
            "SELECT COUNT(*) FROM cards WHERE released_at IS NOT NULL"
        ).fetchone()[0]
        total = con.execute("SELECT COUNT(*) FROM cards").fetchone()[0]
        print(f"cards.released_at populated: {count} / {total} ({100.0 * count / max(1, total):.1f}%)")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
