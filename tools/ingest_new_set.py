"""
ingest_new_set — Mega-task v3 Phase 2 CLI.

Fetch a Scryfall set's cards, diff against the corpus, write new +
errata rows into the cards + cards_raw tables. Idempotent — re-runs
produce no new inserts when nothing has changed.

Usage:
  python tools/ingest_new_set.py <set_code>
  python tools/ingest_new_set.py <set_code> --snapshot <id>
  python tools/ingest_new_set.py <set_code> --db <path>
  python tools/ingest_new_set.py <set_code> --dry-run

The --dry-run flag fetches + diffs but doesn't write to the DB.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


DEFAULT_SNAPSHOT_ID = "20260217_190902_tagpass_20260222"
DEFAULT_DB_PATH = r"E:\MTG Root\mtg-engine\data\mtg.sqlite"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("set_code", help="Scryfall 3-letter set code (case-insensitive)")
    parser.add_argument("--snapshot", default=DEFAULT_SNAPSHOT_ID)
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch + diff but don't write to the DB.")
    parser.add_argument("--no-ledger", action="store_true",
                        help="Don't append set_code to the known-codes ledger.")
    args = parser.parse_args(argv)

    from api.engine.integrations.scryfall_set_ingest_v1 import (
        diff_against_corpus, fetch_set_cards, ingest_new_set,
    )

    db_path = Path(args.db)
    if not db_path.is_file():
        print(f"ERROR: db not found at {db_path}", file=sys.stderr)
        return 2

    print(f"Fetching set '{args.set_code}' from Scryfall...", file=sys.stderr)
    try:
        cards = fetch_set_cards(args.set_code)
    except Exception as exc:
        print(f"ERROR: Scryfall fetch failed: {exc}", file=sys.stderr)
        return 2
    print(f"  fetched {len(cards)} cards", file=sys.stderr)

    if args.dry_run:
        diff = diff_against_corpus(cards, db_path, args.snapshot)
        print(json.dumps({
            "set_code": args.set_code, "fetched": len(cards),
            "new_cards_count": len(diff["new_cards"]),
            "reprints_count": len(diff["reprints"]),
            "errata_count": len(diff["errata"]),
            "status": "dry_run",
        }, indent=2))
        return 0

    result = ingest_new_set(
        args.set_code, db_path, args.snapshot, cards=cards,
        update_ledger=not args.no_ledger,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
