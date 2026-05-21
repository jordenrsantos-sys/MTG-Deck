"""
check_new_sets — Mega-task v3 Phase 1 CLI.

Runs the Scryfall set-release watcher once. Exits:
  0  no new sets detected (nothing to do)
  1  new sets detected (parent scheduler should trigger ingestion)
  2  error (Scryfall unreachable, ledger corrupt, etc.)

Usage:
  python tools/check_new_sets.py
  python tools/check_new_sets.py --json   # print JSON to stdout
  python tools/check_new_sets.py --init-from-corpus
      Seed `known_set_codes_v1.json` from cards_raw (one-time install
      step). After this, repeat-running the tool will only flag sets
      Scryfall added since the seed.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Ensure repo root is on sys.path so `api.engine.integrations` imports work
# when the tool is invoked from anywhere.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--init-from-corpus", action="store_true",
        help="Seed the known-codes ledger from the cards_raw table.",
    )
    parser.add_argument(
        "--db", default=r"E:\MTG Root\mtg-engine\data\mtg.sqlite",
        help="Path to mtg.sqlite (used by --init-from-corpus).",
    )
    parser.add_argument(
        "--json", action="store_true", default=False,
        help="Print JSON output instead of human-readable text.",
    )
    args = parser.parse_args(argv)

    from api.engine.integrations.scryfall_sets_watcher_v1 import (
        fetch_set_index,
        find_new_sets,
        initialize_known_set_codes_from_corpus,
        load_known_set_codes,
        save_known_set_codes,
    )

    if args.init_from_corpus:
        codes = initialize_known_set_codes_from_corpus(Path(args.db))
        save_known_set_codes(codes)
        msg = f"Seeded known_set_codes_v1.json with {len(codes)} codes."
        if args.json:
            print(json.dumps({"status": "initialized", "code_count": len(codes)}))
        else:
            print(msg)
        return 0

    try:
        set_index = fetch_set_index()
    except Exception as exc:
        if args.json:
            print(json.dumps({"status": "error", "error": str(exc)}))
        else:
            print(f"ERROR: could not fetch Scryfall /sets: {exc}", file=sys.stderr)
        return 2

    known_codes = load_known_set_codes()
    new_sets = find_new_sets(set_index, known_codes)

    if args.json:
        print(json.dumps({
            "status": "ok",
            "known_code_count": len(known_codes),
            "scryfall_set_count": len(set_index),
            "new_sets": [
                {"code": s.get("code"), "name": s.get("name"),
                 "released_at": s.get("released_at"),
                 "card_count": s.get("card_count")}
                for s in new_sets
            ],
        }))
    else:
        if not new_sets:
            print(f"No new sets detected (known: {len(known_codes)}, "
                  f"Scryfall reports: {len(set_index)}).")
        else:
            print(f"NEW SETS DETECTED ({len(new_sets)}):")
            for s in new_sets:
                print(
                    f"  - {s.get('code')}  {s.get('name')}  "
                    f"released={s.get('released_at')}  "
                    f"cards={s.get('card_count')}"
                )

    return 1 if new_sets else 0


if __name__ == "__main__":
    sys.exit(main())
