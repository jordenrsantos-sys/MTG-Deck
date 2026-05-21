"""
backfill_primitives_v2 — Pillar C ontology v1 backfill (iter 5 Phase 3).

Walks the cards table, runs the v2 extractor (regex pass over the
ontology v1 patterns), optionally calls the LLM supplement for cards
with <2 regex tags, writes results to `cards.primitives_v1_json`.

Usage:
  python -m tools.backfill_primitives_v2
      --snapshot 20260217_190902_tagpass_20260222
      --limit N
      --llm-supplement
      --llm-budget-usd 10.0
      --commander-legal-only

Idempotent. Re-runs produce identical regex output (deterministic).
The LLM supplement adds slight non-determinism (model output varies);
cards with LLM-added tags are recorded in a separate audit JSON.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set


def _resolve_db_path(arg: Optional[str]) -> Path:
    if arg:
        return Path(arg)
    repo_root = Path(__file__).resolve().parents[1]
    candidates = [
        repo_root.parent / "data" / "mtg.sqlite",
        repo_root / "data" / "mtg.sqlite",
    ]
    for c in candidates:
        if c.is_file() and c.stat().st_size > 0:
            return c
    raise FileNotFoundError(f"Could not locate cards DB. Tried: {candidates}")


def _ensure_column(con: sqlite3.Connection) -> None:
    cols = {r[1] for r in con.execute("PRAGMA table_info(cards)")}
    if "primitives_v1_json" not in cols:
        con.execute("ALTER TABLE cards ADD COLUMN primitives_v1_json TEXT")
        con.commit()


def _commander_legal_filter(legalities_json: Optional[str]) -> bool:
    if not legalities_json:
        return False
    try:
        leg = json.loads(legalities_json)
    except json.JSONDecodeError:
        return False
    return leg.get("commander") == "legal"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", action="append", default=None,
                        help="Snapshot ID to backfill. Default: all.")
    parser.add_argument("--db", default=None)
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap rows per snapshot (for smoke testing).")
    parser.add_argument("--llm-supplement", action="store_true",
                        help="Call the LLM extractor for ambiguous cards.")
    parser.add_argument("--llm-budget-usd", type=float, default=10.0,
                        help="Hard cap on LLM-extractor cost.")
    parser.add_argument("--commander-legal-only", action="store_true",
                        help="Only process Commander-legal cards.")
    args = parser.parse_args(argv)

    db_path = _resolve_db_path(args.db)
    print(f"DB: {db_path}", file=sys.stderr)

    from api.engine.extractors.primitive_extractor_v2 import (
        extract_primitives_v2, load_ontology_v1,
    )
    from api.engine.extractors.primitive_extractor_v1 import (
        load_combo_assembly_names,
    )
    print("Loading ontology v1 + combo registry…", file=sys.stderr)
    ontology = load_ontology_v1()
    combo_assembly = load_combo_assembly_names()
    print(
        f"  ontology tags: {len(ontology)}; combo-assembly set: "
        f"{len(combo_assembly)}",
        file=sys.stderr,
    )

    llm_extractor = None
    llm_cost_so_far = 0.0
    llm_calls = 0
    if args.llm_supplement:
        from api.engine.extractors.primitive_extractor_llm_v1 import (
            is_ambiguous, llm_supplement,
        )
        from api.engine.layers.agent_llm_client_v1 import get_default_client
        llm_client = get_default_client()
        if not llm_client.is_available():
            print("WARNING: LLM client not available; falling back to regex-only.",
                  file=sys.stderr)
            args.llm_supplement = False
        else:
            llm_extractor = (is_ambiguous, llm_supplement, llm_client)

    con = sqlite3.connect(str(db_path))
    try:
        _ensure_column(con)
        if args.snapshot:
            snapshots = list(args.snapshot)
        else:
            snapshots = [r[0] for r in con.execute(
                "SELECT DISTINCT snapshot_id FROM cards"
            ).fetchall()]

        results: List[Dict[str, Any]] = []
        llm_audit_entries: List[Dict[str, Any]] = []
        for sid in snapshots:
            print(f"Backfilling snapshot {sid}…", file=sys.stderr)
            where = "WHERE snapshot_id=?"
            qargs: List[Any] = [sid]
            if args.limit is not None:
                where += " LIMIT ?"
                qargs.append(args.limit)
            rows = list(con.execute(
                f"SELECT rowid, name, oracle_text, type_line, mana_cost, "
                f"legalities_json FROM cards {where}",
                qargs,
            ))
            n_total = len(rows)
            n_tagged = 0
            n_llm_supplemented = 0
            distinct_tags: Set[str] = set()
            t0 = time.perf_counter()
            BATCH = 500
            pending: List = []
            for (rid, name, oracle, tl, mc, leg) in rows:
                if args.commander_legal_only and not _commander_legal_filter(leg):
                    continue
                tags = extract_primitives_v2(
                    oracle_text=oracle or "", type_line=tl or "",
                    mana_cost=mc or "", card_name=name or "",
                    ontology=ontology, combo_assembly_set=combo_assembly,
                )
                # LLM supplement for ambiguous cards (budget-gated).
                if llm_extractor and llm_cost_so_far < args.llm_budget_usd:
                    is_ambig, llm_sup, llm_client = llm_extractor
                    if is_ambig(tags):
                        card_dict = {
                            "name": name, "oracle_text": oracle or "",
                            "type_line": tl or "", "mana_cost": mc or "",
                        }
                        supplemental = llm_sup(
                            card_dict, ontology, existing_tags=tags,
                            llm_client=llm_client,
                        )
                        if supplemental:
                            tags = tags | supplemental
                            n_llm_supplemented += 1
                            llm_audit_entries.append({
                                "name": name, "regex_tags": sorted(tags - supplemental),
                                "llm_added_tags": sorted(supplemental),
                            })
                        # Approximate cost tracking (~$0.001/call regardless of result).
                        llm_cost_so_far += 0.001
                        llm_calls += 1
                if tags:
                    n_tagged += 1
                    distinct_tags |= tags
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
            results.append({
                "snapshot_id": sid,
                "rows_processed": n_total,
                "rows_with_tags": n_tagged,
                "coverage_pct": (n_tagged / n_total * 100.0) if n_total else 0.0,
                "distinct_tags_seen": len(distinct_tags),
                "elapsed_s": round(elapsed, 2),
                "llm_supplemented_count": n_llm_supplemented,
            })
            print(
                f"  {sid}: {n_tagged}/{n_total} "
                f"({(n_tagged / n_total * 100.0) if n_total else 0:.1f}%) "
                f"tagged in {elapsed:.1f}s "
                f"(LLM supplemented: {n_llm_supplemented})",
                file=sys.stderr,
            )
    finally:
        con.close()

    if llm_audit_entries:
        audit_path = (
            Path(__file__).resolve().parents[1]
            / "api" / "engine" / "data" / "primitives"
            / "llm_supplement_audit_v1.json"
        )
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        audit_path.write_text(
            json.dumps({
                "version": "backfill_v2",
                "total_llm_calls": llm_calls,
                "total_llm_cost_estimate_usd": round(llm_cost_so_far, 4),
                "entries": llm_audit_entries[:1000],  # cap for file size
            }, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"Wrote LLM audit: {audit_path}", file=sys.stderr)

    summary = {
        "backfill_results": results,
        "llm_supplement": {
            "enabled": args.llm_supplement,
            "total_calls": llm_calls,
            "total_cost_estimate_usd": round(llm_cost_so_far, 4),
            "budget_usd": args.llm_budget_usd,
        },
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
