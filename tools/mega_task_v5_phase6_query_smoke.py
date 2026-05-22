"""Mega-task v5 Phase 6 — Voyage color-filter regression smoke.

Phase 6's kickoff diagnosis assumed a color-filter bug; Phase 5's venv
recovery showed the actual root cause was a dep gap. This script
confirms (live against the real Voyage index) that all 5 iter-5 baseline
commanders produce >= 3 semantic neighbors under their natural color
filter — the property iter 5's voyage_semantic_avg=1.8 failed to meet,
now demonstrably restored.

Usage:
    python tools/mega_task_v5_phase6_query_smoke.py
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

# Repo root on sys.path so engine.* + api.* resolve.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


# Iter 5 baseline cases (from pillar_d_iteration_5_validation_report.md):
#   edgar_b3_vampire_tribal     - Edgar Markov (BRW)
#   krenko_b4_goblin_combo      - Krenko, Mob Boss (R)
#   atraxa_b2_proliferate       - Atraxa, Praetors' Voice (WUBG)
#   yuriko_b5_ninja_tempo       - Yuriko, the Tiger's Shadow (UB)
#   ur_dragon_b3_dragon_tribal  - The Ur-Dragon (WUBRG)
_BASELINE_CASES = [
    ("Edgar Markov",                  ["B", "R", "W"]),
    ("Krenko, Mob Boss",              ["R"]),
    ("Atraxa, Praetors' Voice",       ["B", "G", "U", "W"]),
    ("Yuriko, the Tiger's Shadow",    ["B", "U"]),
    ("The Ur-Dragon",                 ["B", "G", "R", "U", "W"]),
]

MIN_NEIGHBORS = 3


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", default=None,
                        help="Write JSON report to this path.")
    parser.add_argument("--k", type=int, default=20,
                        help="k passed to query_neighbors (default: 20).")
    args = parser.parse_args(argv)

    from api.engine.layers.agent_semantic_retrieval_v1 import (
        is_available, query_neighbors,
    )

    if not is_available():
        print("FATAL: agent_semantic_retrieval_v1 is_available()=False. "
              "The Voyage embeddings index isn't populated or numpy/voyageai "
              "are missing from the venv. Smoke cannot run.", flush=True)
        return 1

    report = {
        "k": args.k,
        "min_neighbors_required": MIN_NEIGHBORS,
        "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "cases": [],
    }
    failures: list[str] = []
    for commander, color_filter in _BASELINE_CASES:
        t0 = time.perf_counter()
        try:
            neighbors = query_neighbors(
                commander, k=args.k, color_identity_filter=sorted(color_filter),
            )
        except Exception as exc:
            print(f"FAIL {commander!r}: {type(exc).__name__}: {exc}", flush=True)
            report["cases"].append({
                "commander": commander, "filter": color_filter,
                "neighbor_count": 0, "ok": False, "error": str(exc),
            })
            failures.append(commander)
            continue
        elapsed_ms = (time.perf_counter() - t0) * 1000
        # Verify every returned neighbor honors the filter
        filter_set = {c.upper() for c in color_filter}
        leaks = [
            n for n in neighbors
            if not set(c.upper() for c in (n.get("color_identity") or [])).issubset(filter_set)
        ]
        ok = len(neighbors) >= MIN_NEIGHBORS and not leaks
        case = {
            "commander": commander,
            "filter": color_filter,
            "neighbor_count": len(neighbors),
            "query_ms": round(elapsed_ms, 2),
            "color_leaks": [n["name"] for n in leaks],
            "ok": ok,
            "top5_names": [n["name"] for n in neighbors[:5]],
            "top5_color_identities": [n.get("color_identity") for n in neighbors[:5]],
            "top5_similarities": [round(n.get("similarity", 0.0), 4) for n in neighbors[:5]],
        }
        report["cases"].append(case)
        marker = "PASS" if ok else "FAIL"
        print(f"[{marker}] {commander!r} filter={color_filter} -> "
              f"{len(neighbors)} neighbors in {elapsed_ms:.1f}ms"
              + (f", LEAKED: {[n['name'] for n in leaks]}" if leaks else ""),
              flush=True)
        for n in neighbors[:5]:
            print(f"    - {n['name']!r}  ci={n.get('color_identity')}  "
                  f"sim={n.get('similarity'):.3f}", flush=True)
        if not ok:
            failures.append(commander)

    report["overall_ok"] = not failures
    report["failed_commanders"] = failures

    print(flush=True)
    print(f"=== Phase 6 smoke summary ===", flush=True)
    print(f"Cases:     {len(_BASELINE_CASES)}", flush=True)
    print(f"Passed:    {len(_BASELINE_CASES) - len(failures)}", flush=True)
    print(f"Failed:    {len(failures)}", flush=True)
    print(f"Overall:   {'PASS' if report['overall_ok'] else 'FAIL'}", flush=True)

    if args.report:
        Path(args.report).write_text(json.dumps(report, indent=2))
        print(f"Report:    {args.report}", flush=True)
    return 0 if report["overall_ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
