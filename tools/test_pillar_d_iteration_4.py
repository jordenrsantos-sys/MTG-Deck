"""Pillar D iter 4 — 5-case validation sweep against the iter-4 success criteria.

Same 5 test cases as iter 2/3 (Edgar / Krenko / Atraxa / Yuriko /
Ur-Dragon). Captures:

  - All iter-3 metrics (iter1, creativity_delta, novel, cost, wallclock,
    ur_dragon envelope).
  - Voyage semantic-retrieval contribution count (from semantic-neighbor
    pool injections + final-deck cards tagged `|from_semantic_neighbor`).
  - Atraxa archetype detection (Phase 2 fix — expects `counters_matter`).
  - Pillar E v0.2 card-advantage block (Phase 4) + critique fire events.
  - Pillar C primitive coverage on the sweep deck cards (Phase 5).
  - Pillar F pod_winrate (Phase 6 primitive-grounded upgrade).
  - LLM call decomposition with E_card_advantage_critique included.
  - Outer-chain parallelization timing (Phase 3).
  - 10 random per-card rationale samples per deck.

Iter-4 success criteria (kickoff Phase 7):
  1. iter1_structural_pass_5_of_5
  2. mean_creativity_delta >= 35
  3. mean_novel_combo >= 5
  4. mean_cost <= $0.45
  5. mean_wallclock <= 95s
  6. ur_dragon Hellkite Charger absent
  7. voyage_semantic_contribution_avg >= 5
  8. pillar_c_primitive_coverage >= 95% (sweep deck cards)
  9. pillar_f_winrate_ordering_sane (Yuriko > Krenko > Edgar ≈ Ur-Dragon > Atraxa)
  10. atraxa_archetype_is_counters_matter

Halt if <8 of 10 pass.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional


_BASIC_LAND_NAMES = {"Plains", "Island", "Swamp", "Mountain", "Forest", "Wastes"}
DEFAULT_SNAPSHOT_ID = "20260217_190902_tagpass_20260222"
REPO_ROOT = Path(__file__).resolve().parents[1]
REPORT_PATH = REPO_ROOT / "api" / "engine" / "data" / "agent" / "pillar_d_iteration_4_validation_report.md"


TEST_CASES: List[Dict[str, Any]] = [
    {"id": "edgar_b3_vampire_tribal", "commander": "Edgar Markov", "bracket": "B3",
     "theme_hints": ["TYPAL_VAMPIRES"],
     "must_include_cards": ["Vito, Thorn of the Dusk Rose", "Bloodthirsty Conqueror"]},
    {"id": "krenko_b4_goblin_combo", "commander": "Krenko, Mob Boss", "bracket": "B4",
     "theme_hints": ["TYPAL_GOBLINS"],
     "must_include_cards": ["Conspicuous Snoop", "Kiki-Jiki, Mirror Breaker"]},
    {"id": "atraxa_b2_proliferate", "commander": "Atraxa, Praetors' Voice", "bracket": "B2",
     "theme_hints": ["THEME_PROLIFERATE", "THEME_PLUS1_COUNTERS"],
     "must_include_cards": ["Doubling Season", "Pir, Imaginative Rascal"]},
    {"id": "yuriko_b5_ninja_tempo", "commander": "Yuriko, the Tiger's Shadow", "bracket": "B5",
     "theme_hints": ["TYPAL_NINJAS"],
     "must_include_cards": ["Thassa's Oracle", "Demonic Consultation"]},
    {"id": "ur_dragon_b3_dragon_tribal", "commander": "The Ur-Dragon", "bracket": "B3",
     "theme_hints": ["TYPAL_DRAGONS"],
     "must_include_cards": ["Dragon Tempest", "Tiamat"]},
]


def _run_case(case: Dict[str, Any], snapshot_id: str) -> Dict[str, Any]:
    from api.engine.layers.agent_build_deck_v1 import compute_agent_build_deck_v1
    t0 = time.perf_counter()
    try:
        result = compute_agent_build_deck_v1(
            db_snapshot_id=snapshot_id,
            commander=case["commander"], bracket=case["bracket"],
            theme_hints=case["theme_hints"],
            must_include_cards=case["must_include_cards"],
            seed=42, skip_strength_check=True,
        )
    except Exception as exc:
        return {
            "case_id": case["id"], "status": "EXCEPTION",
            "error": f"{exc.__class__.__name__}: {exc}",
            "wall_clock_ms": int((time.perf_counter() - t0) * 1000),
        }
    return {
        "case_id": case["id"], "status": result.get("status"),
        "wall_clock_ms": int((time.perf_counter() - t0) * 1000),
        "result": result, "case": case,
    }


def _primitive_coverage_for_deck(deck: List[Dict[str, Any]], snapshot_id: str) -> float:
    """% of deck cards (excluding basics) with non-empty primitives_v1_json."""
    from engine.db import resolve_db_path
    names = [
        c.get("card_name") for c in deck
        if c.get("card_name") and c.get("card_name") not in _BASIC_LAND_NAMES
    ]
    if not names:
        return 1.0
    con = sqlite3.connect(str(resolve_db_path()))
    try:
        qmarks = ",".join("?" * len(names))
        rows = con.execute(
            f"SELECT name, primitives_v1_json FROM cards "
            f"WHERE snapshot_id=? AND name IN ({qmarks})",
            tuple([snapshot_id] + names),
        ).fetchall()
    finally:
        con.close()
    by_name = {r[0]: r[1] for r in rows}
    have = 0
    for n in names:
        raw = by_name.get(n) or "[]"
        if raw and raw != "[]":
            have += 1
    return have / len(names)


def _validate_case(captured: Dict[str, Any], snapshot_id: str) -> Dict[str, Any]:
    if captured.get("status") == "EXCEPTION":
        return {"case_id": captured["case_id"], "passed": False,
                "exception": captured.get("error")}
    result = captured["result"]
    case = captured["case"]
    deck = result.get("deck") or []
    summary = result.get("summary") or {}

    must_include = case["must_include_cards"]
    deck_names_lower = {(c.get("card_name") or "").strip().lower() for c in deck}
    must_present = [m for m in must_include if m.strip().lower() in deck_names_lower]
    must_dropped = [m for m in must_include if m.strip().lower() not in deck_names_lower]

    non_basic_counts: Counter = Counter()
    for c in deck:
        n = c.get("card_name") or ""
        if n and n not in _BASIC_LAND_NAMES:
            non_basic_counts[n] += 1
    singleton_violations = {n: c for n, c in non_basic_counts.items() if c > 1}

    metrics = summary.get("creativity_envelope_metrics") or {}
    novel_combo_flags = summary.get("novel_combo_flags") or []
    guard = summary.get("combo_anchor_guard") or {}
    llm = summary.get("llm_metrics") or {}

    archetype = None
    for c in llm.get("calls") or []:
        if c.get("phase") == "C2_2_wild_combo_discovery":
            archetype = c.get("archetype")

    # Voyage semantic contribution = cards whose source includes "semantic"
    # (either source=='semantic_neighbor' for unswapped pool entries, or
    # source contains '|from_semantic_neighbor' for C2.2-applied picks).
    semantic_in_deck = sum(
        1 for c in deck
        if "semantic" in (c.get("source") or "").lower()
    )

    # Parallel timing check: derive overlap window from per-call latencies.
    c21_latency = next(
        (c.get("latency_ms", 0) for c in (llm.get("calls") or [])
         if c.get("phase") == "C2_1_candidate_critic"), 0,
    )
    c22_latency = next(
        (c.get("latency_ms", 0) for c in (llm.get("calls") or [])
         if c.get("phase") == "C2_2_wild_combo_discovery"), 0,
    )

    iter1_pass = len(deck) == 100 and not singleton_violations and not must_dropped

    mana_base = summary.get("mana_base") or {}
    card_advantage = summary.get("card_advantage") or {}

    primitive_coverage = _primitive_coverage_for_deck(deck, snapshot_id)

    # Pillar F approximator with v1 primitives.
    try:
        from api.engine.layers.agent_statistical_approximator_v1 import (
            approximate_pod_winrate,
        )
        pillar_f_report = approximate_pod_winrate(
            deck=deck, db_snapshot_id=snapshot_id,
        ).to_dict()
    except Exception as exc:
        pillar_f_report = {"error": f"{exc.__class__.__name__}: {exc}"}

    rationale_samples = []
    for c in deck[1:]:
        if c.get("card_name") in _BASIC_LAND_NAMES:
            continue
        rationale_samples.append({
            "card": c["card_name"], "reason": (c.get("reason", "") or "")[:200],
            "source": c.get("source", ""),
        })
        if len(rationale_samples) >= 10:
            break

    return {
        "case_id": case["id"],
        "iter1_passed": iter1_pass,
        "deck_size": len(deck),
        "must_includes_resolved": must_present,
        "must_includes_dropped": must_dropped,
        "theme_coherence_score": metrics.get("theme_coherence_score"),
        "wall_clock_ms": captured["wall_clock_ms"],
        "creativity_delta_count": metrics.get("creativity_delta_count"),
        "novel_combo_count": sum(
            1 for f in novel_combo_flags if not f.get("in_spellbook")
        ),
        "novel_combo_flags": novel_combo_flags,
        "summary_narrative": summary.get("summary_narrative"),
        "consider_adding": summary.get("consider_adding") or [],
        "rationale_samples": rationale_samples,
        "llm": {
            "calls": llm.get("calls") or [],
            "total_cost_usd": round(llm.get("total_cost_usd") or 0.0, 4),
            "total_latency_ms": llm.get("total_latency_ms", 0),
            "archetype": archetype,
        },
        "combo_anchor_guard": {
            "active": guard.get("active"),
            "forbidden_set_size": guard.get("forbidden_set_size", 0),
            "guard_fire_count": guard.get("guard_fire_count", 0),
            "guard_fire_events": guard.get("guard_fire_events") or [],
        },
        "semantic_in_deck": semantic_in_deck,
        "outer_chain": {
            "c21_latency_ms": c21_latency, "c22_latency_ms": c22_latency,
            "parallel_window_ms": max(c21_latency, c22_latency),
            "serial_baseline_ms": c21_latency + c22_latency,
        },
        "mana_base": mana_base,
        "card_advantage": card_advantage,
        "primitive_coverage_pct": round(primitive_coverage * 100.0, 1),
        "pillar_f_approximator": pillar_f_report,
        "ur_dragon_check": _ur_dragon_check(deck) if case["id"] == "ur_dragon_b3_dragon_tribal" else None,
    }


def _ur_dragon_check(deck: List[Dict[str, Any]]) -> Dict[str, Any]:
    names_lower = {(c.get("card_name") or "").strip().lower() for c in deck}
    return {
        "hellkite_charger_absent": "hellkite charger" not in names_lower,
        "old_gnawbone_in_deck": "old gnawbone" in names_lower,
    }


def _aggregate(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    creativity = [r["creativity_delta_count"] for r in results if r.get("creativity_delta_count") is not None]
    novel = [r["novel_combo_count"] for r in results if r.get("novel_combo_count") is not None]
    cost = [r["llm"]["total_cost_usd"] for r in results if r.get("llm")]
    wall = [r["wall_clock_ms"] for r in results if r.get("wall_clock_ms")]
    iter1_all = all(r.get("iter1_passed") for r in results)
    semantic = [r.get("semantic_in_deck", 0) for r in results]
    coverage = [r.get("primitive_coverage_pct", 0) for r in results]

    mean_creativity = sum(creativity) / max(1, len(creativity))
    mean_novel = sum(novel) / max(1, len(novel))
    mean_cost = sum(cost) / max(1, len(cost))
    mean_wall_s = (sum(wall) / max(1, len(wall))) / 1000.0
    mean_semantic = sum(semantic) / max(1, len(semantic))
    mean_coverage = sum(coverage) / max(1, len(coverage))

    ur = next(
        (r.get("ur_dragon_check") for r in results
         if r["case_id"] == "ur_dragon_b3_dragon_tribal"), None,
    )

    atraxa = next(
        (r for r in results if r["case_id"] == "atraxa_b2_proliferate"),
        None,
    )

    pillar_f = {r["case_id"]: r.get("pillar_f_approximator", {}).get("pod_winrate", 0.0) for r in results}
    yuriko = pillar_f.get("yuriko_b5_ninja_tempo", 0)
    krenko = pillar_f.get("krenko_b4_goblin_combo", 0)
    edgar = pillar_f.get("edgar_b3_vampire_tribal", 0)
    ur_drag = pillar_f.get("ur_dragon_b3_dragon_tribal", 0)
    atrax = pillar_f.get("atraxa_b2_proliferate", 0)
    ordering_sane = (
        yuriko > krenko > max(edgar, ur_drag)
        and abs(edgar - ur_drag) < 0.15
        and min(edgar, ur_drag) > atrax
    )

    criteria = {
        "iter1_structural_pass_5_of_5": {"value": iter1_all, "passed": iter1_all},
        "mean_creativity_delta_geq_35": {
            "value": round(mean_creativity, 2), "threshold": 35,
            "passed": mean_creativity >= 35,
        },
        "mean_novel_combo_geq_5": {
            "value": round(mean_novel, 2), "threshold": 5,
            "passed": mean_novel >= 5,
        },
        "mean_cost_usd_leq_0_45": {
            "value": round(mean_cost, 4), "threshold": 0.45,
            "passed": mean_cost <= 0.45,
        },
        "mean_wallclock_s_leq_95": {
            "value": round(mean_wall_s, 1), "threshold": 95,
            "passed": mean_wall_s <= 95,
        },
        "ur_dragon_hellkite_charger_absent": {
            "details": ur,
            "passed": bool(ur and ur.get("hellkite_charger_absent")),
        },
        "voyage_semantic_contribution_avg_geq_5": {
            "value": round(mean_semantic, 2), "threshold": 5,
            "passed": mean_semantic >= 5,
        },
        "pillar_c_primitive_coverage_geq_95pct": {
            "value": round(mean_coverage, 1), "threshold": 95,
            "passed": mean_coverage >= 95,
        },
        "pillar_f_winrate_ordering_sane": {
            "value": {
                "yuriko": yuriko, "krenko": krenko,
                "edgar": edgar, "ur_dragon": ur_drag, "atraxa": atrax,
            },
            "passed": ordering_sane,
        },
        "atraxa_archetype_is_counters_matter": {
            "value": (atraxa or {}).get("llm", {}).get("archetype"),
            "passed": (atraxa or {}).get("llm", {}).get("archetype") == "counters_matter",
        },
    }
    passed_count = sum(1 for v in criteria.values() if v.get("passed") is True)
    return {
        "criteria": criteria,
        "passed_count": passed_count,
        "total": 10,
        "means": {
            "creativity_delta": round(mean_creativity, 2),
            "novel_combo": round(mean_novel, 2),
            "cost_usd": round(mean_cost, 4),
            "wallclock_s": round(mean_wall_s, 1),
            "voyage_semantic_contribution": round(mean_semantic, 2),
            "pillar_c_primitive_coverage_pct": round(mean_coverage, 1),
        },
    }


def _format_report(results: List[Dict[str, Any]], aggregate: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("# Pillar D Iteration 4 - Validation Report")
    lines.append("")
    lines.append(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Snapshot: `{DEFAULT_SNAPSHOT_ID}`")
    lines.append("")
    lines.append("## Headline")
    lines.append("")
    lines.append(f"**Passed: {aggregate['passed_count']} / {aggregate['total']} success criteria.**")
    lines.append("")
    for name, val in aggregate["criteria"].items():
        check = val.get("passed")
        mark = "PASS" if check is True else "FAIL"
        line = f"- [{mark}] **{name}**"
        if "value" in val:
            line += f" — value `{val['value']}`"
            if "threshold" in val:
                line += f" (threshold `{val['threshold']}`)"
        lines.append(line)
    lines.append("")
    lines.append("## Per-case summary")
    lines.append("")
    lines.append("| Case | iter1 | wall (s) | cost ($) | calls | creativity | novel | semantic | coverage | archetype | pod_winrate |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for r in results:
        llm = r.get("llm") or {}
        lines.append(
            f"| {r['case_id']} | "
            f"{'PASS' if r.get('iter1_passed') else 'FAIL'} | "
            f"{(r.get('wall_clock_ms') or 0) / 1000.0:.1f} | "
            f"${llm.get('total_cost_usd', 0):.4f} | "
            f"{len(llm.get('calls') or [])} | "
            f"{r.get('creativity_delta_count', '?')} | "
            f"{r.get('novel_combo_count', 0)} | "
            f"{r.get('semantic_in_deck', 0)} | "
            f"{r.get('primitive_coverage_pct', 0):.1f}% | "
            f"{llm.get('archetype') or 'n/a'} | "
            f"{(r.get('pillar_f_approximator') or {}).get('pod_winrate', 'n/a')} |"
        )
    lines.append("")
    lines.append("## Means")
    lines.append("")
    for k, v in aggregate["means"].items():
        lines.append(f"- {k}: {v}")
    lines.append("")
    lines.append("## Outer-chain parallelization (Phase 3)")
    lines.append("")
    lines.append("| Case | C2.1 latency (s) | C2.2 latency (s) | parallel window (s) | serial baseline (s) | savings (s) |")
    lines.append("|---|---|---|---|---|---|")
    for r in results:
        oc = r.get("outer_chain") or {}
        lines.append(
            f"| {r['case_id']} | "
            f"{oc.get('c21_latency_ms', 0)/1000:.1f} | "
            f"{oc.get('c22_latency_ms', 0)/1000:.1f} | "
            f"{oc.get('parallel_window_ms', 0)/1000:.1f} | "
            f"{oc.get('serial_baseline_ms', 0)/1000:.1f} | "
            f"{(oc.get('serial_baseline_ms', 0) - oc.get('parallel_window_ms', 0))/1000:.1f} |"
        )
    lines.append("")
    lines.append("## Iter 4 -> 5 hand-off")
    lines.append("")
    lines.append("See `mega_task_v2_final_report.md` for the full hand-off.")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", default=DEFAULT_SNAPSHOT_ID)
    parser.add_argument("--out", default=str(REPORT_PATH))
    args = parser.parse_args(argv)

    snapshot_id = args.snapshot
    captured = []
    for case in TEST_CASES:
        print(f"Running case: {case['id']}…", file=sys.stderr)
        c = _run_case(case, snapshot_id)
        captured.append(c)
        ms = c.get("wall_clock_ms", 0)
        status = c.get("status")
        print(f"  {case['id']}: status={status} wall={ms/1000:.1f}s", file=sys.stderr)

    results = [_validate_case(c, snapshot_id) for c in captured]
    aggregate = _aggregate(results)
    report = _format_report(results, aggregate)
    Path(args.out).write_text(report, encoding="utf-8")
    try:
        print(report)
    except UnicodeEncodeError:
        # Fall back to ascii-safe transcoding for Windows cp1252 stdout.
        sys.stdout.write(report.encode("ascii", "replace").decode("ascii"))
        sys.stdout.write("\n")
    print(json.dumps({"aggregate": aggregate}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
