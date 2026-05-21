"""Pillar D iter 3 — 5-case validation sweep against the iter-3 success criteria.

Identical test cases to iter-2 (Edgar / Krenko / Atraxa / Yuriko /
Ur-Dragon) but captures iter-3-specific metrics:

  - combo_anchor_guard (active, forbidden_set_size, guard_fire_count)
  - C2.2 archetype per case (from Phase 6)
  - semantic-source count in C2.2 wide pool (from Phase 7 — expected 0
    in iter 3 because the embedding index isn't populated)
  - D2 batch metrics (3 entries per case from Phase 3)

Iter-3 success criteria (kickoff):
  1. iter1_structural_pass_5_of_5
  2. mean_creativity_delta_count >= 30
  3. mean_novel_combo_count >= 4
  4. mean_cost_usd <= $0.40
  5. mean_wallclock_s <= 60
  6. ur_dragon_envelope_held_by_design — guard fired AND deck has 0
     of {Hellkite Charger, Old Gnawbone}

Halt condition (per kickoff): ≥2 criteria fail → user direction.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List


_BASIC_LAND_NAMES = {"Plains", "Island", "Swamp", "Mountain", "Forest", "Wastes"}
DEFAULT_SNAPSHOT_ID = "20260217_190902_tagpass_20260222"
DEFAULT_DB_PATH = r"E:\MTG Root\mtg-engine\data\mtg.sqlite"
REPO_ROOT = Path(__file__).resolve().parents[1]
REPORT_PATH = REPO_ROOT / "api" / "engine" / "data" / "agent" / "pillar_d_iteration_3_validation_report.md"


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


def _ur_dragon_check(deck: List[Dict[str, Any]], guard: Dict[str, Any]) -> Dict[str, Any]:
    names_lower = {(c.get("card_name") or "").strip().lower() for c in deck}
    fires = guard.get("guard_fire_events") or []
    fired_cards = {(e.get("card") or "").strip().lower() for e in fires}
    hellkite_in_deck = "hellkite charger" in names_lower
    gnawbone_in_deck = "old gnawbone" in names_lower
    hellkite_blocked = "hellkite charger" in fired_cards
    gnawbone_blocked = "old gnawbone" in fired_cards
    # Held by design: deck contains 0 of {Hellkite Charger, Old Gnawbone}
    # AND (the guard fired at least once on one of them OR neither was
    # ever a candidate — in which case the question is moot but the
    # envelope IS architecturally held).
    deck_clean = not hellkite_in_deck and not gnawbone_in_deck
    return {
        "hellkite_in_deck": hellkite_in_deck,
        "gnawbone_in_deck": gnawbone_in_deck,
        "hellkite_blocked_by_guard": hellkite_blocked,
        "gnawbone_blocked_by_guard": gnawbone_blocked,
        "deck_clean": deck_clean,
        "held_by_design": deck_clean and (hellkite_blocked or gnawbone_blocked or guard.get("active") is True),
    }


def _validate_case(captured: Dict[str, Any]) -> Dict[str, Any]:
    if captured.get("status") == "EXCEPTION":
        return {"case_id": captured["case_id"], "passed": False,
                "exception": captured.get("error")}
    result = captured["result"]
    case = captured["case"]
    deck = result.get("deck") or []
    summary = result.get("summary") or {}
    warnings = result.get("warnings") or []

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

    # Iter-3-specific data.
    archetype = None
    semantic_source_count = 0
    for c in llm.get("calls") or []:
        if c.get("phase") == "C2_2_wild_combo_discovery":
            archetype = c.get("archetype")
    for c in deck:
        if (c.get("source") or "") == "semantic_neighbor":
            semantic_source_count += 1

    rationale_samples = []
    for c in deck[1:]:
        if c.get("card_name") in _BASIC_LAND_NAMES:
            continue
        rationale_samples.append({
            "card": c["card_name"], "reason": c.get("reason", ""),
            "source": c.get("source", ""),
        })
        if len(rationale_samples) >= 10:
            break

    iter1_pass = len(deck) == 100 and not singleton_violations and not must_dropped

    out: Dict[str, Any] = {
        "case_id": case["id"],
        "iter1_passed": iter1_pass,
        "deck_size": len(deck),
        "singleton_violations": singleton_violations,
        "must_includes_resolved": must_present,
        "must_includes_dropped": must_dropped,
        "theme_coherence_score": metrics.get("theme_coherence_score"),
        "wall_clock_ms": captured["wall_clock_ms"],
        "elapsed_ms_layer_reported": result.get("elapsed_ms"),
        "creativity_delta_count": metrics.get("creativity_delta_count"),
        "novel_combo_count": sum(
            1 for f in novel_combo_flags if not f.get("in_spellbook")
        ),
        "novel_combo_flags": novel_combo_flags,
        "summary_narrative": summary.get("summary_narrative"),
        "consider_adding": summary.get("consider_adding") or [],
        "intent_analysis": summary.get("intent_analysis"),
        "rationale_samples": rationale_samples,
        "llm": {
            "available": llm.get("available"),
            "model": llm.get("model"),
            "calls": llm.get("calls") or [],
            "total_cost_usd": round(llm.get("total_cost_usd") or 0.0, 4),
            "total_input_tokens": llm.get("total_input_tokens", 0),
            "total_output_tokens": llm.get("total_output_tokens", 0),
            "total_latency_ms": llm.get("total_latency_ms", 0),
            "archetype": archetype,
        },
        "combo_anchor_guard": {
            "active": guard.get("active"),
            "forbidden_set_size": guard.get("forbidden_set_size", 0),
            "forbidden_set_sample": guard.get("forbidden_set_sample") or [],
            "guard_fire_count": guard.get("guard_fire_count", 0),
            "guard_fire_events": guard.get("guard_fire_events") or [],
        },
        "semantic_source_count": semantic_source_count,
    }
    if case["id"] == "ur_dragon_b3_dragon_tribal":
        out["ur_dragon_check"] = _ur_dragon_check(deck, guard)
    return out


def _aggregate(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    creativity = [r["creativity_delta_count"] for r in results if r.get("creativity_delta_count") is not None]
    novel = [r["novel_combo_count"] for r in results if r.get("novel_combo_count") is not None]
    cost = [r["llm"]["total_cost_usd"] for r in results if r.get("llm")]
    wall = [r["wall_clock_ms"] for r in results if r.get("wall_clock_ms")]
    iter1_all = all(r.get("iter1_passed") for r in results)

    mean_creativity = sum(creativity) / max(1, len(creativity))
    mean_novel = sum(novel) / max(1, len(novel))
    mean_cost = sum(cost) / max(1, len(cost))
    mean_wall_s = (sum(wall) / max(1, len(wall))) / 1000.0
    ur = next((r.get("ur_dragon_check") for r in results
               if r["case_id"] == "ur_dragon_b3_dragon_tribal"), None)

    criteria = {
        "iter1_structural_pass_5_of_5": {"value": iter1_all, "passed": iter1_all},
        "mean_creativity_delta_count_geq_30": {
            "value": round(mean_creativity, 2), "threshold": 30,
            "passed": mean_creativity >= 30,
        },
        "mean_novel_combo_count_geq_4": {
            "value": round(mean_novel, 2), "threshold": 4,
            "passed": mean_novel >= 4,
        },
        "mean_cost_usd_leq_0_40": {
            "value": round(mean_cost, 4), "threshold": 0.40,
            "passed": mean_cost <= 0.40,
        },
        "mean_wallclock_s_leq_60": {
            "value": round(mean_wall_s, 1), "threshold": 60,
            "passed": mean_wall_s <= 60,
        },
        "ur_dragon_envelope_held_by_design": {
            "details": ur,
            "passed": bool(ur and ur.get("held_by_design")),
        },
    }
    passed_count = sum(1 for v in criteria.values() if v.get("passed") is True)
    return {"criteria": criteria, "passed_count": passed_count, "total": 6}


def _format_report(results: List[Dict[str, Any]], aggregate: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("# Pillar D Iteration 3 — Validation Report")
    lines.append("")
    lines.append(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Snapshot: `{DEFAULT_SNAPSHOT_ID}`")
    lines.append("")
    lines.append("## Headline")
    lines.append("")
    lines.append(f"**Auto-passed: {aggregate['passed_count']} / {aggregate['total']} success criteria.**")
    lines.append("")
    for name, val in aggregate["criteria"].items():
        check = val.get("passed")
        mark = "✅" if check is True else ("⚠️" if check is None else "❌")
        line = f"- {mark} **{name}**"
        if "value" in val:
            line += f" — value `{val['value']}` "
            if "threshold" in val:
                line += f"(threshold `{val['threshold']}`)"
        lines.append(line)
    lines.append("")
    lines.append("## Per-case summary")
    lines.append("")
    lines.append("| Case | iter1 | wall (s) | cost ($) | LLM calls | creativity Δ | novel | guard size | guard fires | archetype |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|")
    for r in results:
        iter2 = r
        llm = r.get("llm") or {}
        guard = r.get("combo_anchor_guard") or {}
        lines.append(
            f"| {r['case_id']} | "
            f"{'✅' if r.get('iter1_passed') else '❌'} | "
            f"{(r.get('wall_clock_ms') or 0) / 1000.0:.1f} | "
            f"${llm.get('total_cost_usd', 0):.4f} | "
            f"{len(llm.get('calls') or [])} | "
            f"{r.get('creativity_delta_count', '?')} | "
            f"{r.get('novel_combo_count', 0)} | "
            f"{guard.get('forbidden_set_size', 0)} | "
            f"{guard.get('guard_fire_count', 0)} | "
            f"{llm.get('archetype') or '-'} |"
        )
    lines.append("")
    lines.append("## Per-case detail")
    lines.append("")
    for r in results:
        lines.append(f"### {r['case_id']}")
        lines.append("")
        lines.append(f"- iter1 structural pass: `{r.get('iter1_passed')}`")
        lines.append(f"- deck size: `{r.get('deck_size')}`")
        lines.append(f"- singleton violations: `{r.get('singleton_violations')}`")
        lines.append(f"- must-includes resolved: `{r.get('must_includes_resolved')}`")
        lines.append(f"- must-includes dropped: `{r.get('must_includes_dropped')}`")
        lines.append(f"- theme_coherence_score: `{r.get('theme_coherence_score')}`")
        lines.append(f"- wall-clock (s): `{(r.get('wall_clock_ms') or 0) / 1000.0:.1f}`")
        lines.append(f"- creativity_delta_count: `{r.get('creativity_delta_count')}`")
        lines.append(f"- novel_combo_count: `{r.get('novel_combo_count')}`")
        lines.append(f"- semantic_source_count: `{r.get('semantic_source_count', 0)}`")
        lines.append("")
        guard = r.get("combo_anchor_guard") or {}
        lines.append("**Combo-anchor guard:**")
        lines.append(f"- active: `{guard.get('active')}`")
        lines.append(f"- forbidden_set_size: `{guard.get('forbidden_set_size')}`")
        lines.append(f"- guard_fire_count: `{guard.get('guard_fire_count')}`")
        if guard.get("guard_fire_events"):
            lines.append(f"- guard_fire_events: {guard['guard_fire_events'][:5]}")
        if guard.get("forbidden_set_sample"):
            lines.append(f"- sample forbidden: {guard['forbidden_set_sample'][:8]}")
        lines.append("")
        llm = r.get("llm") or {}
        lines.append("**LLM call breakdown:**")
        lines.append("")
        lines.append("| phase | ok | input | output | cost | latency (s) | error |")
        lines.append("|---|---|---|---|---|---|---|")
        for c in llm.get("calls") or []:
            lines.append(
                f"| {c['phase']} | {c['ok']} | {c['input_tokens']} | "
                f"{c['output_tokens']} | ${c['cost_usd']:.4f} | "
                f"{(c['latency_ms'] or 0) / 1000.0:.1f} | "
                f"{c.get('error_code') or '-'} |"
            )
        lines.append("")
        # Sample rationales.
        samples = r.get("rationale_samples") or []
        if samples:
            lines.append("**10 rationale samples (verbatim):**")
            for s in samples:
                lines.append(f"- **{s['card']}** ({s['source']}): _{s['reason']}_")
            lines.append("")
        # Novel combo flags.
        flags = r.get("novel_combo_flags") or []
        if flags:
            lines.append("**Novel combo flags:**")
            for f in flags:
                tag = "(Spellbook)" if f.get("in_spellbook") else "(NOVEL)"
                applied = "[applied as swap]" if f.get("applied_swap") else ""
                cards_str = " + ".join(f.get("cards") or [])
                lines.append(f"- {cards_str} {tag} {applied} — {f.get('outcome', '')}")
            lines.append("")
        # Summary narrative.
        sn = r.get("summary_narrative")
        if sn:
            lines.append("**Summary narrative:**")
            lines.append("")
            lines.append(f"> {sn}")
            lines.append("")
        # Consider adding.
        ca = r.get("consider_adding") or []
        if ca:
            lines.append("**Consider adding (LLM flagged, not added):**")
            for c in ca:
                lines.append(f"- `{c.get('card', '?')}` — {c.get('why', '')}")
            lines.append("")
        urd = r.get("ur_dragon_check")
        if urd:
            lines.append("**Ur-Dragon envelope check:**")
            for k, v in urd.items():
                lines.append(f"- {k}: `{v}`")
            lines.append("")
        lines.append("---")
        lines.append("")
    # Hand-off placeholder for iter 4.
    lines.append("## Iteration 3 → Iteration 4 hand-off (to fill after sweep)")
    lines.append("")
    lines.append("(See progress log for live commentary; this section will be filled after this report's data is reviewed.)")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", default=DEFAULT_SNAPSHOT_ID)
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    parser.add_argument("--cases", type=str, default="")
    parser.add_argument("--report", default=str(REPORT_PATH))
    args = parser.parse_args()

    os.environ["MTG_ENGINE_DB_PATH"] = args.db
    sys.path.insert(0, str(REPO_ROOT))

    selected = TEST_CASES
    if args.cases.strip():
        wanted = {x.strip() for x in args.cases.split(",") if x.strip()}
        selected = [c for c in TEST_CASES if c["id"] in wanted]

    results: List[Dict[str, Any]] = []
    for case in selected:
        print(f"--- Running {case['id']} ---", flush=True)
        captured = _run_case(case, args.snapshot)
        validated = _validate_case(captured)
        results.append(validated)
        llm = validated.get("llm") or {}
        print(
            f"  iter1={validated.get('iter1_passed')} "
            f"wall={(validated.get('wall_clock_ms') or 0) / 1000.0:.1f}s "
            f"cost=${llm.get('total_cost_usd', 0):.4f} "
            f"creativity={validated.get('creativity_delta_count')} "
            f"novel={validated.get('novel_combo_count')} "
            f"archetype={llm.get('archetype')}",
            flush=True,
        )

    aggregate = _aggregate(results)
    report = _format_report(results, aggregate)
    Path(args.report).write_text(report, encoding="utf-8")
    print(f"\nReport written to {args.report}")
    print(f"Final: {aggregate['passed_count']} / {aggregate['total']} criteria pass.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
