"""Pillar D iteration 2 — validation sweep against the 5 canonical test
cases. Mirrors iteration 1's test_pillar_d_agent.py but captures the
iteration-2-specific success criteria from the kickoff plan:

  1. creativity_delta_count ≥ 8 (mean) — cards NOT in top-30 staples per
     archetype_brief.
  2. ≥ 1 deck surfaces ≥ 1 novel combo flag (in_spellbook=false).
  3. Per-card rationale reads substantively different from iteration-1
     template fill — sample 5 cards per deck for inspection.
  4. Mean per-build cost ≤ $0.50.
  5. Mean per-build wall-clock ≤ 45s.
  6. Ur-Dragon + Tiamat case still does NOT auto-include Old Gnawbone +
     Hellkite Charger (iteration-1 creativity-envelope rule preserved).

Writes the report to repo/api/engine/data/agent/
pillar_d_iteration_2_validation_report.md.
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
REPORT_PATH = REPO_ROOT / "api" / "engine" / "data" / "agent" / "pillar_d_iteration_2_validation_report.md"


TEST_CASES: List[Dict[str, Any]] = [
    {
        "id": "edgar_b3_vampire_tribal",
        "commander": "Edgar Markov",
        "bracket": "B3",
        "theme_hints": ["TYPAL_VAMPIRES"],
        "must_include_cards": ["Vito, Thorn of the Dusk Rose", "Bloodthirsty Conqueror"],
    },
    {
        "id": "krenko_b4_goblin_combo",
        "commander": "Krenko, Mob Boss",
        "bracket": "B4",
        "theme_hints": ["TYPAL_GOBLINS"],
        "must_include_cards": ["Conspicuous Snoop", "Kiki-Jiki, Mirror Breaker"],
    },
    {
        "id": "atraxa_b2_proliferate",
        "commander": "Atraxa, Praetors' Voice",
        "bracket": "B2",
        "theme_hints": ["THEME_PROLIFERATE", "THEME_PLUS1_COUNTERS"],
        "must_include_cards": ["Doubling Season", "Pir, Imaginative Rascal"],
    },
    {
        "id": "yuriko_b5_ninja_tempo",
        "commander": "Yuriko, the Tiger's Shadow",
        "bracket": "B5",
        "theme_hints": ["TYPAL_NINJAS"],
        "must_include_cards": ["Thassa's Oracle", "Demonic Consultation"],
    },
    {
        "id": "ur_dragon_b3_dragon_tribal",
        "commander": "The Ur-Dragon",
        "bracket": "B3",
        "theme_hints": ["TYPAL_DRAGONS"],
        "must_include_cards": ["Dragon Tempest", "Tiamat"],
    },
]


# ============================================================
# Per-case capture.
# ============================================================


def _run_case(case: Dict[str, Any], snapshot_id: str) -> Dict[str, Any]:
    from api.engine.layers.agent_build_deck_v1 import compute_agent_build_deck_v1

    t0 = time.perf_counter()
    try:
        result = compute_agent_build_deck_v1(
            db_snapshot_id=snapshot_id,
            commander=case["commander"],
            bracket=case["bracket"],
            theme_hints=case["theme_hints"],
            must_include_cards=case["must_include_cards"],
            seed=42,
            skip_strength_check=True,
        )
    except Exception as exc:
        return {
            "case_id": case["id"],
            "status": "EXCEPTION",
            "error": f"{exc.__class__.__name__}: {exc}",
            "wall_clock_ms": int((time.perf_counter() - t0) * 1000),
        }
    wall_clock_ms = int((time.perf_counter() - t0) * 1000)
    return {
        "case_id": case["id"],
        "status": result.get("status"),
        "wall_clock_ms": wall_clock_ms,
        "result": result,
        "case": case,
    }


# ============================================================
# Iteration-2 metrics.
# ============================================================


def _creativity_delta_count(deck: List[Dict[str, Any]],
                            archetype_brief: Dict[str, Any]) -> int:
    """Count of cards in `deck` (excluding commander + basics) NOT in the
    top-30 staples per archetype_brief.staple_cards. The kickoff plan
    defines this as the creativity metric — higher = more unique to
    this specific build vs. the corpus."""
    staples = archetype_brief.get("staple_cards") or []
    # Top-30 by usage_pct.
    top30 = sorted(
        staples,
        key=lambda s: float(s.get("usage_pct") or 0.0),
        reverse=True,
    )[:30]
    top30_names = {(s.get("name") or "").strip().lower() for s in top30}
    delta = 0
    body = deck[1:]  # skip commander
    for c in body:
        name = (c.get("card_name") or "").strip().lower()
        if not name:
            continue
        if name in {n.lower() for n in _BASIC_LAND_NAMES}:
            continue
        if name not in top30_names:
            delta += 1
    return delta


def _novel_combo_count(novel_combo_flags: List[Dict[str, Any]]) -> int:
    """Count of combos surfaced by the LLM that are NOT in Spellbook
    (in_spellbook=false). These are the "agent saw something the corpus
    didn't tell it" signals."""
    return sum(1 for f in (novel_combo_flags or []) if not f.get("in_spellbook"))


def _sample_rationales(deck: List[Dict[str, Any]], n: int = 5) -> List[Dict[str, str]]:
    """Sample N rationale strings from non-commander, non-basic cards.
    Deterministic (no random) — picks the first N qualifying cards by
    deck order so the report is reproducible."""
    out: List[Dict[str, str]] = []
    for c in deck[1:]:
        name = c.get("card_name") or ""
        if name in _BASIC_LAND_NAMES:
            continue
        if not c.get("reason"):
            continue
        out.append({
            "card": name,
            "reason": c.get("reason", ""),
            "source": c.get("source", ""),
        })
        if len(out) >= n:
            break
    return out


def _check_ur_dragon_combo_avoidance(deck: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Specific regression check from iteration 1: Ur-Dragon + Tiamat
    must NOT auto-include Old Gnawbone + Hellkite Charger via combo
    auto-expansion. Iteration 2 might re-introduce these if the LLM
    over-reaches; we verify the iteration-1 envelope still holds."""
    names_lower = {(c.get("card_name") or "").strip().lower() for c in deck}
    gnawbone_present = "old gnawbone" in names_lower
    charger_present = "hellkite charger" in names_lower
    tiamat_present = "tiamat" in names_lower
    return {
        "tiamat_present": tiamat_present,
        "old_gnawbone_present": gnawbone_present,
        "hellkite_charger_present": charger_present,
        "creativity_envelope_held": tiamat_present and not (gnawbone_present and charger_present),
    }


def _llm_call_breakdown(llm_metrics: Dict[str, Any]) -> Dict[str, Any]:
    calls = llm_metrics.get("calls") or []
    return {
        "available": llm_metrics.get("available", False),
        "model": llm_metrics.get("model"),
        "call_count": len(calls),
        "calls": [
            {
                "phase": c.get("phase"),
                "ok": c.get("ok"),
                "input_tokens": c.get("input_tokens"),
                "output_tokens": c.get("output_tokens"),
                "cost_usd": round(c.get("cost_usd") or 0.0, 4),
                "latency_ms": c.get("latency_ms"),
                "error_code": c.get("error_code"),
                "retries": c.get("retries"),
            }
            for c in calls
        ],
        "total_cost_usd": round(llm_metrics.get("total_cost_usd") or 0.0, 4),
        "total_input_tokens": llm_metrics.get("total_input_tokens"),
        "total_output_tokens": llm_metrics.get("total_output_tokens"),
        "total_latency_ms": llm_metrics.get("total_latency_ms"),
    }


def _validate_case(captured: Dict[str, Any]) -> Dict[str, Any]:
    if captured.get("status") == "EXCEPTION":
        return {
            "case_id": captured["case_id"],
            "passed": False,
            "exception": captured.get("error"),
        }
    result = captured["result"]
    case = captured["case"]
    deck = result.get("deck") or []
    summary = result.get("summary") or {}
    warnings = result.get("warnings") or []

    # Iteration-1 structural checks (must keep passing).
    non_basic_counts: Counter = Counter()
    for c in deck:
        n = c.get("card_name") or ""
        if n and n not in _BASIC_LAND_NAMES:
            non_basic_counts[n] += 1
    singleton_violations = {n: c for n, c in non_basic_counts.items() if c > 1}

    must_include = case["must_include_cards"]
    deck_names_lower = {(c.get("card_name") or "").strip().lower() for c in deck}
    must_present = [m for m in must_include if m.strip().lower() in deck_names_lower]
    must_dropped = [m for m in must_include if m.strip().lower() not in deck_names_lower]

    metrics = summary.get("creativity_envelope_metrics") or {}
    # creativity_delta_count is now computed inside the build itself and
    # surfaced in summary.creativity_envelope_metrics. Falling back to
    # the local recompute is only useful if a future refactor moves the
    # field — kept as a defensive guard.
    creativity_delta = metrics.get("creativity_delta_count")
    if creativity_delta is None:
        archetype_brief = (result.get("debug") or {}).get("archetype_brief", {})
        creativity_delta = _creativity_delta_count(deck, archetype_brief)
    novel_combo_flags = summary.get("novel_combo_flags") or []
    novel_combos = _novel_combo_count(novel_combo_flags)

    rationale_samples = _sample_rationales(deck, n=5)

    llm_breakdown = _llm_call_breakdown(summary.get("llm_metrics") or {})

    fallback_events: List[Dict[str, str]] = []
    for w in warnings:
        code = w.get("code", "")
        if any(tok in code for tok in (
            "INTERPRETER_FAILED", "INTERPRETER_INVALID_JSON",
            "CRITIC_FAILED", "CRITIC_INVALID_JSON",
            "CRITIC_SKIPPED", "WILD_COMBO_FAILED", "WILD_COMBO_SKIPPED",
            "WILD_COMBO_INVALID_JSON",
            "FINAL_CRITIC_FAILED", "FINAL_CRITIC_INVALID_JSON", "FINAL_CRITIC_NO_REWRITES",
            "LLM_LAYER_UNAVAILABLE",
        )):
            fallback_events.append({"code": code, "message": w.get("message", "")})

    iter2_extra: Dict[str, Any] = {
        "creativity_delta_count": creativity_delta,
        "novel_combo_count": novel_combos,
        "novel_combo_flags": novel_combo_flags,
        "rationale_samples": rationale_samples,
        "llm": llm_breakdown,
        "fallback_events": fallback_events,
        "intent_analysis": summary.get("intent_analysis"),
        "summary_narrative": summary.get("summary_narrative"),
        "consider_adding": summary.get("consider_adding"),
    }

    if case["id"] == "ur_dragon_b3_dragon_tribal":
        iter2_extra["ur_dragon_check"] = _check_ur_dragon_combo_avoidance(deck)

    iter1_pass = (
        len(deck) == 100
        and not singleton_violations
        and not must_dropped
    )

    return {
        "case_id": case["id"],
        "iter1_passed": iter1_pass,
        "deck_size": len(deck),
        "singleton_violations": singleton_violations,
        "must_includes_resolved": must_present,
        "must_includes_dropped": must_dropped,
        "theme_coherence_score": metrics.get("theme_coherence_score"),
        "wall_clock_ms": captured["wall_clock_ms"],
        "elapsed_ms_layer_reported": result.get("elapsed_ms"),
        "iter2": iter2_extra,
    }


# ============================================================
# Aggregate + write report.
# ============================================================


def _aggregate(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate iteration-2 success criteria across the 5 cases."""
    n = len(results)
    creativity_deltas = [r["iter2"]["creativity_delta_count"] for r in results
                         if "iter2" in r]
    novel_combos = [r["iter2"]["novel_combo_count"] for r in results
                    if "iter2" in r]
    costs = [r["iter2"]["llm"]["total_cost_usd"] for r in results
             if "iter2" in r]
    wallclocks = [r["wall_clock_ms"] for r in results if "wall_clock_ms" in r]
    iter1_all_pass = all(r.get("iter1_passed") for r in results)

    mean_creativity_delta = sum(creativity_deltas) / max(1, len(creativity_deltas))
    any_novel = any(c >= 1 for c in novel_combos)
    mean_cost = sum(costs) / max(1, len(costs))
    mean_wallclock_s = (sum(wallclocks) / max(1, len(wallclocks))) / 1000.0

    ur_check = next(
        (r["iter2"].get("ur_dragon_check") for r in results
         if r["case_id"] == "ur_dragon_b3_dragon_tribal" and r.get("iter2")),
        None,
    )

    criteria = {
        "iter1_structural_pass_5_of_5": iter1_all_pass,
        "mean_creativity_delta_count_geq_8": {
            "value": round(mean_creativity_delta, 2),
            "threshold": 8.0,
            "passed": mean_creativity_delta >= 8.0,
        },
        "at_least_1_novel_combo_in_any_deck": {
            "value": sum(novel_combos),
            "per_deck": novel_combos,
            "passed": any_novel,
        },
        "rationale_substantively_different": {
            "manual_inspection_required": True,
            "passed": None,
            "note": "Sampled rationales below — human-score each for deck-context awareness.",
        },
        "mean_cost_usd_leq_0_50": {
            "value": round(mean_cost, 4),
            "threshold": 0.50,
            "passed": mean_cost <= 0.50,
        },
        "mean_wallclock_s_leq_45": {
            "value": round(mean_wallclock_s, 1),
            "threshold": 45.0,
            "passed": mean_wallclock_s <= 45.0,
        },
        "ur_dragon_creativity_envelope_held": {
            "details": ur_check,
            "passed": ur_check["creativity_envelope_held"] if ur_check else None,
        },
    }
    auto_passed = sum(
        1 for v in criteria.values()
        if isinstance(v, dict) and v.get("passed") is True
    )
    # Iter1 pass is a single boolean.
    auto_passed += 1 if iter1_all_pass else 0
    return {
        "criteria": criteria,
        "auto_passed_count": auto_passed,
        "total_criteria": 6,  # iter1 + 5 explicit iter2 criteria
    }


def _format_report(results: List[Dict[str, Any]], aggregate: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("# Pillar D Iteration 2 — Validation Report")
    lines.append("")
    lines.append(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Snapshot: `{DEFAULT_SNAPSHOT_ID}`")
    lines.append("")
    lines.append("## Headline")
    lines.append("")
    lines.append(f"**Auto-passed: {aggregate['auto_passed_count']} / {aggregate['total_criteria']} success criteria.**")
    lines.append("")
    for name, val in aggregate["criteria"].items():
        if isinstance(val, dict):
            check = val.get("passed")
            mark = "✅" if check is True else ("⚠️" if check is None else "❌")
            line = f"- {mark} **{name}**"
            if "value" in val:
                line += f" — value `{val['value']}` "
                if "threshold" in val:
                    line += f"(threshold `{val['threshold']}`)"
            if val.get("note"):
                line += f" — _{val['note']}_"
            lines.append(line)
        else:
            mark = "✅" if val else "❌"
            lines.append(f"- {mark} **{name}**")
    lines.append("")
    lines.append("## Per-case summary")
    lines.append("")
    lines.append("| Case | iter1 pass | wall (s) | cost ($) | LLM calls | creativity Δ | novel combos | "
                 "theme coh. | must-inc resolved |")
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for r in results:
        iter2 = r.get("iter2") or {}
        llm = iter2.get("llm") or {}
        lines.append(
            f"| {r['case_id']} | {'✅' if r.get('iter1_passed') else '❌'} | "
            f"{(r.get('wall_clock_ms') or 0) / 1000.0:.1f} | "
            f"${llm.get('total_cost_usd', 0):.4f} | "
            f"{llm.get('call_count', 0)} | "
            f"{iter2.get('creativity_delta_count', '?')} | "
            f"{iter2.get('novel_combo_count', 0)} | "
            f"{r.get('theme_coherence_score', 0.0):.2f} | "
            f"{len(r.get('must_includes_resolved') or [])}/{len(r.get('must_includes_resolved') or []) + len(r.get('must_includes_dropped') or [])} |"
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
        iter2 = r.get("iter2") or {}
        llm = iter2.get("llm") or {}
        lines.append("")
        lines.append("**LLM call breakdown:**")
        lines.append("")
        lines.append("| phase | ok | input | output | cost | latency (s) | error |")
        lines.append("|---|---|---|---|---|---|---|")
        for c in llm.get("calls") or []:
            lines.append(
                f"| {c['phase']} | {c['ok']} | {c['input_tokens']} | {c['output_tokens']} | "
                f"${c['cost_usd']:.4f} | {(c['latency_ms'] or 0) / 1000.0:.1f} | "
                f"{c.get('error_code') or '-'} |"
            )
        lines.append("")
        lines.append(f"- LLM total cost (this case): `${llm.get('total_cost_usd', 0):.4f}`")
        lines.append(f"- LLM total tokens: `{llm.get('total_input_tokens', 0)} in / {llm.get('total_output_tokens', 0)} out`")
        lines.append("")
        lines.append(f"- creativity_delta_count (cards NOT in top-30 staples): `{iter2.get('creativity_delta_count')}`")
        lines.append(f"- novel_combo_count (LLM combos with in_spellbook=false): `{iter2.get('novel_combo_count')}`")
        lines.append("")
        novel_flags = iter2.get("novel_combo_flags") or []
        if novel_flags:
            lines.append("**Novel combo flags:**")
            for f in novel_flags:
                spellbook = "(Spellbook)" if f.get("in_spellbook") else "(NOVEL)"
                applied = "[applied as swap]" if f.get("applied_swap") else ""
                cards_str = " + ".join(f.get("cards") or [])
                lines.append(f"- {cards_str} {spellbook} {applied} — {f.get('outcome', '')}")
            lines.append("")
        # Intent analysis.
        ia = iter2.get("intent_analysis")
        if ia:
            lines.append("**Intent analysis (LLM call #1):**")
            lines.append("")
            lines.append(f"- likely_win_condition: `{ia.get('likely_win_condition', '')!r}`")
            it = ia.get("implicit_themes") or []
            if it:
                lines.append(f"- implicit_themes: `{it}`")
            se = ia.get("suggested_extensions") or []
            if se:
                lines.append("- suggested_extensions:")
                for ext in se[:6]:
                    lines.append(f"  - `{ext.get('card', '?')}` — {ext.get('why', '')}")
            cw = ia.get("conflict_warnings") or []
            if cw:
                lines.append(f"- conflict_warnings: `{cw}`")
            lines.append("")
        # Rationale samples.
        samples = iter2.get("rationale_samples") or []
        if samples:
            lines.append("**5 rationale samples (verbatim):**")
            for s in samples:
                lines.append(f"- **{s['card']}** ({s['source']}): _{s['reason']}_")
            lines.append("")
        # Summary narrative.
        sn = iter2.get("summary_narrative")
        if sn:
            lines.append("**Summary narrative:**")
            lines.append("")
            lines.append(f"> {sn}")
            lines.append("")
        # Consider adding.
        ca = iter2.get("consider_adding") or []
        if ca:
            lines.append("**Consider adding (LLM flagged, not added):**")
            for c in ca:
                lines.append(f"- `{c.get('card', '?')}` — {c.get('why', '')}")
            lines.append("")
        # Fallback events.
        fe = iter2.get("fallback_events") or []
        if fe:
            lines.append("**Fallback events / LLM-layer warnings:**")
            for f in fe:
                lines.append(f"- `{f['code']}`: {f['message']}")
            lines.append("")
        # Ur-dragon specific.
        urd = iter2.get("ur_dragon_check")
        if urd:
            lines.append("**Ur-Dragon creativity-envelope check:**")
            for k, v in urd.items():
                lines.append(f"- {k}: `{v}`")
            lines.append("")
        lines.append("---")
        lines.append("")
    lines.append("## Iteration 2 → Iteration 3 hand-off")
    lines.append("")
    lines.append("Filled in based on the actual measured behavior above. The "
                 "iteration-3 work plan should start from the items below.")
    lines.append("")
    # The hand-off section is filled in post-run by inspecting the report.
    # Print placeholders the runner fills in with `--write-handoff`.
    lines.append("### Where the LLM under-performed")
    lines.append("")
    lines.append("- _Inspect the per-case detail above; flag any phase that "
                 "consistently failed (e.g. C2.2 wild-combo returning 0 "
                 "flags) or under-delivered (e.g. creativity_delta < 8)._")
    lines.append("")
    lines.append("### Which prompts need revision in iteration 3")
    lines.append("")
    lines.append("- _Based on the rationale samples above — note any "
                 "phase whose output reads template-y or generic._")
    lines.append("")
    lines.append("### Is a model upgrade likely to help (Opus 4.6 / 4.7), "
                 "or does prompt-engineering ceiling come first?")
    lines.append("")
    lines.append("- _If the rationales are uniformly excellent and the only "
                 "miss is creativity_delta or wallclock, prompt-engineering "
                 "is likely the cheaper next step. If they're flat / "
                 "template-y, model upgrade probably helps more._")
    lines.append("")
    lines.append("### Is the candidate pool wide enough, or does call #2.5 "
                 "need a broader pool?")
    lines.append("")
    lines.append("- _Inspect novel_combo_count + the C2.2 latency / cost. "
                 "If C2.2 is consistently returning 0 novel flags despite "
                 "a 350-card pool, expand or rethink._")
    lines.append("")
    return "\n".join(lines)


# ============================================================
# Main.
# ============================================================


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", default=DEFAULT_SNAPSHOT_ID)
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    parser.add_argument("--cases", type=str, default="",
                        help="Comma-separated case IDs to run (defaults to all).")
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
        iter2 = validated.get("iter2") or {}
        llm = iter2.get("llm") or {}
        print(
            f"  iter1_passed={validated.get('iter1_passed')} "
            f"wall={(validated.get('wall_clock_ms') or 0) / 1000.0:.1f}s "
            f"cost=${llm.get('total_cost_usd', 0):.4f} "
            f"creativity_delta={iter2.get('creativity_delta_count')} "
            f"novel_combos={iter2.get('novel_combo_count')}",
            flush=True,
        )

    aggregate = _aggregate(results)
    report = _format_report(results, aggregate)
    Path(args.report).write_text(report, encoding="utf-8")
    print(f"\nReport written to {args.report}")
    print(
        f"Final: auto-passed {aggregate['auto_passed_count']} / "
        f"{aggregate['total_criteria']} success criteria."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
