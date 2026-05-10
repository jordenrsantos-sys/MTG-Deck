"""Perf-audit CLI — Phase 6 Stage 6.

Runs the canonical Shelob 1010839 6-card seed through `/build` via FastAPI
TestClient, captures per-layer timing via `TimingProbe`, computes summary
stats (median / min / max / p95) over N=20 warm iterations, emits a
markdown report at `05_VALIDATION/PERF_TIMING_REPORT_<date>.md`, exits
non-zero on any layer that exceeds the 500ms warm threshold OR the
8000ms total-pipeline threshold.

Usage::

    cd repo
    .venv/Scripts/python.exe -m tools.perf.run_perf_timing_audit

The active mtg.sqlite snapshot must be present + the canonical commander
+ seed cards must resolve in that snapshot.
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

# Make `from tools.perf...` resolvable when run as a script.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from api.engine.perf.timing_instrumentation_v1 import TimingProbe  # noqa: E402

CANONICAL_PAYLOAD: Dict[str, Any] = {
    "db_snapshot_id": "20260217_190902",
    "profile_id": "focused",
    "bracket_id": "B2",
    "format": "commander",
    "commander": "Shelob, Child of Ungoliant",
    "cards": [
        "Sol Ring",
        "Arcane Signet",
        "Birthing Pod",
        "Eldritch Evolution",
        "Neoform",
        "Prime Speaker Vannifar",
    ],
    "engine_patches_v0": [],
}

DEFAULT_WARM_ITERATIONS = 20
LAYER_THRESHOLD_MS = 500.0
PIPELINE_THRESHOLD_MS = 8000.0


def _percentile(values: List[float], pct: float) -> float:
    """Naive percentile for small N (no numpy dep). pct in [0, 100]."""
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    s = sorted(values)
    k = (len(s) - 1) * (pct / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    if lo == hi:
        return s[lo]
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def _summarize(values: List[float]) -> Dict[str, float]:
    if not values:
        return {"count": 0, "median": 0.0, "min": 0.0, "max": 0.0, "p95": 0.0, "mean": 0.0}
    return {
        "count": float(len(values)),
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
        "p95": _percentile(values, 95),
        "mean": statistics.mean(values),
    }


def run_audit(warm_iterations: int = DEFAULT_WARM_ITERATIONS) -> Dict[str, Any]:
    """Execute the audit; return a structured result dict."""
    from fastapi.testclient import TestClient
    from api.main import app

    client = TestClient(app)
    # Warm-up via /health then 1 cold /build (discard).
    client.get("/health")
    client.post("/build", json=CANONICAL_PAYLOAD)

    pipeline_totals_ms: List[float] = []
    per_layer_collected: Dict[str, List[float]] = {}
    layer_order_first: List[str] = []  # first-seen order for deterministic reporting

    for i in range(warm_iterations):
        probe = TimingProbe()
        wall_start_ns = time.perf_counter_ns()
        with probe:
            response = client.post("/build", json=CANONICAL_PAYLOAD)
        wall_elapsed_ms = (time.perf_counter_ns() - wall_start_ns) / 1_000_000.0
        if response.status_code != 200:
            return {
                "ok": False,
                "error": f"/build returned HTTP {response.status_code} on iteration {i}",
                "iterations_completed": i,
            }
        pipeline_totals_ms.append(wall_elapsed_ms)
        for entry in probe.entries:
            if entry.layer_id not in per_layer_collected:
                per_layer_collected[entry.layer_id] = []
                layer_order_first.append(entry.layer_id)
            per_layer_collected[entry.layer_id].append(entry.elapsed_ms)

    pipeline_summary = _summarize(pipeline_totals_ms)
    per_layer_summary: Dict[str, Dict[str, float]] = {}
    outliers: List[Dict[str, Any]] = []

    for layer_id in layer_order_first:
        summary = _summarize(per_layer_collected[layer_id])
        per_layer_summary[layer_id] = summary
        if summary["median"] > LAYER_THRESHOLD_MS:
            outliers.append({
                "layer_id": layer_id,
                "median_ms": summary["median"],
                "max_ms": summary["max"],
                "threshold_ms": LAYER_THRESHOLD_MS,
            })

    pipeline_outlier = pipeline_summary["median"] > PIPELINE_THRESHOLD_MS

    return {
        "ok": not pipeline_outlier and not outliers,
        "warm_iterations": warm_iterations,
        "pipeline_summary_ms": pipeline_summary,
        "per_layer_summary_ms": per_layer_summary,
        "layer_order_first": layer_order_first,
        "outliers": outliers,
        "pipeline_outlier": pipeline_outlier,
        "thresholds": {
            "layer_ms": LAYER_THRESHOLD_MS,
            "pipeline_ms": PIPELINE_THRESHOLD_MS,
        },
    }


def emit_report(result: Dict[str, Any], dest_path: Path) -> None:
    """Write the markdown report to dest_path."""
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    pipeline = result["pipeline_summary_ms"]
    layers = result["per_layer_summary_ms"]
    layer_order = result["layer_order_first"]
    outliers = result["outliers"]
    pipeline_outlier = result["pipeline_outlier"]
    iters = result["warm_iterations"]

    lines: List[str] = []
    lines.append("---")
    lines.append("tags:")
    lines.append("  - validation")
    lines.append("  - perf")
    lines.append("  - phase-6")
    lines.append("---")
    lines.append("")
    lines.append(f"# Perf Timing Report — {now_iso}")
    lines.append("")
    lines.append(f"Generated by `tools/perf/run_perf_timing_audit.py` over **{iters} warm iterations** of the canonical Shelob 1010839 6-card seed.")
    lines.append("")
    lines.append("## Result")
    lines.append("")
    if result["ok"]:
        lines.append("**PASS** — every layer median is under the 500ms threshold; pipeline total under 8000ms.")
    else:
        lines.append("**FAIL** — outliers detected (see below).")
    lines.append("")
    lines.append("## Pipeline total (wall-clock)")
    lines.append("")
    lines.append("| metric | ms |")
    lines.append("|---|---|")
    lines.append(f"| median | {pipeline['median']:.1f} |")
    lines.append(f"| min | {pipeline['min']:.1f} |")
    lines.append(f"| max | {pipeline['max']:.1f} |")
    lines.append(f"| p95 | {pipeline['p95']:.1f} |")
    lines.append(f"| mean | {pipeline['mean']:.1f} |")
    lines.append(f"| threshold | {result['thresholds']['pipeline_ms']:.0f} |")
    if pipeline_outlier:
        lines.append("")
        lines.append("> **Pipeline total exceeded threshold.** Investigate dominant layers below.")
    lines.append("")
    lines.append("## Per-layer breakdown")
    lines.append("")
    if not layer_order:
        lines.append("*(No per-layer probe data captured — instrumentation may not be wired into pipeline_build.py yet.)*")
    else:
        lines.append("Layer order is first-seen across iterations (deterministic).")
        lines.append("")
        lines.append("| layer_id | median ms | min ms | max ms | p95 ms | over threshold? |")
        lines.append("|---|---|---|---|---|---|")
        for layer_id in layer_order:
            s = layers[layer_id]
            over = "**YES**" if s["median"] > result["thresholds"]["layer_ms"] else "no"
            lines.append(f"| `{layer_id}` | {s['median']:.1f} | {s['min']:.1f} | {s['max']:.1f} | {s['p95']:.1f} | {over} |")
    lines.append("")
    if outliers:
        lines.append("## Outliers")
        lines.append("")
        for outlier in outliers:
            lines.append(
                f"- `{outlier['layer_id']}` — median {outlier['median_ms']:.1f}ms (max {outlier['max_ms']:.1f}ms) > threshold {outlier['threshold_ms']:.0f}ms"
            )
        lines.append("")
    lines.append("## Acceptance criteria")
    lines.append("")
    lines.append("- Per-layer median < 500ms warm.")
    lines.append("- Pipeline total < 8000ms warm.")
    lines.append("- Probe-OFF overhead < 2% of pipeline wall-clock (verified separately).")
    lines.append("")
    lines.append("Re-run with `python -m tools.perf.run_perf_timing_audit` after any engine-layer change.")
    lines.append("")
    dest_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Engine perf-timing audit (Phase 6 Stage 6).")
    parser.add_argument("--iterations", type=int, default=DEFAULT_WARM_ITERATIONS)
    parser.add_argument(
        "--report-dir",
        type=str,
        default=None,
        help="Override report output directory (default: ../Mtg deck building brain/05_VALIDATION/).",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON to stdout instead of markdown.")
    args = parser.parse_args()

    result = run_audit(warm_iterations=args.iterations)

    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        if args.report_dir:
            report_dir = Path(args.report_dir)
        else:
            # Default: vault location. Vault is a sibling of mtg-engine/
            # (`_REPO_ROOT.parent.parent` → "E:/MTG Root/" or analog), not
            # nested inside it.
            report_dir = _REPO_ROOT.parent.parent / "Mtg deck building brain" / "05_VALIDATION"
        report_dir.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        dest = report_dir / f"PERF_TIMING_REPORT_{date_str}.md"
        emit_report(result, dest)
        print(f"Report: {dest}")

    if not result.get("ok", False):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
