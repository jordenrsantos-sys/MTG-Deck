"""
mpa_calibration_log — Phase 5b.3 calibration log writer.

Writes calibration run results to `repo/api/engine/data/playtest/
calibration_log_v1.jsonl` (JSONL, append-only). Each line is one calibration
test (a specific deck pair benchmark) with the result + MPA version.

Used by future benchmark suite: bracket-4-vs-precon must hit ~85%+, mirror must
hit ~50%±5%, etc. Each run is logged so the calibration progress over time is
auditable.

Per DESIGN_DECISIONS.md rule 1.4 — calibration log entries with
`status: "uncalibrated"` are NOT promoted to strength-claim ground truth.
The status flips to "calibrated" only when all benchmark assertions pass.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


CALIBRATION_LOG_VERSION = "calibration_log_v1.0"

_LOG_PATH = Path(__file__).resolve().parents[2] / "engine" / "data" / "playtest" / "calibration_log_v1.jsonl"


def log_calibration_run(
    *,
    benchmark_name: str,
    deck_a_label: str,
    deck_b_label: str,
    n_games: int,
    deck_a_winrate: float,
    expected_winrate_min: Optional[float] = None,
    expected_winrate_max: Optional[float] = None,
    mpa_version: str,
    runner_version: str,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Append a calibration result to the log.

    Args:
        benchmark_name: e.g. "B4_cedh_vs_B1_precon", "mirror_match"
        deck_a_label / deck_b_label: human-readable identifiers
        n_games: number of games played (anti-bias-mirrored pairs × 2)
        deck_a_winrate: observed win rate of deck A
        expected_winrate_min / expected_winrate_max: assertion bounds
        mpa_version / runner_version: provenance strings
        extra: optional dict of additional metadata

    Returns:
        The log entry that was written, plus a `passes_assertion` field.
    """
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    passes = None
    if expected_winrate_min is not None and expected_winrate_max is not None:
        passes = expected_winrate_min <= deck_a_winrate <= expected_winrate_max
    elif expected_winrate_min is not None:
        passes = deck_a_winrate >= expected_winrate_min
    elif expected_winrate_max is not None:
        passes = deck_a_winrate <= expected_winrate_max

    entry: Dict[str, Any] = {
        "timestamp": timestamp,
        "benchmark_name": benchmark_name,
        "deck_a_label": deck_a_label,
        "deck_b_label": deck_b_label,
        "n_games": n_games,
        "deck_a_winrate": round(float(deck_a_winrate), 4),
        "expected_winrate_min": expected_winrate_min,
        "expected_winrate_max": expected_winrate_max,
        "passes_assertion": passes,
        "mpa_version": mpa_version,
        "runner_version": runner_version,
        "extra": extra or {},
    }

    try:
        _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as exc:
        entry["write_error"] = f"{exc.__class__.__name__}: {exc}"

    return entry


def read_calibration_log(limit: int = 100) -> List[Dict[str, Any]]:
    """Read the last `limit` entries from the calibration log. Newest last."""
    if not _LOG_PATH.exists():
        return []
    entries: List[Dict[str, Any]] = []
    try:
        with open(_LOG_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except Exception:
        pass
    return entries[-limit:]


def overall_calibration_status() -> str:
    """Returns 'calibrated' / 'uncalibrated' / 'unknown'.

    The status flips to 'calibrated' only when ALL recent benchmark
    assertions pass:
      - B4_cedh_vs_B1_precon win rate >= 0.80
      - mirror_match win rate within 0.45..0.55
    """
    entries = read_calibration_log(limit=50)
    if not entries:
        return "unknown"
    # Look at the most recent run of each benchmark
    latest_by_name: Dict[str, Dict[str, Any]] = {}
    for e in entries:
        name = e.get("benchmark_name")
        if isinstance(name, str):
            latest_by_name[name] = e
    if not latest_by_name:
        return "unknown"
    # Required benchmarks
    required = ["B4_cedh_vs_B1_precon", "mirror_match"]
    for name in required:
        e = latest_by_name.get(name)
        if e is None or e.get("passes_assertion") is not True:
            return "uncalibrated"
    return "calibrated"
