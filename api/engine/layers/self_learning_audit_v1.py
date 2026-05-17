"""
self_learning_audit_v1 — DESIGN_DECISIONS §1.3 + §4 audit log.

Every event that changes calibration baseline (corpus ingest, weight adjustment,
strength-band recalibration, etc.) writes a structured entry to
`repo/api/engine/data/self_learning/audit_log_v1.jsonl`. Append-only.

The cap discipline (per DESIGN_DECISIONS §1.4):
  - ±0.03 per event on any single calibration weight
  - ±0.03 per 100 games cumulative
  - Architectural shifts (new themes / new primitive vocabulary / new bracket rules)
    → hard halt, queued for user review

This module:
  1. Writes the audit entries (`log_event`)
  2. Reads recent entries (`read_audit_log`)
  3. Reports rolling cumulative deltas per weight (`rolling_delta`) so the
     ±0.03/100-game cap can be enforced by callers before they apply changes
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


AUDIT_VERSION = "self_learning_audit_v1.0"

_AUDIT_PATH = Path(__file__).resolve().parents[2] / "engine" / "data" / "self_learning" / "audit_log_v1.jsonl"

# Caps from DESIGN_DECISIONS §1.4
PER_EVENT_DELTA_CAP = 0.03
PER_100_GAME_DELTA_CAP = 0.03


def log_event(
    *,
    event_type: str,
    source: str,
    description: str,
    delta_summary: Optional[Dict[str, float]] = None,
    within_caps: Optional[bool] = None,
    is_architectural_shift: bool = False,
    requires_user_review: bool = False,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Append a self-learning event entry to the audit log.

    Args:
        event_type: "corpus_ingest" | "weight_adjustment" | "band_recalibration"
            | "architectural_shift" | "manual"
        source: "user_approved" | "playtest_loop" | "bootstrap_expansion"
            | "manual_admin" | etc.
        description: human-readable summary (≤200 chars typical)
        delta_summary: optional dict of {weight_name: delta} for traceability
        within_caps: bool — whether this event respected the ±0.03 caps. None
            = not applicable (e.g. corpus_ingest doesn't change weights directly)
        is_architectural_shift: True for new themes / vocab / bracket rules.
            Triggers a hard halt for user direction per §1.4.
        requires_user_review: True if the event exceeded caps and is queued.
        extra: free-form metadata (corpus_id, source deck, etc.)

    Returns the entry that was written.
    """
    entry: Dict[str, Any] = {
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "audit_version": AUDIT_VERSION,
        "event_type": event_type,
        "source": source,
        "description": description,
        "delta_summary": delta_summary or {},
        "within_caps": within_caps,
        "is_architectural_shift": bool(is_architectural_shift),
        "requires_user_review": bool(requires_user_review),
        "extra": extra or {},
    }
    try:
        _AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(_AUDIT_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as exc:
        entry["write_error"] = f"{exc.__class__.__name__}: {exc}"
    return entry


def read_audit_log(limit: int = 100) -> List[Dict[str, Any]]:
    """Read recent audit entries. Newest last."""
    if not _AUDIT_PATH.exists():
        return []
    entries: List[Dict[str, Any]] = []
    try:
        with open(_AUDIT_PATH, "r", encoding="utf-8") as f:
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


def rolling_delta(weight_name: str, window: int = 100) -> float:
    """Sum of recent deltas for `weight_name` over the last `window` events.

    Caller uses this to gate weight adjustments: if rolling_delta + proposed
    delta > PER_100_GAME_DELTA_CAP, queue for user review instead of applying.
    """
    total = 0.0
    for entry in read_audit_log(limit=window):
        deltas = entry.get("delta_summary") or {}
        if isinstance(deltas, dict) and weight_name in deltas:
            try:
                total += float(deltas[weight_name])
            except (TypeError, ValueError):
                continue
    return round(total, 4)


def check_caps(
    proposed_deltas: Dict[str, float],
    window: int = 100,
) -> Dict[str, Any]:
    """Pre-flight check: does this proposed change fit within both caps?

    Returns:
      {
        "within_per_event": bool,    # each |delta| <= 0.03
        "within_per_100": bool,      # |rolling + delta| <= 0.03
        "violations": [...],          # per-weight detail when over
      }
    """
    within_per_event = True
    within_per_100 = True
    violations: List[Dict[str, Any]] = []
    for weight, delta in (proposed_deltas or {}).items():
        try:
            d = float(delta)
        except (TypeError, ValueError):
            continue
        per_event_ok = abs(d) <= PER_EVENT_DELTA_CAP
        rolling = rolling_delta(weight, window=window)
        per_100_ok = abs(rolling + d) <= PER_100_GAME_DELTA_CAP
        if not per_event_ok:
            within_per_event = False
        if not per_100_ok:
            within_per_100 = False
        if not (per_event_ok and per_100_ok):
            violations.append({
                "weight": weight,
                "proposed_delta": d,
                "rolling_delta": rolling,
                "per_event_cap": PER_EVENT_DELTA_CAP,
                "per_100_cap": PER_100_GAME_DELTA_CAP,
                "per_event_ok": per_event_ok,
                "per_100_ok": per_100_ok,
            })
    return {
        "within_per_event": within_per_event,
        "within_per_100": within_per_100,
        "all_caps_satisfied": within_per_event and within_per_100,
        "violations": violations,
    }
