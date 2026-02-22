from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Set


COMMANDER_DEPENDENCY_V2_VERSION = "commander_dependency_v2"


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _clamp01(value: float) -> float:
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return float(value)


def _round6_half_up(value: float) -> float:
    return float(Decimal(str(float(value))).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP))


def _sorted_unique_strings(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []
    cleaned = {
        token
        for token in (_nonempty_str(value) for value in values)
        if token is not None
    }
    return sorted(cleaned)


def _sorted_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: data[key]
        for key in sorted(data.keys(), key=lambda item: str(item))
    }


def _base_payload(
    *,
    status: str,
    access_required: bool,
    amplifier: bool,
    line_share_percent: float,
    signals: Dict[str, Any],
    notes: Set[str],
    codes: Set[str],
) -> Dict[str, Any]:
    return {
        "version": COMMANDER_DEPENDENCY_V2_VERSION,
        "status": status,
        "commander_dependency_v2": {
            "access_required": bool(access_required),
            "amplifier": bool(amplifier),
            "line_share_percent": _round6_half_up(_clamp01(float(line_share_percent))),
        },
        "signals": _sorted_dict(signals),
        "notes": sorted(notes),
        "codes": sorted(codes),
    }


def run_commander_dependency_v2(
    *,
    engine_requirement_detection_v1_payload: Any,
    structural_snapshot_v1_payload: Any,
    engine_coherence_v1_payload: Any,
) -> Dict[str, Any]:
    if not isinstance(engine_requirement_detection_v1_payload, dict) and not isinstance(structural_snapshot_v1_payload, dict):
        return _base_payload(
            status="SKIP",
            access_required=False,
            amplifier=False,
            line_share_percent=0.0,
            signals={
                "commander_dependent_v1": None,
                "commander_dependency_signal_v1": None,
                "dead_slot_count_v1": None,
                "dead_slot_ratio_v1": None,
                "line_share_source_v1": None,
                "primitive_concentration_index_v1": None,
            },
            notes={"No upstream commander dependency inputs were available."},
            codes={"COMMANDER_DEPENDENCY_V2_UPSTREAM_UNAVAILABLE"},
        )

    engine_req_payload = (
        engine_requirement_detection_v1_payload
        if isinstance(engine_requirement_detection_v1_payload, dict)
        else {}
    )
    structural_payload = structural_snapshot_v1_payload if isinstance(structural_snapshot_v1_payload, dict) else {}
    coherence_payload = engine_coherence_v1_payload if isinstance(engine_coherence_v1_payload, dict) else {}

    engine_requirements = engine_req_payload.get("engine_requirements_v1")
    engine_requirements = engine_requirements if isinstance(engine_requirements, dict) else {}

    commander_dependent_v1 = _nonempty_str(engine_requirements.get("commander_dependent")) or "UNKNOWN"

    structural_signal_raw = structural_payload.get("commander_dependency_signal_v1")
    structural_signal = None
    if _is_number(structural_signal_raw):
        structural_signal = _round6_half_up(_clamp01(float(structural_signal_raw)))

    primitive_concentration_raw = structural_payload.get("primitive_concentration_index_v1")
    primitive_concentration = None
    if _is_number(primitive_concentration_raw):
        primitive_concentration = _round6_half_up(_clamp01(float(primitive_concentration_raw)))

    dead_slot_ids = _sorted_unique_strings(structural_payload.get("dead_slot_ids_v1"))
    dead_slot_count = len(dead_slot_ids)

    coherence_summary = coherence_payload.get("summary") if isinstance(coherence_payload.get("summary"), dict) else {}
    dead_slot_ratio_raw = coherence_summary.get("dead_slot_ratio")
    dead_slot_ratio = None
    if _is_number(dead_slot_ratio_raw):
        dead_slot_ratio = _round6_half_up(_clamp01(float(dead_slot_ratio_raw)))

    codes: Set[str] = set()
    notes: Set[str] = set()

    if structural_signal is not None:
        line_share_percent = structural_signal
        line_share_source = "structural_snapshot.commander_dependency_signal_v1"
    else:
        proxy_by_dependency = {
            "HIGH": 0.75,
            "MED": 0.5,
            "LOW": 0.25,
            "UNKNOWN": 0.5,
        }
        line_share_percent = _round6_half_up(
            _clamp01(float(proxy_by_dependency.get(commander_dependent_v1, 0.5)))
        )
        line_share_source = "proxy_from_commander_dependent_v1"
        codes.add("COMMANDER_DEPENDENCY_V2_LINE_SHARE_PROXY_USED")
        notes.add("line_share_percent used deterministic proxy because structural commander dependency signal was unavailable.")

    access_required = bool(
        commander_dependent_v1 == "HIGH"
        or line_share_percent >= 0.55
    )
    amplifier = bool((not access_required) and line_share_percent >= 0.2)

    if dead_slot_ratio is None:
        codes.add("COMMANDER_DEPENDENCY_V2_DEAD_SLOT_RATIO_UNAVAILABLE")
        notes.add("dead_slot_ratio_v1 was unavailable from engine_coherence_v1 summary.")

    if dead_slot_count > 0:
        codes.add("COMMANDER_DEPENDENCY_V2_DEAD_SLOTS_PRESENT")
        notes.add("dead slots are present and may weaken practical commander access consistency.")

    status = "OK"
    if len(codes) > 0:
        status = "WARN"

    signals = {
        "commander_dependent_v1": commander_dependent_v1,
        "commander_dependency_signal_v1": structural_signal,
        "dead_slot_count_v1": int(dead_slot_count),
        "dead_slot_ratio_v1": dead_slot_ratio,
        "line_share_source_v1": line_share_source,
        "primitive_concentration_index_v1": primitive_concentration,
    }

    return _base_payload(
        status=status,
        access_required=access_required,
        amplifier=amplifier,
        line_share_percent=line_share_percent,
        signals=signals,
        notes=notes,
        codes=codes,
    )
