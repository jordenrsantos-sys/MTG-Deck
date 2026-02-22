from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Set

from api.engine.layers.engine_coherence_v1 import run_engine_coherence_v1


ENGINE_COHERENCE_V2_VERSION = "engine_coherence_v2"


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _round6_half_up(value: float) -> float:
    return float(Decimal(str(float(value))).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP))


def _clamp01(value: float) -> float:
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return float(value)


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _sorted_codes(codes: Set[str]) -> List[str]:
    return sorted({code for code in codes if isinstance(code, str) and code.strip() != ""})


def _top_share(top_rows: Any) -> float | None:
    rows = [row for row in _as_list(top_rows) if isinstance(row, dict)]
    shares = [
        _round6_half_up(_clamp01(float(row.get("share"))))
        for row in rows
        if _is_number(row.get("share"))
    ]
    if len(shares) == 0:
        return None
    return _round6_half_up(max(shares))


def _base_payload(*, status: str, reason_code: str | None) -> Dict[str, Any]:
    return {
        "version": ENGINE_COHERENCE_V2_VERSION,
        "status": status,
        "reason_code": reason_code,
        "codes": [],
        "summary": {
            "playable_slots_total": 0,
            "non_dead_slots_total": 0,
            "dead_slots_total": 0,
            "dead_slot_ratio": 0.0,
            "primitive_concentration_index": 0.0,
            "overlap_score": 0.0,
            "bridge_amplification_proxy_v1": 0.0,
            "diversity_proxy_v1": 0.0,
        },
        "signals": {
            "commander_dependency_signal_v1": None,
            "missing_required_count_v1": None,
            "top_primitive_share_v1": None,
        },
        "dead_slots": [],
        "top_primitive_concentration": [],
    }


def run_engine_coherence_v2(
    *,
    primitive_index_by_slot: Any,
    deck_slot_ids_playable: Any,
    structural_snapshot_v1_payload: Any,
) -> Dict[str, Any]:
    v1_payload = run_engine_coherence_v1(
        primitive_index_by_slot=primitive_index_by_slot,
        deck_slot_ids_playable=deck_slot_ids_playable,
    )

    v1_status = _nonempty_str(v1_payload.get("status"))
    if v1_status == "SKIP":
        return _base_payload(status="SKIP", reason_code="PRIMITIVE_INDEX_UNAVAILABLE")

    v1_summary = _as_dict(v1_payload.get("summary"))
    structural = _as_dict(structural_snapshot_v1_payload)

    dead_slot_ratio = _round6_half_up(_clamp01(float(v1_summary.get("dead_slot_ratio") or 0.0)))
    primitive_concentration = _round6_half_up(
        _clamp01(float(v1_summary.get("primitive_concentration_index") or 0.0))
    )
    overlap_score = _round6_half_up(_clamp01(float(v1_summary.get("overlap_score") or 0.0)))

    commander_dependency_signal = structural.get("commander_dependency_signal_v1")
    commander_dependency_signal_norm = None
    if _is_number(commander_dependency_signal):
        commander_dependency_signal_norm = _round6_half_up(_clamp01(float(commander_dependency_signal)))

    top_share = _top_share(v1_payload.get("top_primitive_concentration"))
    if top_share is None:
        top_share = primitive_concentration

    diversity_proxy = _round6_half_up(_clamp01(1.0 - float(top_share or 0.0)))

    bridge_amplification_proxy = 0.0
    if commander_dependency_signal_norm is not None:
        bridge_amplification_proxy = _round6_half_up(
            _clamp01(float(commander_dependency_signal_norm) * (1.0 - dead_slot_ratio) * (1.0 - primitive_concentration))
        )

    missing_required_count = None
    structural_health = _as_dict(structural.get("structural_health_summary_v1"))
    if isinstance(structural_health.get("missing_required_count"), int) and not isinstance(
        structural_health.get("missing_required_count"),
        bool,
    ):
        missing_required_count = int(structural_health.get("missing_required_count"))

    codes: Set[str] = set(_sorted_codes(set(_as_list(v1_payload.get("codes")))))
    if commander_dependency_signal_norm is None:
        codes.add("ENGINE_COHERENCE_V2_COMMANDER_SIGNAL_UNAVAILABLE")

    status = "OK"
    if len(codes) > 0 or v1_status == "WARN":
        status = "WARN"

    return {
        "version": ENGINE_COHERENCE_V2_VERSION,
        "status": status,
        "reason_code": None,
        "codes": _sorted_codes(codes),
        "summary": {
            "playable_slots_total": int(v1_summary.get("playable_slots_total") or 0),
            "non_dead_slots_total": int(v1_summary.get("non_dead_slots_total") or 0),
            "dead_slots_total": int(v1_summary.get("dead_slots_total") or 0),
            "dead_slot_ratio": dead_slot_ratio,
            "primitive_concentration_index": primitive_concentration,
            "overlap_score": overlap_score,
            "bridge_amplification_proxy_v1": bridge_amplification_proxy,
            "diversity_proxy_v1": diversity_proxy,
        },
        "signals": {
            "commander_dependency_signal_v1": commander_dependency_signal_norm,
            "missing_required_count_v1": missing_required_count,
            "top_primitive_share_v1": _round6_half_up(_clamp01(float(top_share or 0.0))),
        },
        "dead_slots": _as_list(v1_payload.get("dead_slots")),
        "top_primitive_concentration": _as_list(v1_payload.get("top_primitive_concentration")),
    }
