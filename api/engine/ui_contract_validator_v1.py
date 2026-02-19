from __future__ import annotations

from typing import Any, Dict, List


UI_CONTRACT_VERSION_V1 = "ui_contract_v1"

_REQUIRED_TOP_LEVEL_FIELDS_V1 = [
    "engine_version",
    "ruleset_version",
    "bracket_definition_version",
    "game_changers_version",
    "db_snapshot_id",
    "profile_id",
    "bracket_id",
    "status",
    "unknowns",
    "result",
]

_KNOWN_TOP_LEVEL_FIELDS_V1 = [
    "engine_version",
    "ruleset_version",
    "bracket_definition_version",
    "game_changers_version",
    "db_snapshot_id",
    "profile_id",
    "bracket_id",
    "status",
    "deck_size_total",
    "deck_status",
    "cards_needed",
    "cards_to_cut",
    "build_hash_v1",
    "graph_hash_v2",
    "unknowns",
    "result",
]

_KNOWN_RESULT_FIELDS_V1 = [
    "ui_contract_version",
    "available_panels_v1",
    "ui_index_v1",
    "canonical_slots_all",
    "unknowns_canonical",
    "structural_snapshot_v1",
    "graph_v1",
    "graph_nodes",
    "graph_edges",
    "graph_typed_edges_total",
    "graph_typed_nodes_total",
    "graph_typed_components_total",
    "snapshot_preflight_v1",
    "scoring_summary_v1",
]


def _is_bool_map(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    for key, item in value.items():
        if not isinstance(key, str):
            return False
        if not isinstance(item, bool):
            return False
    return True


def validate_build_response_ui_contract_v1(payload: Any) -> Dict[str, Any]:
    missing_required_fields: List[str] = []
    type_mismatches: List[str] = []

    if not isinstance(payload, dict):
        missing_required_fields = list(_REQUIRED_TOP_LEVEL_FIELDS_V1)
        return {
            "ui_contract_version_detected": None,
            "available_panels_v1": {},
            "missing_required_fields": missing_required_fields,
            "type_mismatches": [],
            "unknown_top_level_fields": [],
            "unknown_result_fields": [],
            "contract_compliance": "FAIL",
            "warnings": [],
        }

    for field in _REQUIRED_TOP_LEVEL_FIELDS_V1:
        if field not in payload:
            missing_required_fields.append(field)

    if "engine_version" in payload and not isinstance(payload.get("engine_version"), str):
        type_mismatches.append("engine_version")
    if "ruleset_version" in payload and not isinstance(payload.get("ruleset_version"), str):
        type_mismatches.append("ruleset_version")
    if "bracket_definition_version" in payload and not isinstance(payload.get("bracket_definition_version"), str):
        type_mismatches.append("bracket_definition_version")
    if "game_changers_version" in payload and not isinstance(payload.get("game_changers_version"), str):
        type_mismatches.append("game_changers_version")
    if "db_snapshot_id" in payload and not isinstance(payload.get("db_snapshot_id"), str):
        type_mismatches.append("db_snapshot_id")
    if "profile_id" in payload and not isinstance(payload.get("profile_id"), str):
        type_mismatches.append("profile_id")
    if "bracket_id" in payload and not isinstance(payload.get("bracket_id"), str):
        type_mismatches.append("bracket_id")
    if "status" in payload and not isinstance(payload.get("status"), str):
        type_mismatches.append("status")
    if "unknowns" in payload and not isinstance(payload.get("unknowns"), list):
        type_mismatches.append("unknowns")

    result_raw = payload.get("result")
    if not isinstance(result_raw, dict):
        type_mismatches.append("result")
        result: Dict[str, Any] = {}
    else:
        result = result_raw

    ui_contract_version_detected = result.get("ui_contract_version") if isinstance(result, dict) else None
    if "ui_contract_version" not in result:
        missing_required_fields.append("result.ui_contract_version")
    elif not isinstance(ui_contract_version_detected, str):
        type_mismatches.append("result.ui_contract_version")
    elif ui_contract_version_detected != UI_CONTRACT_VERSION_V1:
        type_mismatches.append("result.ui_contract_version")

    available_panels_raw = result.get("available_panels_v1") if isinstance(result, dict) else {}
    if "available_panels_v1" not in result:
        missing_required_fields.append("result.available_panels_v1")
        available_panels_raw = {}
    elif not _is_bool_map(available_panels_raw):
        type_mismatches.append("result.available_panels_v1")

    unknown_top_level_fields = [key for key in payload.keys() if key not in _KNOWN_TOP_LEVEL_FIELDS_V1]
    unknown_result_fields = [key for key in result.keys() if key not in _KNOWN_RESULT_FIELDS_V1]

    warnings: List[str] = []
    if unknown_top_level_fields:
        warnings.append("unknown_top_level_fields present (WARN_ONLY)")
    if unknown_result_fields:
        warnings.append("unknown_result_fields present (WARN_ONLY)")

    contract_compliance = "PASS" if (len(missing_required_fields) == 0 and len(type_mismatches) == 0) else "FAIL"

    return {
        "ui_contract_version_detected": ui_contract_version_detected,
        "available_panels_v1": available_panels_raw,
        "missing_required_fields": missing_required_fields,
        "type_mismatches": type_mismatches,
        "unknown_top_level_fields": unknown_top_level_fields,
        "unknown_result_fields": unknown_result_fields,
        "contract_compliance": contract_compliance,
        "warnings": warnings,
    }
