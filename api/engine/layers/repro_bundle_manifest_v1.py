from __future__ import annotations

from typing import Any, Dict, List, Tuple

from api.engine.utils import sha256_hex, stable_json_dumps


REPRO_BUNDLE_MANIFEST_V1_VERSION = "repro_bundle_manifest_v1"

_LOCAL_PATH_REDACTED = "<LOCAL_PATH_REDACTED>"

_LAYER_GATE_MAP: List[Tuple[str, str]] = [
    ("snapshot_preflight_v1", "has_snapshot_preflight_v1"),
    ("typed_graph_invariants_v1", "has_typed_graph_invariants_v1"),
    ("profile_bracket_enforcement_v1", "has_profile_bracket_enforcement_v1"),
    ("bracket_compliance_summary_v1", "has_bracket_compliance_summary_v1"),
    ("graph_analytics_summary_v1", "has_graph_analytics_summary_v1"),
    ("graph_pathways_summary_v1", "has_graph_pathways_summary_v1"),
    ("disruption_surface_v1", "has_disruption_surface_v1"),
    ("vulnerability_index_v1", "has_vulnerability_index_v1"),
    ("structural_scorecard_v1", "has_structural_scorecard_v1"),
]


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _status_payload_present(value: Any) -> bool:
    return isinstance(value, dict) and _nonempty_str(value.get("status")) is not None


def _copy_sorted_dict(input_dict: Any) -> Dict[str, Any]:
    if not isinstance(input_dict, dict):
        return {}

    return {str(key): input_dict[key] for key in sorted(input_dict.keys(), key=lambda item: str(item))}


def _is_ui_redacted_path_marker(value: Any) -> bool:
    return isinstance(value, str) and value.strip() == _LOCAL_PATH_REDACTED


def _strip_ui_redacted_path_fields(value: Any) -> Any:
    if isinstance(value, dict):
        cleaned: Dict[str, Any] = {}
        for key in sorted(value.keys(), key=lambda item: str(item)):
            child = value[key]
            if _is_ui_redacted_path_marker(child):
                continue
            cleaned[str(key)] = _strip_ui_redacted_path_fields(child)
        return cleaned

    if isinstance(value, list):
        out: List[Any] = []
        for item in value:
            if _is_ui_redacted_path_marker(item):
                continue
            out.append(_strip_ui_redacted_path_fields(item))
        return out

    return value


def _layer_is_included(build_result: Dict[str, Any], *, layer_key: str, panel_key: str) -> bool:
    available_panels = build_result.get("available_panels_v1")
    if isinstance(available_panels, dict):
        panel_value = available_panels.get(panel_key)
        if isinstance(panel_value, bool):
            return panel_value

    return _status_payload_present(build_result.get(layer_key))


def build_repro_bundle_manifest_v1(build_result: dict) -> dict:
    result_payload = build_result if isinstance(build_result, dict) else {}

    included_layers = [
        layer_key
        for layer_key, panel_key in _LAYER_GATE_MAP
        if _layer_is_included(result_payload, layer_key=layer_key, panel_key=panel_key)
    ]

    build_hash_v1_raw = result_payload.get("build_hash_v1")
    build_hash_v1 = build_hash_v1_raw if isinstance(build_hash_v1_raw, str) and build_hash_v1_raw != "" else None

    sanitized_result = _strip_ui_redacted_path_fields(result_payload)
    normalized_json_sha256 = sha256_hex(stable_json_dumps(sanitized_result))

    return {
        "version": REPRO_BUNDLE_MANIFEST_V1_VERSION,
        "engine_versions": _copy_sorted_dict(result_payload.get("pipeline_versions")),
        "included_layers": included_layers,
        "hashes": {
            "build_hash_v1": build_hash_v1,
            "normalized_json_sha256": normalized_json_sha256,
        },
    }
