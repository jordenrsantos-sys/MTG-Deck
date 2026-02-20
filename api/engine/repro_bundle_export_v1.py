from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

from api.engine.layers.repro_bundle_manifest_v1 import build_repro_bundle_manifest_v1
from api.engine.utils import sha256_hex, stable_json_dumps


REPRO_BUNDLE_EXPORT_V1_VERSION = "repro_bundle_export_v1"

_REPRO_BUNDLE_FILE_ORDER: List[str] = [
    "repro_bundle_manifest_v1.json",
    "request_input.json",
    "build_result.json",
    "rules/gc_limits_v1.json",
    "rules/bracket_rules_v2.json",
    "rules/two_card_combos_v1.json",
]

_RULE_FILE_SOURCE_BY_BUNDLE_PATH: Dict[str, Path] = {
    "rules/gc_limits_v1.json": Path(__file__).resolve().parent / "data" / "brackets" / "gc_limits_v1.json",
    "rules/bracket_rules_v2.json": Path(__file__).resolve().parent / "data" / "brackets" / "bracket_rules_v2.json",
    "rules/two_card_combos_v1.json": Path(__file__).resolve().parent / "data" / "combos" / "two_card_combos_v1.json",
}


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _load_json_file(abs_path: Path, *, file_path: str) -> Any:
    if not abs_path.is_file():
        raise RuntimeError(f"REPRO_BUNDLE_EXPORT_V1_FILE_MISSING: {file_path}")

    try:
        return json.loads(abs_path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - deterministic error path
        raise RuntimeError(f"REPRO_BUNDLE_EXPORT_V1_FILE_INVALID_JSON: {file_path}") from exc


def _build_file_entry(path: str, payload: Any) -> Dict[str, Any]:
    normalized = stable_json_dumps(payload)
    return {
        "path": path,
        "sha256": sha256_hex(normalized),
        "json": payload,
    }


def build_repro_bundle_export_v1(
    *,
    request_input: Any,
    build_payload: Any,
) -> Dict[str, Any]:
    request_payload = _as_dict(request_input)
    build_report = _as_dict(build_payload)
    build_result = _as_dict(build_report.get("result"))

    manifest_payload = build_repro_bundle_manifest_v1(build_result)

    file_payload_by_path: Dict[str, Any] = {
        "repro_bundle_manifest_v1.json": manifest_payload,
        "request_input.json": request_payload,
        "build_result.json": build_report,
    }

    for bundle_path, source_path in _RULE_FILE_SOURCE_BY_BUNDLE_PATH.items():
        file_payload_by_path[bundle_path] = _load_json_file(source_path, file_path=bundle_path)

    files = [
        _build_file_entry(path=bundle_path, payload=file_payload_by_path[bundle_path])
        for bundle_path in _REPRO_BUNDLE_FILE_ORDER
    ]

    return {
        "version": REPRO_BUNDLE_EXPORT_V1_VERSION,
        "file_paths": list(_REPRO_BUNDLE_FILE_ORDER),
        "files": files,
    }
