from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


_DEPENDENCY_SIGNATURES_FILE = (
    Path(__file__).resolve().parent
    / "data"
    / "sufficiency"
    / "dependency_signatures_v1.json"
)


def _runtime_error(code: str, detail: str) -> RuntimeError:
    return RuntimeError(f"{code}: {detail}")


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _normalize_signature_primitives(raw: Any, *, field_path: str) -> List[str]:
    if not isinstance(raw, list):
        raise _runtime_error("DEPENDENCY_SIGNATURES_V1_INVALID", f"{field_path} must be an array")

    normalized = {
        token
        for token in (_nonempty_str(item) for item in raw)
        if token is not None
    }

    if len(normalized) == 0:
        raise _runtime_error("DEPENDENCY_SIGNATURES_V1_INVALID", f"{field_path} must include at least one primitive")

    return sorted(normalized)


def load_dependency_signatures_v1() -> Dict[str, Any]:
    if not _DEPENDENCY_SIGNATURES_FILE.is_file():
        raise _runtime_error("DEPENDENCY_SIGNATURES_V1_MISSING", str(_DEPENDENCY_SIGNATURES_FILE))

    try:
        parsed = json.loads(_DEPENDENCY_SIGNATURES_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        raise _runtime_error("DEPENDENCY_SIGNATURES_V1_INVALID_JSON", str(_DEPENDENCY_SIGNATURES_FILE)) from exc

    if not isinstance(parsed, dict):
        raise _runtime_error("DEPENDENCY_SIGNATURES_V1_INVALID", "root must be an object")

    version = _nonempty_str(parsed.get("version"))
    if version is None:
        raise _runtime_error("DEPENDENCY_SIGNATURES_V1_INVALID", "version must be a non-empty string")

    signatures_raw = parsed.get("signatures")
    if not isinstance(signatures_raw, dict):
        raise _runtime_error("DEPENDENCY_SIGNATURES_V1_INVALID", "signatures must be an object")

    normalized_signatures: Dict[str, Dict[str, List[str]]] = {}
    for signature_key_raw in sorted(signatures_raw.keys(), key=lambda item: str(item)):
        signature_key = _nonempty_str(signature_key_raw)
        if signature_key is None:
            raise _runtime_error(
                "DEPENDENCY_SIGNATURES_V1_INVALID",
                "signatures keys must be non-empty strings",
            )

        signature_payload = signatures_raw.get(signature_key_raw)
        if not isinstance(signature_payload, dict):
            raise _runtime_error(
                "DEPENDENCY_SIGNATURES_V1_INVALID",
                f"signatures.{signature_key} must be an object",
            )

        if "any_required_primitives" not in signature_payload:
            raise _runtime_error(
                "DEPENDENCY_SIGNATURES_V1_INVALID",
                f"signatures.{signature_key} must include any_required_primitives",
            )

        normalized_signatures[signature_key] = {
            "any_required_primitives": _normalize_signature_primitives(
                signature_payload.get("any_required_primitives"),
                field_path=f"signatures.{signature_key}.any_required_primitives",
            )
        }

    return {
        "version": version,
        "signatures": normalized_signatures,
    }
