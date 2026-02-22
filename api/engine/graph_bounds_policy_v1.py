from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


_GRAPH_BOUNDS_SPEC_FILE = (
    Path(__file__).resolve().parent
    / "data"
    / "graph"
    / "graph_bounds_spec_v1.json"
)

_GRAPH_BOUNDS_POLICY_FILE = (
    Path(__file__).resolve().parent
    / "data"
    / "sufficiency"
    / "graph_bounds_policy_v1.json"
)


_REQUIRED_BOUNDS_KEYS = {
    "MAX_PRIMS_PER_SLOT",
    "MAX_SLOTS_PER_PRIM",
    "MAX_CARD_CARD_EDGES_TOTAL",
}


def _runtime_error(code: str, detail: str) -> RuntimeError:
    return RuntimeError(f"{code}: {detail}")


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _require_nonnegative_int(value: Any, *, field_path: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _runtime_error("GRAPH_BOUNDS_POLICY_V1_INVALID", f"{field_path} must be int")
    if value < 0:
        raise _runtime_error("GRAPH_BOUNDS_POLICY_V1_INVALID", f"{field_path} must be >= 0")
    return int(value)


def _resolve_graph_bounds_spec_file() -> Path | None:
    candidates = (_GRAPH_BOUNDS_SPEC_FILE, _GRAPH_BOUNDS_POLICY_FILE)
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def load_graph_bounds_spec_v1() -> Dict[str, Any]:
    spec_file = _resolve_graph_bounds_spec_file()
    if spec_file is None:
        raise _runtime_error("GRAPH_BOUNDS_POLICY_V1_MISSING", str(_GRAPH_BOUNDS_SPEC_FILE))

    try:
        parsed = json.loads(spec_file.read_text(encoding="utf-8"))
    except Exception as exc:
        raise _runtime_error(
            "GRAPH_BOUNDS_POLICY_V1_INVALID_JSON",
            str(spec_file),
        ) from exc

    if not isinstance(parsed, dict):
        raise _runtime_error("GRAPH_BOUNDS_POLICY_V1_INVALID", "root must be an object")

    expected_keys = {"version", "bounds"}
    if set(parsed.keys()) != expected_keys:
        raise _runtime_error(
            "GRAPH_BOUNDS_POLICY_V1_INVALID",
            f"root keys must be exactly {sorted(expected_keys)}",
        )

    version = _nonempty_str(parsed.get("version"))
    if version is None:
        raise _runtime_error("GRAPH_BOUNDS_POLICY_V1_INVALID", "version must be a non-empty string")

    bounds = parsed.get("bounds")
    if not isinstance(bounds, dict):
        raise _runtime_error("GRAPH_BOUNDS_POLICY_V1_INVALID", "bounds must be an object")

    if set(bounds.keys()) != _REQUIRED_BOUNDS_KEYS:
        raise _runtime_error(
            "GRAPH_BOUNDS_POLICY_V1_INVALID",
            f"bounds keys must be exactly {sorted(_REQUIRED_BOUNDS_KEYS)}",
        )

    normalized_bounds = {
        key: _require_nonnegative_int(bounds.get(key), field_path=f"bounds.{key}")
        for key in sorted(_REQUIRED_BOUNDS_KEYS)
    }

    return {
        "version": version,
        "bounds": normalized_bounds,
    }


def load_graph_bounds_policy_v1() -> Dict[str, Any]:
    return load_graph_bounds_spec_v1()
