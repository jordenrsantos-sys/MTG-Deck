from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


_STRESS_MODELS_FILE = (
    Path(__file__).resolve().parent
    / "data"
    / "sufficiency"
    / "stress_models_v1.json"
)


_ALLOWED_OPS = {
    "BOARD_WIPE",
    "GRAVEYARD_HATE_WINDOW",
    "STAX_TAX",
    "TARGETED_REMOVAL",
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
        raise _runtime_error("STRESS_MODELS_V1_INVALID", f"{field_path} must be int")
    if value < 0:
        raise _runtime_error("STRESS_MODELS_V1_INVALID", f"{field_path} must be >= 0")
    return int(value)


def _require_numeric(value: Any, *, field_path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _runtime_error("STRESS_MODELS_V1_INVALID", f"{field_path} must be numeric")
    return float(value)


def _normalize_turns(value: Any, *, field_path: str) -> List[int]:
    if not isinstance(value, list):
        raise _runtime_error("STRESS_MODELS_V1_INVALID", f"{field_path} must be an array")

    turns: List[int] = []
    seen: set[int] = set()
    for index, item in enumerate(value):
        turn = _require_nonnegative_int(item, field_path=f"{field_path}[{index}]")
        if turn in seen:
            continue
        seen.add(turn)
        turns.append(turn)

    if len(turns) == 0:
        raise _runtime_error("STRESS_MODELS_V1_INVALID", f"{field_path} must be non-empty")

    return sorted(turns)


def _normalize_operator(row: Any, *, field_path: str) -> Dict[str, Any]:
    if not isinstance(row, dict):
        raise _runtime_error("STRESS_MODELS_V1_INVALID", f"{field_path} must be an object")

    op = _nonempty_str(row.get("op"))
    if op is None or op not in _ALLOWED_OPS:
        raise _runtime_error("STRESS_MODELS_V1_INVALID", f"{field_path}.op must be one of {sorted(_ALLOWED_OPS)}")

    if op == "TARGETED_REMOVAL":
        expected_keys = {"op", "count"}
        if set(row.keys()) != expected_keys:
            raise _runtime_error(
                "STRESS_MODELS_V1_INVALID",
                f"{field_path} keys must be exactly {sorted(expected_keys)}",
            )
        return {
            "op": op,
            "count": _require_nonnegative_int(row.get("count"), field_path=f"{field_path}.count"),
        }

    if op == "BOARD_WIPE":
        expected_keys = {"op", "by_turn", "surviving_engine_fraction"}
        if set(row.keys()) != expected_keys:
            raise _runtime_error(
                "STRESS_MODELS_V1_INVALID",
                f"{field_path} keys must be exactly {sorted(expected_keys)}",
            )

        by_turn = _require_nonnegative_int(row.get("by_turn"), field_path=f"{field_path}.by_turn")
        surviving = _require_numeric(
            row.get("surviving_engine_fraction"),
            field_path=f"{field_path}.surviving_engine_fraction",
        )
        if surviving < 0.0 or surviving > 1.0:
            raise _runtime_error(
                "STRESS_MODELS_V1_INVALID",
                f"{field_path}.surviving_engine_fraction must be in [0.0, 1.0]",
            )
        return {
            "op": op,
            "by_turn": by_turn,
            "surviving_engine_fraction": float(surviving),
        }

    if op == "GRAVEYARD_HATE_WINDOW":
        expected_keys = {"op", "turns", "graveyard_penalty"}
        if set(row.keys()) != expected_keys:
            raise _runtime_error(
                "STRESS_MODELS_V1_INVALID",
                f"{field_path} keys must be exactly {sorted(expected_keys)}",
            )

        turns = _normalize_turns(row.get("turns"), field_path=f"{field_path}.turns")
        penalty = _require_numeric(
            row.get("graveyard_penalty"),
            field_path=f"{field_path}.graveyard_penalty",
        )
        if penalty < 0.0 or penalty > 1.0:
            raise _runtime_error(
                "STRESS_MODELS_V1_INVALID",
                f"{field_path}.graveyard_penalty must be in [0.0, 1.0]",
            )
        return {
            "op": op,
            "turns": turns,
            "graveyard_penalty": float(penalty),
        }

    expected_keys = {"op", "by_turn", "inflation_factor"}
    if set(row.keys()) != expected_keys:
        raise _runtime_error(
            "STRESS_MODELS_V1_INVALID",
            f"{field_path} keys must be exactly {sorted(expected_keys)}",
        )

    by_turn = _require_nonnegative_int(row.get("by_turn"), field_path=f"{field_path}.by_turn")
    inflation = _require_numeric(
        row.get("inflation_factor"),
        field_path=f"{field_path}.inflation_factor",
    )
    if inflation < 0.0:
        raise _runtime_error(
            "STRESS_MODELS_V1_INVALID",
            f"{field_path}.inflation_factor must be >= 0.0",
        )

    return {
        "op": op,
        "by_turn": by_turn,
        "inflation_factor": float(inflation),
    }


def _normalize_operators(raw: Any, *, field_path: str) -> List[Dict[str, Any]]:
    if not isinstance(raw, list):
        raise _runtime_error("STRESS_MODELS_V1_INVALID", f"{field_path} must be an array")

    operators: List[Dict[str, Any]] = [
        _normalize_operator(row, field_path=f"{field_path}[{index}]")
        for index, row in enumerate(raw)
    ]

    if len(operators) == 0:
        raise _runtime_error("STRESS_MODELS_V1_INVALID", f"{field_path} must be non-empty")

    return sorted(
        operators,
        key=lambda entry: (
            str(entry.get("op") or ""),
            json.dumps(entry, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
        ),
    )


def _normalize_models(raw: Any, *, field_path: str) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise _runtime_error("STRESS_MODELS_V1_INVALID", f"{field_path} must be an object")
    if len(raw) == 0:
        raise _runtime_error("STRESS_MODELS_V1_INVALID", f"{field_path} must be non-empty")

    normalized: Dict[str, Any] = {}
    for model_id_raw in sorted(raw.keys(), key=lambda item: str(item)):
        model_id = _nonempty_str(model_id_raw)
        if model_id is None:
            raise _runtime_error(
                "STRESS_MODELS_V1_INVALID",
                f"{field_path} keys must be non-empty strings",
            )

        model_payload = raw.get(model_id_raw)
        if not isinstance(model_payload, dict):
            raise _runtime_error(
                "STRESS_MODELS_V1_INVALID",
                f"{field_path}.{model_id} must be an object",
            )

        normalized[model_id] = {
            "operators": _normalize_operators(
                model_payload.get("operators"),
                field_path=f"{field_path}.{model_id}.operators",
            )
        }

    return normalized


def _normalize_selector_map(raw: Any, *, field_path: str, valid_model_ids: set[str]) -> Dict[str, str]:
    if not isinstance(raw, dict):
        raise _runtime_error("STRESS_MODELS_V1_INVALID", f"{field_path} must be an object")

    normalized: Dict[str, str] = {}
    for selector_key_raw in sorted(raw.keys(), key=lambda item: str(item)):
        selector_key = _nonempty_str(selector_key_raw)
        if selector_key is None:
            raise _runtime_error(
                "STRESS_MODELS_V1_INVALID",
                f"{field_path} keys must be non-empty strings",
            )

        model_id = _nonempty_str(raw.get(selector_key_raw))
        if model_id is None:
            raise _runtime_error(
                "STRESS_MODELS_V1_INVALID",
                f"{field_path}.{selector_key} must map to a non-empty model_id",
            )
        if model_id not in valid_model_ids:
            raise _runtime_error(
                "STRESS_MODELS_V1_INVALID",
                f"{field_path}.{selector_key} references unknown model_id {model_id}",
            )

        normalized[selector_key] = model_id

    return normalized


def _normalize_profile_bracket_rows(raw: Any, *, field_path: str, valid_model_ids: set[str]) -> List[Dict[str, str]]:
    if not isinstance(raw, list):
        raise _runtime_error("STRESS_MODELS_V1_INVALID", f"{field_path} must be an array")

    rows: List[Dict[str, str]] = []
    seen_pairs: set[tuple[str, str]] = set()

    for index, row in enumerate(raw):
        if not isinstance(row, dict):
            raise _runtime_error("STRESS_MODELS_V1_INVALID", f"{field_path}[{index}] must be an object")

        expected_keys = {"profile_id", "bracket_id", "model_id"}
        if set(row.keys()) != expected_keys:
            raise _runtime_error(
                "STRESS_MODELS_V1_INVALID",
                f"{field_path}[{index}] keys must be exactly {sorted(expected_keys)}",
            )

        profile_id = _nonempty_str(row.get("profile_id"))
        bracket_id = _nonempty_str(row.get("bracket_id"))
        model_id = _nonempty_str(row.get("model_id"))

        if profile_id is None or bracket_id is None or model_id is None:
            raise _runtime_error(
                "STRESS_MODELS_V1_INVALID",
                f"{field_path}[{index}] profile_id/bracket_id/model_id must be non-empty strings",
            )

        if model_id not in valid_model_ids:
            raise _runtime_error(
                "STRESS_MODELS_V1_INVALID",
                f"{field_path}[{index}] references unknown model_id {model_id}",
            )

        pair = (profile_id, bracket_id)
        if pair in seen_pairs:
            raise _runtime_error(
                "STRESS_MODELS_V1_INVALID",
                f"{field_path}[{index}] duplicate profile_id/bracket_id pair {pair}",
            )
        seen_pairs.add(pair)

        rows.append(
            {
                "profile_id": profile_id,
                "bracket_id": bracket_id,
                "model_id": model_id,
            }
        )

    return sorted(
        rows,
        key=lambda entry: (
            str(entry.get("profile_id") or ""),
            str(entry.get("bracket_id") or ""),
            str(entry.get("model_id") or ""),
        ),
    )


def _normalize_selection(raw: Any, *, field_path: str, valid_model_ids: set[str]) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise _runtime_error("STRESS_MODELS_V1_INVALID", f"{field_path} must be an object")

    default_model_id_raw = raw.get("default_model_id")
    default_model_id = None
    if default_model_id_raw is not None:
        default_model_id = _nonempty_str(default_model_id_raw)
        if default_model_id is None:
            raise _runtime_error(
                "STRESS_MODELS_V1_INVALID",
                f"{field_path}.default_model_id must be a non-empty string or null",
            )
        if default_model_id not in valid_model_ids:
            raise _runtime_error(
                "STRESS_MODELS_V1_INVALID",
                f"{field_path}.default_model_id references unknown model_id {default_model_id}",
            )

    by_profile_id = _normalize_selector_map(
        raw.get("by_profile_id"),
        field_path=f"{field_path}.by_profile_id",
        valid_model_ids=valid_model_ids,
    )
    by_bracket_id = _normalize_selector_map(
        raw.get("by_bracket_id"),
        field_path=f"{field_path}.by_bracket_id",
        valid_model_ids=valid_model_ids,
    )
    by_profile_bracket = _normalize_profile_bracket_rows(
        raw.get("by_profile_bracket"),
        field_path=f"{field_path}.by_profile_bracket",
        valid_model_ids=valid_model_ids,
    )

    expected_keys = {
        "default_model_id",
        "by_profile_id",
        "by_bracket_id",
        "by_profile_bracket",
    }
    if set(raw.keys()) != expected_keys:
        raise _runtime_error(
            "STRESS_MODELS_V1_INVALID",
            f"{field_path} keys must be exactly {sorted(expected_keys)}",
        )

    return {
        "default_model_id": default_model_id,
        "by_profile_id": by_profile_id,
        "by_bracket_id": by_bracket_id,
        "by_profile_bracket": by_profile_bracket,
    }


def _normalize_format_defaults(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise _runtime_error("STRESS_MODELS_V1_INVALID", "format_defaults must be an object")

    normalized: Dict[str, Any] = {}
    for format_key_raw in sorted(raw.keys(), key=lambda item: str(item)):
        format_key = _nonempty_str(format_key_raw)
        if format_key is None:
            raise _runtime_error("STRESS_MODELS_V1_INVALID", "format_defaults keys must be non-empty strings")

        format_payload = raw.get(format_key_raw)
        if not isinstance(format_payload, dict):
            raise _runtime_error("STRESS_MODELS_V1_INVALID", f"format_defaults.{format_key} must be an object")

        expected_keys = {"selection", "models"}
        if set(format_payload.keys()) != expected_keys:
            raise _runtime_error(
                "STRESS_MODELS_V1_INVALID",
                f"format_defaults.{format_key} keys must be exactly {sorted(expected_keys)}",
            )

        models = _normalize_models(
            format_payload.get("models"),
            field_path=f"format_defaults.{format_key}.models",
        )
        model_ids = set(models.keys())

        selection = _normalize_selection(
            format_payload.get("selection"),
            field_path=f"format_defaults.{format_key}.selection",
            valid_model_ids=model_ids,
        )

        normalized[format_key] = {
            "selection": selection,
            "models": models,
        }

    return normalized


def load_stress_models_v1() -> Dict[str, Any]:
    if not _STRESS_MODELS_FILE.is_file():
        raise _runtime_error("STRESS_MODELS_V1_MISSING", str(_STRESS_MODELS_FILE))

    try:
        parsed = json.loads(_STRESS_MODELS_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        raise _runtime_error("STRESS_MODELS_V1_INVALID_JSON", str(_STRESS_MODELS_FILE)) from exc

    if not isinstance(parsed, dict):
        raise _runtime_error("STRESS_MODELS_V1_INVALID", "root must be an object")

    version = _nonempty_str(parsed.get("version"))
    if version is None:
        raise _runtime_error("STRESS_MODELS_V1_INVALID", "version must be a non-empty string")

    return {
        "version": version,
        "format_defaults": _normalize_format_defaults(parsed.get("format_defaults")),
    }
