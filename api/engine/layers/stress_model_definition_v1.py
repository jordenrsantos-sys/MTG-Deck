from __future__ import annotations

import json
from typing import Any, Dict, List, Set


STRESS_MODEL_DEFINITION_V1_VERSION = "stress_model_definition_v1"

_ERROR_CODES = {
    "STRESS_MODEL_SELECTED_ID_INVALID",
    "STRESS_MODEL_PAYLOAD_INVALID",
}
_WARN_CODES = {
    "STRESS_MODEL_OVERRIDE_UNKNOWN",
}


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _is_nonnegative_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and int(value) >= 0


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _operator_sort_key(entry: Dict[str, Any]) -> tuple[str, str]:
    return (
        str(entry.get("op") or ""),
        json.dumps(entry, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
    )


def _normalize_operator(row: Any) -> Dict[str, Any] | None:
    if not isinstance(row, dict):
        return None

    op = _nonempty_str(row.get("op"))
    if op is None:
        return None

    if op == "TARGETED_REMOVAL":
        count = row.get("count")
        if not _is_nonnegative_int(count):
            return None
        return {
            "op": op,
            "count": int(count),
        }

    if op == "BOARD_WIPE":
        by_turn = row.get("by_turn")
        surviving = row.get("surviving_engine_fraction")
        if not _is_nonnegative_int(by_turn):
            return None
        if not _is_number(surviving):
            return None
        surviving_float = float(surviving)
        if surviving_float < 0.0 or surviving_float > 1.0:
            return None
        return {
            "op": op,
            "by_turn": int(by_turn),
            "surviving_engine_fraction": float(surviving_float),
        }

    if op == "GRAVEYARD_HATE_WINDOW":
        turns_raw = row.get("turns")
        penalty = row.get("graveyard_penalty")
        if not isinstance(turns_raw, list):
            return None
        if not _is_number(penalty):
            return None

        turns = sorted({int(item) for item in turns_raw if _is_nonnegative_int(item)})
        if len(turns) == 0:
            return None

        penalty_float = float(penalty)
        if penalty_float < 0.0 or penalty_float > 1.0:
            return None

        return {
            "op": op,
            "turns": turns,
            "graveyard_penalty": float(penalty_float),
        }

    if op == "STAX_TAX":
        by_turn = row.get("by_turn")
        inflation = row.get("inflation_factor")
        if not _is_nonnegative_int(by_turn):
            return None
        if not _is_number(inflation):
            return None
        inflation_float = float(inflation)
        if inflation_float < 0.0:
            return None
        return {
            "op": op,
            "by_turn": int(by_turn),
            "inflation_factor": float(inflation_float),
        }

    return None


def _base_payload(
    *,
    status: str,
    reason_code: str | None,
    codes: List[str],
    stress_models_version: str | None,
    format_token: str,
    profile_id_token: str,
    bracket_id_token: str,
    request_override_model_id: str | None,
    selected_model_id: str | None,
    selection_source: str | None,
    operators: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "version": STRESS_MODEL_DEFINITION_V1_VERSION,
        "status": status,
        "reason_code": reason_code,
        "codes": sorted(set(codes)),
        "stress_models_version": stress_models_version,
        "format": format_token,
        "profile_id": profile_id_token,
        "bracket_id": bracket_id_token,
        "request_override_model_id": request_override_model_id,
        "selected_model_id": selected_model_id,
        "selection_source": selection_source,
        "operators": operators,
    }


def _resolve_selected_model_id(
    *,
    selection: Dict[str, Any],
    models: Dict[str, Any],
    profile_id_token: str,
    bracket_id_token: str,
    request_override_model_id: str | None,
    codes: Set[str],
) -> tuple[str | None, str | None]:
    if request_override_model_id is not None:
        if request_override_model_id in models:
            return request_override_model_id, "override"
        codes.add("STRESS_MODEL_OVERRIDE_UNKNOWN")

    by_profile_bracket = selection.get("by_profile_bracket")
    if isinstance(by_profile_bracket, list):
        for row in by_profile_bracket:
            if not isinstance(row, dict):
                continue
            if _nonempty_str(row.get("profile_id")) != profile_id_token:
                continue
            if _nonempty_str(row.get("bracket_id")) != bracket_id_token:
                continue
            model_id = _nonempty_str(row.get("model_id"))
            if model_id is None:
                codes.add("STRESS_MODEL_PAYLOAD_INVALID")
                return None, None
            return model_id, "profile_bracket"

    by_profile_id = selection.get("by_profile_id")
    if isinstance(by_profile_id, dict):
        profile_model_id = _nonempty_str(by_profile_id.get(profile_id_token))
        if profile_model_id is not None:
            return profile_model_id, "profile"

    by_bracket_id = selection.get("by_bracket_id")
    if isinstance(by_bracket_id, dict):
        bracket_model_id = _nonempty_str(by_bracket_id.get(bracket_id_token))
        if bracket_model_id is not None:
            return bracket_model_id, "bracket"

    default_model_id = _nonempty_str(selection.get("default_model_id"))
    if default_model_id is not None:
        return default_model_id, "default"

    return None, None


def run_stress_model_definition_v1(
    *,
    format: Any,
    bracket_id: Any,
    profile_id: Any,
    request_override_model_id: Any,
    stress_models_payload: Any,
) -> Dict[str, Any]:
    format_token = _nonempty_str(format) or ""
    profile_id_token = _nonempty_str(profile_id) or ""
    bracket_id_token = _nonempty_str(bracket_id) or ""
    request_override_token = _nonempty_str(request_override_model_id)

    if not isinstance(stress_models_payload, dict):
        return _base_payload(
            status="SKIP",
            reason_code="STRESS_MODELS_UNAVAILABLE",
            codes=[],
            stress_models_version=None,
            format_token=format_token,
            profile_id_token=profile_id_token,
            bracket_id_token=bracket_id_token,
            request_override_model_id=request_override_token,
            selected_model_id=None,
            selection_source=None,
            operators=[],
        )

    stress_models_version = _nonempty_str(stress_models_payload.get("version"))
    format_defaults = stress_models_payload.get("format_defaults")
    if not isinstance(format_defaults, dict):
        return _base_payload(
            status="ERROR",
            reason_code=None,
            codes=["STRESS_MODEL_PAYLOAD_INVALID"],
            stress_models_version=stress_models_version,
            format_token=format_token,
            profile_id_token=profile_id_token,
            bracket_id_token=bracket_id_token,
            request_override_model_id=request_override_token,
            selected_model_id=None,
            selection_source=None,
            operators=[],
        )

    format_entry = format_defaults.get(format_token)
    if not isinstance(format_entry, dict):
        format_entry = format_defaults.get(format_token.lower()) if isinstance(format_token, str) else None

    if not isinstance(format_entry, dict):
        return _base_payload(
            status="SKIP",
            reason_code="FORMAT_STRESS_MODELING_UNAVAILABLE",
            codes=[],
            stress_models_version=stress_models_version,
            format_token=format_token,
            profile_id_token=profile_id_token,
            bracket_id_token=bracket_id_token,
            request_override_model_id=request_override_token,
            selected_model_id=None,
            selection_source=None,
            operators=[],
        )

    selection = format_entry.get("selection")
    models = format_entry.get("models")
    if not isinstance(selection, dict) or not isinstance(models, dict):
        return _base_payload(
            status="ERROR",
            reason_code=None,
            codes=["STRESS_MODEL_PAYLOAD_INVALID"],
            stress_models_version=stress_models_version,
            format_token=format_token,
            profile_id_token=profile_id_token,
            bracket_id_token=bracket_id_token,
            request_override_model_id=request_override_token,
            selected_model_id=None,
            selection_source=None,
            operators=[],
        )

    codes: Set[str] = set()
    selected_model_id, selection_source = _resolve_selected_model_id(
        selection=selection,
        models=models,
        profile_id_token=profile_id_token,
        bracket_id_token=bracket_id_token,
        request_override_model_id=request_override_token,
        codes=codes,
    )

    if selected_model_id is None:
        if len(codes.intersection(_ERROR_CODES)) > 0:
            return _base_payload(
                status="ERROR",
                reason_code=None,
                codes=sorted(codes),
                stress_models_version=stress_models_version,
                format_token=format_token,
                profile_id_token=profile_id_token,
                bracket_id_token=bracket_id_token,
                request_override_model_id=request_override_token,
                selected_model_id=None,
                selection_source=None,
                operators=[],
            )
        if len(codes.intersection(_WARN_CODES)) > 0:
            return _base_payload(
                status="SKIP",
                reason_code="STRESS_MODEL_SELECTION_UNAVAILABLE",
                codes=sorted(codes),
                stress_models_version=stress_models_version,
                format_token=format_token,
                profile_id_token=profile_id_token,
                bracket_id_token=bracket_id_token,
                request_override_model_id=request_override_token,
                selected_model_id=None,
                selection_source=None,
                operators=[],
            )
        return _base_payload(
            status="SKIP",
            reason_code="STRESS_MODEL_SELECTION_UNAVAILABLE",
            codes=[],
            stress_models_version=stress_models_version,
            format_token=format_token,
            profile_id_token=profile_id_token,
            bracket_id_token=bracket_id_token,
            request_override_model_id=request_override_token,
            selected_model_id=None,
            selection_source=None,
            operators=[],
        )

    model_payload = models.get(selected_model_id)
    if not isinstance(model_payload, dict):
        codes.add("STRESS_MODEL_SELECTED_ID_INVALID")
        return _base_payload(
            status="ERROR",
            reason_code=None,
            codes=sorted(codes),
            stress_models_version=stress_models_version,
            format_token=format_token,
            profile_id_token=profile_id_token,
            bracket_id_token=bracket_id_token,
            request_override_model_id=request_override_token,
            selected_model_id=selected_model_id,
            selection_source=selection_source,
            operators=[],
        )

    operators_raw = model_payload.get("operators")
    if not isinstance(operators_raw, list):
        codes.add("STRESS_MODEL_PAYLOAD_INVALID")
        return _base_payload(
            status="ERROR",
            reason_code=None,
            codes=sorted(codes),
            stress_models_version=stress_models_version,
            format_token=format_token,
            profile_id_token=profile_id_token,
            bracket_id_token=bracket_id_token,
            request_override_model_id=request_override_token,
            selected_model_id=selected_model_id,
            selection_source=selection_source,
            operators=[],
        )

    normalized_operators: List[Dict[str, Any]] = []
    for row in operators_raw:
        normalized = _normalize_operator(row)
        if normalized is None:
            codes.add("STRESS_MODEL_PAYLOAD_INVALID")
            continue
        normalized_operators.append(normalized)

    normalized_operators = sorted(normalized_operators, key=_operator_sort_key)

    if len(codes.intersection(_ERROR_CODES)) > 0:
        return _base_payload(
            status="ERROR",
            reason_code=None,
            codes=sorted(codes),
            stress_models_version=stress_models_version,
            format_token=format_token,
            profile_id_token=profile_id_token,
            bracket_id_token=bracket_id_token,
            request_override_model_id=request_override_token,
            selected_model_id=selected_model_id,
            selection_source=selection_source,
            operators=normalized_operators,
        )

    if len(codes.intersection(_WARN_CODES)) > 0:
        return _base_payload(
            status="WARN",
            reason_code=None,
            codes=sorted(codes),
            stress_models_version=stress_models_version,
            format_token=format_token,
            profile_id_token=profile_id_token,
            bracket_id_token=bracket_id_token,
            request_override_model_id=request_override_token,
            selected_model_id=selected_model_id,
            selection_source=selection_source,
            operators=normalized_operators,
        )

    return _base_payload(
        status="OK",
        reason_code=None,
        codes=[],
        stress_models_version=stress_models_version,
        format_token=format_token,
        profile_id_token=profile_id_token,
        bracket_id_token=bracket_id_token,
        request_override_model_id=request_override_token,
        selected_model_id=selected_model_id,
        selection_source=selection_source,
        operators=normalized_operators,
    )
