from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Tuple


_PROFILE_THRESHOLDS_FILE = (
    Path(__file__).resolve().parent
    / "data"
    / "sufficiency"
    / "profile_thresholds_v1.json"
)

_REQUIRED_DOMAIN_KEYS = (
    "required_effects",
    "baseline_prob",
    "stress_prob",
    "coherence",
    "resilience",
    "commander",
)


def _runtime_error(code: str, detail: str) -> RuntimeError:
    return RuntimeError(f"{code}: {detail}")


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _require_probability(value: Any, *, field_path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _runtime_error("PROFILE_THRESHOLDS_V1_INVALID", f"{field_path} must be numeric")
    token = float(value)
    if token < 0.0 or token > 1.0:
        raise _runtime_error("PROFILE_THRESHOLDS_V1_INVALID", f"{field_path} must be in [0.0, 1.0]")
    return float(token)


def _require_nonnegative_int(value: Any, *, field_path: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or int(value) < 0:
        raise _runtime_error("PROFILE_THRESHOLDS_V1_INVALID", f"{field_path} must be a non-negative integer")
    return int(value)


def _normalize_required_effects(raw: Any, *, field_path: str) -> Dict[str, int]:
    if not isinstance(raw, dict):
        raise _runtime_error("PROFILE_THRESHOLDS_V1_INVALID", f"{field_path} must be an object")

    expected = {"max_missing", "max_unknowns"}
    if set(raw.keys()) != expected:
        raise _runtime_error(
            "PROFILE_THRESHOLDS_V1_INVALID",
            f"{field_path} keys must be exactly {sorted(expected)}",
        )

    return {
        "max_missing": _require_nonnegative_int(raw.get("max_missing"), field_path=f"{field_path}.max_missing"),
        "max_unknowns": _require_nonnegative_int(raw.get("max_unknowns"), field_path=f"{field_path}.max_unknowns"),
    }


def _normalize_baseline_prob(raw: Any, *, field_path: str) -> Dict[str, float]:
    if not isinstance(raw, dict):
        raise _runtime_error("PROFILE_THRESHOLDS_V1_INVALID", f"{field_path} must be an object")

    expected = {
        "cast_reliability_t3_min",
        "cast_reliability_t4_min",
        "cast_reliability_t6_min",
    }
    if set(raw.keys()) != expected:
        raise _runtime_error(
            "PROFILE_THRESHOLDS_V1_INVALID",
            f"{field_path} keys must be exactly {sorted(expected)}",
        )

    return {
        "cast_reliability_t3_min": _require_probability(
            raw.get("cast_reliability_t3_min"),
            field_path=f"{field_path}.cast_reliability_t3_min",
        ),
        "cast_reliability_t4_min": _require_probability(
            raw.get("cast_reliability_t4_min"),
            field_path=f"{field_path}.cast_reliability_t4_min",
        ),
        "cast_reliability_t6_min": _require_probability(
            raw.get("cast_reliability_t6_min"),
            field_path=f"{field_path}.cast_reliability_t6_min",
        ),
    }


def _normalize_stress_prob(raw: Any, *, field_path: str) -> Dict[str, float]:
    if not isinstance(raw, dict):
        raise _runtime_error("PROFILE_THRESHOLDS_V1_INVALID", f"{field_path} must be an object")

    expected = {
        "engine_continuity_after_removal_min",
        "rebuild_after_wipe_min",
        "graveyard_fragility_delta_max",
    }
    if set(raw.keys()) != expected:
        raise _runtime_error(
            "PROFILE_THRESHOLDS_V1_INVALID",
            f"{field_path} keys must be exactly {sorted(expected)}",
        )

    return {
        "engine_continuity_after_removal_min": _require_probability(
            raw.get("engine_continuity_after_removal_min"),
            field_path=f"{field_path}.engine_continuity_after_removal_min",
        ),
        "rebuild_after_wipe_min": _require_probability(
            raw.get("rebuild_after_wipe_min"),
            field_path=f"{field_path}.rebuild_after_wipe_min",
        ),
        "graveyard_fragility_delta_max": _require_probability(
            raw.get("graveyard_fragility_delta_max"),
            field_path=f"{field_path}.graveyard_fragility_delta_max",
        ),
    }


def _normalize_coherence(raw: Any, *, field_path: str) -> Dict[str, float]:
    if not isinstance(raw, dict):
        raise _runtime_error("PROFILE_THRESHOLDS_V1_INVALID", f"{field_path} must be an object")

    required = {
        "dead_slot_ratio_max",
        "overlap_score_min",
    }
    optional = {
        "bridge_amplification_bonus_v1",
    }
    keys = set(raw.keys())
    if not required.issubset(keys):
        raise _runtime_error(
            "PROFILE_THRESHOLDS_V1_INVALID",
            f"{field_path} missing required keys: {sorted(required - keys)}",
        )
    unknown_keys = keys - required - optional
    if len(unknown_keys) > 0:
        raise _runtime_error(
            "PROFILE_THRESHOLDS_V1_INVALID",
            f"{field_path} has unknown keys: {sorted(unknown_keys)}",
        )

    bridge_bonus = raw.get("bridge_amplification_bonus_v1", 0.0)

    return {
        "dead_slot_ratio_max": _require_probability(
            raw.get("dead_slot_ratio_max"),
            field_path=f"{field_path}.dead_slot_ratio_max",
        ),
        "overlap_score_min": _require_probability(
            raw.get("overlap_score_min"),
            field_path=f"{field_path}.overlap_score_min",
        ),
        "bridge_amplification_bonus_v1": _require_probability(
            bridge_bonus,
            field_path=f"{field_path}.bridge_amplification_bonus_v1",
        ),
    }


def _normalize_resilience(raw: Any, *, field_path: str) -> Dict[str, float]:
    if not isinstance(raw, dict):
        raise _runtime_error("PROFILE_THRESHOLDS_V1_INVALID", f"{field_path} must be an object")

    expected = {
        "commander_fragility_delta_max",
    }
    if set(raw.keys()) != expected:
        raise _runtime_error(
            "PROFILE_THRESHOLDS_V1_INVALID",
            f"{field_path} keys must be exactly {sorted(expected)}",
        )

    return {
        "commander_fragility_delta_max": _require_probability(
            raw.get("commander_fragility_delta_max"),
            field_path=f"{field_path}.commander_fragility_delta_max",
        ),
    }


def _normalize_commander(raw: Any, *, field_path: str) -> Dict[str, float]:
    if not isinstance(raw, dict):
        raise _runtime_error("PROFILE_THRESHOLDS_V1_INVALID", f"{field_path} must be an object")

    expected = {
        "protection_coverage_proxy_min",
        "commander_fragility_delta_max",
    }
    if set(raw.keys()) != expected:
        raise _runtime_error(
            "PROFILE_THRESHOLDS_V1_INVALID",
            f"{field_path} keys must be exactly {sorted(expected)}",
        )

    return {
        "protection_coverage_proxy_min": _require_probability(
            raw.get("protection_coverage_proxy_min"),
            field_path=f"{field_path}.protection_coverage_proxy_min",
        ),
        "commander_fragility_delta_max": _require_probability(
            raw.get("commander_fragility_delta_max"),
            field_path=f"{field_path}.commander_fragility_delta_max",
        ),
    }


def _normalize_domains(raw: Any, *, field_path: str) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise _runtime_error("PROFILE_THRESHOLDS_V1_INVALID", f"{field_path} must be an object")

    expected = set(_REQUIRED_DOMAIN_KEYS)
    if set(raw.keys()) != expected:
        raise _runtime_error(
            "PROFILE_THRESHOLDS_V1_INVALID",
            f"{field_path} keys must be exactly {sorted(expected)}",
        )

    return {
        "required_effects": _normalize_required_effects(
            raw.get("required_effects"),
            field_path=f"{field_path}.required_effects",
        ),
        "baseline_prob": _normalize_baseline_prob(
            raw.get("baseline_prob"),
            field_path=f"{field_path}.baseline_prob",
        ),
        "stress_prob": _normalize_stress_prob(
            raw.get("stress_prob"),
            field_path=f"{field_path}.stress_prob",
        ),
        "coherence": _normalize_coherence(
            raw.get("coherence"),
            field_path=f"{field_path}.coherence",
        ),
        "resilience": _normalize_resilience(
            raw.get("resilience"),
            field_path=f"{field_path}.resilience",
        ),
        "commander": _normalize_commander(
            raw.get("commander"),
            field_path=f"{field_path}.commander",
        ),
    }


def _normalize_profiles(raw: Any, *, field_path: str) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise _runtime_error("PROFILE_THRESHOLDS_V1_INVALID", f"{field_path} must be an object")
    if len(raw) == 0:
        raise _runtime_error("PROFILE_THRESHOLDS_V1_INVALID", f"{field_path} must be non-empty")

    profiles: Dict[str, Any] = {}
    for profile_key_raw in sorted(raw.keys(), key=lambda item: str(item)):
        profile_key = _nonempty_str(profile_key_raw)
        if profile_key is None:
            raise _runtime_error("PROFILE_THRESHOLDS_V1_INVALID", f"{field_path} keys must be non-empty strings")

        profile_payload = raw.get(profile_key_raw)
        if not isinstance(profile_payload, dict):
            raise _runtime_error("PROFILE_THRESHOLDS_V1_INVALID", f"{field_path}.{profile_key} must be an object")

        expected = {"domains"}
        if set(profile_payload.keys()) != expected:
            raise _runtime_error(
                "PROFILE_THRESHOLDS_V1_INVALID",
                f"{field_path}.{profile_key} keys must be exactly {sorted(expected)}",
            )

        profiles[profile_key] = {
            "domains": _normalize_domains(
                profile_payload.get("domains"),
                field_path=f"{field_path}.{profile_key}.domains",
            )
        }

    return profiles


def _normalize_format_defaults(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise _runtime_error("PROFILE_THRESHOLDS_V1_INVALID", "format_defaults must be an object")

    format_defaults: Dict[str, Any] = {}
    for format_key_raw in sorted(raw.keys(), key=lambda item: str(item)):
        format_key = _nonempty_str(format_key_raw)
        if format_key is None:
            raise _runtime_error("PROFILE_THRESHOLDS_V1_INVALID", "format_defaults keys must be non-empty strings")

        format_payload = raw.get(format_key_raw)
        if not isinstance(format_payload, dict):
            raise _runtime_error("PROFILE_THRESHOLDS_V1_INVALID", f"format_defaults.{format_key} must be an object")

        expected = {"default_profile_id", "profiles"}
        if set(format_payload.keys()) != expected:
            raise _runtime_error(
                "PROFILE_THRESHOLDS_V1_INVALID",
                f"format_defaults.{format_key} keys must be exactly {sorted(expected)}",
            )

        profiles = _normalize_profiles(
            format_payload.get("profiles"),
            field_path=f"format_defaults.{format_key}.profiles",
        )

        default_profile_id = _nonempty_str(format_payload.get("default_profile_id"))
        if default_profile_id is None:
            raise _runtime_error(
                "PROFILE_THRESHOLDS_V1_INVALID",
                f"format_defaults.{format_key}.default_profile_id must be a non-empty string",
            )
        if default_profile_id not in profiles:
            raise _runtime_error(
                "PROFILE_THRESHOLDS_V1_INVALID",
                f"format_defaults.{format_key}.default_profile_id must reference an existing profile",
            )

        format_defaults[format_key] = {
            "default_profile_id": default_profile_id,
            "profiles": profiles,
        }

    return format_defaults


def load_profile_thresholds_v1() -> Dict[str, Any]:
    if not _PROFILE_THRESHOLDS_FILE.is_file():
        raise _runtime_error("PROFILE_THRESHOLDS_V1_MISSING", str(_PROFILE_THRESHOLDS_FILE))

    try:
        parsed = json.loads(_PROFILE_THRESHOLDS_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        raise _runtime_error("PROFILE_THRESHOLDS_V1_INVALID_JSON", str(_PROFILE_THRESHOLDS_FILE)) from exc

    if not isinstance(parsed, dict):
        raise _runtime_error("PROFILE_THRESHOLDS_V1_INVALID", "root must be an object")

    version = _nonempty_str(parsed.get("version"))
    if version is None:
        raise _runtime_error("PROFILE_THRESHOLDS_V1_INVALID", "version must be a non-empty string")

    calibration_snapshot_version = _nonempty_str(parsed.get("calibration_snapshot_version"))
    if calibration_snapshot_version is None:
        raise _runtime_error(
            "PROFILE_THRESHOLDS_V1_INVALID",
            "calibration_snapshot_version must be a non-empty string",
        )

    return {
        "version": version,
        "calibration_snapshot_version": calibration_snapshot_version,
        "format_defaults": _normalize_format_defaults(parsed.get("format_defaults")),
    }


def resolve_profile_thresholds_v1(*, format: Any, profile_id: Any) -> Tuple[Dict[str, Any], str, str]:
    payload = load_profile_thresholds_v1()

    profile_thresholds_version = payload["version"]
    calibration_snapshot_version = payload["calibration_snapshot_version"]
    format_defaults = payload["format_defaults"]

    format_token = _nonempty_str(format) or ""
    requested_profile_id = _nonempty_str(profile_id) or ""

    format_entry = format_defaults.get(format_token)
    if not isinstance(format_entry, dict):
        format_entry = format_defaults.get(format_token.lower()) if isinstance(format_token, str) else None

    if not isinstance(format_entry, dict):
        return {
            "profile_thresholds_version": profile_thresholds_version,
            "calibration_snapshot_version": calibration_snapshot_version,
            "format": format_token,
            "requested_profile_id": requested_profile_id,
            "selected_profile_id": "",
            "selection_source": "unavailable",
            "domains": {},
        }, profile_thresholds_version, calibration_snapshot_version

    profiles = format_entry.get("profiles") if isinstance(format_entry.get("profiles"), dict) else {}
    default_profile_id = _nonempty_str(format_entry.get("default_profile_id")) or ""

    selected_profile_id = ""
    selection_source = "unavailable"

    if requested_profile_id in profiles:
        selected_profile_id = requested_profile_id
        selection_source = "profile"
    elif default_profile_id in profiles:
        selected_profile_id = default_profile_id
        selection_source = "default"
    elif len(profiles) > 0:
        selected_profile_id = sorted(profiles.keys(), key=lambda item: str(item))[0]
        selection_source = "first_sorted"

    selected_domains = {}
    selected_profile = profiles.get(selected_profile_id)
    if isinstance(selected_profile, dict):
        domains = selected_profile.get("domains")
        if isinstance(domains, dict):
            selected_domains = {
                key: domains[key]
                for key in _REQUIRED_DOMAIN_KEYS
                if key in domains
            }

    return {
        "profile_thresholds_version": profile_thresholds_version,
        "calibration_snapshot_version": calibration_snapshot_version,
        "format": format_token,
        "requested_profile_id": requested_profile_id,
        "selected_profile_id": selected_profile_id,
        "selection_source": selection_source,
        "domains": selected_domains,
    }, profile_thresholds_version, calibration_snapshot_version
