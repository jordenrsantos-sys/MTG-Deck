from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Tuple

_BRACKET_RULES_FILE = Path(__file__).resolve().parent / "data" / "brackets" / "bracket_rules_v2.json"
_POLICY_ENUM = ("ALLOW", "DISALLOW", "TRACK_ONLY")
_TRACKED_CATEGORIES = ("mass_land_denial", "extra_turn_chains", "two_card_combos")
_TRACKED_BRACKETS = ("B1", "B2", "B3", "B4", "B5")


def _runtime_error(code: str, detail: str) -> RuntimeError:
    return RuntimeError(f"{code}: {detail}")


def _normalize_policy(value: Any, *, field_path: str) -> str:
    if not isinstance(value, str):
        raise _runtime_error("BRACKET_RULES_V2_INVALID", f"{field_path} must be a string")

    token = value.strip()
    if token not in _POLICY_ENUM:
        raise _runtime_error("BRACKET_RULES_V2_INVALID", f"{field_path} has unsupported policy '{token}'")
    return token


def _normalize_bracket_rules(raw: Any, *, bracket_id: str) -> Dict[str, str]:
    if not isinstance(raw, dict):
        raise _runtime_error("BRACKET_RULES_V2_INVALID", f"brackets.{bracket_id} must be an object")

    category_keys = tuple(sorted(raw.keys()))
    if category_keys != tuple(sorted(_TRACKED_CATEGORIES)):
        raise _runtime_error(
            "BRACKET_RULES_V2_INVALID",
            f"brackets.{bracket_id} must contain exactly {list(_TRACKED_CATEGORIES)}",
        )

    return {
        category: _normalize_policy(raw.get(category), field_path=f"brackets.{bracket_id}.{category}")
        for category in _TRACKED_CATEGORIES
    }


def load_bracket_rules_v2() -> dict:
    if not _BRACKET_RULES_FILE.is_file():
        raise _runtime_error("BRACKET_RULES_V2_MISSING", str(_BRACKET_RULES_FILE))

    try:
        parsed = json.loads(_BRACKET_RULES_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        raise _runtime_error("BRACKET_RULES_V2_INVALID_JSON", str(_BRACKET_RULES_FILE)) from exc

    if not isinstance(parsed, dict):
        raise _runtime_error("BRACKET_RULES_V2_INVALID", "root must be an object")

    version_raw = parsed.get("version")
    if not isinstance(version_raw, str) or version_raw.strip() == "":
        raise _runtime_error("BRACKET_RULES_V2_INVALID", "version must be a non-empty string")
    version = version_raw.strip()

    policy_enum_raw = parsed.get("policy_enum")
    if not isinstance(policy_enum_raw, list):
        raise _runtime_error("BRACKET_RULES_V2_INVALID", "policy_enum must be a list")
    policy_enum_clean = [item.strip() for item in policy_enum_raw if isinstance(item, str)]
    if tuple(policy_enum_clean) != _POLICY_ENUM:
        raise _runtime_error("BRACKET_RULES_V2_INVALID", f"policy_enum must equal {list(_POLICY_ENUM)}")

    brackets_raw = parsed.get("brackets")
    if not isinstance(brackets_raw, dict):
        raise _runtime_error("BRACKET_RULES_V2_INVALID", "brackets must be an object")

    bracket_keys = tuple(sorted(brackets_raw.keys()))
    if bracket_keys != tuple(sorted(_TRACKED_BRACKETS)):
        raise _runtime_error(
            "BRACKET_RULES_V2_INVALID",
            f"brackets must contain exactly {list(_TRACKED_BRACKETS)}",
        )

    brackets: Dict[str, Dict[str, str]] = {
        bracket_id: _normalize_bracket_rules(brackets_raw.get(bracket_id), bracket_id=bracket_id)
        for bracket_id in _TRACKED_BRACKETS
    }

    return {
        "version": version,
        "policy_enum": list(_POLICY_ENUM),
        "brackets": brackets,
    }


def resolve_bracket_rules_v2(bracket_id: str) -> Tuple[Dict[str, str], str, bool]:
    payload = load_bracket_rules_v2()

    version = payload["version"]
    brackets = payload["brackets"]
    bracket_token = bracket_id.strip() if isinstance(bracket_id, str) else ""

    bracket_rules = brackets.get(bracket_token)
    if isinstance(bracket_rules, dict):
        return {category: bracket_rules.get(category) for category in _TRACKED_CATEGORIES}, version, False

    return {}, version, True
