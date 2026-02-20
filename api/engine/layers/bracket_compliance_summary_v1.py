from __future__ import annotations

from typing import Any, Dict, List


BRACKET_COMPLIANCE_SUMMARY_V1_VERSION = "bracket_compliance_summary_v1"


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _nonnegative_int(value: Any) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return int(value)
    return None


def _normalize_violations(raw: Any) -> List[Dict[str, str]]:
    violations: List[Dict[str, str]] = []

    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            violations.append(
                {
                    "code": _nonempty_str(item.get("code")) or "",
                    "category": _nonempty_str(item.get("category")) or "",
                    "message": _nonempty_str(item.get("message")) or "",
                }
            )

    return sorted(
        violations,
        key=lambda entry: (
            str(entry.get("code") or ""),
            str(entry.get("category") or ""),
            str(entry.get("message") or ""),
        ),
    )


def _normalize_unknowns(raw: Any) -> List[Dict[str, str]]:
    unknowns: List[Dict[str, str]] = []

    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            unknowns.append(
                {
                    "code": _nonempty_str(item.get("code")) or "",
                    "message": _nonempty_str(item.get("message")) or "",
                }
            )

    return sorted(
        unknowns,
        key=lambda entry: (
            str(entry.get("code") or ""),
            str(entry.get("message") or ""),
        ),
    )


def _extract_category_count(category_results: Any, category: str) -> int | None:
    if not isinstance(category_results, dict):
        return None

    entry = category_results.get(category)
    if not isinstance(entry, dict):
        return None

    return _nonnegative_int(entry.get("count"))


def _resolve_flags(category_results: Any) -> List[Dict[str, str]]:
    flags: List[Dict[str, str]] = []

    if isinstance(category_results, dict):
        two_card_entry = category_results.get("two_card_combos")
        if isinstance(two_card_entry, dict):
            policy = _nonempty_str(two_card_entry.get("policy")) or ""
            count = _nonnegative_int(two_card_entry.get("count"))
            if policy == "TRACK_ONLY" and isinstance(count, int) and count > 0:
                flags.append(
                    {
                        "code": "TWO_CARD_COMBOS_PRESENT_TRACK_ONLY",
                        "category": "two_card_combos",
                        "message": "Two-card combos are present and tracked by policy (not a violation).",
                    }
                )

    return sorted(
        flags,
        key=lambda entry: (
            str(entry.get("code") or ""),
            str(entry.get("category") or ""),
            str(entry.get("message") or ""),
        ),
    )


def _status_for(violations: List[Dict[str, str]], unknowns: List[Dict[str, str]]) -> str:
    if len(violations) > 0:
        return "ERROR"
    if len(unknowns) > 0:
        return "WARN"
    return "OK"


def run_bracket_compliance_summary_v1(enforcement_payload: dict | None) -> dict:
    if not isinstance(enforcement_payload, dict):
        return {
            "version": BRACKET_COMPLIANCE_SUMMARY_V1_VERSION,
            "status": "SKIP",
            "bracket_id": None,
            "counts": {
                "game_changers": None,
                "mass_land_denial": None,
                "extra_turns": None,
                "two_card_combos": None,
            },
            "violations": [],
            "flags": [],
            "unknowns": [],
            "versions": {
                "gc_limits_version": None,
                "bracket_rules_version": None,
                "two_card_combos_version": None,
            },
        }

    counts_raw = enforcement_payload.get("counts") if isinstance(enforcement_payload.get("counts"), dict) else {}
    category_results = (
        enforcement_payload.get("category_results") if isinstance(enforcement_payload.get("category_results"), dict) else {}
    )

    violations = _normalize_violations(enforcement_payload.get("violations"))
    unknowns = _normalize_unknowns(enforcement_payload.get("unknowns"))
    flags = _resolve_flags(category_results)

    status = _status_for(violations, unknowns)

    return {
        "version": BRACKET_COMPLIANCE_SUMMARY_V1_VERSION,
        "status": status,
        "bracket_id": _nonempty_str(enforcement_payload.get("bracket_id")),
        "counts": {
            "game_changers": _nonnegative_int(counts_raw.get("game_changers_in_deck")),
            "mass_land_denial": _extract_category_count(category_results, "mass_land_denial"),
            "extra_turns": _extract_category_count(category_results, "extra_turn_chains"),
            "two_card_combos": _extract_category_count(category_results, "two_card_combos"),
        },
        "violations": violations,
        "flags": flags,
        "unknowns": unknowns,
        "versions": {
            "gc_limits_version": _nonempty_str(enforcement_payload.get("gc_limits_version")),
            "bracket_rules_version": _nonempty_str(enforcement_payload.get("bracket_rules_version")),
            "two_card_combos_version": _nonempty_str(enforcement_payload.get("two_card_combos_version")),
        },
    }
