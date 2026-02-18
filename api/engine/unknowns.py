from typing import Any, Dict, List


def add_unknown(
    unknowns: List[Dict[str, Any]],
    code: str,
    input_value: str,
    message: str,
    reason: str,
    suggestions=None,
) -> None:
    if suggestions is None:
        suggestions = []
    unknowns.append(
        {
            "code": code,
            "input": input_value,
            "message": message,
            "reason": reason,
            "suggestions": suggestions,
        }
    )


def sort_unknowns(unknowns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    unknowns.sort(key=lambda u: (u.get("code", ""), u.get("input", "")))
    return unknowns
