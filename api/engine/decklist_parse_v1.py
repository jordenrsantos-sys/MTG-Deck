from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List


DECKLIST_PARSE_VERSION = "decklist_parse_v1"

_SECTION_HEADER_MAP = {
    "commander": "commander",
    "deck": "mainboard",
    "mainboard": "mainboard",
    "sideboard": "sideboard",
    "maybeboard": "maybeboard",
}

_COUNT_WITH_X_RE = re.compile(r"^(?P<count>\d+)\s*[xX]\s+(?P<name>.+)$")
_COUNT_WITH_SPACE_RE = re.compile(r"^(?P<count>\d+)\s+(?P<name>.+)$")


def _collapse_whitespace(value: str) -> str:
    return " ".join(value.strip().split())


def normalize_decklist_name(value: Any) -> str | None:
    if not isinstance(value, str):
        return None

    token = _collapse_whitespace(value)
    if token == "":
        return None

    return token.casefold()


def _line_is_comment(line: str) -> bool:
    return line.startswith("#") or line.startswith("//")


def _resolve_section_header(line: str) -> str | None:
    token = _collapse_whitespace(line)
    if token.endswith(":"):
        token = token[:-1].strip()
    if token == "":
        return None

    return _SECTION_HEADER_MAP.get(token.casefold())


def _parse_count_and_name(line: str) -> tuple[int, str]:
    for pattern in (_COUNT_WITH_X_RE, _COUNT_WITH_SPACE_RE):
        match = pattern.match(line)
        if match is None:
            continue

        count_raw = match.group("count")
        name_raw = match.group("name").strip()
        if name_raw == "":
            continue

        try:
            parsed_count = int(count_raw)
        except Exception:
            continue

        if parsed_count >= 1:
            return parsed_count, name_raw

    return 1, line.strip()


def parse_decklist_text(raw: str) -> Dict[str, Any]:
    raw_text = raw if isinstance(raw, str) else ""
    lines = raw_text.splitlines()

    current_section = "mainboard"
    items: List[Dict[str, Any]] = []
    ignored_line_total = 0

    for line_no, line in enumerate(lines, start=1):
        stripped = line.strip()
        if stripped == "":
            ignored_line_total += 1
            continue

        if _line_is_comment(stripped):
            ignored_line_total += 1
            continue

        section = _resolve_section_header(stripped)
        if section is not None:
            current_section = section
            ignored_line_total += 1
            continue

        count, name_raw = _parse_count_and_name(stripped)
        name_norm = normalize_decklist_name(name_raw)
        if name_norm is None:
            ignored_line_total += 1
            continue

        items.append(
            {
                "count": int(count),
                "name_raw": name_raw,
                "name_norm": name_norm,
                "line_no": int(line_no),
                "section": current_section,
            }
        )

    normalized_items = [
        {
            "count": int(item["count"]),
            "name_norm": item["name_norm"],
            "section": item["section"],
        }
        for item in items
    ]

    normalized_representation = "\n".join(
        f"{row['section']}|{row['count']}|{row['name_norm']}"
        for row in normalized_items
    )
    normalized_sha256 = hashlib.sha256(normalized_representation.encode("utf-8")).hexdigest()

    return {
        "version": DECKLIST_PARSE_VERSION,
        "items": items,
        "totals": {
            "items_total": len(items),
            "card_count_total": sum(int(item["count"]) for item in items),
            "ignored_line_total": int(ignored_line_total),
        },
        "normalized_items": normalized_items,
        "normalized_representation": normalized_representation,
        "normalized_sha256": normalized_sha256,
    }
