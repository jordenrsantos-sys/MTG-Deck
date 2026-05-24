"""Mulligan + bottom-picker response parsers + validators.

Parses the LLM's JSON for the mulligan-decider prompt + the
bottom-picker prompt. Both return (parsed=None, error_message) on
validation failure for the responder's re-prompt loop.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from api.engine.pillar_f.v0_2.policy.parsers.action_parser import (
    _extract_first_json_object,
)


MULLIGAN_PARSER_VERSION = "pillar_f_v0_2_policy_mulligan_parser_v1"


@dataclass
class MulliganResponse:
    """Parsed mulligan-decider response.

    `keep` here matches the LLM-facing semantic (True = "keep this
    hand"). The MulliganDeciderFn factory inverts this to the
    substrate's convention (True = "mulligan").
    """
    keep: bool
    rationale: str = ""


@dataclass
class BottomPickerResponse:
    """Parsed bottom-picker response."""
    cards_to_bottom: List[str] = field(default_factory=list)
    rationale: str = ""


def parse_mulligan_response(
    raw_text: str,
) -> Tuple[Optional[MulliganResponse], Optional[str]]:
    """Parse the LLM's mulligan JSON. Expects `{"keep": bool}`."""
    if not raw_text or not raw_text.strip():
        return None, "Empty LLM response for mulligan decider."
    text = raw_text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```\s*$", "", text)
    obj_str = _extract_first_json_object(text)
    if obj_str is None:
        return None, f"No JSON object found in: {text[:200]!r}"
    try:
        parsed = json.loads(obj_str)
    except json.JSONDecodeError as exc:
        return None, f"JSON parse error: {exc}"
    if not isinstance(parsed, dict):
        return None, f"Expected JSON object, got {type(parsed).__name__}"
    if "keep" not in parsed:
        return None, "Missing required 'keep' key."
    keep_val = parsed["keep"]
    if not isinstance(keep_val, bool):
        # Coerce common truthy/falsy strings.
        if isinstance(keep_val, str):
            lower = keep_val.strip().lower()
            if lower in ("true", "yes", "keep"):
                keep_val = True
            elif lower in ("false", "no", "mulligan", "muligan"):
                keep_val = False
            else:
                return None, (
                    f"'keep' must be a boolean (got string {keep_val!r})"
                )
        else:
            return None, (
                f"'keep' must be a boolean (got {type(keep_val).__name__})"
            )
    rationale = str(parsed.get("rationale") or "")[:500]
    return MulliganResponse(keep=keep_val, rationale=rationale), None


def parse_bottom_picker_response(
    raw_text: str,
    hand_card_ids: List[str],
    n_to_put_on_bottom: int,
) -> Tuple[Optional[BottomPickerResponse], Optional[str]]:
    """Parse the LLM's bottom-picker JSON. Validates each card_id is
    in the player's hand AND no duplicates AND count == n."""
    if not raw_text or not raw_text.strip():
        return None, "Empty LLM response for bottom-picker."
    text = raw_text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```\s*$", "", text)
    obj_str = _extract_first_json_object(text)
    if obj_str is None:
        return None, f"No JSON object found in: {text[:200]!r}"
    try:
        parsed = json.loads(obj_str)
    except json.JSONDecodeError as exc:
        return None, f"JSON parse error: {exc}"
    if not isinstance(parsed, dict):
        return None, f"Expected JSON object, got {type(parsed).__name__}"
    cards = parsed.get("cards_to_bottom")
    if cards is None:
        return None, "Missing required 'cards_to_bottom' key."
    if not isinstance(cards, list):
        return None, (
            f"'cards_to_bottom' must be a list, "
            f"got {type(cards).__name__}"
        )
    if len(cards) != n_to_put_on_bottom:
        return None, (
            f"'cards_to_bottom' has {len(cards)} entries; "
            f"exactly {n_to_put_on_bottom} required."
        )
    hand_set = set(hand_card_ids)
    seen: set = set()
    out: List[str] = []
    for i, cid in enumerate(cards):
        if not isinstance(cid, str):
            return None, f"cards_to_bottom[{i}] must be a string card_id"
        if cid not in hand_set:
            return None, (
                f"cards_to_bottom[{i}] = {cid!r} not in YOUR_HAND "
                f"({sorted(hand_set)})"
            )
        if cid in seen:
            return None, f"cards_to_bottom[{i}] = {cid!r} is duplicated"
        seen.add(cid)
        out.append(cid)
    rationale = str(parsed.get("rationale") or "")[:500]
    return BottomPickerResponse(cards_to_bottom=out, rationale=rationale), None
