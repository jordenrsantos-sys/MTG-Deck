"""Action response parser + validator.

Parses the LLM's JSON output for a main-phase / response-window action
prompt. Validates against the eligible_actions list. On invalid:
returns (action=None, error_message) so the caller can re-prompt or
fall back to pass.

Re-prompt loop: caller wraps parser+validator in a 2-attempt retry
embedding the error_message in the next prompt's last_error_message
slot. On 3rd failure, fall back to pass_priority per scoping doc
section 10 risk mitigation.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


ACTION_PARSER_VERSION = "pillar_f_v0_2_policy_action_parser_v1"


@dataclass
class ActionResponse:
    """Parsed + validated action ready to apply via the engine."""
    action_type: str
    action_index: int
    rationale: str
    eligible_action: Dict[str, Any]  # full eligible_actions[action_index] entry

    @property
    def card_id(self) -> Optional[str]:
        return self.eligible_action.get("card_id")

    @property
    def targets(self) -> List[Any]:
        return list(self.eligible_action.get("targets") or [])

    @property
    def payment(self) -> Dict[str, Any]:
        return dict(self.eligible_action.get("payment") or {})

    @property
    def description(self) -> str:
        return self.eligible_action.get("description") or self.action_type


def parse_action_response(
    raw_text: str,
    eligible_actions: List[Dict[str, Any]],
) -> Tuple[Optional[ActionResponse], Optional[str]]:
    """Parse the LLM's JSON output. Returns (response, error_message).
    `response` is None on parse/validation failure, with a human-readable
    error_message describing what went wrong.

    Handles common LLM output quirks:
      - Surrounding ```json fences (strip)
      - Trailing prose after the JSON object (extract first {...})
      - Wrong keys (action_index vs index vs action_idx — accept several)
    """
    if not raw_text or not raw_text.strip():
        return None, "Empty LLM response."
    text = raw_text.strip()
    # Strip markdown fences.
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```\s*$", "", text)
    # Find first JSON object via brace matching.
    obj_str = _extract_first_json_object(text)
    if obj_str is None:
        return None, f"Could not find JSON object in LLM response: {text[:200]!r}"
    try:
        parsed = json.loads(obj_str)
    except json.JSONDecodeError as exc:
        return None, f"JSON parse error: {exc} | text={obj_str[:200]!r}"
    if not isinstance(parsed, dict):
        return None, f"Expected JSON object, got {type(parsed).__name__}"

    # action_index — accept a few common keys.
    idx_raw = (parsed.get("action_index")
               if parsed.get("action_index") is not None
               else parsed.get("index")
               if parsed.get("index") is not None
               else parsed.get("action_idx"))
    if idx_raw is None:
        return None, (
            "Response missing required key 'action_index'. "
            f"Got keys: {sorted(parsed.keys())}"
        )
    try:
        idx = int(idx_raw)
    except (TypeError, ValueError):
        return None, f"action_index must be int, got {idx_raw!r}"

    if idx < 0 or idx >= len(eligible_actions):
        return None, (
            f"action_index {idx} out of range; valid range is "
            f"[0, {len(eligible_actions) - 1}]"
        )

    chosen = eligible_actions[idx]
    action_type = chosen.get("action_type") or parsed.get("action_type") or "?"

    # Validate the LLM's declared action_type matches the chosen index
    # (if both present). LLM-declared field is informational; the
    # index is authoritative.
    declared_type = parsed.get("action_type")
    if declared_type and declared_type != action_type:
        # Warn but don't reject — the index is what matters. Caller can
        # log this as a soft drift signal.
        pass

    rationale = str(parsed.get("rationale") or "")[:500]

    return ActionResponse(
        action_type=action_type,
        action_index=idx,
        rationale=rationale,
        eligible_action=chosen,
    ), None


def _extract_first_json_object(text: str) -> Optional[str]:
    """Extract the first balanced {...} block from text. Returns None
    if no balanced object found."""
    start = text.find("{")
    if start < 0:
        return None
    depth = 0
    in_string = False
    escape_next = False
    for i in range(start, len(text)):
        ch = text[i]
        if escape_next:
            escape_next = False
            continue
        if in_string:
            if ch == "\\":
                escape_next = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return None


def validate_eligible_action_present(
    eligible_actions: List[Dict[str, Any]],
) -> Tuple[bool, str]:
    """Sanity check that eligible_actions has at least one entry and
    every entry has the required keys. Returns (ok, error_message)."""
    if not eligible_actions:
        return False, "Empty eligible_actions list — must have at least pass_priority."
    for i, a in enumerate(eligible_actions):
        if not isinstance(a, dict):
            return False, f"eligible_actions[{i}] is not a dict: {type(a).__name__}"
        if "action_type" not in a:
            return False, f"eligible_actions[{i}] missing 'action_type' key"
    return True, ""


# ============================================================
# Pass-priority fallback
# ============================================================


def fallback_pass_response(eligible_actions: List[Dict[str, Any]]) -> Optional[ActionResponse]:
    """Returns the pass_priority action wrapped in ActionResponse, or
    None if no pass action is in the eligible list (should never
    happen — pass is always legal). Used by the priority-loop's
    fallback chain on 3rd validation failure."""
    for i, a in enumerate(eligible_actions):
        if a.get("action_type") == "pass_priority":
            return ActionResponse(
                action_type="pass_priority",
                action_index=i,
                rationale="fallback: validator failed 3x → pass",
                eligible_action=a,
            )
    return None
