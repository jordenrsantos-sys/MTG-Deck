"""Response-window prompt builder.

Fires when a spell or ability is on the stack and a player (active or
non-active) has priority to respond before the stack resolves. Scoping
doc section 2c. Tighter context than main-phase: focus on the stack-top
object + the viewer's response options (instants in hand, activated
abilities on the battlefield).

Output JSON shape is identical to main-phase (action_type +
action_index + rationale) so the existing main-phase action parser is
reused — see parsers/action_parser.parse_action_response.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


RESPONSE_WINDOW_PROMPT_VERSION = "pillar_f_v0_2_policy_response_window_v1"


RESPONSE_WINDOW_SYSTEM_PROMPT = """You have priority during a response window: a spell or ability is on the stack and you may respond before it resolves, or pass priority and let it resolve.

Output VALID JSON ONLY with this exact shape:
{
  "action_type": "cast_spell" | "activate_ability" | "pass_priority",
  "action_index": <int>,
  "rationale": "<short reason, ≤ 200 chars>"
}

The action_index is the 0-based index into the "ELIGIBLE RESPONSES" list shown in the user prompt. Pick the index of the response you choose.

Default to pass. Most stack objects do not need a response — burning a counterspell or removal spell on a low-impact target costs you tempo and card advantage. Respond only when the stack object would meaningfully harm your position OR when you have a clear value-trade (counter their 6-mana threat with your 2-mana counter).

When a counter war is escalating, weigh the mana investment: if you've already spent 4 mana defending the stack object and they keep adding 2-mana counters, the math may flip against you. Honor table politics — if an ally cast the spell on the stack, do not counter it.

Output ONLY the JSON. No prose, no markdown fences."""


def build_response_window_prompt(
    compact_view_text: str,
    stack_top_summary: str,
    eligible_actions: List[Dict[str, Any]],
    *,
    politics_context: Optional[Dict[str, Any]] = None,
    deck_archetype_hint: Optional[str] = None,
    rationale_history: Optional[List[str]] = None,
    last_error_message: Optional[str] = None,
) -> str:
    """Builds the user-message body for a response-window decision.

    Args:
        compact_view_text: condensed view from compact_view().
        stack_top_summary: human-readable summary of the stack-top
            entry — produced by summarize_stack_top().
        eligible_actions: list of action dicts pre-computed by
            compute_eligible_actions(). During a response window the
            engine has already filtered to instants + activated abilities
            + pass.
        politics_context: per-opponent threat scores (Phase 7 populates).
        deck_archetype_hint: Pillar D theme_profile primary archetype.
        rationale_history: rolling per-player rationale log.
        last_error_message: re-prompt error message if a prior response
            failed validation.
    """
    parts: List[str] = []
    parts.append("=== STACK TOP (RESPOND OR PASS) ===")
    parts.append(stack_top_summary)
    parts.append("")

    parts.append("=== CURRENT GAME STATE ===")
    parts.append(compact_view_text)
    parts.append("")

    if deck_archetype_hint:
        parts.append(f"=== YOUR DECK ARCHETYPE: {deck_archetype_hint} ===")
        parts.append("")

    if politics_context:
        parts.append("=== POLITICS CONTEXT ===")
        threats = politics_context.get("threats") or {}
        for opp_id, info in sorted(threats.items(), key=lambda kv: str(kv[0])):
            score = info.get("score", 0.0)
            parts.append(f"  P{opp_id}: threat_score={score:.2f}")
        alliances = politics_context.get("alliances") or {}
        if alliances:
            parts.append("  Alliances: " + ", ".join(
                f"P{k}={v}" for k, v in sorted(
                    alliances.items(), key=lambda kv: str(kv[0]),
                )
            ))
        parts.append("")

    if rationale_history:
        parts.append("=== YOUR RECENT ACTION RATIONALES ===")
        for r in rationale_history[-5:]:
            parts.append(f"  - {r}")
        parts.append("")

    parts.append("=== ELIGIBLE RESPONSES ===")
    if not eligible_actions:
        parts.append("  (none — must pass)")
    for i, a in enumerate(eligible_actions):
        desc = a.get("description") or a.get("action_type", "?")
        parts.append(f"  [{i}] {desc}")
    parts.append("")

    if last_error_message:
        parts.append("=== ATTENTION: PRIOR RESPONSE FAILED VALIDATION ===")
        parts.append(f"  {last_error_message}")
        parts.append(
            "  Pick a different action from the ELIGIBLE RESPONSES list."
        )
        parts.append("")

    parts.append(
        "Return JSON with the action_index of your chosen response and a "
        "short rationale. To let the stack object resolve, pick the "
        "pass_priority entry."
    )
    return "\n".join(parts)


def summarize_stack_top(stack_entry: Any) -> str:
    """Compact one-block summary of a stack entry for the prompt.

    Accepts either a StackEntry dataclass or its to_dict() form so
    callers can pass perspective_view['stack'][-1] directly.
    """
    if stack_entry is None:
        return "(stack is empty — no response window)"
    if hasattr(stack_entry, "to_dict"):
        d = stack_entry.to_dict()
    elif isinstance(stack_entry, dict):
        d = stack_entry
    else:
        return f"(unparseable stack entry: {type(stack_entry).__name__})"

    controller = d.get("controller", "?")
    entry_type = d.get("entry_type", "spell")
    description = d.get("description") or "(no description)"
    targets = d.get("targets") or []
    payment = d.get("payment") or {}
    resolver = payment.get("resolver", "?") if isinstance(payment, dict) else "?"

    lines = [
        f"  Controller: P{controller}",
        f"  Type: {entry_type}",
        f"  Description: {description}",
    ]
    if targets:
        targets_str = ", ".join(str(t) for t in targets)
        lines.append(f"  Targets: {targets_str}")
    else:
        lines.append("  Targets: (none)")
    lines.append(f"  Resolver: {resolver}")
    return "\n".join(lines)
