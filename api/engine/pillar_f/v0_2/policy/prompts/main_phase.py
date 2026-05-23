"""Main-phase action prompt builder.

Sub-mega-task B Phase 2. The LLM is given:
  - compact_view of current state
  - eligible_actions list pre-computed by the engine
  - politics_context (threat scores; Phase 7 populates this)
  - deck_archetype_hint (from Pillar D theme_profile)
  - last_3_turns_action_history (compressed)

The LLM picks ONE action from eligible_actions (action_type + optional
card_id / ability_idx / targets / payment + rationale). The engine
applies it.

Pre-computing eligible_actions means the LLM is CHOOSING, not
GENERATING. Keeps illegal-action rate near zero.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


MAIN_PHASE_PROMPT_VERSION = "pillar_f_v0_2_policy_main_phase_v1"


MAIN_PHASE_SYSTEM_PROMPT = """You are piloting one seat in a multiplayer Commander (EDH) game. You will be shown the current game state from your seat's perspective, a list of legal actions you may take right now, and recent context. Pick ONE action and explain your reasoning briefly.

Output VALID JSON ONLY with this exact shape:
{
  "action_type": "cast_spell" | "activate_ability" | "play_land" | "pass_priority",
  "action_index": <int>,
  "rationale": "<short reason, ≤ 200 chars>"
}

The action_index is the 0-based index into the "ELIGIBLE ACTIONS" list shown in the user prompt. Pick the index of the action you choose.

Pass priority freely. Holding mana for instant-speed responses is normal. Don't burn cards just because they're in hand — wait for the right turn.

Honor table politics: don't pick fights you can't win. If multiple opponents threaten you equally, target the one with the highest threat score. If you have a deal in place with an opponent and they've honored it, honor yours back.

Common sense: a one-mana cantrip in main_1 is usually fine. A 6-mana wrath when you'd lose your own better board is usually wrong. A counter targeting an opponent's win-con on the stack is usually right.

Output ONLY the JSON. No prose, no markdown fences."""


def build_main_phase_prompt(
    compact_view_text: str,
    eligible_actions: List[Dict[str, Any]],
    *,
    politics_context: Optional[Dict[str, Any]] = None,
    deck_archetype_hint: Optional[str] = None,
    rationale_history: Optional[List[str]] = None,
    last_error_message: Optional[str] = None,
) -> str:
    """Builds the user-message body. The system prompt is separate
    (MAIN_PHASE_SYSTEM_PROMPT).

    Args:
        compact_view_text: result of compact_view().
        eligible_actions: list of action dicts pre-computed by the
            engine. Each has shape:
              {"action_type": str, "card_id": Optional[str],
               "ability_idx": Optional[int], "targets": List,
               "payment": dict, "description": str}
        politics_context: per-opponent threat scores (Phase 7 populates).
        deck_archetype_hint: Pillar D theme_profile primary archetype.
        rationale_history: list of recent rationale strings from this
            player's prior LLM calls (last ~5).
        last_error_message: if this is a re-prompt after a validator
            failure, include the error message to help LLM correct.
    """
    parts: List[str] = []
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
            board = info.get("board_strength", 0.0)
            tempo = info.get("tempo", 0.0)
            life = info.get("life_pressure", 0.0)
            agg = info.get("recent_aggression", 0.0)
            parts.append(
                f"  P{opp_id}: threat_score={score:.2f} "
                f"(board={board:.1f}, tempo={tempo:.1f}, "
                f"life_pressure={life:.2f}, recent_aggression={agg:.1f})"
            )
        alliances = politics_context.get("alliances") or {}
        if alliances:
            parts.append("  Alliances: " + ", ".join(
                f"P{k}={v}" for k, v in sorted(alliances.items(), key=lambda kv: str(kv[0]))
            ))
        deals = politics_context.get("deals") or []
        recent_deals = deals[-5:] if len(deals) > 5 else deals
        if recent_deals:
            parts.append("  Recent deals:")
            for d in recent_deals:
                parts.append(
                    f"    - P{d.get('opponent_player_id')} "
                    f"{d.get('deal_type', '?')} (turn {d.get('agreed_turn', '?')}, "
                    f"kept={d.get('kept', '?')})"
                )
        parts.append("")

    if rationale_history:
        parts.append("=== YOUR RECENT ACTION RATIONALES ===")
        for r in rationale_history[-5:]:
            parts.append(f"  - {r}")
        parts.append("")

    parts.append("=== ELIGIBLE ACTIONS ===")
    for i, a in enumerate(eligible_actions):
        desc = a.get("description") or a.get("action_type", "?")
        parts.append(f"  [{i}] {desc}")
    parts.append("")

    if last_error_message:
        parts.append("=== ATTENTION: PRIOR RESPONSE FAILED VALIDATION ===")
        parts.append(f"  {last_error_message}")
        parts.append("  Pick a different action that is in the ELIGIBLE ACTIONS list.")
        parts.append("")

    parts.append(
        "Return JSON with the action_index of your chosen action and a short rationale. "
        "If you don't want to take any action right now, pick the pass_priority entry."
    )
    return "\n".join(parts)


def compute_eligible_actions_passes_only() -> List[Dict[str, Any]]:
    """Iter-11 minimum eligible-actions stub: just the pass action.
    Real `compute_eligible_actions(state, player_id)` lives in the
    substrate side; Phase 3 will compose pass_priority + cast_spell
    options from cards in hand + activated abilities on battlefield.

    For Phase 2 unit tests, this stub is enough to verify prompt
    assembly and parsing. Phase 3 integration test exercises real
    eligible-action computation."""
    return [{
        "action_type": "pass_priority",
        "card_id": None,
        "ability_idx": None,
        "targets": [],
        "payment": {},
        "description": "Pass priority.",
    }]
