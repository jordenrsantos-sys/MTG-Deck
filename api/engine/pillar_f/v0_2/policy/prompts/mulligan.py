"""Mulligan + bottom-picker prompt builders.

Sub-mega-task B Phase 6. Two prompts:

1. **Mulligan decider** — at game start (and after each mulligan), the
   LLM is shown the opening hand and asked whether to keep or
   mulligan. London mulligan rules apply (CR 103.4d): hand is always
   7 cards, but N cards go to bottom of library AFTER keeping when
   N mulligans were taken.

2. **Bottom-picker** — after a keep with num_mulligans > 0, the LLM
   picks N specific cards to put on the bottom of the library.

Output JSON contracts:
- Mulligan: `{"keep": bool, "rationale": str}`. (Note: scoping doc
  uses "keep" as "I keep this hand"; the internal MulliganDeciderFn
  returns True = mulligan. The parser inverts.)
- Bottom-picker: `{"cards_to_bottom": [card_id, ...], "rationale": str}`.

Per scoping section 2d: ~1000 input + ~200 output tokens, ~$0.005/call.
Max 7 mulligans × 2 calls (decider + bottom-picker) = 14 calls per
player at game start; cap ≈ $0.07 per player game-start.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


MULLIGAN_PROMPT_VERSION = "pillar_f_v0_2_policy_mulligan_v1"


MULLIGAN_SYSTEM_PROMPT = """You are deciding whether to keep your opening hand in a multiplayer Commander (EDH) game using the London mulligan rule.

Output VALID JSON ONLY with this exact shape:
{
  "keep": true | false,
  "rationale": "<short reason, ≤ 200 chars>"
}

London mulligan: every mulligan still draws 7 cards, but after keeping with N mulligans taken, you put N cards on the bottom of your library. So a 1-mulligan hand effectively plays a 6-card opener, a 2-mulligan hand plays a 5-card opener, and so on. Be increasingly conservative with each mulligan — going below 5 cards is usually game-losing.

Keep heuristics (Commander, 40-life multiplayer):
- 2-5 lands of correct colors is the usual keep range
- 0 or 1 land = mulligan (color requirements + curve fail)
- 6-7 lands = mulligan (no business)
- Look for a curve from turns 1-4 — castable plays matter more than power level
- Free interaction (Force of Will, Pact of Negation) is a strong keep signal

Output ONLY the JSON. No prose, no markdown fences."""


BOTTOM_PICKER_SYSTEM_PROMPT = """You kept a London-mulligan hand and now must put N cards on the bottom of your library — choose which N. The remaining 7 − N cards are your real opening hand.

Output VALID JSON ONLY with this exact shape:
{
  "cards_to_bottom": ["<card_id>", ...],
  "rationale": "<short reason, ≤ 200 chars>"
}

The cards_to_bottom array must:
- Contain EXACTLY N card_ids (where N is shown in the prompt as "PUT_ON_BOTTOM")
- Each card_id must appear in YOUR_HAND list shown in the prompt
- No duplicates

Bottom selection heuristics:
- Bottom your weakest late-game cards if you have enough mana sources
- Bottom an excess land if you have 5+ lands and a low curve
- Bottom unaffordable bombs (e.g., a 7-mana finisher with no ramp)
- Keep cheap interaction + early plays + your ramp pieces

Output ONLY the JSON. No prose, no markdown fences."""


def build_mulligan_prompt(
    hand_descriptions: List[Dict[str, Any]],
    num_mulligans_taken: int,
    *,
    deck_archetype_hint: Optional[str] = None,
    last_error_message: Optional[str] = None,
) -> str:
    """Builds the user-message body for the mulligan decider prompt.

    Args:
        hand_descriptions: list of dicts describing each card in hand,
            each with keys: card_id, name, type_line, mana_cost,
            oracle_text. Caller builds this from card_id lookups.
        num_mulligans_taken: how many mulligans have already been
            taken (0 on first decision, 1 after first mulligan, etc.).
        deck_archetype_hint: Pillar D theme_profile primary archetype.
        last_error_message: re-prompt error message if a prior response
            failed validation.
    """
    parts: List[str] = []
    parts.append(f"=== MULLIGANS TAKEN: {num_mulligans_taken} ===")
    if num_mulligans_taken > 0:
        parts.append(
            f"(If you keep, you put {num_mulligans_taken} card(s) on "
            f"the bottom of your library after this decision.)"
        )
    parts.append("")

    if deck_archetype_hint:
        parts.append(f"=== YOUR DECK ARCHETYPE: {deck_archetype_hint} ===")
        parts.append("")

    parts.append("=== YOUR OPENING HAND (7 cards) ===")
    for i, c in enumerate(hand_descriptions):
        name = c.get("name", "?")
        type_line = c.get("type_line", "")
        mana_cost = c.get("mana_cost", "")
        oracle = c.get("oracle_text", "") or ""
        oracle_short = oracle[:120] + ("..." if len(oracle) > 120 else "")
        parts.append(f"  [{i}] {name} {mana_cost} — {type_line}")
        if oracle_short:
            parts.append(f"      {oracle_short}")
    parts.append("")

    if last_error_message:
        parts.append("=== ATTENTION: PRIOR RESPONSE FAILED VALIDATION ===")
        parts.append(f"  {last_error_message}")
        parts.append("")

    parts.append(
        'Return JSON with {"keep": true|false, "rationale": "..."}. '
        'true = keep this hand; false = mulligan (shuffle back + redraw 7).'
    )
    return "\n".join(parts)


def build_bottom_picker_prompt(
    hand_descriptions: List[Dict[str, Any]],
    n_to_put_on_bottom: int,
    *,
    deck_archetype_hint: Optional[str] = None,
    last_error_message: Optional[str] = None,
) -> str:
    """Builds the user-message body for the bottom-picker prompt.

    Args:
        hand_descriptions: list of dicts describing each card in hand
            (same shape as build_mulligan_prompt). Each MUST include
            card_id so the LLM can echo it back.
        n_to_put_on_bottom: number of cards to put on bottom of library.
        deck_archetype_hint: Pillar D theme_profile primary archetype.
        last_error_message: re-prompt error message.
    """
    parts: List[str] = []
    parts.append(f"=== PUT_ON_BOTTOM: {n_to_put_on_bottom} ===")
    parts.append(
        f"You kept after {n_to_put_on_bottom} mulligan(s). Choose "
        f"exactly {n_to_put_on_bottom} card_id(s) from your hand to put "
        f"on the bottom of your library. The rest (7 − {n_to_put_on_bottom} "
        f"= {7 - n_to_put_on_bottom}) is your real opener."
    )
    parts.append("")

    if deck_archetype_hint:
        parts.append(f"=== YOUR DECK ARCHETYPE: {deck_archetype_hint} ===")
        parts.append("")

    parts.append("=== YOUR_HAND (7 cards — pick card_ids) ===")
    for i, c in enumerate(hand_descriptions):
        cid = c.get("card_id", "?")
        name = c.get("name", "?")
        type_line = c.get("type_line", "")
        mana_cost = c.get("mana_cost", "")
        oracle = c.get("oracle_text", "") or ""
        oracle_short = oracle[:120] + ("..." if len(oracle) > 120 else "")
        parts.append(f"  [{i}] card_id={cid}  {name} {mana_cost} — {type_line}")
        if oracle_short:
            parts.append(f"      {oracle_short}")
    parts.append("")

    if last_error_message:
        parts.append("=== ATTENTION: PRIOR RESPONSE FAILED VALIDATION ===")
        parts.append(f"  {last_error_message}")
        parts.append("")

    parts.append(
        f'Return JSON: {{"cards_to_bottom": [<{n_to_put_on_bottom} '
        f'card_ids from YOUR_HAND>], "rationale": "..."}}.'
    )
    return "\n".join(parts)
