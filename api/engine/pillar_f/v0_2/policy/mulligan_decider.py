"""LLM-driven MulliganDeciderFn + BottomPickerFn factories.

Plugs into the iter-10 substrate's `turn/mulligan.mulligan_setup`
callback hooks (`decider_fn` + `bottom_picker_fn`). Each factory
returns a closure that holds a shared LLM client + cost tracker so
the same accumulators flow through the whole game.

If the LLM client is unavailable (no API key), the factories fall
back to:
- decider: always keep (substrate's `always_keep_decider`)
- bottom_picker: take the last N (substrate's `default_bottom_picker`)
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from api.engine.pillar_f.v0_2.state import Card, GameState
from api.engine.pillar_f.v0_2.policy.cost import CostTracker
from api.engine.pillar_f.v0_2.policy.prompts.mulligan import (
    MULLIGAN_SYSTEM_PROMPT,
    BOTTOM_PICKER_SYSTEM_PROMPT,
    build_mulligan_prompt,
    build_bottom_picker_prompt,
)
from api.engine.pillar_f.v0_2.policy.parsers import (
    parse_mulligan_response,
    parse_bottom_picker_response,
)


MULLIGAN_LLM_VERSION = "pillar_f_v0_2_policy_mulligan_llm_v1"

# Token budgets per scoping doc section 2d.
DEFAULT_MAX_INPUT_TOKENS = 2000
DEFAULT_MAX_OUTPUT_TOKENS = 300
MAX_REPROMPTS = 2  # → at most 3 calls per decision


def _describe_hand(
    state: GameState, hand_card_ids: List[str],
) -> List[Dict[str, Any]]:
    """Build the per-card descriptor list the prompts consume."""
    out: List[Dict[str, Any]] = []
    for cid in hand_card_ids:
        card = state.get_card(cid)
        if card is None:
            out.append({
                "card_id": cid, "name": "(unknown)",
                "type_line": "", "mana_cost": "", "oracle_text": "",
            })
            continue
        out.append({
            "card_id": cid,
            "name": getattr(card, "name", "?"),
            "type_line": getattr(card, "type_line", "") or "",
            "mana_cost": getattr(card, "mana_cost", "") or "",
            "oracle_text": getattr(card, "oracle_text", "") or "",
        })
    return out


def make_llm_mulligan_decider(
    *,
    llm_client: Any,
    cost_tracker: CostTracker,
    deck_archetype_hint_by_player: Optional[Dict[int, str]] = None,
) -> Callable[[GameState, int, List[str], int], bool]:
    """Returns a MulliganDeciderFn-compatible closure.

    The substrate's MulliganDeciderFn returns True = mulligan, False =
    keep. The LLM is asked the inverse-friendly question "do you keep?"
    and we negate.

    Args:
        llm_client: AnthropicClient (or compatible mock).
        cost_tracker: shared per-game CostTracker.
        deck_archetype_hint_by_player: optional per-player archetype.
    """
    if deck_archetype_hint_by_player is None:
        deck_archetype_hint_by_player = {}

    def _decider(
        state: GameState, player_id: int,
        current_hand: List[str], num_mulligans: int,
    ) -> bool:
        # Cost guardrail tripped → always keep (fastest legal exit).
        if cost_tracker.game_halted_for_cost:
            return False
        if not llm_client.is_available():
            return False
        # Substrate hard-caps mulligans to max_mulligans; if hand is
        # already at minimum size, we must keep.
        if num_mulligans >= 7:
            return False
        hand_desc = _describe_hand(state, current_hand)
        deck_hint = deck_archetype_hint_by_player.get(player_id)
        last_error: Optional[str] = None
        for _ in range(MAX_REPROMPTS + 1):
            prompt = build_mulligan_prompt(
                hand_desc, num_mulligans,
                deck_archetype_hint=deck_hint,
                last_error_message=last_error,
            )
            result = llm_client.call_with_budget(
                system=MULLIGAN_SYSTEM_PROMPT,
                user=prompt,
                max_input_tokens=DEFAULT_MAX_INPUT_TOKENS,
                max_output_tokens=DEFAULT_MAX_OUTPUT_TOKENS,
            )
            cost_tracker.record_call(
                player_id=player_id, turn_number=0,  # pre-game = turn 0
                cost_usd=getattr(result, "cost_usd", 0.0),
                purpose="mulligan_decider",
            )
            if cost_tracker.game_halted_for_cost:
                return False
            if not result.ok:
                last_error = (
                    f"LLM call failed: {result.error_message}. "
                    f"Respond with valid JSON."
                )
                continue
            parsed, err = parse_mulligan_response(result.text or "")
            if parsed is not None:
                # LLM said keep=True → substrate wants False (don't mulligan).
                return not parsed.keep
            last_error = err or "Unknown parse error."
        # All re-prompts failed → conservative default = keep.
        return False

    return _decider


def make_llm_bottom_picker(
    *,
    llm_client: Any,
    cost_tracker: CostTracker,
    deck_archetype_hint_by_player: Optional[Dict[int, str]] = None,
) -> Callable[[GameState, int, List[str], int], List[str]]:
    """Returns a BottomPickerFn-compatible closure.

    Returns the card_ids to bottom in the LLM's chosen order (bottom-
    to-top). If the LLM fails or is unavailable, falls back to taking
    the last N cards in insertion order.
    """
    if deck_archetype_hint_by_player is None:
        deck_archetype_hint_by_player = {}

    def _picker(
        state: GameState, player_id: int,
        hand: List[str], n_to_put_on_bottom: int,
    ) -> List[str]:
        if n_to_put_on_bottom <= 0:
            return []
        # Cost guardrail tripped → fallback to last N.
        if cost_tracker.game_halted_for_cost:
            return list(hand[-n_to_put_on_bottom:])
        if not llm_client.is_available():
            return list(hand[-n_to_put_on_bottom:])
        if n_to_put_on_bottom > len(hand):
            n_to_put_on_bottom = len(hand)
        hand_desc = _describe_hand(state, hand)
        deck_hint = deck_archetype_hint_by_player.get(player_id)
        last_error: Optional[str] = None
        for _ in range(MAX_REPROMPTS + 1):
            prompt = build_bottom_picker_prompt(
                hand_desc, n_to_put_on_bottom,
                deck_archetype_hint=deck_hint,
                last_error_message=last_error,
            )
            result = llm_client.call_with_budget(
                system=BOTTOM_PICKER_SYSTEM_PROMPT,
                user=prompt,
                max_input_tokens=DEFAULT_MAX_INPUT_TOKENS,
                max_output_tokens=DEFAULT_MAX_OUTPUT_TOKENS,
            )
            cost_tracker.record_call(
                player_id=player_id, turn_number=0,
                cost_usd=getattr(result, "cost_usd", 0.0),
                purpose="bottom_picker",
            )
            if cost_tracker.game_halted_for_cost:
                return list(hand[-n_to_put_on_bottom:])
            if not result.ok:
                last_error = (
                    f"LLM call failed: {result.error_message}. "
                    f"Respond with valid JSON."
                )
                continue
            parsed, err = parse_bottom_picker_response(
                result.text or "", hand, n_to_put_on_bottom,
            )
            if parsed is not None:
                return list(parsed.cards_to_bottom)
            last_error = err or "Unknown parse error."
        # All re-prompts failed → last-N fallback.
        return list(hand[-n_to_put_on_bottom:])

    return _picker
