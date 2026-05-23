"""LLM-driven priority responder.

Implements scoping doc section 7a. Factory function returns a
`PriorityResponderFn`-compatible callable that:

1. Builds compact_view from state.perspective_view(player_id).
2. Calls compute_eligible_actions(state, player_id).
3. Builds main-phase prompt.
4. Calls Anthropic Sonnet via the existing AnthropicClient wrapper.
5. Parses + validates the response with up-to-2 re-prompts.
6. Records cost via CostTracker.
7. Returns push_to_stack kwargs OR None (pass).

If cost guardrail is hit, switches to cheap-fallback (always pass)
for the remainder of the turn.

Factory pattern allows the closure to capture per-game state
(LLM client, cost tracker, action log, politics state) without
GameState carrying these as fields (keeps the rules engine clean).
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from api.engine.pillar_f.v0_2.state import GameState
from api.engine.pillar_f.v0_2.policy.eligible_actions import (
    apply_action, compute_eligible_actions,
)
from api.engine.pillar_f.v0_2.policy.cost import CostTracker
from api.engine.pillar_f.v0_2.policy.prompts import (
    compact_view, build_main_phase_prompt,
    MAIN_PHASE_SYSTEM_PROMPT,
)
from api.engine.pillar_f.v0_2.policy.parsers import (
    ActionResponse, parse_action_response, fallback_pass_response,
)


LLM_RESPONDER_VERSION = "pillar_f_v0_2_policy_llm_responder_v1"

# Token budgets per scoping doc section 6.
DEFAULT_MAX_INPUT_TOKENS = 5000
DEFAULT_MAX_OUTPUT_TOKENS = 500
MAX_REPROMPTS = 2  # so total LLM calls per priority window ≤ 3


def make_llm_priority_responder(
    *,
    llm_client: Any,
    cost_tracker: CostTracker,
    action_log: Optional[List[str]] = None,
    politics_state_by_player: Optional[Dict[int, Dict[str, Any]]] = None,
    deck_archetype_hint_by_player: Optional[Dict[int, str]] = None,
    rationale_history_by_player: Optional[Dict[int, List[str]]] = None,
) -> Callable[[GameState, int], Optional[Dict[str, Any]]]:
    """Factory: returns a PriorityResponderFn-compatible closure.

    Args:
        llm_client: an AnthropicClient instance (or compatible mock
            with the same call_with_budget signature returning CallResult).
        cost_tracker: shared per-game CostTracker. Recorded calls
            update its buckets + trigger fallback flags.
        action_log: shared per-game action log (list of "T<n> P<x> ..."
            strings). Appended on each apply_action.
        politics_state_by_player: optional per-player politics context
            (Phase 7 populates).
        deck_archetype_hint_by_player: optional per-player archetype hint.
        rationale_history_by_player: optional per-player rolling
            rationale log (last N rationales the LLM emitted).

    Returns:
        responder_fn(state, player_id) → push_to_stack kwargs dict
        OR None (pass priority).
    """
    # Use `is None` checks so the closure captures the caller's
    # references (an empty dict is falsy under `or`; `or {}` would
    # replace it with a NEW dict the caller can't observe).
    action_log = action_log if action_log is not None else []
    if politics_state_by_player is None:
        politics_state_by_player = {}
    if deck_archetype_hint_by_player is None:
        deck_archetype_hint_by_player = {}
    if rationale_history_by_player is None:
        rationale_history_by_player = {}

    def _responder(state: GameState, player_id: int) -> Optional[Dict[str, Any]]:
        # If game halted for cost, pass everything.
        if cost_tracker.game_halted_for_cost:
            return None
        # If this player hit the per-turn ceiling, pass for the rest
        # of the turn (cheap-fallback).
        if cost_tracker.is_player_in_fallback(player_id, state.turn_number):
            return None
        # If LLM client is unavailable, default to pass (sub-B is
        # opt-in; if no API key, the game runs all-pass which still
        # progresses through phases).
        if not llm_client.is_available():
            return None

        # Compute eligible actions FIRST. If only pass is available,
        # skip the LLM call (save tokens).
        eligible = compute_eligible_actions(state, player_id)
        if len(eligible) == 1 and eligible[0]["action_type"] == "pass_priority":
            return None

        # Build prompt context.
        view = state.perspective_view(viewer_player_id=player_id)
        compact = compact_view(view, viewer_player_id=player_id,
                                action_log=action_log)
        deck_hint = deck_archetype_hint_by_player.get(player_id)
        politics_ctx = politics_state_by_player.get(player_id)
        rationale_hist = rationale_history_by_player.get(player_id, [])

        last_error: Optional[str] = None
        response: Optional[ActionResponse] = None
        attempts = 0
        while attempts <= MAX_REPROMPTS:
            user_prompt = build_main_phase_prompt(
                compact, eligible,
                politics_context=politics_ctx,
                deck_archetype_hint=deck_hint,
                rationale_history=rationale_hist,
                last_error_message=last_error,
            )
            result = llm_client.call_with_budget(
                system=MAIN_PHASE_SYSTEM_PROMPT,
                user=user_prompt,
                max_input_tokens=DEFAULT_MAX_INPUT_TOKENS,
                max_output_tokens=DEFAULT_MAX_OUTPUT_TOKENS,
            )
            cost_tracker.record_call(
                player_id=player_id, turn_number=state.turn_number,
                cost_usd=getattr(result, "cost_usd", 0.0),
                purpose="main_phase_priority",
            )
            # If cost guardrail flipped DURING this call, fall back now.
            if cost_tracker.game_halted_for_cost:
                return None
            if cost_tracker.is_player_in_fallback(
                player_id, state.turn_number,
            ):
                response = fallback_pass_response(eligible)
                break
            if not result.ok:
                last_error = (
                    f"LLM call failed (error_code={result.error_code}, "
                    f"message={result.error_message}). Pick a legal action."
                )
                attempts += 1
                continue
            parsed, err = parse_action_response(result.text or "", eligible)
            if parsed is not None:
                response = parsed
                break
            last_error = err or "Unknown parse error."
            attempts += 1
        if response is None:
            response = fallback_pass_response(eligible)
        if response is None:
            return None
        # Apply rationale to history.
        if response.rationale:
            rationale_hist.append(
                f"T{state.turn_number} {response.action_type}: {response.rationale}"
            )
            rationale_history_by_player[player_id] = rationale_hist[-10:]
        # Apply the action to engine state (move card, push stack, etc.).
        # apply_action mutates state; for cast_spell it ALREADY pushes
        # to stack, which is what priority_round expects to handle (it
        # returns None from this responder because the action is already
        # applied; priority_round's stack-mutation check sees the push).
        # For play_land, no stack push; responder returns None.
        # For pass_priority, no action; responder returns None.
        if response.action_type == "pass_priority":
            return None
        apply_action(state, player_id, response.eligible_action)
        # Log action for compact_view's recent-actions section.
        desc = response.description or response.action_type
        action_log.append(f"T{state.turn_number} P{player_id} {desc}")
        # Return None from the responder: the action has already been
        # applied to state. priority_round's "did the stack change?"
        # check will see the push (for cast_spell) and reset passes.
        return None

    return _responder


# ============================================================
# Cheap-fallback responder (always pass)
# ============================================================


def cheap_fallback_responder(state: GameState, player_id: int) -> Optional[Dict[str, Any]]:
    """Always returns None (pass). Used when cost guardrails trip."""
    return None
