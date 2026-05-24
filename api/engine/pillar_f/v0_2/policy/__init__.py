"""Pillar F v0.2 sub-mega-task B — LLM strategic policy layer.

Plugs into the iter-10 substrate's callback hooks:
  - PriorityResponderFn (stack/stack.py)
  - MulliganDeciderFn + BottomPickerFn (turn/mulligan.py)
  - register_replacement_fn (replacement/replacement.py) — for per-card
    handlers used by sub-B's LLM-piloted decks
  - register_layer_effect (layers/layer_engine.py) — same

Sub-mega-task A (iter 10) shipped the substrate. Sub-mega-task B (this
iter, iter 11) ships the LLM-driven decision-making on top. Sub-mega-
task C (iter 12+) will orchestrate Stage 2 graduated playtest with N
games per deck for measured win-rate validation.

Module layout:
  prompts/    — prompt template builders (main-phase, combat,
                response-window, mulligan, bottom-picker)
  parsers/    — JSON response parsing + validator
  politics/   — threat-vector + politics state tracker
  cost/       — per-turn + per-game cost guardrails

Sub-mega-task B scoping doc:
  ../../../MTG-Deck-Builder-Claude/pillar_f_v0_2_sub_b_llm_policy_scoping.md
"""

POLICY_VERSION = "pillar_f_v0_2_policy_v1_sub_b"

from api.engine.pillar_f.v0_2.policy.eligible_actions import (
    ELIGIBLE_ACTIONS_VERSION,
    compute_eligible_actions,
    apply_action,
)
from api.engine.pillar_f.v0_2.policy.llm_responder import (
    LLM_RESPONDER_VERSION,
    DEFAULT_MAX_INPUT_TOKENS,
    DEFAULT_MAX_OUTPUT_TOKENS,
    MAX_REPROMPTS,
    make_llm_priority_responder,
    cheap_fallback_responder,
)
from api.engine.pillar_f.v0_2.policy.mulligan_decider import (
    MULLIGAN_LLM_VERSION,
    make_llm_mulligan_decider,
    make_llm_bottom_picker,
)

__all__ = [
    "POLICY_VERSION",
    "ELIGIBLE_ACTIONS_VERSION", "compute_eligible_actions", "apply_action",
    "LLM_RESPONDER_VERSION", "DEFAULT_MAX_INPUT_TOKENS",
    "DEFAULT_MAX_OUTPUT_TOKENS", "MAX_REPROMPTS",
    "make_llm_priority_responder", "cheap_fallback_responder",
    "MULLIGAN_LLM_VERSION",
    "make_llm_mulligan_decider", "make_llm_bottom_picker",
]
