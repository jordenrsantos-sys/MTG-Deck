"""Prompt template builders. See compact_view.py + main_phase.py +
combat.py + response_window.py + mulligan.py."""
from api.engine.pillar_f.v0_2.policy.prompts.compact_view import (
    COMPACT_VIEW_VERSION,
    compact_view,
    estimate_tokens,
)
from api.engine.pillar_f.v0_2.policy.prompts.main_phase import (
    MAIN_PHASE_PROMPT_VERSION,
    MAIN_PHASE_SYSTEM_PROMPT,
    build_main_phase_prompt,
    compute_eligible_actions_passes_only,
)
from api.engine.pillar_f.v0_2.policy.prompts.combat import (
    COMBAT_PROMPT_VERSION,
    ATTACKERS_SYSTEM_PROMPT,
    BLOCKERS_SYSTEM_PROMPT,
    build_attackers_prompt,
    build_blockers_prompt,
)
from api.engine.pillar_f.v0_2.policy.prompts.response_window import (
    RESPONSE_WINDOW_PROMPT_VERSION,
    RESPONSE_WINDOW_SYSTEM_PROMPT,
    build_response_window_prompt,
    summarize_stack_top,
)

__all__ = [
    "COMPACT_VIEW_VERSION", "compact_view", "estimate_tokens",
    "MAIN_PHASE_PROMPT_VERSION", "MAIN_PHASE_SYSTEM_PROMPT",
    "build_main_phase_prompt", "compute_eligible_actions_passes_only",
    "COMBAT_PROMPT_VERSION", "ATTACKERS_SYSTEM_PROMPT",
    "BLOCKERS_SYSTEM_PROMPT",
    "build_attackers_prompt", "build_blockers_prompt",
    "RESPONSE_WINDOW_PROMPT_VERSION", "RESPONSE_WINDOW_SYSTEM_PROMPT",
    "build_response_window_prompt", "summarize_stack_top",
]
