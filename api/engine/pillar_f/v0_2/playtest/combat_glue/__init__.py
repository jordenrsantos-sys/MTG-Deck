"""LLM-driven attackers + blockers wiring (Phase 1 of sub-C).

Owns sub-B Phase 9 gate 5. Combines sub-B Phase 4's attackers/blockers
prompts + parsers with substrate's combat.declare_attackers /
combat.declare_blockers callbacks at the right step transitions.
"""
from api.engine.pillar_f.v0_2.playtest.combat_glue.combat_decider import (
    COMBAT_DECIDER_VERSION,
    CombatDecisionRecord,
    compute_eligible_attackers,
    compute_attack_targets,
    compute_eligible_blockers,
    compute_attackers_to_block,
    make_llm_attacker_decider,
    make_llm_blocker_decider,
    run_llm_combat_phase,
)

__all__ = [
    "COMBAT_DECIDER_VERSION", "CombatDecisionRecord",
    "compute_eligible_attackers", "compute_attack_targets",
    "compute_eligible_blockers", "compute_attackers_to_block",
    "make_llm_attacker_decider", "make_llm_blocker_decider",
    "run_llm_combat_phase",
]
