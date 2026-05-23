"""Combat phase machinery. Phase 6 of mega-task v9."""
from api.engine.pillar_f.v0_2.combat.combat import (
    COMBAT_VERSION,
    AttackerDeclaration,
    BlockerAssignment,
    CombatState,
    can_attack,
    declare_attackers,
    declare_blockers,
    deal_combat_damage,
    first_strike_phase_active,
    run_combat_phase,
)

__all__ = [
    "COMBAT_VERSION",
    "AttackerDeclaration", "BlockerAssignment", "CombatState",
    "can_attack",
    "declare_attackers", "declare_blockers",
    "deal_combat_damage", "first_strike_phase_active",
    "run_combat_phase",
]
