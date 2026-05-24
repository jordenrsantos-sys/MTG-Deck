"""Cast pipeline cost-modifier consumer (sub-mega-task v14 Phase 3).

Consumers read v11's `cards/continuous/framework.py` static_modifier
registry (existing data layer) and apply the modifier semantics:

- cost_reduction (Medallions, Etherium Sculptor, Foundry Inspector,
  Goblin Anarchomancer) -> modified cast cost
- spell_restriction (Drannith Magistrate, Grand Abolisher) ->
  cast-legality gate
- attack_tax (Propaganda, Ghostly Prison) -> additional attack cost
- additional_land_drops (Azusa, Exploration) -> bonus play_land slots
- additional_mana_when_land_taps (Utopia Sprawl, Wild Growth) ->
  bonus mana on land tap
- uncounterable (Hexing Squelcher) -> counter-resolution gate

These functions take a GameState and a candidate cast (card +
caster_id) and return modified-cost / legality / bonus-effect info.
Sub-B's `compute_eligible_actions` can call them to enrich the
LLM-facing eligible action set; the substrate keeps the modifier
math out of the rules-engine hot path.

The substrate stays runnable in isolation: the lazy import of v11's
static_modifier registry tolerates the cards/ package not being
loaded (returns empty modifier list -> baseline cost / legal /
no bonus).
"""
from api.engine.pillar_f.v0_2.cast.cost_modifier import (
    COST_MODIFIER_VERSION,
    ManaCostDelta,
    effective_cast_cost_delta,
    is_spell_cast_legal,
    is_spell_uncounterable,
    additional_land_drops_available,
    effective_attack_tax,
    extra_mana_from_aura_on_land,
)

__all__ = [
    "COST_MODIFIER_VERSION",
    "ManaCostDelta",
    "effective_cast_cost_delta",
    "is_spell_cast_legal",
    "is_spell_uncounterable",
    "additional_land_drops_available",
    "effective_attack_tax",
    "extra_mana_from_aura_on_land",
]
