"""Counter-war hook: counter_target_spell resolver (Phase 2 of sub-C).

Owns sub-B Phase 9 gate 6. v11 already shipped the substrate-side
counter resolution (counter_target in stack/, plus per-card resolvers
in cards/spell/counterspells.py via register_spell). What sub-C adds
here is:

  1. Backfill counter resolvers for ~7 family cards v11 did not ship
     (Force of Will, Force of Negation, Dovin's Veto, Mindbreak Trap,
     Mental Misstep, Pact of Negation, Daze). Stubs use substrate's
     counter_target; side effects (Force of Will's exile-blue-card
     alt cost, Mana Drain's mana-add) are iter-12+ refinement.
  2. `make_counterspell_annotation(card_name)` builds the
     iter10_annotation dict that sub-B's compute_eligible_actions
     consumes — includes `target_stack_top: True` flag so the LLM's
     `cast_spell` action automatically targets the current top of
     stack at cast time.
  3. Companion patch to compute_eligible_actions (in
     policy/eligible_actions.py) honors the target_stack_top flag:
     if state.stack is empty, the cast action is omitted (no legal
     target); otherwise default_targets = [state.stack[-1].entry_id].
"""
from api.engine.pillar_f.v0_2.playtest.counter_war.counterspell_annotations import (
    COUNTERSPELL_ANNOTATIONS_VERSION,
    COUNTERSPELL_FAMILY_NAMES,
    make_counterspell_annotation,
    attach_counterspell_annotation,
)

__all__ = [
    "COUNTERSPELL_ANNOTATIONS_VERSION", "COUNTERSPELL_FAMILY_NAMES",
    "make_counterspell_annotation", "attach_counterspell_annotation",
]
