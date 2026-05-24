"""Pillar F v0.2 sub-mega-task C — Stage 2 graduated playtest harness.

Plugs sub-B (LLM strategic policy, iter 11) into a multi-game
orchestrator that runs N=30-game cycles per deck through the iter-10
substrate, producing measured (not statistically predicted) win-rate
validation that the Pillar D dispatcher can read.

This package:
- Does NOT modify the iter-10 substrate.
- Builds on top of iter-11's policy callbacks (PriorityResponderFn,
  MulliganDeciderFn, BottomPickerFn).
- Owns the two deferred sub-B Phase 9 gates: combat multi-block
  (Phase 1 of sub-C) and counter-war depth >= 2 (Phase 2 of sub-C).
- Integrates with Pillar D's existing `agent_graduated_playtest_v1`
  Stage 1 orchestrator via an opt-in Stage 2 flag (Phase 5 of sub-C).

Module layout:
  orchestrator/   — pod construction + per-game runner (Phase 3)
  cycle/          — N-game cycle runner + cost ceiling (Phase 4, 6)
  combat_glue/    — LLM-driven attackers + blockers (Phase 1)
  counter_war/    — counter_target_spell resolver + card annotation
                    updates (Phase 2)
  aggregation/    — StageTwoReport aggregation + GREEN/YELLOW/RED
                    recommendation (Phase 4)
  reports/        — markdown + JSON report writers (Phase 4)

Sub-C scoping doc:
  ../../../MTG-Deck-Builder-Claude/pillar_f_v0_2_sub_c_stage_2_playtest_scoping.md
"""

PLAYTEST_VERSION = "pillar_f_v0_2_playtest_v1_sub_c"
