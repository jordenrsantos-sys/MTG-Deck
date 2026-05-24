# Mega-task v12 / iter 12 / Pillar F v0.2 sub-mega-task C — progress log

Iter 12 dispatch on top of v10 ship (`a53cfcb68`) + v11 ship (`c105285c5`,
parallel arc, already merged). Kickoff:
`mega_task_v12_kickoff.md`. Sub-mega-task C only — Stage 2 graduated
playtest harness + the two deferred sub-B Phase 9 gates (combat
multi-block, counter-war depth >= 2).

10 phases (0-9). Budget: $400 API spend, ~4-5 weeks CC.

---

## Phase 0 — Pre-flight + scoping read + module scaffold (2026-05-23)

**Baseline verified:**
- `pytest tests/ -q -x` -> 2234 pass + 25 skip + 88 subtests passed
  in 258s. Matches iter-11 baseline.
- vitest baseline 774 + 2 pre-existing fails (unchanged) -- verified
  in v10 Phase 10; not re-run for Phase 0.

**Scoping doc + reports read:**
- `MTG-Deck-Builder-Claude/pillar_f_v0_2_sub_c_stage_2_playtest_scoping.md`
  (sub-C scoping; this iter implements it).
- `MTG-Deck-Builder-Claude/mega_task_v10_final_report.md` (sub-B ship
  state + deferred gates 5/6 owned by sub-C here).
- `MTG-Deck-Builder-Claude/pillar_f_v0_2_sub_b_llm_policy_scoping.md`
  (sub-B prompt-template + cost-tracker contracts sub-C reuses).
- `api/engine/pillar_f/v0_2/` walk-through (substrate + policy + cards).
- `api/engine/layers/agent_graduated_playtest_v1.py` (Stage 1 orchestrator
  sub-C extends in Phase 5).

**Substrate boundary confirmed:** sub-C will ONLY add via
`register_resolver` (Phase 2) and the existing
`combat.declare_attackers` / `combat.declare_blockers` / callback
APIs (Phase 1). No state/stack/turn/replacement/layers/combat edits.

**Module scaffold created** at `api/engine/pillar_f/v0_2/playtest/`:

```
playtest/
  __init__.py            (PLAYTEST_VERSION)
  orchestrator/__init__.py   (Phase 3)
  cycle/__init__.py          (Phase 4 + Phase 6)
  combat_glue/__init__.py    (Phase 1)
  counter_war/__init__.py    (Phase 2)
  aggregation/__init__.py    (Phase 4)
  reports/__init__.py        (Phase 4)
```

**Coordination note.** v11 (per-card oracle compilation) shipped
already as `c105285c5`. Its module tree (`pillar_f/v0_2/cards/`) is
disjoint from sub-C's (`pillar_f/v0_2/playtest/`). No conflict
expected.

**Commit message:** "Phase 0 (mega-task v12): pre-flight + scoping read + playtest module scaffold".

Committed as `0eaeeb7fc`.

---

## Phase 1 — Combat hook glue (LLM-driven attackers + blockers) (2026-05-23)

**Owns sub-B Phase 9 deferred gate 5.**

**Implementation** in `api/engine/pillar_f/v0_2/playtest/combat_glue/`:

1. **combat_decider.py** (~400 LOC) — three public layers:
   - **Eligibility primitives** — pure substrate-readers:
     `compute_eligible_attackers(state, active_player)` filters
     active player's untapped creatures via substrate's
     `combat.can_attack(card, chars)`; `compute_attack_targets(state,
     active_player)` enumerates alive opponents + their planeswalkers;
     `compute_eligible_blockers(state, defending_player)` returns
     untapped creatures (summoning-sick OK per CR 509.1);
     `compute_attackers_to_block(combat_state, defending_player,
     state)` returns only attackers targeting THIS defender (player
     OR their planeswalker -- planeswalker controller lookup).
   - **Factory deciders**:
     `make_llm_attacker_decider(llm_client, cost_tracker,
     politics_state_by_player, deck_archetype_hint_by_player,
     decision_log)` returns `(state, active_player) ->
     List[AttackerDeclaration]`. Builds sub-B Phase 4 attackers
     prompt, calls LLM, parses, re-prompts up to 2x on
     parse/validation failure, falls back to "no attack" on 3rd
     failure. Cost recorded with `purpose="combat_attackers"`. Defense-
     in-depth: re-validates each declaration via `can_attack` after
     parser conversion.
     `make_llm_blocker_decider(...)` returns `(state, combat_state,
     defending_player) -> List[BlockerAssignment]`. One call per
     defender. Cost purpose `"combat_blockers"`. Preserves the LLM's
     multi-block damage-assignment order (CR 510.1c: active player
     normally chooses; iter-11 takes LLM's blocker_indices order).
   - **End-to-end orchestrator**:
     `run_llm_combat_phase(state, active_player, attacker_decider,
     blocker_decider) -> (CombatState, action_log)`. Walks the
     substrate's `declare_attackers` -> per-defender
     `declare_blockers` -> `first_strike_phase_active` + 1-2
     `deal_combat_damage` passes flow. SBA loop runs inside the
     substrate; this glue feeds declarations only.

2. **CombatDecisionRecord** dataclass for diagnostics: phase,
   player_id, turn, eligible_count, llm_calls_made, parse_failures,
   fallback_used, final_count, rationale. Phase 4's cycle aggregation
   reads this for reporting.

**Cost guardrails honored.** Both deciders consult
`cost_tracker.game_halted_for_cost` + `is_player_in_fallback` BEFORE
any LLM call. Records cost via shared `CostTracker` so combat calls
bill into the same per-turn/per-game ceilings as priority +
mulligan.

**Tests** in
`tests/pillar_f_v0_2_playtest/test_phase1_combat_glue.py`:
25 tests across 6 classes:
- **EligibleAttackersTests** (6): untapped legal, tapped excluded,
  summoning-sick excluded, summoning-sick + haste included,
  defender keyword excluded, non-creature excluded.
- **AttackTargetsTests** (3): alive opponents listed, eliminated
  opponent excluded, planeswalker target added.
- **EligibleBlockersTests** (2): summoning-sick blocker legal,
  tapped blocker excluded.
- **AttackersToBlockTests** (1): only incoming attackers per defender.
- **AttackerDeciderTests** (6): parsed declarations returned, no
  eligible -> empty (no LLM call), fallback on 3 parse failures,
  game_halted skip, player-in-fallback skip, cost recorded with
  purpose.
- **BlockerDeciderTests** (4): parsed assignments returned, no
  attackers -> empty (no LLM call), no blockers -> empty, multi-
  block assignment order preserved.
- **RunLLMCombatPhaseTests** (3): unopposed attack damages player,
  blocked attack absorbs damage on creatures, no-attack returns
  empty.

**All 25 pass.** ~400 LOC production + ~330 LOC test.

Live multi-block + first-strike + trample integration deferred to
Phase 7 cycle smoke (each combat live test costs $0.03-0.05; the
unit suite covers the structural cases mockable without LLM).

**Commit message:** "Phase 1 (mega-task v12): combat hook glue (LLM-driven attackers + blockers)".
