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

Committed as `72f146b43`.

---

## Phase 2 — Counter-war hook (counter_target_spell resolver) (2026-05-23)

**Owns sub-B Phase 9 deferred gate 6.**

**v11 substrate context.** v11 already shipped:
- `stack.counter_target(state, target_entry_id)` substrate primitive
- 7 counterspell-family resolvers in `cards/spell/counterspells.py`
  via `register_spell` (Counterspell, Negate, Mana Drain, Swan Song,
  Arcane Denial, An Offer You Can't Refuse, Fierce Guardianship).

**What sub-C added** in `playtest/counter_war/`:

1. **counterspell_annotations.py** —
   - `COUNTERSPELL_FAMILY_NAMES`: 14-card list per kickoff Phase 2 spec.
   - `_register_missing_counterspells()`: backfill resolvers for the
     7 family cards v11 didn't ship (Force of Will, Force of Negation,
     Dovin's Veto, Mindbreak Trap, Mental Misstep, Pact of Negation,
     Daze). Each uses substrate's counter_target; alt-cost side
     effects (FoW's exile-blue, Daze's bounce-Island) stubbed for
     iter-12+.
   - `make_counterspell_annotation(card_name)`: builds the
     iter10_annotation dict sub-B's compute_eligible_actions
     consumes -- includes `target_stack_top: True`.
   - `attach_counterspell_annotation(card)`: convenience helper to
     set on a Card instance at deck-build time.

2. **policy/eligible_actions.py patch** -- honors target_stack_top:
   - When set AND state.stack is empty: skip emitting the cast_spell
     action (no legal target).
   - When set AND state.stack is non-empty: resolve default_targets
     to `[state.stack[-1].entry_id]` so the counter actually points
     at something.

   Edit is policy-layer only; substrate untouched.

**Tests** in
`tests/pillar_f_v0_2_playtest/test_phase2_counter_war.py`:
11 tests across 4 classes:
- **CounterspellRegistrationTests** (4): all 14 family resolvers
  registered, annotation builder works for v11-shipped + sub-C-
  backfilled cards, unknown card rejected.
- **EligibleActionsCounterTests** (3): counter NOT eligible when
  stack empty, eligible when non-empty (with correct top entry_id
  in default_targets), targets resolve to TOP at cast time even with
  multiple stack entries.
- **CounterResolutionTests** (2): counterspell cast + resolve removes
  target from stack; substrate's counter_target primitive sanity.
- **CounterChainDepth3Test** (2): full 4-depth chain (P1 spell ->
  P2 counter -> P3 counter -> P0 counter) resolves correctly with
  correct cancellation pattern; simpler 3-depth chain where P3's
  counter saves P1's spell from P2's counter.

**All 11 pass. Full regression: 427/427 (224 substrate + 167 policy
+ 36 playtest).**

~150 LOC production + ~270 LOC test.

**Commit message:** "Phase 2 (mega-task v12): counter-war hook (counter_target_spell resolver + 14-card annotation helper)".

Committed as `1849aa963`.

---

## Phase 3 — Pod orchestrator + per-game runner (Mode A) (2026-05-23)

**Implementation** in `api/engine/pillar_f/v0_2/playtest/orchestrator/`:

1. **types.py** -- Dataclasses per scoping doc sections 2 + 3:
   - `StageTwoDeck(deck_id, commander_name, mainboard: List[str],
     archetype_hint, bracket)`. Validates deck_id + commander_name
     non-empty.
   - `StageTwoGameConfig(seed, decks: List[StageTwoDeck] (exactly 4),
     deck_under_test_pid, max_turns=25, max_mulligans=2,
     per_turn_cost_ceiling=$0.30, per_game_cost_ceiling=$5,
     enable_combat=True, starting_life=40)`. Validates pid + deck
     count.
   - `StageTwoGameResult` (game_idx, winner_pid, turns_run,
     halted_for_cost, halted_reason, elimination_order, final
     life/threat vectors, politics_summary, action_log,
     combat_decisions_log, total_spend, fallback_events, elapsed).
   - `StageTwoCycleConfig(deck_under_test, control_pool, n_games=30,
     parallelism=1, output_dir, cycle_cost_ceiling=$200,
     per-game knobs, seed_base)`.

2. **card_factory.py** -- name -> Card with appropriate annotation:
   - Basic lands (5 entries): type_line set; no annotation (sub-B's
     compute_eligible_actions handles play_land for any is_land card).
   - Counterspells (14 entries): delegates to Phase 2's
     `make_counterspell_annotation`.
   - Damage instants (5 entries: Lightning Bolt, Shock, Lava Spike,
     Searing Blaze, Skewer the Critics): iter10_annotation with
     deal_damage_to_player resolver + default_targets=[next opponent].
   - Known creatures (15 entries covering Krenko goblin tribal,
     mono-W soldiers, mono-U tempo, mono-B reanimator, Edgar vampire,
     Ur-Dragon tribal): power/toughness/keywords/cmc/mana_cost
     populated; no iter10_annotation (creatures fight via combat
     glue, not cast through sub-B's pipeline in iter-11).
   - Unknown card -> vanilla placeholder with explicit diag note
     (deck construction never crashes).

3. **game_runner.py** -- per-game orchestration:
   - `build_game_state(config) -> GameState`: 4 PlayerStates,
     starting_life, shuffled libraries per-deck (seeded for
     reproducibility).
   - `run_single_game(config, llm_client) -> StageTwoGameResult`:
     full mulligan (sub-B Phase 6) -> turn loop (sub-B priority
     responder + sub-C combat glue at DECLARE_ATTACKERS) ->
     game-end detection (3-of-4 lost OR max_turns OR per_game ceiling)
     -> result aggregation (threat vectors per surviving viewer,
     politics summary, fallback events, combat decision diagnostics).
   - Cost-halt tie-break: when per_game ceiling fires, winner =
     highest-life surviving player.
   - Damage events wired into politics_state in flight (post-resolve
     diff of life totals).

**Tests** in
`tests/pillar_f_v0_2_playtest/test_phase3_game_runner.py`:
16 tests across 4 classes:
- **StageTwoTypesTests** (3): deck/config validation.
- **CardFactoryTests** (6): basic land, counterspell annotation,
  damage instant targeting, known creature stats, unknown placeholder,
  is_x helpers.
- **BuildGameStateTests** (2): 4 players with 30-card libraries each,
  shuffle deterministic with seed.
- **RunSingleGameTests** (5): short game completes via max_turns,
  total_spend tracked, per_game ceiling halts + tie-breaks,
  threat_vectors + politics populated, result JSON-serializable.

**All 16 pass. Full regression: 443/443 (224 substrate + 167 policy
+ 52 playtest).**

~600 LOC production + ~310 LOC test.

Live multi-LLM end-to-end deferred to Phase 7 cycle smoke (single-
game LLM runs cost ~$0.50 each; pytest CI cost-effective only with
mocks).

**Commit message:** "Phase 3 (mega-task v12): pod orchestrator + per-game runner (Mode A) + StageTwo types + card factory".

Committed as `c9087987a`.

---

## Budget pivot — user dispatched $20 hard cap (2026-05-23)

Kickoff's nominal $400 budget unaffordable. User chose Option 1
(reduced live scope): build Phases 4-6 infrastructure + run Phase 7
as a 3-game mini-smoke (~$10-15) + defer Phase 8 to v13+. Phase 9
ships sub-C with the reduced-scope signal noted in the final
report.

Task list updated: Phase 8 marked completed-as-deferred; Phase 7
subject revised to "Live 3-game mini-smoke (REDUCED scope)".

---

## Phase 4 — Cycle runner + aggregation + report writer (2026-05-23)

**Implementation** in `api/engine/pillar_f/v0_2/playtest/`:

1. **aggregation/aggregator.py** -- `aggregate_cycle(
   deck_under_test_id, deck_under_test_archetype, game_results,
   halted_for_cycle_cost) -> StageTwoReport`:
   - Win-rate: wins / total games where deck-under-test pid won.
   - Avg turn eliminated when lost: only counts losses.
   - Damage proxy: politics damage_log_count summed (iter-12+ refines).
   - Politics aggregation: per-seat alliance distribution rolled up;
     deals made + honored tallied; games-with-alliance-transition +
     games-with-deals counted.
   - Cost summary: total + per-game avg/max + halted-for-cost count
     + fallback events.
   - Combat summary: games with attacks, games with multi-block,
     attacker/blocker decision counts, combat fallbacks.
   - Recommendation tier: **GREEN** (win_rate >= 0.30 AND (no losses
     OR avg_turn_eliminated >= 12)); **YELLOW** (win_rate >= 0.20
     OR survives mid-game); **RED** (deck folds early); **INCOMPLETE**
     (cycle halted for cost or no games completed).
   - Tier-tuning note: "no losses" satisfies the survives-mid-game
     gate vacuously (else a perfect 30/30 win record would be YELLOW
     because avg_turn_eliminated would be 0).
   - Per-game brief: game_idx, seed, outcome (WIN/LOSS/DRAW),
     turns_run, spend for the report's "notable games" section.

2. **reports/writer.py**:
   - `write_cycle_report_json(report, path)`: dataclass -> JSON dict
     via recursive asdict; int keys -> str for JSON compatibility.
   - `write_cycle_report_markdown(report, path)`: 8-section layout
     (header + win-rate + damage + politics + cost + combat +
     notable games + recommendation prose). Notable-games section
     picks the most-decisive-win + most-decisive-loss from per-game
     brief.
   - `write_per_game_json(game_result, path)`: per-game JSON for
     replay tooling.

3. **cycle/cycle_runner.py** -- `run_stage_two_cycle(cycle_config,
   llm_client, progress_callback) -> StageTwoReport`:
   - For game_idx in range(n_games): build per-game config (seed =
     seed_base + idx; decks = [deck_under_test, 3 controls sampled
     from pool]); call run_single_game; persist
     game_<idx:03d>.json; accumulate cost; halt + mark INCOMPLETE if
     cycle_cost_ceiling exceeded.
   - Aggregate via aggregate_cycle.
   - Write cycle.json + cycle_report.md to output_dir.
   - Returns StageTwoReport (also written to disk if output_dir set).
   - Serial execution (parallelism=1 iter-11; asyncio is iter-12+).
   - Control-deck picker: random.sample if pool >= 3; cycle else.
     Color-anti-stack heuristic deferred to iter-12+ (curator's
     responsibility).

**Tests** in
`tests/pillar_f_v0_2_playtest/test_phase4_cycle.py`:
11 tests across 3 classes:
- **AggregatorWinRateTests** (6): all-wins -> GREEN, zero-wins -> RED,
  borderline 0.20 winrate -> YELLOW, halted-for-cycle-cost ->
  INCOMPLETE, avg_turn_eliminated counts losses only, politics
  aggregation across games.
- **ReportWriterTests** (2): markdown has all 8 sections, JSON round-
  trips with correct fields.
- **CycleRunnerTests** (3): 3-game cycle completes with mocks, all
  artifacts written (cycle.json + cycle_report.md + per-game JSONs),
  cycle halts on cycle_cost_ceiling.

**All 11 pass. Full regression: 454/454 (224 substrate + 167 policy
+ 63 playtest).**

**Aggregator tuning fix.** Initial GREEN gate required
`avg_turn_eliminated >= 12` -- which excluded perfect-record decks
that never lost (avg = 0). Patched the recommendation logic to treat
"no losses" as satisfying the survives-mid-game gate vacuously.
Captured in `survives_mid_game` boolean.

~420 LOC production + ~310 LOC test.

**Commit message:** "Phase 4 (mega-task v12): cycle runner + aggregation + markdown/JSON report writer".
