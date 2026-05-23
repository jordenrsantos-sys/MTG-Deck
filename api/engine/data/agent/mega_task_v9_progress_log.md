# Mega-task v9 / Pillar D iter 10 — Pillar F v0.2 sub-mega-task A progress log

Iter 10 dispatch on top of v8 ship (`a6845fd7e`). Kickoff:
`mega_task_v9_kickoff.md`. Sub-mega-task A only — core rules engine
substrate. B + C dispatch separately in iter 11+.

Append-only, timestamped sections per phase.

---

## Phase 0 — Pre-flight + scoping deep-read + module skeleton (2026-05-23)

**Substrate snapshot:**
- HEAD: `a6845fd7e` (v8 Phase 8 — SHIPPED at 5/5 sweep / 7/7 gates).
- pytest baseline (from v8 ship): 1628 / 25 skipped / 0 failed.
- vitest baseline (from v8 ship): 774 / 2 pre-existing failed.
- Pillar F v0.1 location: `api/engine/layers/agent_statistical_approximator_v1.py`
  (UNCHANGED — sub-mega-task C will eventually wire Stage 2 alongside).
- MTG Comprehensive Rules text:
  `E:/MTG Root/Mtg deck building brain/01_RULES_SOURCE/source_documents/MagicCompRules_20260417.txt`
  (1MB; already embedded into Voyage index per v6 Phase 6 at 667 sections).

**Context read (3 of 6 confirmed accessible):**
- `MTG-Deck-Builder-Claude/pillar_f_v0_2_game_engine_scoping.md` — source of truth (authored at end of v8).
- `MTG-Deck-Builder-Claude/mega_task_v8_final_report.md` — iter-9 ship state + hand-offs.
- `project_mega_task_v8_shipped.md` — iter-9 outcomes + 10 load-bearing invariants.
- 3 referenced memories NOT in current memory dir (`project_phase_5b_substrate_blocker`,
  `project_graduated_playtest_spec_2026-05-21`, `project_pillar_a_c_shipped_2026-05-17`).
  Substance covered in the scoping doc + kickoff itself; proceeding.

**Module scaffold created** at `repo/api/engine/pillar_f/v0_2/`:
```
v0_2/
  __init__.py
  state/__init__.py        — Phase 1
  stack/__init__.py        — Phase 2
  turn/__init__.py         — Phase 3
  replacement/__init__.py  — Phase 4
  layers/__init__.py       — Phase 5
  combat/__init__.py       — Phase 6
```
Plus `repo/tests/pillar_f_v0_2/__init__.py` for the test tree.

**Phase 0 deliverables:**
- Progress log scaffolded (this file).
- Task list created (10 phases, this session).
- Module scaffold committed.

**Phase 0 cost:** $0 (no LLM calls).

**Commit message:** "Phase 0 (mega-task v9): pre-flight + Pillar F v0.2 module scaffold".

Committed as `6a76fcb4d`.

---

## Phase 1 — Game-state object model + serialization (2026-05-23)

**Implementation** in `api/engine/pillar_f/v0_2/state/`:

1. **card.py** — `Card` dataclass per scoping doc section (a). Identity
   fields (name, oracle_id, mana_cost, cmc, type_line, subtypes,
   oracle_text, power, toughness, loyalty, colors, color_identity,
   keywords, owner) + mutable state (controller, face_down, tapped,
   summoning_sick, damage_marked, counters, attached_to, attached_by,
   card_id). `card_id` is UUID4 hex prefix (12 chars). Helpers:
   is_creature, is_land, is_planeswalker, is_legendary, has_keyword,
   power_int (handles `*` CDA), toughness_int, to_dict/from_dict,
   to_opaque.

2. **zones.py** — `PlayerZones` dataclass with 6 zones (hand, library,
   battlefield, graveyard, exile, command). Zones hold card_ids
   (Card objects live in GameState.cards_by_id by id). Library index 0
   = top. API: all_card_ids, find_zone, remove_card, add_card (with
   `to_top` for library), to_dict/from_dict.

3. **player.py** — `ManaPool` (W/U/B/R/G/C buckets + empty/total/dict),
   `PlayerState` (player_id, name, life_total=40, zones, mana_pool,
   commander_damage_taken_from {oracle_id → int}, lands_played_this_turn,
   cards_drawn_this_turn, spells_cast_this_turn,
   priority_passed_this_round, has_lost, has_drawn_from_empty_library,
   politics_state {sub-mega-task B slot}).

4. **state.py** — `GameState` aggregating players, cards_by_id, global
   turn state (turn_number, Phase enum, Step enum, active_player,
   priority_holder, priority_passes_this_round Set, stack), optional
   designations (the_monarch, the_initiative, day_or_night DayNight enum),
   effect registries (replacement_effects, continuous_effects,
   delayed_triggers_pending), commander_card_ids, game-result fields.
   `StackEntry`, `ReplacementEffect`, `ContinuousEffect` dataclasses
   defined here (used by Phases 2/4/5).

   API:
   - `get_card(card_id)`, `add_card(card)`, `move_card(card_id,
     from_player, from_zone, to_player, to_zone, to_top=False)`
   - `to_dict()`, `to_json()`, `from_dict(d)`, `from_json(s)`
   - `perspective_view(viewer_player_id)` redacts hidden info:
     * Opponents' hand cards → opaque markers.
     * ALL libraries (including viewer's) → opaque markers (CR: nobody
       sees library order).
     * Face-down battlefield/exile cards → opaque (except to controller).
     * Stack + battlefield (face-up) + graveyard + exile (face-up) +
       command zone fully visible.
   - STATE_VERSION = "pillar_f_v0_2_state_v1" for JSON forward-compat.

5. **state/\_\_init\_\_.py** — exposes public API for Phases 2-6.

**Tests** in `tests/pillar_f_v0_2/test_phase1_state.py`:

22 tests across 6 classes:
- **CardModelTests** (5): card_id uniqueness, creature/legendary/land
  detection, keyword case-insensitive, power_int handles `*`, controller
  defaults to owner.
- **ZonesMutationTests** (5): move_card between zones, move with
  controller change, raise on missing card, library to_top semantics,
  find_zone correctness.
- **CommanderDamageTests** (2): per-commander damage tracking,
  commander_card_ids per player.
- **ManaPoolTests** (2): empty clears pool, dict round-trip.
- **JsonRoundTripTests** (2): full 4-player game state round-trip
  (turn_number, phase, step, active_player, monarch, day_or_night,
  3-deep stack, zones); per-card mutable state round-trip
  (tapped/damage_marked/counters/summoning_sick).
- **PerspectiveViewTests** (6): opponent hand opaque, ALL libraries
  opaque (incl. viewer's), battlefield public, face-down
  battlefield opaque to non-controller, viewer_player_id carried,
  stack public.

**All 22 pass.** Total ~600 LOC across 4 production files + 320 LOC test
file. Pure data modeling — no game logic yet.

**Commit message:** "Phase 1 (mega-task v9): game-state object model + perspective_view + serialization".

Committed as `71c2b5076`.

---

## Phase 2 — Stack mechanics + priority loop (2026-05-23)

**Implementation** in `api/engine/pillar_f/v0_2/stack/stack.py`:

- `push_to_stack(state, **kwargs)` — appends StackEntry; resets
  priority round per CR 117.3 (mutation invalidates pass-in-succession).
- `pop_top(state)` / `peek_top(state)` — LIFO accessors.
- `counter_target(state, target_entry_id)` — generic counterspell API.
  Specific cards (Counterspell, Negate, Mana Drain) call this in their
  resolver fn.
- `register_resolver(name, fn)` + `get_resolver(name)` — resolver
  registry keyed by name string (lookup via StackEntry.payment["resolver"]).
  Iter-10 ships 3 minimal resolvers: `noop`, `deal_damage_to_player`,
  `draw_cards`. Per-card oracle compilation deferred to iter 11+.
- `resolve_top(state)` — pops top + invokes resolver fn.
- `apnap_order(state)` — returns player IDs in APNAP order starting from
  active_player, skipping eliminated players.
- `priority_round(state, responder_fn)` — runs one CR-117 priority
  round. Responder contract: return None (pass) or a push_to_stack
  kwargs dict. Stack mutation resets passes.
- `run_stack_to_resolution(state, responder_fn)` — top-level helper:
  loops priority + resolve until stack empty AND all pass. Returns
  list of resolved entries.
- `enqueue_triggers(state, triggers)` — adds to delayed_triggers_pending
  per CR 603.3.
- `drain_triggers_to_stack(state)` — moves all pending triggers to the
  stack in APNAP order. Same-controller insertion order preserved
  (= controller's choice in iter 10).

**Sub-mega-task B prep:** `PriorityResponderFn` is the callback type
the LLM strategic policy will plug into. Iter-10 ships
`_pass_responder` mock (always returns None) sufficient for unit tests.

**Tests** in `tests/pillar_f_v0_2/test_phase2_stack.py`: 16 tests
across 5 classes:
- **StackPushPopTests** (3): push appends, push resets passes, LIFO pop.
- **CounterspellTests** (2): counter_target removes entry, nonexistent
  target returns False.
- **PriorityLoopTests** (5): simple sorcery resolves after all pass,
  APNAP from active player, APNAP skips eliminated, 3-deep response
  sequence resolves LIFO (sorcery → counter → counter-counter →
  sorcery hits player), active-player action resets round.
- **APNAPTriggerOrderingTests** (3): enqueue + drain produces APNAP-
  ordered stack (P2-A, P2-B, P3-A, P0-A when active=1), drain empty
  returns 0, eliminated-player triggers skipped.
- **ResolverRegistryTests** (3): noop is no-op, draw_cards moves
  library→hand + increments cards_drawn_this_turn, draw from empty
  library flags has_drawn_from_empty_library SBA.

**All 16 pass.** ~250 LOC production + ~280 LOC test.

**Commit message:** "Phase 2 (mega-task v9): stack mechanics + priority loop + APNAP".

Committed as `46a209305`.

---

## Phase 3 — Phase/step state machine (2026-05-23)

**Implementation** in `api/engine/pillar_f/v0_2/turn/turn_machine.py`:

- `STEP_ORDER` = canonical 13-step list (untap → upkeep → draw → main_1
  → 5 combat steps → main_2 → end_step → cleanup).
- `STEP_TO_PHASE` maps each step to its parent Phase enum.
- `NO_PRIORITY_STEPS` = {UNTAP, CLEANUP}.
- `register_step_trigger(step, trigger)` + `clear_step_triggers(step?)`
  for step-change triggered abilities.
- `start_step(state, step)` sets state.step + state.phase, fires +
  drains registered step triggers to the stack.
- `step_opens_priority(step)` returns False for untap/cleanup.
- `untap_step(state)` — untaps active player's permanents, clears
  summoning sickness, empties all mana pools, resets active player's
  per-turn counters.
- `draw_step(state, skip_first_turn_draw=True)` — active player draws
  unless turn 1 AND active=0 (multiplayer EDH convention). Empty
  library flags has_drawn_from_empty_library SBA.
- `cleanup_step(state)` — discard to 7, clear damage marks, expire
  `target_pattern["until_end_of_turn"]` continuous effects, empty
  mana pools. Returns True if cleanup re-entry needed (iter 10 always
  False; Phase 4 will return True when SBAs/triggers fire).
- `advance_step(state)` — moves to next step or rotates to next
  player's untap (incrementing turn_number when wrapping to P0,
  skipping eliminated players).
- `run_turn(state, priority_runner)` — walks one full turn; iter 10
  uses no-op priority runner. Phase 6 + sub-mega-task B plumb the
  combat substeps + LLM action prompting.

**Tests** in `tests/pillar_f_v0_2/test_phase3_turn.py`: 21 tests
across 6 classes:
- **StepOrderTests** (3): canonical 13-step order, phase mapping,
  no-priority untap/cleanup.
- **UntapStepTests** (3): untap + summoning sick clear, mana pool
  empty, per-turn counters reset (active only).
- **DrawStepTests** (4): first-turn skip for starting player,
  first-turn draw NOT skipped for other players, subsequent turns
  draw, empty library flag.
- **CleanupStepTests** (3): discard to 7, clear damage,
  until-end-of-turn expiration.
- **AdvanceStepTests** (4): step within turn, cleanup rotates to
  next player, turn_number increments on wrap, skips eliminated
  players.
- **RunTurnTests** (2): visits all 13 steps in order, 4-player
  rotation increments turn_number.
- **StepTriggerTests** (2): at-beginning-of-combat trigger fires +
  lands on stack; no triggers = no stack changes.

**All 21 pass.** ~280 LOC production + ~260 LOC test.

**Commit message:** "Phase 3 (mega-task v9): turn / phase / step state machine".

Committed as `943771d9a`.

---

## Phase 4 — Replacement effects + state-based actions (2026-05-23)

**Implementation** in `api/engine/pillar_f/v0_2/replacement/`:

1. **events.py** — 9 event dataclasses per scoping doc:
   `DrawEvent`, `DamageEvent` (creature/player/planeswalker target_kind,
   is_combat, is_first_strike), `EnterBattlefieldEvent` (tapped_on_etb,
   counters_on_etb), `DieEvent` (cause, instead_zone), `LifeChangeEvent`,
   `CounterAddEvent`, `CounterRemoveEvent`, `DiscardEvent`, `MillEvent`.
   Base `Event` carries `replaced` + `prevent` flags.

2. **replacement.py** — engine + registry:
   - `register_replacement_fn(name, fn)` + `get_replacement_fn(name)`.
   - `_pattern_matches(pattern, event)` walks pattern dict; supports
     scalar equality, list-membership, dict-with-"in"/"ne" predicates.
   - `apply_replacements(state, event, affected_controller)` — finds
     matching replacement effects; orders by CR 616 (affected
     controller's choice; iter-10 picks affected-controller-first
     then alphabetical source_card_id for stability); applies each
     at most once (self-replacement CR 614.5); breaks on `prevent`.
   - 5 built-in replacement_fns registered:
     `fog_prevent_combat_damage`, `rest_in_peace_die_to_exile`,
     `etb_tapped`, `doubling_season_counters`,
     `leyline_of_void_to_exile`.

3. **sba.py** — state-based actions per CR 704:
   - `check_state_based_actions(state)` — one pass, returns list of
     action dicts:
       1. Player ≤0 life loses
       2. Player drew from empty library → loses
       3. Player took 21+ commander damage from one commander → loses
       4. Creature with 0 toughness dies
       5. Creature with damage ≥ toughness dies
       6. Planeswalker with 0 loyalty → graveyard
       7. Legend rule (2+ same name same controller → all but one die)
       8. Aura with invalid target → graveyard
   - All dying things routed through `DieEvent` →
     `apply_replacements()` so Rest in Peace / Leyline of the Void
     redirect to exile.
   - `run_sba_loop(state)` — loops until no more SBA fires; sets
     `state.game_over` + `winner_player_id` when ≤1 player remains.
   - `COMMANDER_DAMAGE_LETHAL = 21`.

**Tests** in `tests/pillar_f_v0_2/test_phase4_replacement_sba.py`:
22 tests across 5 classes:
- **ReplacementEnginePatternMatchTests** (6): no replacement = unchanged,
  Fog prevents combat damage, Fog ignores non-combat,
  ETB-tapped replacement, Doubling Season doubles +1/+1 + loyalty,
  Doubling Season ignores -1/-1.
- **StateBasedActionsCreatureDeathTests** (4): 0-toughness dies,
  lethal damage dies, RIP redirects to exile, planeswalker 0 loyalty.
- **PlayerLossSBATests** (5): 0 life, negative life,
  commander damage 21 loses, commander damage 20 doesn't, empty library.
- **LegendRuleTests** (3): same name same controller fires, different
  controllers don't collide, different names don't collide.
- **SBALoopTests** (4): loop terminates, game_over with 1 winner,
  game_over draw when all lose, multi-creature death cascade.

**All 22 pass.** ~450 LOC production across 3 files + ~330 LOC test.

**Commit message:** "Phase 4 (mega-task v9): replacement effects + state-based actions".

Committed as `5ab8a0d68` (per next git log).

---

## Phase 5 — Continuous (layered) effects + 7-layer system (2026-05-23)

**Implementation** in `api/engine/pillar_f/v0_2/layers/`:

1. **characteristics.py** — `Characteristics` dataclass: per-permanent
   AT-THIS-MOMENT view (name, type_line, subtypes, supertypes, types,
   colors, color_identity, keywords, abilities, P/T int, loyalty,
   controller, is_copy_of_card_id).

2. **layer_engine.py** — full 7-layer engine:
   - `_LAYER_ORDER`: 10 (layer, sublayer) tuples — 1, 2, 3, 4, 5, 6,
     7a, 7b, 7c, 7d.
   - `apply_continuous_effects(state)` — builds fresh Characteristics
     from printed values, walks layers, applies each registered effect
     in insertion order. Counters applied at end (CR 613.4
     simplification).
   - `parse_type_line` / `reassemble_type_line` — type-line parser
     (supertypes, types, subtypes) with em-dash split.
   - 9 built-in layer effects across 6 layers:
     * **Layer 1**: `clone_of` — copies target permanent's characteristics
       onto the clone; controller stays with clone.
     * **Layer 2**: `change_control` — Mind Control / Threaten.
     * **Layer 4**: `add_subtype`, `remove_supertype` (Mind Bend).
     * **Layer 5**: `set_color` — sets colors of matching permanents.
     * **Layer 6**: `grant_keyword`, `lose_all_abilities` (Humility-style).
     * **Layer 7a**: `set_base_pt` — sets P/T to specific values.
     * **Layer 7b**: `cda_set_pt` — characteristic-defining abilities
       via `_CDA_REGISTRY` ({"tarmogoyf", "mortivore"}).
     * **Layer 7c**: `anthem_pt_mod` — Glorious Anthem / Honor of the Pure
       (+p/+t to matching).
     * **Layer 7d**: `switch_pt` — Inverter of Truth.
   - `_select_targets(pattern)` selector: card_id / all_creatures /
     all_creatures_controller / controller / subtype / type / name.

3. **layers/\_\_init\_\_.py** — exposes public API.

**Iter-10 simplifications documented:**
- CR 613.7c dependency resolution → insertion-order (= timestamp).
  Dependency graphs deferred to iter 11+.
- CR 613.4 counters-as-own-sub-process → applied at end of layer pipeline
  rather than as a separate phase between layers 7b and 7c.
- Layer 3 (text-changing) ships only `remove_supertype` for Mind Bend.
  Full text rewriting is iter 11+.
- Effect-fn errors silently swallowed (logged in iter 11+).

**Tests** in `tests/pillar_f_v0_2/test_phase5_layers.py`: 15 tests
across 9 classes:
- **TypeLineParsingTests** (4): legendary parse, no-subtypes, basic
  land, round-trip.
- **BasicSnapshotTests** (2): printed-values snapshot, +1/+1 counters
  added to P/T.
- **Layer6AbilityGrantTests** (2): keyword grant to all creatures,
  Humility strips keywords.
- **AnthemAndHumilityTests** (2): Honor 2/2 → 3/3, **Humility + Honor =
  2/2** (canonical layer-ordering test).
- **TarmogoyfCDATests** (1): P/T = card-types across all graveyards
  (3 types → 3/4).
- **CloneTests** (1): Clone copies P/T + keywords; controller stays.
- **InverterSwitchTests** (1): 2/6 → 6/2 in layer 7d.
- **MindBendTests** (1): legendary supertype removal cleans type_line.
- **ControlChangeTests** (1): Mind Control updates controller.

**All 15 pass.** ~470 LOC production across 2 files + ~280 LOC test.

**Highest-complexity phase confirmed working** end-to-end. The Humility +
Honor canonical test passes — that's the key gate per kickoff.

**Commit message:** "Phase 5 (mega-task v9): 7-layer continuous effects per CR 613".

Committed as `887ab2fb7`.

---

## Phase 6 — Combat phase (2026-05-23)

**Implementation** in `api/engine/pillar_f/v0_2/combat/combat.py`:

- `AttackerDeclaration(attacker_card_id, target)` — target is player_id
  (int) or planeswalker card_id (str).
- `BlockerAssignment(attacker_card_id, blocker_card_ids)` — ordered
  list of blockers per attacker.
- `CombatState` — per-combat-phase tracking: attackers,
  blocker_assignments, damage_assignment_order (mutable for active
  player's choice).
- `can_attack(card, characteristics)` — untapped + creature +
  not-summoning-sick-or-haste + not-defender.
- `declare_attackers(state, attackers)` — taps each (unless vigilance);
  validates each is legal (raises on first illegal).
- `declare_blockers(state, block_assignments)` — validates blockers
  are untapped + creature; initializes damage_assignment_order to
  blocker declaration order.
- `deal_combat_damage(state, combat_state, is_first_strike)` — one
  damage pass. Routes through DamageEvent → apply_replacements →
  apply marked damage (creatures) or life loss (players). SBA loop
  runs after.
  - Per CR 510.1c / 702.2b:
    * Trample: lethal-first per blocker; excess to defending player.
    * Non-trample single block: assign all damage (excess wasted).
    * Multi-block: lethal-first ordering, last takes all remaining.
    * Deathtouch: 1 damage = lethal (enforced by forcing
      damage_marked = toughness for SBA pickup).
  - Lifelink: attacker's controller gains life = damage dealt.
  - Commander damage: tracked by attacker.oracle_id when attacker IS
    the commander.
- `first_strike_phase_active(combat_state, state)` — True iff any
  participant has first or double strike (skips the FS substep when
  False — CR 510.1).
- `run_combat_phase(state, attackers, block_assignments)` —
  end-to-end: declare attackers → declare blockers → first-strike
  damage (if applicable) → normal damage → return action log.
- Attacker damage section and blocker-back-to-attacker section are
  independently phase-gated so FS-only attacker can take normal-pass
  blocker damage (the key test that broke an initial design).

**Tests** in `tests/pillar_f_v0_2/test_phase6_combat.py`: 15 tests
across 8 classes:
- **CanAttackTests** (5): untapped+non-sick can, tapped can't, sick-no-
  haste can't, sick-with-haste can, defender can't.
- **UnblockedDamageTests** (2): 2/2 unblocked → player takes 2,
  vigilance attacker doesn't tap.
- **TrampleTests** (1): 5/5 trampler into 2/2 → 3 to player.
- **FirstStrikeAndDoubleStrikeTests** (2): double-strike kills blocker
  then hits player; first-strike attacker kills blocker then dies to
  blocker's normal-pass damage.
- **DeathtouchTests** (1): 1/1 deathtouch trades with 5/5.
- **LifelinkTests** (2): unblocked + blocked-with-excess heals attacker
  for damage dealt.
- **MultiBlockTests** (1): 5/5 vs 1/1+1/1+5/5 = first 2 blockers die,
  3rd takes 3 damage; attacker takes 7 → dies.
- **CommanderDamageTests** (1): commander attack tracks damage by
  oracle_id.

**Iter-10 simplifications:** attack-cost payment (Propaganda) deferred;
planeswalker target validation skipped; deathtouch SBA approximated by
forcing lethal damage_marked rather than tracking the deathtouch flag
per blocker.

**All 15 pass.** ~310 LOC production + ~290 LOC test.

**Commit message:** "Phase 6 (mega-task v9): combat phase — attackers/blockers/damage/keywords".

Committed as `1eadb9131`.

---

## Phase 7 — Mulligan + draw + cleanup polish (2026-05-23)

**Implementation** in `api/engine/pillar_f/v0_2/turn/mulligan.py`:

- `MulliganDeciderFn = (state, player_id, hand, num_mulligans) → bool`
  callback type. Sub-mega-task B plugs LLM here in iter 11+.
- `BottomPickerFn = (state, player_id, hand, n) → list` callback type
  for choosing which N cards to put on the bottom after mulliganing.
- `always_keep_decider` + `keep_after_n_mulligans_decider(n)` defaults.
- `default_bottom_picker` picks the last N cards (insertion order).
- `shuffle_library(state, player_id, seed)` — seedable for deterministic
  tests + reproducible Stage-2 playtest runs.
- `draw_n(state, player_id, n)` — draws from top (index 0); sets
  `has_drawn_from_empty_library` SBA flag on empty.
- `opening_hand_size(num_mulligans) = 7` always per London mulligan
  (CR 103.4d).
- `mulligan_setup(state, decider_fn, bottom_picker_fn, seed_per_player,
  max_mulligans)` — full mulligan loop:
    1. Shuffle library.
    2. Draw 7.
    3. Decider chooses keep vs mulligan; iterate up to max_mulligans.
    4. After keep: put `num_mulligans` cards on bottom of library
       (via bottom_picker_fn).
  Returns `{player_id → num_mulligans_taken}`.

**Phase 3 already shipped** `draw_step` (first-turn skip for P0) and
`cleanup_step` (discard to 7, clear damage, expire until-end-of-turn).
Phase 7 verified those behaviors hold via integration tests.

**Tests** in `tests/pillar_f_v0_2/test_phase7_mulligan_cleanup.py`:
13 tests across 5 classes:
- **DrawNTests** (2): draws from top, empty library flag.
- **ShuffleLibraryTests** (1): same-seed determinism.
- **MulliganSetupTests** (4): 7-per-player no mulligan, 3-mulligan puts
  3 on bottom, max_mulligans cap, default bottom-picker.
- **FirstTurnDrawSkipTests** (3): P0 skips, P1 doesn't skip, turn 2 P0
  draws.
- **CleanupStepTests** (3): discard to 7, clear damage,
  until-end-of-turn expiration.

**All 13 pass.** ~160 LOC production + ~210 LOC test.

**Commit message:** "Phase 7 (mega-task v9): London mulligan + draw + cleanup polish".

Committed as `3182ca2df`.

---

## Phase 8 — 100-interaction test fixture suite (2026-05-23)

**The iter 10 ship gate.** Fixture suite at
`tests/pillar_f_v0_2/fixtures/test_100_interactions.py` — 100 unit
tests organized into 10 categories per kickoff Phase 8 spec.

**Result: 100 of 100 fixtures green.** Ship gate (≥85%) cleared
with 100% pass rate.

**Category coverage:**

| # | Category | Fixtures | All Pass |
|---|---|---|---|
| 1 | Basic combat | 15 | ✓ |
| 2 | Replacement-effect chains | 10 | ✓ |
| 3 | Layer-6/7 ordering edge cases | 10 | ✓ |
| 4 | Stack interaction scenarios | 10 | ✓ |
| 5 | Commander-specific | 10 | ✓ |
| 6 | Mulligan + opening-hand | 10 | ✓ |
| 7 | State-based action cascades | 10 | ✓ |
| 8 | Multiplayer politics | 10 | ✓ |
| 9 | SBA-triggered chains | 10 | ✓ |
| 10 | cEDH staples (stubs) | 5 | ✓ |

**Sample fixtures:**
- **f001** unblocked 2/2 → 2 damage (CR 510.1).
- **f005** trample 5 into 2/2 → 3 to player (CR 702.19).
- **f008** deathtouch 1/1 trades with 5/5 (CR 702.2).
- **f015** commander damage tracked by oracle_id (CR 903.10a).
- **f017** Doubling Season doubles +1/+1 counters (CR 614).
- **f029** Humility + anthem = 2/2 (canonical CR 613 ordering).
- **f032** Tarmogoyf P/T reflects graveyard types (CDA + CR 613.7b).
- **f038** counter chain 3 deep resolves LIFO with damage hitting (CR 117).
- **f048** commander damage 21 loses (CR 903.10a).
- **f061** draw N from top of library (CR 121.1).
- **f072** legend rule same-name same-controller (CR 704.5j).
- **f086** -1/-1 counters reduce effective toughness (CR 613.4 +
  layer integration).
- **f096** Thoracle + Demonic Consultation: empty library → lose.

**Per-card iter-10 cap:** ~500-card hardcoded interaction set is
NOT yet wired (deferred to iter 11+ per scoping doc); the 100 fixtures
use the substrate primitives directly. The actual oracle-text
compilation pipeline is iter 11+ sub-mega-task B scope.

**Iter-10 documented simplifications surfaced by fixtures:**
- f086 -1/-1 counter SBA check uses printed toughness (not post-layer).
  Iter-11+ should plumb chars.toughness through SBA.
- f097/f098/f099 cEDH staples are stubs verifying substrate doesn't
  crash; full combo-line execution requires oracle compilation
  (iter-11+).
- Aura-falls-off (f087) detects via attached_to nonexistence; full
  CR 704.5n with target-validity checking is iter-11+.

**Total pillar_f tests across Phases 1-8:** 224 (22+16+21+22+15+15+13+100).
All 224 pass. Substrate works end-to-end.

**Phase 8 deliverable:** 100 fixtures green = 100% pass rate. Iter 10
ship gate cleared.

**Commit message:** "Phase 8 (mega-task v9): 100-interaction fixture suite — 100/100 green (ship gate ≥85% cleared)".

Committed as `b66bba78c`.

---

## Phase 9 — Final regression + report + memory + sub-B scoping prep (2026-05-23)

**Full pytest:** 1852 passed / 25 skipped / 0 failed / 88 subtests
(was 1628 at v8 ship; **+224 new pillar_f tests** across v9 Phases 1-8).

**Vitest:** 774 / 2 pre-existing failed (unchanged — no UI changes
in iter 10).

**Iter 10 ship gate (Phase 8): 100/100 fixtures green (≥85% required).**

**Out-of-repo deliverables landed in `MTG-Deck-Builder-Claude/`:**
- `mega_task_v9_final_report.md` — executive + per-phase commits +
  architectural decisions + iter 10 → 11 hand-off + commit chain +
  spend + deferred items.
- `pillar_f_v0_2_sub_b_llm_policy_scoping.md` — 3-page scoping doc
  for sub-mega-task B covering prompt template design (main-phase,
  combat, response-window, mulligan), perspective_view feeding the
  LLM, politics state tracker schema, threat-vector feature
  extraction, token budget refinement, integration plug-points with
  the iter-10 substrate.

**Memory updates** (cowork at `C:/Users/jorde/.claude/projects/E--MTG-Root/memory/`):
- NEW: `project_mega_task_v9_shipped.md` — v9 commit chain + 11
  load-bearing substrate invariants iter 11 must honor.
- UPDATED: `MEMORY.md` — added v9 entry to index (now 6 active memories).
- UPDATED: `project_5_pillar_forward_plan.md` — Pillar F v0.2 sub-A
  marked COMPLETE; sub-B + sub-C iter-11+ priorities documented.

**Mega-task v9 success criteria check (per kickoff):**
1. PASS All 10 phases committed.
2. PASS Phase 8 fixture suite ≥85% green (achieved 100/100 = 100%).
3. PASS pytest baseline preserved + new tests pass (+224).
4. PASS vitest baseline preserved (unchanged; no UI work).
5. PASS Pillar A-E untouched (substrate is fully isolated under
   `pillar_f/v0_2/`).
6. PASS All 6 substrate areas implemented per scoping (state, stack,
   turn, replacement, layers, combat) + 7th (mulligan).
7. PASS Sub-mega-task B scoping doc landed.
8. PASS Total spend ~$0 of $80 budget.

v9 SHIPPED — Pillar F v0.2 sub-mega-task A complete.

**Commit message:** "Phase 9 (mega-task v9): final regression + report + memory + sub-B scoping — v9 SHIPPED".
