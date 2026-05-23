# Mega-task v9 / Pillar D iter 10 — Pillar F v0.2 sub-mega-task A kickoff

**Dispatch date:** 2026-05-23 (queued for user dispatch)
**Parent commit:** HEAD of `main` post-iter-9 (mega-task v8 ship `a6845fd7e`)
**Budget ceiling:** $80 API spend / ~6 weeks CC time
**Scope:** **Sub-mega-task A only** — core MTG rules engine substrate. Sub-mega-task B (LLM strategic policy) and Sub-mega-task C (Stage 2 graduated playtest) are EXPLICITLY OUT of iter 10 scope. They dispatch as their own mega-tasks in iter 11+.

This is a brand-new substrate. **No deck-build sweep matrix at the end** — the validation gate is a 100-interaction test fixture suite verified against MTG comprehensive rules. The deliverable is a rules engine module that can be invoked offline (think: combo-line validation tool), not a Pillar D quality improvement.

Pillar A through E remain untouched and must not regress. Iter 10's work lives in a new module tree under `repo/api/engine/pillar_f/v0_2/` (or equivalent — choose the path during Phase 0). All architectural rules from iter 9 continue to apply.

---

## Phase 0 — Pre-flight + scoping deep-read + module skeleton

Read in full before writing any production code:

- `MTG Root/mtg-engine/MTG-Deck-Builder-Claude/pillar_f_v0_2_game_engine_scoping.md` — the 6-section scoping document you authored at end of mega-task v8. This is the source of truth for iter 10's design.
- `MTG Root/mtg-engine/MTG-Deck-Builder-Claude/mega_task_v8_final_report.md` — v8 ship state + iter-10 hand-offs.
- `spaces/.../memory/project_mega_task_v8_shipped_2026-05-23.md` — iter 9 outcomes, reusable patterns (tiered archetype-relevance ranking, pool+DB swap-target fallback), and the load-bearing "Two reusable architectural patterns" section.
- `spaces/.../memory/project_phase_5b_substrate_blocker.md` — the original "stack + triggered/activated abilities + archetype-aware policy" substrate gap that Pillar F v0.2 closes. Three forward paths documented there; iter 10 follows the full substrate rebuild path.
- `spaces/.../memory/project_graduated_playtest_spec_2026-05-21.md` — Stage 1 is Pillar F v0.1 (shipped iter 6); Stage 2 needs Pillar F v0.2 (this iter's eventual unlock when sub-mega-task C ships in iter 12+).
- `repo/api/engine/pillar_f/` (current path — find via grep) — Pillar F v0.1 statistical approximator. This is what Stage 2 will eventually replace. Do NOT delete it; sub-mega-task C will wire Stage 2 alongside Stage 1.
- MTG Comprehensive Rules (already integrated into the project per `project_pillar_a_c_shipped_2026-05-17`). Verify the rules text is accessible to engine code; if not, surface the gap and fix in Phase 0.

Run `pytest -x` and `cd ui_harness && npm test`. Confirm baseline green (pytest 1628+, vitest 774+) before starting Phase 1. If anything red, **HALT** and surface.

Decide the module path: suggest `repo/api/engine/pillar_f/v0_2/` with subdirs `state/`, `stack/`, `replacement/`, `layers/`, `combat/`, `tests/fixtures/`. Create the empty scaffold with `__init__.py` files only. Commit as Phase 0.

---

## Phase 1 — Game-state object model + serialization (~1 week)

Implement the full state model per scoping doc section (a). Per-player zones: hand, library, battlefield, graveyard, exile, command, stack_membership, life_total, commander_damage_taken_from, mana_pool, lands_played_this_turn, cards_drawn_this_turn, spells_cast_this_turn, priority_passed_this_round. Global state: turn_number, phase, step_within_phase, active_player, priority_holder, stack, the_monarch, the_initiative, day_or_night, replacement_effects_active, continuous_effects_active, delayed_triggers_pending. Card object model: name, oracle_id, mana_cost, cmc, type_line, subtypes, oracle_text, power, toughness, loyalty, colors, color_identity, keywords, face_down, tapped, summoning_sick, damage_marked, counters, attached_to, attached_by, controller, owner.

Hidden-information handling is load-bearing. Implement `perspective_view(player_id)` that returns the same state structure with hidden zones redacted: opponents' hand contents become counts only, library contents become counts only, face-down exile/battlefield cards become flagged opaque. The LLM (eventually sub-mega-task B) receives this perspective view, never raw state.

**Serialization.** State must round-trip through JSON cleanly (for LLM observation + checkpointing + replay). Add `to_json()` / `from_json()` with version field. Round-trip test: load 4-player game state with 30 cards on each board, 7 cards in each hand, stack of 3 spells, serialize, deserialize, deep-equal check.

**Gates.** Unit tests covering: zone-by-zone state mutation under direct calls (e.g., `state.move_card(card_id, from='hand', to='battlefield')`); perspective_view redaction (opponent state matches expected hidden counts); commander_damage_taken_from tracking on actual combat damage stub; JSON round-trip for full 4-player state.

**Estimated effort:** 1 week. Pure data modeling + serialization; no game logic yet.

---

## Phase 2 — Stack mechanics + priority loop (~1 week)

Implement stack push/pop semantics + priority handoff per scoping doc section (b). The stack is a LIFO ordered list of `StackEntry` objects: spell or activated ability + controller + targets + payment. A push triggers a priority round starting with the active player.

**Priority loop.** Implement `priority_round(state)` that iterates clockwise from active player, polls each for a response (mocked for iter 10 — the real LLM hookup is sub-mega-task B), and exits when all players have passed in succession without changing the stack. On exit: if stack is non-empty, top entry resolves; if stack empty, step advances.

**Stack resolution.** Top entry pops; its `resolve(state)` method runs (concrete resolve logic comes in Phase 4+5+6). Resolution may trigger replacement effects (Phase 4) and may push new entries onto the stack (triggered abilities).

**APNAP ordering.** Multiple triggers from the same event get queued in active-player-next-active-player order; same-controller stacking is controller's choice. Implement as `enqueue_triggers(triggers, source_event)` → `state.delayed_triggers_pending`.

**Counterspell mechanics.** A counterspell pops the targeted stack entry without resolving it. Implement as a generic `counter(target_entry)` API; specific cards (Counterspell, Negate, Mana Drain) call this in their resolve.

**Gates.** Unit tests: simple sorcery resolution (1 stack push, all pass, resolves); response sequence (sorcery → counterspell → counter-counter → all 3 resolve in LIFO); APNAP trigger ordering with same-controller pile-up; priority returning to active player after stack empties.

**Estimated effort:** 1 week. Core combinatorial logic; APNAP edge cases are the time sink.

---

## Phase 3 — Phase/step state machine (~3 days)

Implement the turn structure per scoping doc section (b) timing: untap → upkeep → draw → main_1 → beginning_of_combat → declare_attackers → declare_blockers → combat_damage → end_of_combat → main_2 → end_step → cleanup. Each step has its own priority-open behavior (untap + cleanup are special — no priority unless state-based actions or triggers fire).

**Step transitions.** `advance_step(state)` moves to next step, fires phase-change triggers (e.g., "at beginning of combat"), opens priority. State-based actions (Phase 4) run between every priority pass.

**Cleanup-step quirk.** Cleanup discards down to 7, removes damage marks, processes "until end of turn" effects. Priority only opens if state-based actions fire something pending OR a triggered ability waits to be put on stack. If priority opens, return to "another cleanup step" loop.

**Turn-end / active-player rotation.** End of cleanup → next player's untap. `active_player` advances clockwise.

**Gates.** Unit tests: full turn cycle with no actions completes in correct step order; "at beginning of combat" trigger fires at the right step; cleanup-step discard-to-7; turn rotation cycles through 4 players.

**Estimated effort:** 3 days. Mostly straightforward state-machine work; cleanup is the gotcha.

---

## Phase 4 — Replacement effects + state-based actions (~1 week)

Per scoping doc section (b). Replacement effects fire BEFORE the event they replace (would-deal-damage → instead-prevent, would-draw → instead-draw-2, would-die → instead-exile, would-ETB → instead-tapped). Implement as a registered list of `Replacement(event_pattern, replacement_fn, controller)` that the engine checks before any event resolution.

**Self-replacement.** Each replacement applies once per event. Controller of the affected event chooses ordering when multiple replacements apply. Implement as `apply_replacements(event)` → returns final modified event after all applicable replacements ordered + applied.

**State-based actions (SBAs).** Per CR 704: creatures with 0 toughness die, creatures with damage ≥ toughness die, players with 0 or less life lose, players with 21+ commander damage lose, attached auras with no valid target unattach, legend rule, etc. Implement as `check_state_based_actions(state)` → returns list of mutations to apply. SBAs run before any priority window opens and again after each priority pass, until no SBAs fire.

**Event types to plumb.** `DrawEvent`, `DamageEvent` (creature + player + planeswalker), `EnterBattlefieldEvent`, `DieEvent`, `LifeChangeEvent`, `CounterAddEvent`, `CounterRemoveEvent`, `DiscardEvent`, `MillEvent`. Each is a dataclass; replacements key off the type + optional predicates.

**Gates.** Unit tests: would-die replaced by exile (e.g., Rest in Peace stub); would-deal-damage prevented by Fog stub; legend rule fires on duplicate legendaries; commander damage SBA at 21; planeswalker -loyalty SBA at 0. Coverage: 15+ replacement-effect interactions + 8+ SBA categories.

**Estimated effort:** 1 week. Event taxonomy + replacement ordering is the work.

---

## Phase 5 — Continuous (layered) effects + 7-layer system (~2 weeks)

Per scoping doc section (b). 7 layers: (1) copy effects, (2) control-changing, (3) text-changing, (4) type/subtype/supertype, (5) color, (6) ability adding/removing, (7) power/toughness with 4 sublayers (7a base, 7b CDA, 7c base +/-, 7d switches). Each layer requires re-application after every state change.

**Implementation.** `apply_continuous_effects(state)` iterates layers 1 through 7, applying each registered effect in dependency order within its layer. Output is the "characteristic-defining" view of every permanent — what its type/colors/abilities/P/T are RIGHT NOW. Cache the result; invalidate on state changes.

**Layer 6 is dominant.** Most cards live here — anthem effects (Honor of the Pure, Crusade), keyword grants (Skyward Eye Prophets giving creatures flying), ability removal (Humility). Build out at least 30 layer-6 effects in the initial fixture set covering: static anthems, conditional anthems (Captivating Vampire, Lord of the Undead), keyword grants, ability removal, type-bypass.

**Layer 7 sublayers.** 7a (base P/T like Battle Cry creature's printed values), 7b (CDA: Tarmogoyf, Mortivore — power/toughness depends on game state), 7c (+1/+1 from Glorious Anthem, Crusade), 7d (switches: Inverter of Truth's switch power and toughness). Order matters — same card may participate in multiple sublayers.

**Dependency resolution.** When effect A's application depends on effect B's outcome (e.g., a Clone copying a creature that's already been Anthem'd), apply in dependency order. CR 613.7 governs this; implement the textbook algorithm.

**Gates.** Unit tests: Honor of the Pure + base 2/2 creature = 3/3; Humility + Honor = 1/1 (because ability removal in layer 6 strips the anthem grant); Clone + Tarmogoyf = correct P/T based on graveyard composition; legendary supertype removal via Mind Bend (layer 3 → 4 cascade); Inverter of Truth switch applied last (layer 7d).

**Estimated effort:** 2 weeks. **This is the highest-complexity phase.** Layer ordering + dependency resolution are where most engines fail. Plan for diagnostic-heavy debugging and frequent reference back to CR 613.

---

## Phase 6 — Combat phase (~1 week)

Per scoping doc section (e). Implement the full combat substep machinery: beginning_of_combat → declare_attackers → declare_blockers → combat_damage (first-strike pass → normal pass) → end_of_combat.

**Declare attackers.** Active player chooses creatures to attack (must be untapped, not summoning-sick unless haste, not affected by "can't attack"). Multi-target attack: each attacker targets a player or planeswalker. Pay attack costs (Propaganda, Ghostly Prison) if applicable.

**Declare blockers.** Defending player(s) assign blockers per attacker. Multi-block legal; ordering matters for damage assignment (active player declares damage assignment order on attacking creatures with multi-blockers).

**Combat damage.** Two passes: first-strike/double-strike pass (only creatures with first or double strike deal damage), then normal pass. Damage assignment respects assignment order. Trample carries excess to defending player/planeswalker. Lifelink heals controller. Deathtouch — any damage from deathtouch creature is lethal regardless of toughness. Indestructible — damage doesn't kill (SBA in Phase 4 already handles this).

**End_of_combat.** "At end of combat" triggers fire. Combat ends; main_2 opens.

**Gates.** Unit tests: simple 2/2 attacks unblocked, player takes 2; trample with 5-damage attacker into 2/2 blocker = 3 to player; double strike attacker first-strike kills blocker then normal-pass hits player; deathtouch 1/1 trades with 5/5; multi-block damage assignment order (3 blockers, attacker assigns 1/1/1 vs 1/1/5).

**Estimated effort:** 1 week.

---

## Phase 7 — Mulligan + draw + cleanup state-based polish (~3 days)

**Mulligan.** London mulligan per current rules: shuffle hand back, draw 7, choose to keep or mulligan. Each mulligan → put N cards from hand on bottom of library (where N = number of mulligans taken). Implement as opening-state setup before turn 1.

**Draw step.** First-turn-no-draw for the starting player (multiplayer EDH convention). Subsequent turns: 1 card drawn at start of draw step.

**Cleanup-step polish.** Discard to 7 (active player chooses); remove damage marks from all permanents; "until end of turn" effects expire. SBAs and triggers may fire — re-enter cleanup loop if so.

**Gates.** Unit tests: London mulligan correctly puts cards on bottom; first-turn skip-draw for starting player; "until end of turn" P/T pump expires at cleanup; cleanup discard-to-7 honors active player's choice.

**Estimated effort:** 3 days.

---

## Phase 8 — Test fixture suite: 100 known interactions (~1 week)

The validation gate for iter 10. Build a curated fixture set of 100 known MTG interactions, each with an explicit expected outcome traceable to a CR rules citation or a well-known judge ruling.

**Categories (suggested distribution):**

- 15 basic combat scenarios (trample, first strike, deathtouch, lifelink, indestructible interactions)
- 10 replacement-effect chains (Doubling Season + planeswalker, Rest in Peace + Reanimate, Leyline of the Void)
- 10 layer-6/7 ordering edge cases (Humility + anthems, Clone of CDA creature, Mind Bend casts)
- 10 stack interaction scenarios (Counterspell variants, split-second, can't-be-countered, redirect)
- 10 commander-specific (commander tax, command-zone replacement, commander damage SBA)
- 10 mulligan + opening-hand edge cases
- 10 state-based action cascades (board wipe with multiple replacement effects fighting)
- 10 multiplayer politics (monarch transfer, goad, voting)
- 10 SBA-triggered chains (creature with 0 toughness from -1/-1 counter, planeswalker loyalty 0, legend rule with multiple copies)
- 5 high-bracket cEDH staples (Underworld Breach + Brainfreeze, Thoracle + Consultation, Food Chain + Eternal Scourge, Dockside + Temur Sabertooth, Ad Nauseam line)

**Each fixture is a Python test function** that constructs the relevant pre-game or mid-game state, applies the trigger, and asserts the final state matches the expected outcome. Tag each fixture with its CR citation for traceability.

**Test coverage target.** 100/100 fixtures green is the iter 10 ship gate. If any sub-section fails repeatedly, halt and surface — likely a Phase 5 layer-ordering bug or a Phase 4 replacement-effect ordering bug.

**Card-pool note.** Per scoping doc, hardcode the ~500 most-played EDH cards' specific interactions (top cards from corpus). For cards outside the 500: "best-effort interpret" fallback that parses oracle text into a generic ability handler. The full oracle-text compilation pipeline is deferred to iter 11+ (estimated +4 weeks per scoping doc).

**Gates.** All 100 fixtures green; coverage report shows the 500-card hardcoded set + the best-effort fallback handles every fixture without crashes; documentation generated listing which fixtures use hardcoded vs fallback paths.

**Estimated effort:** 1 week. The fixtures themselves are mostly authoring time; the bugs they catch are the actual work.

---

## Phase 9 — Final regression + report + memory + sub-mega-task B scoping prep

**Regression.** Full pytest + vitest from clean. Must hit iter-9 baseline (pytest 1628+, vitest 774+) + the new fixtures from Phase 8 (+100 minimum from Phase 8's gate). No iter 1-9 regressions.

**Deliverable: `mega_task_v9_final_report.md`** in `MTG-Deck-Builder-Claude/`. Executive summary + per-phase commit + diff + fixture coverage report + budget burn. Hand-off section for sub-mega-task B (LLM strategic policy).

**Deliverable: sub-mega-task B scoping prep.** A 2-3 page scoping note `pillar_f_v0_2_sub_b_llm_policy_scoping.md` covering: prompt template design for main-phase / combat / response-window / mulligan calls; perspective-view feeding the LLM; politics state tracker schema; threat-vector feature extraction; token budget refinement; integration points with the rules engine substrate (which callbacks the engine exposes for LLM action prompting). This is the planning artifact for iter 11 dispatch.

**Memory update.** Write `project_mega_task_v9_shipped_2026-05-XX.md` capturing: phase commits, fixture suite results, baseline+addition counts, spend, sub-mega-task B hand-off, sub-mega-task C still queued for iter 12+. Add a one-line index entry to `MEMORY.md` under 200 chars.

**Halt-on-ship-criteria-revision pattern.** If the 100-fixture gate lands at <85%, surface a criteria-revision-or-iterate question. Iter 3-9 default is "revise + ship" rather than over-iterate. 85% is the suggested floor; below that, layer-ordering or replacement-effect bugs are likely systemic and need targeted fix-up before shipping.

---

## Architectural rules that MUST continue to be honored

These are locked feedback memories. Iter 10 must not regress any of them. Some are less directly relevant to a rules engine but still apply where they touch:

- **User intent locks deck shape** — N/A directly to rules engine but applies the moment sub-mega-task B's LLM policy comes online (iter 11). Flag in sub-B scoping note.
- **Mana base serves spells** — N/A to rules engine.
- **Corpus is descriptive not prescriptive** — N/A to rules engine.
- **Pool score does not drive LLM picking** — applies to sub-mega-task B; not iter 10.
- **Live-test catches what unit tests miss** — applies. Every phase ships with at least one ad-hoc playthrough of a known game scenario, not just unit tests. The 100-fixture suite is the structured live-test.
- **Cowork Write/Edit may silently truncate large code-file writes** — applies. Phase 5 in particular will produce large layer-resolution files; verify with `ast.parse` after every Edit on files >500 lines.
- **Sandbox bash mount can lie about file size** — applies. Cross-check via Windows `dir` if bash reports anomalous sizes after large writes.

---

## Wins from iter 9 that iter 10 MUST NOT regress

- Slot-fallback A-prefix wave closed (Edgar 32 → 2)
- Pillar E v0.7 iterate-until-target across all 6 categories
- Singleton safety-net upstream miss fixed
- Bracket-proportional interaction bounds
- Pillar E critique 5/5 coverage
- All 6 architectural rules continue honored
- pytest 1628+, vitest 774+, SSE UI build wallclock under 150s on Edgar B3
- Per-set automation pipeline still operational (~$1-2/year extrapolated)

A periodic spot-check via `python -m api.main` + one live SSE UI Edgar B3 build at Phase 3 + Phase 6 + Phase 8 milestones is sufficient to catch regressions. Full sweep matrix not required for iter 10.

---

## Halt-trigger reference

Halt and surface a question to the user if any of the following occur:

- Baseline pytest or vitest red before Phase 1 starts
- Phase 5 layer-ordering produces wrong outcome on > 5 of the 30+ planned layer-6/7 fixtures despite a CR 613 implementation pass — likely a dependency-graph bug; surface for design review
- Phase 8 fixture suite lands at <85% green — likely systemic layer-or-replacement bug, halt before shipping
- A periodic spot-check at Phase 3/6/8 finds the SSE UI Edgar build broken — surface immediately (means a Pillar A-E regression sneaked in)
- Cumulative spend trends past 80% of the $80 ceiling before Phase 7 — halt and surface scope-trim option
- The hardcoded ~500-card set turns out to require >800 cards to make the fixture suite pass — surface; either expand the budget or trim fixture scope
- Sub-mega-task A reveals a foundational design flaw in the scoping document — halt, don't paper over it; sub-mega-task A's value is its correctness

Default user-revision pattern across iter 3-9 has been "revise criteria + ship" when sweeps land at partial pass. Apply the same discipline at the Phase 8 fixture gate — pass-or-revise, not over-iterate.

---

## Budget + scope reminder

$80 API ceiling. ~6 weeks CC time. 10 phases (0-9). Headline goal is the 100-interaction test fixture suite green at ≥85%. Sub-mega-task A's value is reusable even without B + C — the rules engine can be invoked as an offline combo-line validation tool the moment Phase 8 passes.

Sub-mega-task B (LLM strategic policy, ~6 weeks) and Sub-mega-task C (Stage 2 graduated playtest, ~4 weeks) are EXPLICITLY OUT of iter 10. They dispatch separately in iter 11+ once iter 10 lands. Don't pre-plumb them; that's scope creep that kills the 6-week estimate.

The `primitive_to_cards` v2 ontology rebuild from iter-9 hand-off list (Tier-3 skip + safety net inversion) is also OUT of iter 10 scope. It dispatches separately whenever you want a cleanup sprint between major arcs.

---

## Iter 10 dispatch checklist

Before pasting to CC:

1. Confirm parent commit (`a6845fd7e` from v8 ship) is what `main` is on
2. Confirm baseline pytest 1628 + vitest 774 green
3. Confirm `MTG Root/mtg-engine/MTG-Deck-Builder-Claude/pillar_f_v0_2_game_engine_scoping.md` is present and readable from CC's path
4. Confirm $80 budget envelope is acceptable for ~6 weeks autonomous work

Good luck. This is the multi-month substrate the project has been pointing toward since `project_phase_5b_substrate_blocker` was filed in May 2026. Sub-mega-task A is the foundational chunk; B + C build on it.
