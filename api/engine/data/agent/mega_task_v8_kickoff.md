# Mega-task v8 / Pillar D iter 9 — kickoff

**Dispatch date:** 2026-05-23
**Parent commit:** HEAD of `main` post-iter-8 (mega-task v7 ship)
**Budget ceiling:** $70 API spend / ~2-3 weeks CC time
**Scope:** 9 phases. The headline goal is to close the slot-fallback alphabetical-fill gap surfaced in the post-iter-8 Edgar Markov B3 live walkthrough (25 random "A-prefix" cards in flex/other), tighten Pillar E v0.7 swap aggression so it iterates-until-target across all categories, fix the singleton safety-net upstream miss, and clear the three CC iter-8 hand-off items (vocab tech debt, bracket-proportional interaction bounds, Pillar E critique coverage extension). Pillar F v0.2 game-engine substrate is OUT of iter 9 scope but should be scoped during iter 9 for iter 10+ dispatch.

---

## Phase 0 — Pre-flight + memory sync

Read the following before writing any code:

- `spaces/.../memory/project_iter_9_prep_notes_2026-05-23.md` — full scope context with live evidence (A-prefix card list, slot-fallback warnings, Pillar E swap counters)
- `spaces/.../memory/project_iter_8_prep_notes_2026-05-22.md` — preceding iter scope (most items closed in iter 8)
- `spaces/.../memory/project_coherence_sweep_3_shipped_2026-05-22.md` — substrate health snapshot + four queued items (voyage_downgrade wiring, voyage_rules at scale, ENGINE_API_GUIDE.md overhaul, 8 pre-existing test failures) that may still be open
- `spaces/.../memory/feedback_user_intent_locks_deck_shape_not_corpus_optimum.md` — Priority #1 fix MUST continue honoring this rule
- `spaces/.../memory/feedback_pool_score_does_not_drive_llm_picking.md` — the slot-fallback fix is at the POOL level (deterministic filtering/scoring), distinct from LLM-picking
- `spaces/.../memory/feedback_mana_base_serves_spells_not_reverse.md` — mana_base swaps via Pillar E v0.7 must continue honoring this
- `spaces/.../memory/feedback_corpus_descriptive_not_prescriptive.md` — slot-fallback archetype-relevance scoring must not turn into corpus-frequency ranking
- `repo/api/engine/data/agent/coherence_sweep_3_health_report.md` — punch list status; before starting Phase 1, confirm which sweep #3 items shipped in iter 7/8 and which are still open
- `repo/agent_wide_candidate_pool_v1.py` (path TBD — locate via grep for `slot_fallback`) — Phase 1's primary surface
- `repo/api/engine/pillar_e/*` — Phase 3's primary surface (whichever module owns the v0.7 aggressive swap)
- Latest mega-task v7 final report (when it lands in `MTG-Deck-Builder-Claude/`) — for iter 8 ship state, deferred items, baseline metrics

Run `pytest -x` and `cd ui_harness && npm test` from the repo root. Confirm baseline green before Phase 1. If anything is red at baseline, **HALT** — fix or document before starting iter 9 work. Carry forward the iter-8 baseline counts (pytest 1489+; vitest 758+) and flag any deltas in your final report.

Run `python -m api.main` and one end-to-end SSE build through the UI on the same Edgar Markov B3 spec the prep notes describe. Capture the warning log lines you observe (`POOL_SLOT_FALLBACK_TRIGGERED`, `PILLAR_E_AGGRESSIVE_SWAPS_APPLIED`, `STRUCTURAL_SAFETY_NET_SINGLETON_FIXED`, `MANA_BASE_DISCREPANCY_UNJUSTIFIED`, `CARD_ADVANTAGE_DISCREPANCY_UNJUSTIFIED`) and treat those as the iter-9 starting baseline. Final validation in Phase 7 compares against them.

---

## Phase 1 (BLOCKING) — Slot-fallback algorithm: archetype-relevance scoring

**The headline iter-9 fix.** Live evidence from the post-iter-8 Edgar B3 build: ~25 alphabetical "A-prefix" cards landed in flex/other (A-Karn, A-Carnelian Orb of Dragonkind, A-Excavation Explosion, A-Lantern of Revealing, A-Base Camp, A-Dungeon Descent, A-Hall of Tagsin, A-Town, A-Visions of Phyrexia, Aang's Defense, Aang's Journey, Abeyance, Abstruse Appropriation, Absorb Vis, Academic Dispute, Accelerate, Active Volcano, Aetherflux Car, Affa Guard Hound, Agonizing Demise, Aim for the Head, Airbender's Reversal, Akoum, Alhammarret's Archive, Angrath's Fury, Asmodeus the Archfiend, Blood Scrivener). These are bracket-legal + color-identity-legal but have ZERO archetype fit with Edgar Markov vampires. The warning `POOL_SLOT_FALLBACK_TRIGGERED` reports `per-slot adds: {'ramp': 9, 'card_draw': 9, 'removal': 8, 'win_condition': 4}` — 30 cards came from slot-fallback and most are A-prefix alphabetical noise.

**Investigation path.** Locate the slot-fallback query logic (grep for `slot_fallback:` source tag construction; trace back to where the candidate gets emitted with that tag). Instrument the filter chain to log what's actually being applied: color identity ✓, bracket legality ✓, slot category match ✓, archetype relevance ✗ ← the missing piece. The current ordering is functionally alphabetical because no archetype-relevance signal participates in the ranking, so when the archetype-tagged candidates run short the next-tier fallback sorts by something that resolves to alphabetical.

**Fix.** Add archetype-relevance scoring to the fallback ranking before any final cut: `fallback_score = bracket_legality × archetype_theme_overlap × primitive_diversity_fit`. The theme-overlap signal pulls from the deck's structured `theme_profile` (the same B2 output used downstream); the primitive-diversity signal pulls from Pillar C v2 ontology tags. Sort fallback candidates by descending fallback_score; take top N per slot. **When archetype-overlapping candidates simply don't exist in the corpus for a slot** (e.g., Edgar has no good white-aligned card draw beyond Welcoming Vampire/Skullclamp/Champion of Dusk), the fallback should still pull from broader corpus archetypes that AT LEAST share a primitive (death-trigger, lifegain, token-producer) rather than collapse to alphabetical. Document the tiered fallback chain explicitly: tier 1 archetype-tagged → tier 2 primitive-overlapping → tier 3 last-resort generic-staple; alphabetical is NEVER a tier.

**Cross-applies to Pillar E v0.7.** The "A-Karn, Living Legacy" and "A-Visions of Phyrexia" picks in the Edgar build came from the Pillar E v0.7 aggressive swap layer choosing them as bracket-legal mana-base alternatives — same root cause: swap targets aren't scored by archetype fit either. Apply the same archetype-relevance scoring inside Pillar E v0.7's swap-target selection (Phase 3 will integrate).

**Honor user-intent rule.** The fix MUST continue to honor `feedback_user_intent_locks_deck_shape_not_corpus_optimum.md`. Don't let archetype-relevance scoring drift into corpus-frequency ranking — that would silently re-introduce the corpus-optimum anti-pattern.

**Gates.** New unit tests covering: (a) tier-1 archetype-tagged fallback produces theme-coherent cards on Edgar/Krenko/Ur-Dragon/Atraxa/Yuriko; (b) tier-2 primitive-overlap fallback fires when tier 1 underfills; (c) tier-3 generic-staple fallback fires when tier 2 underfills; (d) at no point does alphabetical ordering survive into the final ranked output. Live-rebuild Edgar B3 through SSE UI; confirm the A-prefix wave is gone and the flex/other section reads as recognizable vampire-tribal-adjacent picks.

**Estimated effort:** 3-5 days CC time. Most of it is diagnosis + scoring function tuning. Fix once isolated should be small.

---

## Phase 2 (BLOCKING) — Singleton safety-net upstream fix

**Live evidence:** `STRUCTURAL_SAFETY_NET_SINGLETON_FIXED: Singleton violation: 'Edgar Markov' appeared 2× → reduced to 1 + 1 basic(s).` Commander appeared twice in the deck (commander slot + mainboard pick). The engine safety-net corrected it by swapping the duplicate for a basic land. This is masking an upstream miss: the user-supplied commander card_id should be excluded from the candidate pool used for mainboard selection.

**Fix.** In the candidate pool filter (likely `compute_deck_candidate_pool_v1` or the agent-wide variant), explicitly exclude the user-supplied commander card_id from mainboard candidates. Keep the safety net in place as belt-and-suspenders, but the warning should no longer fire on clean builds.

**Gates.** Unit test confirming commander card_id never appears in the candidate-pool output. Live rebuild on Edgar B3 + Krenko B4 + Ur-Dragon B4; confirm `STRUCTURAL_SAFETY_NET_SINGLETON_FIXED` warning does not fire on any of them. Regress the existing safety-net unit test (it should still pass — the net catches synthetic duplicates injected in test).

**Estimated effort:** ~half day. Small surgical change.

---

## Phase 3 (BLOCKING) — Pillar E v0.7 swap aggression: iterate-until-target + category extension

**Live evidence.** `PILLAR_E_AGGRESSIVE_SWAPS_APPLIED: Pillar E v0.7 applied 2 swap(s) to close optimizer-flagged discrepancies: {'mana_base': 2}` but `MANA_BASE_DISCREPANCY_UNJUSTIFIED: actual=48, target=36 (delta +12)`. The swap layer closed 2 of the 12-card excess and stopped. Should have iterated until target hit (within tolerance) OR LLM critique explicitly justified the remaining gap. Same build: `CARD_ADVANTAGE_DISCREPANCY_UNJUSTIFIED: Total card-advantage count 2 is 8 below the target of 10` with no card_advantage swaps applied at all — the swap layer is currently mana_base-only.

**Fix part A — iterate-until-target.** Wrap the current single-pass swap logic in a loop. Continue swapping until either (a) the gap is within bracket tolerance, (b) no eligible swap candidates remain in the pool, or (c) an iteration cap is hit (suggest 8-10 iterations max as a runaway guard). On exit due to (b) or (c), emit an explicit LLM critique pass that either justifies the residual gap with archetype-specific reasoning or proposes manual swap targets.

**Fix part B — category extension.** Extend Pillar E v0.7 coverage beyond mana_base to ALL Pillar E categories: card_advantage, interaction, curve, win_con, anti_meta. Each category gets the same iterate-until-target logic. Order of swap attempts within an iteration: (1) check which categories are flagged over/under target by their respective Pillar E v0.x optimizer; (2) for each flagged category, attempt the swap; (3) honor the user-intent rule (commander + must-includes are IMMUTABLE) and the mana-base-serves-spells rule (mana-base swaps adjust last, not first).

**Fix part C — archetype-relevance in swap-target selection.** Reuse Phase 1's archetype-relevance scoring inside the swap-target picker so we don't replace one A-prefix card with another A-prefix card. Same tier chain: archetype-tagged > primitive-overlap > generic-staple > never alphabetical.

**Gates.** Live rebuild Edgar B3 + Krenko B4 + Ur-Dragon B4 + Atraxa B4 + Yuriko B5. For each, confirm: mana_base delta ≤ bracket tolerance OR an explicit LLM justification fires; card_advantage count within target tolerance OR justification; same for interaction/curve/win_con/anti_meta. Add a Pillar E v0.7 telemetry log line that reports per-category swap counts + final deltas so the report card surfaces this. Unit tests covering: iterate-until-target convergence, iteration cap behavior, category coverage matrix.

**Estimated effort:** 2-3 days.

---

## Phase 4 — Vocabulary tech debt: rebuild `primitive_to_cards` from v2 ontology

**Context.** Iter 7 Phase 3 ontology v2 backfill produced UPPERCASE_UNDERSCORED primitive names, but `interaction_designer._PRIMITIVES_TO_CATEGORY`, `win_con_coherence._WIN_CON_PATTERNS`, and `_classify_card` were patched with lowercase-hyphenated aliases as Tier-1 fixes in iter 8. The proper fix is rebuilding the `primitive_to_cards` data structure from v2 ontology directly so the dual-vocabulary patches can be retired.

**Fix.** Identify the canonical primitive vocabulary (decide: UPPERCASE_UNDERSCORED is the iter-7 ontology v2 convention — keep that, retire the lowercase-hyphenated aliases). Regenerate `primitive_to_cards` from the v2 ontology source. Update the three patched consumers to use the canonical vocabulary directly. Add a regression test that fails if dual-vocabulary aliases reappear anywhere.

**Gates.** All previously-aliased lookups still resolve. Pillar C primitive coverage stays at or above iter-7's 93.0% measured coverage_v1. Pillar F primitive-grounded archetype-impact still produces non-zero scores on new cards from per-set automation.

**Estimated effort:** 3-5 days. Diagnosis + careful migration; the consumer count is small but each consumer needs verification.

---

## Phase 5 — Bracket-proportional interaction bounds (CC iter-8 hand-off)

**Context.** Iter 7 Phase 4 eval-script multi-primitive counting shipped, then iter 8 surfaced a new problem: `INTERACTION_DISCREPANCY: targeted_creature_removal above per-category max: 10 vs [4,7]`. Current per-category bounds exceed bracket allocation for low brackets; high-bracket decks naturally accumulate more interaction than the bound permits.

**Fix.** Replace the static per-category bounds with bracket-proportional bounds. Each interaction category (targeted_creature_removal, targeted_nc_removal, mass_removal, counterspells, graveyard_hate, etc.) gets a `bounds_by_bracket: {B1: [a,b], B2: [c,d], B3: [e,f], B4: [g,h], B5: [i,j]}` table. The interaction_designer optimizer reads the row matching the deck's bracket.

**Gates.** Live rebuild Edgar B3 + Krenko B4 + Ur-Dragon B4 + Atraxa B4 + Yuriko B5. For each, confirm no spurious `INTERACTION_DISCREPANCY` fires when the category count fits the bracket. Add unit tests covering bound lookups at every bracket × every category.

**Estimated effort:** 1-2 days.

---

## Phase 6 — Pillar E critique coverage extension (CC iter-8 hand-off)

**Context.** Iter 8 swap layer fires on 2/5 cases (Atraxa + Yuriko); doesn't trigger on Edgar/Krenko/Ur-Dragon. Investigate why. Likely related to archetype-specific bypass logic or threshold gating.

**Fix.** Trace the critique path on Edgar/Krenko/Ur-Dragon. Identify the gate that's short-circuiting. Remove the gate OR make it bracket-aware so all 5 cases get the critique pass. Confirm the critique LLM has authority to propose swaps and is wired to Pillar E v0.7's swap target picker (Phase 3 integration).

**Gates.** Live rebuild all 5 cases. All 5 emit a Pillar E critique transcript in the build SSE stream. The transcript either applies swaps to close optimizer gaps or contains an explicit justification for the residual gap.

**Estimated effort:** 1-2 days.

---

## Phase 7 — Iter 9 final validation sweep + report

**Sweep matrix.** Five commander × bracket cases: Edgar Markov B3, Krenko B4, Ur-Dragon B4, Atraxa B4, Yuriko B5. For each:

1. Build through the SSE UI (not just the Python tool harness).
2. Verify no `POOL_SLOT_FALLBACK_TRIGGERED` warning fires with alphabetical fallback as the cause. Verify no A-prefix wave in flex/other (visual inspection of decklist).
3. Verify no `STRUCTURAL_SAFETY_NET_SINGLETON_FIXED` warning.
4. Verify Pillar E v0.7 critique transcript present.
5. Verify mana_base + card_advantage + interaction + curve + win_con + anti_meta deltas within tolerance OR explicit justification.
6. Verify graduated playtest Tier 0 pass rate >= 62% (iter-8 baseline; should improve with archetype-relevant flex cards).
7. Capture build wall-clock; flag any regression past iter-8's 98s baseline by more than 25%.

**Report deliverable.** `MTG-Deck-Builder-Claude/mega_task_v8_final_report.md` covering: per-phase commits + diff summary, sweep matrix results, pytest + vitest counts, API spend, any deferred items, recommendations for iter 10.

**Halt discipline.** If any sweep case fails 2 or more gates, halt and surface a criteria-revision-or-iterate question to the user. Do not ship at <3/5 sweep cases passing.

---

## Phase 8 — Final regression + memory + Pillar F v0.2 scoping note for iter 10

**Regression.** Full pytest + vitest run from clean. Counts must be >= iter-8 baseline (pytest 1489+, vitest 758+) plus whatever new tests you added. No new test failures introduced by iter-9 work. The 8 pre-existing test failures carried since mega-task v4 — if any remain open after iter 7's nominal review window, decide fix-vs-retire and document.

**Memory update.** Write `project_mega_task_v8_shipped_2026-05-XX.md` capturing: commits + summary, sweep matrix outcome, spend, criteria pass/fail, deferred items, iter-10 priority queue. Add a one-line index entry to MEMORY.md (under ~200 chars — the index is already over its size limit, so prune aggressively).

**Pillar F v0.2 scoping deliverable.** During iter 9 (any time after Phase 1 lands), read the existing Pillar F v0.1 statistical approximator, the `project_phase_5b_substrate_blocker.md` memory, and the `project_graduated_playtest_spec_2026-05-21.md` memory. Produce a scoping document `MTG-Deck-Builder-Claude/pillar_f_v0_2_game_engine_scoping.md` covering: (a) game-state object model (4 hands, 4 boards, 4 graveyards, 4 libraries with hidden information); (b) stack + priority + replacement effects + layers; (c) LLM strategic policy with politics + threat assessment; (d) eventual distilled fast policy; (e) estimated multi-month timeline broken into sub-mega-tasks; (f) integration plan with the existing graduated playtest framework so Stage 2 measured outcomes can replace Stage 1 statistical predictions. This is a planning artifact, not implementation. Iter 10+ will dispatch the actual build work.

---

## Architectural rules that MUST continue to be honored across all phases

These are locked feedback memories. Iter 9 must not regress any of them:

- **User intent locks deck shape.** Commander + must-includes are IMMUTABLE. Theme hints are OPTIONAL. Phase 1's archetype-relevance scoring must not drift into corpus-frequency ranking.
- **Mana base serves spells, computed last not locked first.** Phase 3's mana_base swaps must continue adjusting based on final spell composition, not pre-locking land identities.
- **Corpus is descriptive reference, not prescriptive.** Phase 1's tier-2 primitive-overlap fallback must not become "what does the corpus say is most-played."
- **Pool score does not drive LLM picking.** Phase 1 is a POOL-level fix (deterministic filtering/scoring). It does NOT change LLM prompt requirements. Don't conflate the two layers.
- **Live-test catches what unit tests miss.** Every phase ships with at least one live SSE UI build, not just `pytest` + `python -m tools.x` runs.
- **Cowork Write/Edit may silently truncate large code-file writes.** When editing large files (>500 lines), verify via bash `wc -l` + `python -c "import ast; ast.parse(open('x.py').read())"` after every Edit.

---

## Wins from iter 8 that iter 9 MUST NOT regress

- Pool returns 60+ spell candidates per build (iter-8 measured 724 on Edgar B3)
- Commander typeahead + fuzzy match works on the AI Build view
- Pillar E v0.7 swap layer exists and fires (iter 9 extends, does not remove)
- Semantic injection working (3-4 cards per build from Voyage neighbors)
- 5+ themes classified per build
- 5+ combos surfaced per build with Applied-as-swap flags
- Graduated playtest report fires honestly (Tier 0 passed 62% on Edgar B3 — iter 9 should improve this)
- SSE streaming + 480s timeout + cancel + auto-snapshot + uvicorn 2-workers all working

---

## Halt-trigger reference

Halt and surface a question to the user if any of the following occur:

- Baseline pytest or vitest red before Phase 1 starts
- Phase 1 archetype-relevance scoring lands but live Edgar rebuild still shows A-prefix wave
- Phase 3 iterate-until-target loops run away (hits the iteration cap on multiple categories — suggests a deeper modeling issue)
- Phase 4 vocabulary migration breaks Pillar C primitive coverage below iter-7's 93.0%
- Phase 7 sweep matrix lands at <3/5 cases passing
- Sweep #3 deferred items (voyage_downgrade wiring, voyage_rules at scale, ENGINE_API_GUIDE.md overhaul, 8 pre-existing test failures) discovered to still be open and the user did not pre-authorize folding them in
- Any architectural rule listed above appears to be at risk of regression — halt before merging and confirm the user wants to ship the change anyway

Default user-revision pattern across iter 3-8 has been "revise criteria + ship" when sweeps land at partial pass. Continue that pattern in iter 9 — do not over-iterate on validation. Land 3-4 sweep passes, take the criteria-revision question to the user, ship.

---

## Budget + scope reminder

$70 API ceiling. 2-3 weeks CC time. 9 phases. Headline goal is closing the slot-fallback alphabetical-fill gap; everything else is supporting work. If wallclock or spend trends toward the ceiling before Phase 7, halt and surface a scope-trim option to the user — do not silently push past either bound.

Pillar F v0.2 game engine substrate is OUT of iter 9 implementation scope. Phase 8 produces a scoping document only; the actual build work dispatches as iter 10+ as its own dedicated multi-mega-task arc.

Good luck.
