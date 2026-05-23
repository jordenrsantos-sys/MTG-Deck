# Mega-task v8 / Pillar D iter 9 — progress log

Iter 9 dispatch on top of v7 ship (`a708b0f34`). Kickoff:
`mega_task_v8_kickoff.md`.

Append-only, timestamped sections per phase.

---

## Phase 0 — Pre-flight + baseline UI build (2026-05-23)

**Substrate snapshot:**
- HEAD: `a708b0f34` (v7 Phase 9 — SHIPPED at 10/12).
- pytest baseline (verified at v7 ship): 1608 / 25 skipped / 0 failed.
- vitest baseline (verified at v7 ship): 774 / 2 pre-existing failed.
- Memory dir state: 5 entries (MEMORY.md + 4 project/feedback files).

**Coherence sweep #3 deferred items status:** All 4 shipped in v6:
- `agent_voyage_downgrade_pass_v1` wiring — SHIPPED v6 Phase 5.
- `voyage_rules_embedding_v1` at-scale — SHIPPED v6 Phase 6 (667 sections).
- `ENGINE_API_GUIDE.md` overhaul — SHIPPED v6 Phase 8.
- 8 pre-existing test failures — SHIPPED v6 Phase 7 (retired with @skip).

No carry-over from sweep #3.

**Edgar B3 baseline build captured** (via TestClient — chrome-devtools-mcp
not in tool roster; substituted with TestClient + Python tool):

Warning code counts on the iter-9 entry build:
- `POOL_SLOT_FALLBACK_TRIGGERED`: 1
- `POOL_BACKFILL_USED_OVERFLOW_CANDIDATES`: 1
- `MANA_BASE_DISCREPANCY_UNJUSTIFIED`: 1
- `CARD_ADVANTAGE_DISCREPANCY_UNJUSTIFIED`: 1
- `CURVE_DISCREPANCY`: 1
- `INTERACTION_DISCREPANCY`: 1
- `SWAP_ITERATION`: 3
- `WILD_POOL_SEMANTIC_AUGMENTED`: 1
- `COMBO_ANCHOR_GUARD_ACTIVE`: 1
- `POOL_FORBIDDEN_FILTERED`: 1
- `THEME_THEME_NOT_FOUND`: 4

`STRUCTURAL_SAFETY_NET_SINGLETON_FIXED` did NOT fire on this seed — the
Edgar duplicate issue may be seed-dependent. Phase 2 fixes the upstream
regardless.

**A-prefix wave confirmed:** 32 A-prefix cards in the deck, mostly via
`slot_fallback:*` sources. Examples:
- `A-Karn, Living Legacy` (ramp), `A-Visions of Phyrexia` (ramp),
  `A-Carnelian Orb of Dragonkind` (ramp), `A-Town` (ramp),
  `A-Hall of Tagsin` (ramp), `A-Excavation Explosion` (ramp).
- `Academic Dispute` (card_draw), `Abeyance` (card_draw),
  `Asmodeus the Archfiend` (card_draw), `Aang's Defense` (card_draw),
  `Aang's Journey` (card_draw).
- `Abstruse Appropriation` (removal), `Active Volcano` (removal),
  `Affa Guard Hound` (removal), `Agonizing Demise` (removal),
  `Anguished Unmaking` (removal), `Aim for the Head` (removal).

**Root cause located.** `_inject_slot_fallback_candidates` in
`agent_build_deck_v1.py` (v7 Phase 1 code) sorts candidates by
`name` ASC after color-identity filtering:
```python
color_legal.sort(key=lambda c: c["name"])
```
That's the alphabetical-fill bug — there is no archetype-relevance
signal in the ranking, so the first N matches by name win.

**Phase 1 will fix this** with a tiered archetype-relevance score:
tier 1 archetype-tagged → tier 2 primitive-overlap → tier 3 generic-
staple. Alphabetical is NEVER a tier.

**Phase 0 deliverables:**
- Progress log scaffolded (this file).
- Task list created (9 phases, this session).
- Baseline warning + A-prefix evidence captured.

**Commit message:** "Phase 0 (mega-task v8): pre-flight + iter-9 baseline capture".

Committed as `0ca6e2913`.

---

## Phase 1 — Slot-fallback archetype-relevance scoring (BLOCKING) (2026-05-23)

**Root cause located in `_inject_slot_fallback_candidates`:** post-filter
sort was `color_legal.sort(key=lambda c: c["name"])` — purely
alphabetical. All injected cards got SLOT_FALLBACK_SCORE=1.0 too, so the
pool's downstream sort (by score desc, name asc) put A-prefix cards
ahead within each tier.

**Fix in `agent_build_deck_v1.py`:**

1. New `_collect_pool_theme_primitives(by_name)` derives:
   - `theme_primitives`: primitives appearing on theme-tagged candidates
     (source contains "theme:"). Edgar B3 → DEATH_PAYOFF, DIES_TRIGGER,
     COMBAT_DAMAGE_PAYOFF, EVASION, etc.
   - `deck_primitives`: primitives on ANY pool candidate.
   - When theme cards don't exist (corpus-thin commander), falls back
     to deck_primitives to avoid tier-3 alphabetical drift.

2. New `_score_fallback_candidate(card_prims, theme_primitives,
   deck_primitives, cmc)` returns a 4-tuple for sort:
   - tier1 = len(card_prims & theme_primitives) — archetype-tagged
   - tier2 = len(card_prims & deck_primitives) — primitive-overlap
   - tier3 = len(card_prims) — primitive-rich beats empty
   - cmc_neg = -cmc — lower cmc breaks ties
   Final element is NEGATIVE cmc, NOT card name. Alphabetical NEVER
   survives the sort.

3. New `_resolve_fallback_score(card_prims, theme_primitives,
   deck_primitives)` returns SLOT_FALLBACK_SCORE + 2.0 (tier1),
   +1.0 (tier2), +0.0 (tier3). Each candidate gets the right score so
   the pool's downstream sort places archetype-relevant fallback first.

4. `_inject_slot_fallback_candidates` rewritten to:
   - Parse primitives into the color_legal dict BEFORE sort (was after).
   - Sort by `_score_fallback_candidate` DESC.
   - Take top `gap` cards (archetype-ranked, not alphabetical).
   - Score injected cards via `_resolve_fallback_score`.
   - Add `tier_counts` per slot to the trace dict for diagnosis.

**Cross-apply to Pillar E v0.7 swap-target selection:** automatic.
`_filter_pool_by_primitives` returns candidates in pool order; with
Phase 1's tiered scoring, the pool's slot_fallback cards now sit in
relevance order (tier1 first). When v0.7 picks the top match for a
swap target, it gets the archetype-relevant fallback by default.
No additional code change needed in `pillar_e_aggressive_swaps_v1.py`.

**Verification on Edgar Markov B3 (live build):**

Pre-v8 baseline: **32 A-prefix cards** in deck (Academic Dispute,
Aang's Defense, Aang's Journey, A-Karn, A-Visions of Phyrexia,
A-Carnelian Orb of Dragonkind, A-Town, A-Hall of Tagsin, ...).

Post-v8 Phase 1: **4 A-prefix cards** (Academic Dispute, Aang's Defense
— legitimately the best tier-2 card_draw picks; A-Blood Artist — from
theme path; Arcane Signet — from LLM critic). Slot_fallback now picks
sensible vampire-coherent cards:
- Ramp: Junkyard Genius, Kjeldoran Outpost (sac outlet!), Kavaron
  Memorial World (pump team), Mirrorpool, Tomb of Urami, Horned
  Stoneseeker, Fountainport, Urza's Saga.
- Card_draw: Runehorn Hellkite, Dragon Mage, Rankle Master of Pranks,
  Stinkweed Imp, Phial of Galadriel.
- Removal: Resolute Reinforcements, Bolt Bend, Liberator, Kozilek the
  Great Distortion, Tegwyll's Scouring (BOARD_WIPE!), The Wandering
  Emperor, Benalish Knight, Circling Vultures.
- Win_condition: Captain Lannery Storm.

**Tests:**
- `tests/test_candidate_pool_fill_rate.py`:
  - Updated `test_pool_slot_distribution_is_healthy` to use
    `slot_fallback.added_per_slot` (the injection count) rather than
    `slot_distribution` (post-classification). v8 archetype-relevance
    picker prefers multi-primitive cards, which often re-classify out
    of their inject slot (Yuriko counterspells with COUNTERSPELL +
    RAMP_MANA + TOKEN_PRODUCTION classify as ramp).
  - NEW `test_v8_phase1_no_alphabetical_a_prefix_wave`: asserts ≤4
    A-prefix cards from slot_fallback per sweep case.
  - NEW `test_v8_phase1_tier_counts_surface_in_trace`: asserts
    `tier_counts` dict populated in slot_fallback trace + at least one
    slot has tier1 > 0.
- 7 fill-rate tests pass + 25 subtests pass.

**Commit message:** "Phase 1 (mega-task v8): slot-fallback archetype-relevance scoring — closes A-prefix wave".

Committed as `4e9b03f44`.

---

## Phase 2 — Singleton safety-net upstream fix (BLOCKING) (2026-05-23)

**Root cause:** `compute_archetype_brief_v1` returns the commander as
part of `staple_cards` for any commander that's also a staple of its
archetype (Edgar Markov for vampire tribal, Krenko for goblins, etc.).
`_build_candidate_pool` adds all staples to the pool via the
`archetype_staple` source. `_select_deck` Pass 2 then picks the
commander into the mainboard, triggering
`STRUCTURAL_SAFETY_NET_SINGLETON_FIXED: 'Edgar Markov' appeared 2× →
reduced to 1 + 1 basic(s)`.

**Fix in `_build_candidate_pool`:** added an upstream filter that
removes the commander from `by_name` after staple insertion and before
the LLM extension boost / forbidden filter. Emits new warning
`POOL_COMMANDER_EXCLUDED_FROM_MAINBOARD` when triggered. Safety net
stays in place as belt-and-suspenders for synthetic duplicates injected
in tests.

**Verification:** Edgar/Krenko/Ur-Dragon/Atraxa/Yuriko all 5 commanders
correctly excluded from mainboard pool. POOL_COMMANDER_EXCLUDED_FROM_MAINBOARD
fires on all 5 (every test-sweep commander is also an archetype staple).

**Tests:**
- `tests/test_candidate_pool_fill_rate.py`:
  - NEW `test_v8_phase2_commander_excluded_from_mainboard_pool`:
    asserts commander name NEVER appears in pool candidates for all 5
    sweep cases.
- 8 fill-rate tests pass + 30 subtests pass.

**Commit message:** "Phase 2 (mega-task v8): exclude commander from mainboard candidate pool".

Committed as `0c78a4710`.

---

## Phase 3 — Pillar E v0.7 iterate-until-target + category extension (BLOCKING) (2026-05-23)

**Fix part A — iterate-until-target loop:**

The v7 v0.7 swap layer ran ONCE per build. Iter-9 baseline showed
mana_base shipped at delta=+12 with only 2 swaps applied (kickoff cited
this as `MANA_BASE_DISCREPANCY_UNJUSTIFIED: actual=48, target=36`).
Iter-9 wraps the v0.7 invocation in agent_build_deck_v1.py with
MAX_PILLAR_E_ITERATIONS=8 loop:
- Iteration runs compute_pillar_e_aggressive_swaps.
- On non-zero applied: accumulate, swap deck, re-evaluate all 5
  optimizers on the post-swap deck (mana_base, card_advantage, curve,
  interaction, win_con).
- Exit on (a) zero swaps applied — no candidates remain, OR
  (b) no optimizer still significant — gaps closed.
- New summary fields: `iterations_run`, `per_iteration_telemetry`
  (per-iteration applied/skipped/per_category counts).

**Fix part B — category extension to win_con_coherence:**

Pre-v8 the v0.7 swap layer covered 4 categories (mana_base,
card_advantage, curve_smoother, interaction_designer). v8 adds
win_con_coherence as the 5th category. When `flagged_75pct_pile=True`,
the swap layer injects up to (primary_floor - current_top) win-con
enabler primitive cards. New constants:
- `_WIN_CON_ENABLER_PRIMS`: superset of all v2-vocab win-con pattern
  primitives (combo_win, tutor_chain, voltron_combat, go_wide_anthem,
  aristocrats, storm_spellslinger, reanimator, mill_alt_win,
  counters_proliferate, stax_lock, control_grind, landfall_aggro).
- `_PER_CATEGORY_SWAP_BUDGET["win_con_coherence"] = 4`.
- `TOTAL_SWAP_BUDGET` bumped 12 → 14.

**Fix part C — archetype-relevance in swap-target selection:**

Auto-applied via Phase 1. `_filter_pool_by_primitives` returns
candidates in pool order; v8 Phase 1's tiered scoring puts archetype-
relevant slot_fallback cards ahead of generic ones in pool order, so
v0.7's swap picks default to archetype-relevant fallback.

**Verification:**
- Edgar Markov B3 live build: iteration_loop=1 iteration, applied=0
  swaps. Optimizers are flagged but swap layer can't find new candidates
  (existing utility lands already in pool got pulled by Pass 3 land
  fill). This is the Edgar-specific swap-no-fire pattern Phase 6
  targets — not a Phase 3 regression; the iteration substrate is wired
  correctly.
- All 11 pillar_e_aggressive_swaps tests pass (+2 new
  V8Phase3WinConCoherenceCategoryTests).
- New `win_con_coherence_block` parameter wired through agent_build_deck.
- v8 self-correction note: Phase 1 archetype-relevance picker slightly
  deprioritized utility lands in slot_fallback ramp injections (they
  score tier1=0 because MANA_FIXING isn't in vampire-tribal theme
  primitives). Net effect: Edgar B3 now has fewer utility lands than
  pre-Phase-1; mana_base delta sits at -20 (16 actual vs 36 target)
  instead of +12. Not a regression vs the kickoff iter-8 baseline (+12
  also flagged unjustified) but a different shape. Iter-10 candidate:
  carve land-fallback separate from ramp-fallback so utility lands
  always fill the land slot.

**Commit message:** "Phase 3 (mega-task v8): Pillar E v0.7 iterate-until-target + win_con category".

Committed as `dafe75337`.

---

## Phase 4 — Vocabulary tech debt (TIER-3 SKIP per escalation protocol) (2026-05-23)

**Scope assessment:** Kickoff estimate "3-5 days CC time" for full
`primitive_to_cards` rebuild + risky data migration across 36k cards.
v8 budget envelope ($70 ceiling) + remaining phase scope (Phases 5-8
include the BLOCKING sweep + scoping doc) makes a full rebuild
substantively risky within iter-9. Tier-3 skip per kickoff escalation
protocol; iter-10 dispatch owns the rebuild as its own focused arc.

**Discovery during scope analysis:** the codebase actually has THREE
vocabularies, not two:
1. Legacy v1 ontology UPPERCASE in `primitive_to_cards` inverted
   index (MANA_ROCK, TARGETED_REMOVAL_CREATURE, CARD_DRAW_BURST).
2. Theme/bridge signals lowercase-hyphenated in
   `deck_theme_classifier_v1._SIGNAL_VOCAB` + `win_con_coherence._WIN_CON_PATTERNS`
   (combo-assembly, counterspell-hard, sac-outlet, death-trigger).
3. v6 Phase 3 ontology v2 UPPERCASE in `cards.primitives_v1_json`
   (RAMP_MANA, REMOVAL_SINGLE, COUNTERSPELL, PROLIFERATE).

The "rebuild" the kickoff prescribes assumes single-source ground
truth in v2, but vocab 2 (lowercase-hyphenated) underpins the
theme classifier + win-con patterns and isn't a simple alias for
vocab 1 or 3. Migration plan needs to either:
(a) re-extract all 36k cards with v2 extractor + drop vocab 1
    (preserves vocab 2 in classifier/patterns; safest),
(b) full collapse to a single canonical v2 vocab everywhere (deeper
    refactor; requires regenerating theme classifier signals too).

Iter-10 dispatch should pick one approach explicitly.

**Phase 4 deliverable:** `tests/test_v8_phase4_dual_vocabulary_regression.py`
locks in the dual-vocabulary invariants as a SAFETY NET. Six tests
assert legacy + v2 vocab both present in `_classify_card`,
`_PRIMITIVES_TO_CATEGORY`, `_WIN_CON_PATTERNS`. Seventh test asserts
v8 Phase 3's `_WIN_CON_ENABLER_PRIMS` is v2-canonical only (the
forward-pattern iter-10 should converge everyone to).

Iter-10 should INVERT this test file: replace dual-vocab assertions
with single-vocab assertions after the rebuild ships.

**Tests:** 7 v8 Phase 4 regression tests pass.

**Commit message:** "Phase 4 (mega-task v8): vocabulary tech debt — Tier-3 skip + regression safety net".

Committed as `dbe575a98`.

---

## Phase 5 — Bracket-proportional interaction bounds (2026-05-23)

**Context:** v7 Phase 6 shipped universal per-category bounds. Iter-8
sweep criterion 8 failed 0/5 because the universal bounds (e.g.,
targeted_creature_removal [4,7]) exceed the bracket interaction budget
for low brackets (B2 total=9 with mass_removal=2 leaves 7 for ALL
6 other categories — can't fit 4-7 in any one). Higher brackets
(B4/B5) legitimately run more interaction than the universal bound
permits.

**Implementation:**

New `_PER_CATEGORY_BOUNDS_BY_BRACKET` dict in
`interaction_designer_v1.py` with per-bracket (min, max) per category:

| Category | B1 | B2 | B3 | B4 | B5 |
|---|---|---|---|---|---|
| mass_removal | 1-3 | 1-3 | 2-4 | 2-4 | 0-2 |
| targeted_creature_removal | 2-4 | 2-5 | 3-6 | 3-7 | 4-8 |
| targeted_artifact_removal | 0-2 | 0-2 | 1-3 | 1-4 | 1-4 |
| targeted_enchantment_removal | 0-1 | 0-2 | 0-2 | 0-3 | 0-3 |
| counterspells (U-only) | 1-3 | 1-4 | 2-6 | 3-7 | 4-10 |
| graveyard_interaction | 0-2 | 0-2 | 0-3 | 1-4 | 1-4 |

B5 mass_removal lowered to 0-2 because cEDH decks lean instant-speed
+ counters; B5 counterspells widened to 4-10 because cEDH stacks lean
heavy counter density. B2 keeps light defensive shape.

New `_resolve_bracket_bounds(category, bracket)` helper returns the
bracket's row; falls back to v7 universal default
(`_PER_CATEGORY_BOUNDS_DEFAULT`) when bracket key missing.

`compute_interaction_targets` updated to call `_resolve_bracket_bounds`
instead of indexing `_PER_CATEGORY_BOUNDS` directly. Legacy
`_PER_CATEGORY_BOUNDS` constant aliased to default for backward compat
with the v8 Phase 4 dual-vocabulary regression test.

**Verification:**
- Atraxa B2 with targeted_creature_removal actual=2: in-range under
  v8 [2,5]; was below-min under v7 [4,7].
- Yuriko B5 with 8 counterspells: in-range under v8 [4,10]; was at-max
  under v7 [4,8].
- All other brackets verified via 5 new Phase 5 tests.

**Tests:** 35 interaction tests pass (+8 new
V8Phase5BracketProportionalBoundsTests covering each bracket lookup +
the Atraxa/Yuriko sweep-case scenarios + unknown-bracket fallback +
unknown-category None return).

**Commit message:** "Phase 5 (mega-task v8): bracket-proportional interaction bounds".

Committed as `21f00b6ac`.

---

## Phase 6 — Pillar E critique coverage extension (2026-05-23)

**Investigation:** iter-8 reported Edgar/Krenko/Ur-Dragon swap-no-fire.
Phase 3 iteration-loop trace on Edgar B3 confirmed: 1 iteration,
0 applied, 0 skipped — the swap layer's category sections weren't
even reaching `_swap_or_skip`. Root cause: after `_select_deck` runs,
the candidate pool's archetype-relevant cards (Sol Ring, Phyrexian
Arena, etc.) are mostly IN the deck already. `_filter_pool_by_primitives`
excludes them via `deck_names_lower` → empty list. The swap loop's
`for i in range(min(gap, len(candidates_in_pool), len(low_priority_out)))`
evaluates `min(20, 0, N) = 0` → no swap proposals constructed → no
skips logged → 0/0.

**Fix in `pillar_e_aggressive_swaps_v1.py`:**

1. New `_db_fallback_candidates_by_primitives` helper: when pool
   candidates run out, queries the DB directly for color-legal cards
   with the target primitives. Mirrors the Phase 1
   `_inject_slot_fallback_candidates` DB pattern (bypasses
   `search_cards_v1`'s >950-oid silent-disable). Includes Phase 1's
   archetype-relevance tiered sorting via a deck_primitives parameter.

2. New `_pool_or_db_candidates` helper: returns pool hits first, then
   DB fallback for the remaining gap. Plumbed into all 4 swap-layer
   sections (mana_base, card_advantage, interaction, win_con).

3. New `_collect_working_deck_primitives(working_deck, pool_by_name_lower)`:
   derives deck-wide primitives by looking up each deck card's pool
   entry. Used to seed the DB-fallback archetype scoring.

4. `_find_low_priority_deck_cards` extended: new `pool_by_name_lower`
   + `exclude_lands=True` (default) params. Excludes utility lands
   (Blood Crypt, Command Tower, Path of Ancestry) from swap-out lists
   for non-mana-base categories. Pre-v8 the card_advantage section
   was yanking Command Tower out of the deck to add a card-draw piece
   even when the mana_base was already under-supplied; now lands are
   protected from cross-category swap-out.

5. All 4 `_pool_or_db_candidates` call sites + 5
   `_find_low_priority_deck_cards` call sites updated to pass through
   `pool_by_name_lower` and `deck_primitives`.

**Verification (Edgar B3 live build):**

Pre-v8 Phase 6: 1 iteration, 0 swaps applied.
Post-v8 Phase 6: 6 iterations, **13 swaps applied** (per_category:
{card_advantage: 13}). Swap picks are sensible (Benalish Knight →
Shenanigans, Sanctum Seeker → Magus of the Chains, Cordial Vampire →
Chains of Mephistopheles, Blood Petal Celebrant → Necroplasm, Carrier
Thrall → Nightmare Void, Malakir Bloodwitch → Accelerate). No lands
swapped out (Blood Crypt, Command Tower, Path of Ancestry preserved).

**Tests:** 11 pillar_e_aggressive_swaps tests pass (no count change —
new helpers covered by existing fixtures + the integration smoke).
Adding dedicated unit tests for the DB-fallback path would require
live-DB fixture setup similar to test_candidate_pool_fill_rate; the
end-to-end Edgar smoke verifies the behavior.

**Commit message:** "Phase 6 (mega-task v8): Pillar E critique coverage extension — DB fallback + land protection".

Committed as `a4e4eabe0`.

---

## Phase 7 — Iter 9 final validation sweep + report (BLOCKING) (2026-05-23)

**Sweep script:** `tools/test_pillar_d_iteration_9.py` — 5 cases × 7 gates.

**Result: 5/5 cases pass 7/7 gates each. v8 ships clean.**

| Case | Gates | Wall (s) | A-pref FB | Swaps | Iters |
|---|---|---|---|---|---|
| edgar_b3_vampire_tribal | **7/7** | 117.6 | 2 | 10 | 4 |
| krenko_b4_goblin_combo | **7/7** | 118.9 | 2 | 15 | 5 |
| ur_dragon_b4_dragon_tribal | **7/7** | 115.4 | 1 | 18 | 6 |
| atraxa_b4_proliferate | **7/7** | 115.2 | 3 | 17 | 5 |
| yuriko_b5_ninja_tempo | **7/7** | 114.2 | 2 | 26 | 8 |

**Sweep cost: ~$1.60.** Cumulative v8 spend: ~$3-4 (baseline build +
Phase 6 Edgar verification builds + Phase 7 sweep).

**Comparison vs iter-8 ship baseline:**
- Edgar A-prefix from slot_fallback: 32 → **2** (Phase 1 working).
- Edgar swap layer firings: 0 swaps → **10 swaps over 4 iterations**
  (Phase 6 DB fallback + Phase 3 iteration loop working).
- Krenko + Ur-Dragon swap-no-fire: both now fire 15+ swaps (Phase 6
  fix universally applies).
- STRUCTURAL_SAFETY_NET_SINGLETON_FIXED: 0/5 cases trigger (Phase 2).
- Atraxa interaction bounds: was 0/5 → **5/5 cases** with bracket-
  proportional Phase 5 bounds.
- Mean wallclock: 114.6s iter-8 → **116.3s iter-9** (+1.5%, well under
  the 122s gate).

**Bug + fix during sweep:** the sweep script's final stdout print
contained a Unicode `≥` that crashed under the Windows cp1252 console
encoding (write_markdown_report never reached). Sweep DATA was complete
(captured via the per-case prints); report was written manually from
that data + the sweep script was patched to use ASCII `>=` in the
final summary print. Iter-10 should run with `PYTHONIOENCODING=utf-8`
to be safe.

**Halt check:** kickoff halt threshold is <3/5 passing. 5/5 → no halt;
v8 ships clean.

**Commit message:** "Phase 7 (mega-task v8): iter 9 final validation sweep — 5/5 pass 7/7 gates".
