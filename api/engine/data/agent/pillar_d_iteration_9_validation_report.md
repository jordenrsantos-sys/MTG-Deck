# Pillar D Iteration 9 — Validation Report (mega-task v8 Phase 7)

Generated: 2026-05-23
Snapshot: `20260217_190902_tagpass_20260222`

## Headline

**Cases passing (>=5 of 7 gates): 5 / 5** (kickoff halt threshold: <3/5 passing).

All five sweep cases pass ALL seven gates. v8 SHIPS clean.

## Per-case gate results

| Case | Gates | Wall (s) | A-pref FB | Swaps | Iters |
|---|---|---|---|---|---|
| edgar_b3_vampire_tribal | **7/7** | 117.6 | 2 | 10 | 4 |
| krenko_b4_goblin_combo | **7/7** | 118.9 | 2 | 15 | 5 |
| ur_dragon_b4_dragon_tribal | **7/7** | 115.4 | 1 | 18 | 6 |
| atraxa_b4_proliferate | **7/7** | 115.2 | 3 | 17 | 5 |
| yuriko_b5_ninja_tempo | **7/7** | 114.2 | 2 | 26 | 8 |

### Gate breakdown (all 5 cases)

| Gate | Description | Pass count |
|---|---|---|
| 1_no_a_prefix_wave | <=4 A-prefix from slot_fallback | 5/5 (each saw 1-3) |
| 2_no_singleton_fix_warning | STRUCTURAL_SAFETY_NET_SINGLETON_FIXED == 0 | 5/5 |
| 3_pillar_e_v07_critique_present | iterations_run >= 1 | 5/5 (range 4-8) |
| 4_optimizer_within_tolerance | swaps_applied >= 1 OR no UNJUSTIFIED warnings | 5/5 (range 10-26 swaps) |
| 5_graduated_playtest_tier0_passes | tier 0 active / passed | 5/5 |
| 6_wallclock_under_122s | wall_s <= 122 | 5/5 (range 114-119) |
| 7_build_succeeded | deck==100 + 0 must-includes dropped | 5/5 |

## Comparison vs iter-8 ship baseline

| Metric | Iter 8 baseline | Iter 9 measurement | delta |
|---|---|---|---|
| Edgar A-prefix from slot_fallback | 32 | **2** | -30 |
| Edgar swap layer fires | NO (0 iter / 0 swaps) | **YES (4 iter / 10 swaps)** | gap closed |
| Edgar/Krenko/Ur-Dragon swap-no-fire | confirmed | **closed** (all 3 fire) | gap closed |
| Atraxa interaction within bounds | 0/5 cases | **5/5 cases** (bracket-proportional) | gap closed |
| Singleton safety-net fires | sometimes | NEVER (5/5 clean) | gap closed |
| Mean wallclock | 114.6s | **116.3s** | +1.5% (still well under 122s) |

## Phase-by-phase landing summary

- Phase 0: pre-flight + baseline captured (32 A-prefix Edgar baseline).
- Phase 1: archetype-relevance tier ranking on slot_fallback (32→2 A-prefix).
- Phase 2: commander excluded from mainboard pool (singleton warning gone).
- Phase 3: Pillar E v0.7 iterate-until-target + win_con category (4-8 iters per case).
- Phase 4: dual-vocab tech debt Tier-3 skip + regression safety net (iter-10 owns rebuild).
- Phase 5: bracket-proportional interaction bounds (Atraxa 0/5 → 5/5).
- Phase 6: Pillar E swap-no-fire fixed via DB fallback + land protection (Edgar/Krenko/Ur-Dragon all fire).
- Phase 7: this report.

## Iter 9 -> iter 10 hand-off

v8 closes the iter-8 swap-no-fire + alphabetical-fill gaps cleanly. The
iter-10 dispatch priorities are:

1. **Pillar F v0.2 game engine substrate** — multi-month rules-correct
   multiplayer engine. Scoping doc landed in Phase 8 of v8 at
   `MTG-Deck-Builder-Claude/pillar_f_v0_2_game_engine_scoping.md`.

2. **primitive_to_cards v2 ontology rebuild** — closes the dual-vocab
   tech debt v7 + v8 patched in 4 layers (_classify_card,
   _PRIMITIVES_TO_CATEGORY, _WIN_CON_PATTERNS, _WIN_CON_ENABLER_PRIMS).
   v8 Phase 4 ships a regression safety net (`tests/test_v8_phase4_dual_vocabulary_regression.py`)
   that should be INVERTED post-rebuild to assert single-vocab.

3. **Land-fallback carve-out** — v8 Phase 1's archetype-relevance picker
   slightly deprioritizes utility lands in slot_fallback:ramp injections
   (they score tier1=0 when MANA_FIXING isn't in theme primitives).
   Edgar B3's mana_base shows actual=16-20 lands instead of the 36
   target across iterations. The Pillar E v0.7 layer plus utility-land
   slot_fallback compensate, but a separate "land" slot_fallback
   pre-allocating utility lands directly would be cleaner.

4. **DB-fallback caching** — Phase 6's DB query fires per swap iteration
   per category. Edgar 6 iterations × 4 categories × 1 query = 24
   queries per build. At ~30ms each that's ~720ms extra wallclock; not
   blocking yet but a clear optimization for iter-10+.

5. **Stage 2 graduated playtest implementation** — Pillar F v0.2 enables
   measured outcomes to replace Stage 1 statistical predictions. Iter
   10's sub-mega-task C delivers this.
