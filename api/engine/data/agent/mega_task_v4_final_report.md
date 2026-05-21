# Mega-task v4 - Final Report

Generated: 2026-05-21
Span: Phase 0 (2026-05-21 12:46) -> Phase 14 (2026-05-21 ~15:20)
Wall-clock: ~2.5 hours active development + ~25 min for two LLM sweeps.
Total API spend: ~$3.90 (against $100 ceiling - 3.9% utilization)
Final commit chain: 15 atomic commits on top of mega-task v3's `74d9dcfd1`.

## Executive summary

**14 of 14 phases shipped (Phases 0-13 + this Phase 14).** Iter 5 of
Pillar D passes 8 of 11 success criteria under user-revised targets
(criterion 12 retired as Tier-3-skipped per kickoff). All 12 phase-1-
through-12 architectural deliverables ship + tested + integrated.

The substrate consolidates 8 prioritized insights + 2 architectural
feedback rules + 3 iter-4 hand-off findings into a coherent ship:
- Semantic-neighbor selection priority guidance (Phase 1)
- C2.1 prompt compression (Phase 2)
- Pillar C ontology v1 with 17 rules_modifier tags + LLM extractor (Phase 3)
- Voyage rules+rulings embedding pipeline + schema (Phase 4)
- B2 structured weighted theme profile with 4-mode inference (Phase 5)
- Theme profile cascading through C2.1/C2.2/D2 (Phase 6)
- Theme-aware Pillar E target counts via blended matrix (Phase 7)
- User-intent-preservation validation with drift detection (Phase 8)
- Aggressive Pillar E mana-base reconciliation (any-delta gate) (Phase 9)
- Mana-cost-aware Voyage downgrade pass (Phase 10)
- Functional diversity prompt-engineering (Phase 11)
- Combo registry merger + 12-entry curated external seed (Phase 12)

Plus Tier-2 patches post-Phase-13:
- Structural safety net guaranteeing iter1 invariants (must-includes
  present, deck=100, no singleton violations) regardless of downstream
  LLM behavior
- B2 closed-vocabulary canonicalization for downstream drift checks
- Combo-space metric corrected from `merged - canonical` to "external
  additions"
- New feedback memory: pool ranking score does NOT drive LLM picking
  behavior in C2.2; prompt-level requirements are the correct lever

### Final 5-case agent sweep (iter 5 re-sweep, post-patches)

| Case | iter1 | wall (s) | cost ($) | calls | creativity | novel | semantic | coverage_v1 | C2.1 (s) | archetype | drift | pod_winrate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| edgar_b3_vampire_tribal | PASS | 120.1 | $0.2967 | 8 | 37 | 5 | 2 | 90.3% | 40.7 | tribal | 0.441 | 0.282 |
| krenko_b4_goblin_combo | PASS | 118.1 | $0.3094 | 8 | 37 | 6 | 0 | 87.3% | 39.4 | tribal | 0.467 | 0.549 |
| atraxa_b2_proliferate | PASS | 126.7 | $0.2846 | 8 | 32 | 3 | 3 | 100.0% | 0.0 | control | 0.849 | 0.162 |
| yuriko_b5_ninja_tempo | PASS | 110.5 | $0.2928 | 8 | 34 | 2 | 1 | 96.8% | 38.9 | tribal | 0.605 | 0.68 |
| ur_dragon_b3_dragon_tribal | PASS | 114.7 | $0.2949 | 8 | 40 | 7 | 2 | 90.6% | 39.5 | tribal | 0.673 | 0.321 |
| **mean** | **5/5** | **118.0** | **$0.2957** | **8.0** | **36.0** | **4.6** | **1.6** | **93.0%** | — | — | **0.607** |

### Iter 5 success criteria (11 total under user-revised targets)

| # | Criterion | Value | Target | Status |
|---|---|---|---|---|
| 1 | iter1_structural_pass_5_of_5 | True | True | PASS |
| 2 | mean_creativity_delta | 36.0 | >=35 | PASS |
| 3 | mean_novel_combo | 4.6 | >=5 | FAIL (close miss) |
| 4 | mean_cost_usd | $0.2957 | <=$0.45 | PASS |
| 5 | mean_wallclock_s | 118.0 | <=120 (revised from 110) | PASS |
| 6 | voyage_semantic_avg | 1.6 | >=3 (revised from 4) | FAIL |
| 7 | pillar_c_coverage_v1_pct | 93.0% | >=90% | PASS |
| 8 | ur_dragon_hellkite_absent | True | True | PASS |
| 9 | pillar_f_ordering_sane | Yu 0.68 > Kr 0.55 > Ud 0.32 ~ Ed 0.28 > At 0.16 | sane | PASS |
| 10 | theme_profile_structured | 5/5 | 5/5 | PASS |
| 11 | intent_preservation_drift | 0.607 | <0.5 (revised from 0.3) | FAIL |
| 12 | combo_space_expanded | 3 external | — | TIER-3-SKIPPED |

**8 / 11 pass. User accepted via option (a) — proceed to Phase 14 final regression.**

### Regression baselines

| Suite | Phase 0 baseline | Phase 14 actual | Delta |
|---|---|---|---|
| pytest passed | 1283 | **1377** | +94 new tests, all green |
| pytest pre-existing failures | 8 | 8 | unchanged |
| vitest passed | 711 | **711** | unchanged |
| vitest failures | 2 | 2 | unchanged |

No new test failures. Mega-task v3's 1283/711 baselines + v4's 94 new
tests all green.

## Phase-by-phase status

| # | Phase | Status |
|---|---|---|
| 0 | Pre-flight + progress log scaffold | OK |
| 1 | Semantic-neighbor score boost + C2.2 selection priority | OK (mechanism revised in Phase 13 retro) |
| 2 | C2.1 prompt trim - input 16k->7k, output 5k->3k, latency 51s->38s | OK |
| 3 | Pillar C ontology v1 + rules_modifier + LLM extractor + re-backfill | OK (90.8% coverage) |
| 4 | MTG rules + Scryfall rulings Voyage embedding pipeline | OK (at-scale embedding Tier-3 deferred) |
| 5 | B2 structured weighted theme profile + 4-mode inference | OK |
| 6 | Cascade theme_profile through C2.1/C2.2/D2 | OK |
| 7 | Theme-aware Pillar E target counts via blended matrix | OK |
| 8 | User-intent-preservation validation + drift check | OK |
| 9 | Aggressive Pillar E mana-base reconciliation (any-delta gate) | OK |
| 10 | Mana-cost-aware Voyage downgrade pass | OK |
| 11 | Functional diversity prompt-engineering with Pillar E targets | OK |
| 12 | Combo DB merger + 12-entry curated external seed | OK (at-scale extractors Tier-3 deferred) |
| 13 | Iter 5 final validation sweep | 8/11 PASS under revised criteria + post-patch resumption |
| 14 | Final regression + report + memory update | this commit |

### Per-phase test additions

| Phase | New tests |
|---|---|
| 1 | 5 (semantic-neighbor priority + score boost source-tag) |
| 2 | 6 (C2.1 trim) + 1 updated iter-3 test |
| 3 | 18 (ontology v1 + rules-modifier) |
| 4 | 7 (Voyage rules-embedding pipeline + query) |
| 5 | 11 (B2 theme profile normalize + mode inference + prompt schema) |
| 6 | 7 (theme cascade through C2.1/C2.2/D2) |
| 7 | 8 (theme target blender) |
| 8 | 8 (intent-preservation classifier + drift) |
| 9 | 4 (aggressive mana reconciliation) |
| 10 | 10 (Voyage downgrade pass) |
| 11 | 6 (functional diversity block) |
| 12 | 4 (combo registry merger) |
| **Total** | **~94 new tests** |

### Self-correction events log (highlights)

| Phase | Tier | Description |
|---|---|---|
| 1 | T1 | C2.2 user prompt extended with [VOYAGE_SEMANTIC_NEIGHBOR] tags + priority guidance |
| 2 | T1 | C2.1 output budget 5k -> 3k after first smoke showed output tokens dominating latency |
| 3 | T1 (x3) | Ontology parser missed `(parenthetical)` patterns; `{T}` normalization needed for `activated-ability-tap-cost`; regex flags MULTILINE not DOTALL |
| 4 | T3 | At-scale Voyage rules+rulings embedding deferred (source data not pre-staged) |
| 8 | T1 | Drift metric L1 -> missed-intent (overlap-spread was inflating drift on aligned decks) |
| 13 | (user) | Option (c) — Tier-2 patches + re-sweep |
| 13-retro | T1 | Structural safety net `color_identity` NameError fixed |
| 13-retro | (user) | Option (a) — accept 8/11, proceed to Phase 14 |

## Pillar D iter 5 metrics (vs iter 4 baseline)

| Metric | Iter 4 | Iter 5 | Delta |
|---|---|---|---|
| iter1 structural pass | 5/5 | 5/5 | unchanged (structural safety net guarantees) |
| Mean creativity_delta | 37.8 | 36.0 | -1.8 (still above 35 target) |
| Mean novel_combo | 5.2 | 4.6 | -0.6 (close to 5 target) |
| Mean cost | $0.31 | $0.30 | -3% |
| Mean wallclock | 129.3s | **118.0s** | **-8.7%** |
| C2.1 latency (Yuriko) | ~50s | ~38s | -24% |
| Voyage semantic per case | 1.8 | 1.6 | -0.2 (no lift from MUST-SELECT-3 prompt) |
| Pillar C coverage (cards-with-abilities) | 83.8% | 93.0% | +9.2 pp |
| Pillar F pod_winrate ordering | Yu>Kr>Ud~Ed>At | Yu>Kr>Ud~Ed>At | preserved |
| Intent_drift (new metric) | — | 0.607 | new measurement |

## Phase 14 final regression smoke battery

All 5 module-level smokes pass:

| Smoke | Result |
|---|---|
| Phase 3 primitive extractor v2 (100-card sample) | 100% coverage on Commander-legal cards-with-abilities |
| Phase 4 Voyage embeddings integrity | 30,395 card rows + schema columns present + rules table ready for future at-scale run |
| Phase 7 theme-aware blender | storm=32 lands, tribal=37 lands (matches matrix expectations) |
| Phase 8 intent-preservation | aligned-tribal deck drift=0.3 (warning=False) |
| Phase 12 combo merger | 49,659 canonical + 3 external additions in merged registry |

## Resource consumption

- **Wall-clock**: ~2.5 hours active development + 2 LLM sweeps × ~12 min = ~3 hours total. Within the 12-36h budget.
- **API spend**: ~$3.90 cumulative breakdown:
  - Phase 1 Edgar smoke: $0.30
  - Phase 2 Yuriko smokes (×2 incl. Tier-1 retry): $0.60
  - Phase 6 Edgar smoke: $0.30
  - Phase 13 initial sweep: $1.50
  - Phase 13 re-sweep: $1.50
  - Phase 13 Yuriko diagnostic: $0.30
  - Phases 3/4/5/7-12 build no live LLM calls
  - Total well under $100 ceiling
- **Test count**: 94 new tests; pytest 1283 -> 1377, vitest unchanged at 711
- **Lines of code**: ~3000 LoC added across new modules + extractors + tests + tools
- **Files added**: ~20 new modules (extractors, layers, tools, integrations) + ~15 new test files + 1 ontology + 1 matrix data file + 1 external combo seed

## Mega-task v4 -> iter 6 hand-off

### What didn't lift in iter 5

1. **voyage_semantic_avg stayed flat (1.8 -> 1.6) despite Phase 1 + Patch 2.** The pool-score boost did nothing; the MUST-SELECT-3 prompt requirement didn't lift either. **Per-case investigation revealed Krenko at 0 semantic picks** — likely because mono-R Voyage neighbors after color_identity_filter are sparse. The substrate gap is at the **pool builder layer**, not the C2.2 LLM. **Iter 6 priority: diagnose + fix the color-filtered Voyage neighbor count for narrow-color decks**, then re-verify the MUST-SELECT-N prompt works.

2. **intent_drift improved 32% (0.887 -> 0.607) but didn't reach <0.5.** Atraxa (0.849) and Ur-Dragon (0.673) outliers drag the mean. The 13-theme `_THEME_PRIMITIVE_SIGNALS` vocabulary doesn't capture all signals densely enough. **Iter 6 priority: extend the per-theme signal vocabulary** (e.g., `counters_matter` should signal on `etb-trigger` + `attack-trigger` when those fire from `+1/+1 counter` text; `tribal` should account for `tribal-anchor` density relative to deck creature count, not absolute primitive count).

3. **novel_combo 4.6 vs >=5.** Yuriko (novel=2) + Atraxa (novel=3) are deck-shape inherent — those decks have canonical combos ARMED at must-include time (Thoracle+DC, Doubling Season+Pir), so further novel-combo discovery is harder. Marginal improvement available; not a substrate problem.

### What did lift

- **Wallclock 129s -> 118s** (-8.7%) via Phase 2 C2.1 trim. Atraxa C2.1 anomaly (0.0s, suggesting silent budget-guard short-circuit on its larger combo-anchor prompt) is a remaining tuning issue.
- **Coverage 83.8% -> 93.0%** via ontology v1 + rules_modifier dimension. Above target.
- **Theme profile vocabulary** now closed-set canonical: all 5 cases emit valid themes (tribal/counters_matter/control/etc.) for downstream classifiers.
- **Structural invariants guaranteed 5/5** via the post-D2 safety net.

### Iter 6 priority ranking

1. **Voyage color-filter gap diagnosis + fix** — narrow-color decks (mono-R Krenko, mono-W) under-produce semantic neighbors after color_identity_filter. ~1-2 days. Without this, MUST-SELECT-N prompts can't fire.
2. **_THEME_PRIMITIVE_SIGNALS density expansion** — add 2-4 more signals per theme so drift detection has wider primitive coverage. ~1 day.
3. **Atraxa C2.1 silent failure** — diagnose the 0.0s C2.1 latency on Atraxa (input budget guard? prompt size?). ~half-day.
4. **At-scale Voyage rules+rulings embedding** (Phase 4 deferred work) — ~$1 budget + 1 hour wall; pipeline is ready.
5. **Live external combo-DB extractors** (Phase 12 deferred work) — EDHRec scrape extension + TappedOut extractor. ~1 week. Closes criterion 12 properly.

### Architectural insights captured in memory

- **`feedback_pool_score_does_not_drive_llm_picking.md`** — pool-side score boosts don't shift LLM selection; prompt-side requirements do. Iter 4 and iter 5 both verified this empirically.

## What's NOT in iter 5

- Pillar F v0.2 game engine substrate rebuild (multi-week; not scoped here)
- Opus 4.x upgrade (iter 5 didn't ceiling on creativity at Sonnet)
- Pillar E v0.3 curve smoother + v0.4 interaction designer + v0.5 win-con coherence checker (queued in 5-pillar plan)
- Multi-deck cross-pollination + reverse-engineering target decks
- Bracket-partitioned corpus

## Conclusion

Mega-task v4 shipped 15 atomic commits delivering iter 5 of Pillar D
plus 6 architectural extensions (ontology v1 + rules embedding + theme
profile + theme-aware Pillar E + intent validation + combo DB
merger). 8 of 11 iter-5 success criteria pass under user-revised
targets; 3 honest measurement gaps documented for iter 6. $3.90 of
$100 spend. 94 new tests; pytest baseline 1283 -> 1377.

Final commit count for this mega-task: 15 atomic commits on top of
mega-task v3's `74d9dcfd1` (which includes mega-task v3's
`f87486ac7`). See `git log --oneline 74d9dcfd1..` for the full chain.
