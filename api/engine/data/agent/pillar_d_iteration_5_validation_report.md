# Pillar D Iteration 5 - Validation Report

Generated: 2026-05-21 14:52:08
Snapshot: `20260217_190902_tagpass_20260222`

## Headline

**Passed: 8 / 11 success criteria.**

- [PASS] **iter1_structural_pass_5_of_5** - value `True`
- [PASS] **mean_creativity_delta_geq_35** - value `36.0` (threshold `35`)
- [FAIL] **mean_novel_combo_geq_5** - value `4.6` (threshold `5`)
- [PASS] **mean_cost_usd_leq_0_45** - value `0.2957` (threshold `0.45`)
- [PASS] **mean_wallclock_s_leq_120** - value `118.0` (threshold `120`)
- [FAIL] **voyage_semantic_avg_geq_3** - value `1.6` (threshold `3`)
- [PASS] **pillar_c_coverage_v1_geq_90pct** - value `93.0` (threshold `90`)
- [PASS] **ur_dragon_hellkite_absent**
- [PASS] **pillar_f_ordering_sane** - value `{'yu': 0.68, 'kr': 0.549, 'ed': 0.282, 'ud': 0.321, 'at': 0.162}`
- [PASS] **theme_profile_structured** - value `5/5`
- [FAIL] **intent_preservation_drift_lt_0_5** - value `0.607` (threshold `0.5`)

## Per-case summary

| Case | iter1 | wall (s) | cost ($) | calls | creativity | novel | semantic | coverage_v1 | C2.1 (s) | archetype | drift | pod_winrate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| edgar_b3_vampire_tribal | PASS | 120.1 | $0.2967 | 8 | 37 | 5 | 2 | 90.3% | 40.7 | tribal | 0.441 | 0.282 |
| krenko_b4_goblin_combo | PASS | 118.1 | $0.3094 | 8 | 37 | 6 | 0 | 87.3% | 39.4 | tribal | 0.467 | 0.549 |
| atraxa_b2_proliferate | PASS | 126.7 | $0.2846 | 8 | 32 | 3 | 3 | 100.0% | 0.0 | control | 0.849 | 0.162 |
| yuriko_b5_ninja_tempo | PASS | 110.5 | $0.2928 | 8 | 34 | 2 | 1 | 96.8% | 38.9 | tribal | 0.605 | 0.68 |
| ur_dragon_b3_dragon_tribal | PASS | 114.7 | $0.2949 | 8 | 40 | 7 | 2 | 90.6% | 39.5 | tribal | 0.673 | 0.321 |

## Means

- creativity_delta: 36.0
- novel_combo: 4.6
- cost_usd: 0.2957
- wallclock_s: 118.0
- voyage_semantic: 1.6
- primitive_coverage_v1_pct: 93.0
- intent_preservation_drift: 0.607
- combo_space_external_added: 3
- criterion_12_status: TIER-3-SKIPPED (per kickoff; at-scale extractors not run)

## Theme profiles per case

- edgar_b3_vampire_tribal: primary=tribal (0.6) / secondary=tokens (0.2) / tertiary=aristocrats (0.2) [mode=hybrid]
- krenko_b4_goblin_combo: primary=tribal (0.6) / secondary=tokens (0.3) / tertiary=aristocrats (0.1) [mode=hybrid]
- atraxa_b2_proliferate: primary=counters_matter (0.6) / secondary=value_engine (0.3) / tertiary=combo (0.1) [mode=hybrid]
- yuriko_b5_ninja_tempo: primary=tribal (0.6) / secondary=combo (0.3) / tertiary=control (0.1) [mode=hybrid]
- ur_dragon_b3_dragon_tribal: primary=tribal (0.6) / secondary=value_engine (0.3) / tertiary=tokens (0.1) [mode=hybrid]

## Re-sweep after Phase 13 patches — 8/11 pass, HALT per user direction

User authorized option (c): patches 1-7 + re-sweep, proceed at >=10/11.
8/11 fell short. Patch-by-patch delta vs the initial 6/12:

| Criterion | Initial | Re-sweep | Δ | Status |
|---|---|---|---|---|
| iter1_structural_pass | FAIL (4/5) | **PASS (5/5)** | fixed | Safety net worked. |
| creativity_delta | 35.8 | 36.0 | +0.2 | PASS |
| novel_combo | 4.8 | 4.6 | -0.2 | still FAIL (Yuriko + Atraxa novel=2,3) |
| cost | $0.29 | $0.30 | +0.01 | PASS |
| wallclock | 115.8 | 118.0 | +2.2 | PASS (revised ≤120) |
| voyage_semantic | 1.8 | **1.6** | -0.2 | still FAIL even with MUST-SELECT-3 prompt |
| coverage_v1 | 93.4 | 93.0 | -0.4 | PASS |
| Hellkite absent | PASS | PASS | — | PASS |
| pillar_f ordering | PASS | PASS | — | PASS |
| theme_profile_structured | PASS | PASS | — | PASS |
| intent_drift | 0.887 | **0.607** | -0.28 | FAIL (revised <0.5; got to 0.607) |
| criterion 12 | -33 | n/a | retired | TIER-3-SKIPPED |

**What worked:**
- **Patch 1 (structural safety net)** restored iter1 5/5 ✓
- **Patch 3 (B2 vocab constraint)** drove intent_drift from 0.887 → 0.607 (closer; not yet under 0.5). All 5 cases now emit canonical themes: tribal / counters_matter / etc.
- **Patch 4 (combo_space metric)** now reports 3 external additions correctly (vs the misleading -33). Criterion retired per kickoff anyway.
- **Patch 5 (revised thresholds)** correctly absorbed honest-architectural targets.

**What didn't work yet:**

- **Patch 2 (MUST-SELECT-3 prompt)** did NOT lift voyage_semantic_avg. Per-case: edgar 2, krenko 0, atraxa 3, yuriko 1, ur_dragon 2 → mean 1.6. **Krenko at 0** is suspicious — Krenko is mono-R; n_semantic_in_pool may be 0 for that color identity (few mono-R cards cluster with Krenko's Voyage neighbors after color-filter). The MUST-SELECT-3 prompt only fires when ≥1 neighbor is in the pool; if 0 are present the constraint is silent. Hypothesis: the substrate's Voyage retrieval is under-producing color-legal neighbors for low-color decks, not that the LLM is rejecting them.

- **novel_combo 4.6** mostly tracks Yuriko (novel=2) and Atraxa (novel=3). Yuriko is cEDH with locked Thoracle+DC; novel combos are HARD because the deck already armed the canonical combo at must-include time. Atraxa proliferate similar (Doubling Season + Pir already canonical).

- **intent_drift 0.607** improved 30%+ from patch 3 but Atraxa (0.849) and Ur-Dragon (0.673) outliers drag the mean. Diagnosis: the `_THEME_PRIMITIVE_SIGNALS` vocabulary is too narrow for `counters_matter` (only `doubler-effect` and Atraxa's deck has primarily proliferate-flavored signals that don't map cleanly) and underweights tribal-anchor density for Ur-Dragon (which has dragon-typal cards but only some carry the `tribal-anchor` primitive).

**Next-move options for user:**

**Option (a) — Accept 8/11, proceed to Phase 14.** The iter 5 substrate ships; novel_combo and voyage_semantic are inherent-to-corpus issues; intent_drift improvement was substantial and remaining ~0.6 is metric-vocabulary noise, not deck-quality issue. Forward-fix in iter 6.

**Option (b) — One more round of targeted patches:**
- Expand `_THEME_PRIMITIVE_SIGNALS` to include more signals per theme (e.g., `counters_matter` adds `etb-trigger` + `attack-trigger` since proliferate cards often fire on those).
- Investigate Krenko's voyage_semantic=0 — likely a color-filter gap in the pool builder, not the LLM. Cheap to diagnose.
- Lower the primary weight default in B2's prompt from 0.6 → 0.5 (reduces over-expectation drift).
- Re-sweep ($1.50, 12 min).

**Option (c) — Skip more sweeps; accept current 8/11 as ship state and write Phase 14 final report.** Same as (a) effectively. The Phase 1-12 architectural deliverables all shipped; the iter 5 sweep numbers are honest measurements of where the substrate landed.