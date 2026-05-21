# Pillar D Iteration 5 - Validation Report

Generated: 2026-05-21 14:08:30
Snapshot: `20260217_190902_tagpass_20260222`

## Headline

**Passed: 6 / 12 success criteria.**

- [FAIL] **iter1_structural_pass_5_of_5** - value `False`
- [PASS] **mean_creativity_delta_geq_35** - value `35.8` (threshold `35`)
- [FAIL] **mean_novel_combo_geq_5** - value `4.8` (threshold `5`)
- [PASS] **mean_cost_usd_leq_0_45** - value `0.2911` (threshold `0.45`)
- [FAIL] **mean_wallclock_s_leq_110** - value `115.8` (threshold `110`)
- [FAIL] **voyage_semantic_avg_geq_4** - value `1.8` (threshold `4`)
- [PASS] **pillar_c_coverage_v1_geq_90pct** - value `93.4` (threshold `90`)
- [PASS] **ur_dragon_hellkite_absent**
- [PASS] **pillar_f_ordering_sane** - value `{'yu': 0.68, 'kr': 0.549, 'ed': 0.282, 'ud': 0.321, 'at': 0.162}`
- [PASS] **theme_profile_structured** - value `5/5`
- [FAIL] **intent_preservation_drift_lt_0_3** - value `0.887` (threshold `0.3`)
- [FAIL] **combo_space_expanded_geq_500** - value `-33` (threshold `500`)

## Per-case summary

| Case | iter1 | wall (s) | cost ($) | calls | creativity | novel | semantic | coverage_v1 | C2.1 (s) | archetype | drift | pod_winrate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| edgar_b3_vampire_tribal | PASS | 112.0 | $0.2909 | 8 | 37 | 7 | 3 | 90.3% | 38.3 | tribal | 0.761 | 0.282 |
| krenko_b4_goblin_combo | PASS | 102.0 | $0.3042 | 8 | 37 | 5 | 1 | 90.5% | 35.8 | tribal | 1.000 | 0.549 |
| atraxa_b2_proliferate | PASS | 138.6 | $0.2806 | 8 | 32 | 4 | 2 | 97.1% | 0.0 | counters_matter | 1.000 | 0.162 |
| yuriko_b5_ninja_tempo | FAIL | 113.1 | $0.2911 | 8 | 33 | 2 | 0 | 96.8% | 39.5 | combo | 0.907 | 0.68 |
| ur_dragon_b3_dragon_tribal | PASS | 113.3 | $0.2885 | 8 | 40 | 6 | 3 | 92.2% | 37.8 | tribal | 0.769 | 0.321 |

## Means

- creativity_delta: 35.8
- novel_combo: 4.8
- cost_usd: 0.2911
- wallclock_s: 115.8
- voyage_semantic: 1.8
- primitive_coverage_v1_pct: 93.4
- intent_preservation_drift: 0.887
- combo_space_delta: -33

## Theme profiles per case

- edgar_b3_vampire_tribal: primary=vampire_tribal (0.6) / secondary=lifegain_drain (0.3) / tertiary=go_wide_tokens (0.1) [mode=hybrid]
- krenko_b4_goblin_combo: primary=goblin_tribal (0.6) / secondary=token_generation (0.3) / tertiary=top_of_library_manipulation (0.1) [mode=hybrid]
- atraxa_b2_proliferate: primary=proliferate_counters (0.6) / secondary=planeswalker_support (0.3) / tertiary=counters_multiplication (0.1) [mode=hybrid]
- yuriko_b5_ninja_tempo: primary=ninja_tribal (0.6) / secondary=thassa_oracle_combo (0.3) / tertiary=library_manipulation (0.1) [mode=hybrid]
- ur_dragon_b3_dragon_tribal: primary=dragon_tribal (0.6) / secondary=etb_triggers (0.3) / tertiary=ramp_to_fatties (0.1) [mode=hybrid]

## Halt analysis - 6/12 passed, hard halt #5 triggered

Per kickoff hard halt condition #5 ("Phase 13 final validation fails
on >= 3 of 12 success criteria. Halt; don't proceed to Phase 14 final
regression on a broken iter 5"), this report is the halt event.
6 of 12 criteria failed. Honest analysis of each:

**Criterion 1 - iter1_structural_pass_5_of_5: FAIL (4/5 pass)**

Yuriko had iter1=FAIL. The other 4 cases all passed. Likely a
must-include drop or singleton violation specifically on Yuriko's
build. Cause undiagnosed in-session; would need a re-run with extra
instrumentation. Note that the same Yuriko case was clean in iter 4.
**Hypothesis**: Phase 2's tighter C2.1 output budget (5000->3000
tokens) may have caused JSON truncation specifically when C2.1
produced verbose Yuriko picks. The fallback path may have dropped a
must-include in the resulting state.

**Criterion 3 - mean_novel_combo 4.8 vs >=5: CLOSE MISS (0.2 below)**

Iter 4 baseline: 5.2. Slight drop. Yuriko at novel=2 pulls the mean
down; without that case the other 4 average 5.5. Likely tied to the
Yuriko iter1 fail above.

**Criterion 5 - mean_wallclock 115.8s vs <=110s: CLOSE MISS (5.8s above)**

Phase 2's C2.1 trim landed (~38s C2.1 latency, down from ~50s in
iter 4). The remaining gap is **Atraxa at 138.6s** (with C2.1
latency=0 — see below). Without Atraxa the other 4 average 110.1s.

**Atraxa anomaly: C2.1 latency = 0.0s.** Suggests C2.1 short-
circuited / failed silently on Atraxa specifically. Could be related
to the larger combo-anchor guard prompt on Atraxa (Doubling Season +
Pir produce ~30+ forbidden cards) pushing C2.1's input toward / past
the new 10k token budget gate, triggering the budget guard's no-call
fallback. The build still produced creativity_delta=32 and novel=4
(usable but not Phase-2-improved).

**Criterion 6 - voyage_semantic_avg 1.8 vs >=4: FAIL**

The Phase 1 score-boost + prompt-engineering did NOT move the needle
on Voyage selection. Iter 4 was also 1.8. The C2.2 LLM continues to
under-pick semantic neighbors despite the explicit PRIORITY GUIDANCE.
**Diagnosis**: the +0.15 score boost is too small to actually shift
pool ranking (theme-overlap candidates score 10+, semantic neighbors
~0.95 — semantic still ranks well below). And the LLM treats the
prompt guidance as advisory, not binding. **Next moves**: either
(a) much larger score boost (e.g. semantic candidates floor at score
5.0) so they appear in the LLM's visible window, or (b) restructure
C2.2 to do a separate "semantic-only" sub-call.

**Criterion 11 - intent_preservation_drift 0.887 vs <0.3: FAIL**

The B2 LLM emits open-vocabulary theme names (`vampire_tribal`,
`goblin_tribal`, `proliferate_counters`, `lifegain_drain`,
`token_generation`, `thassa_oracle_combo`, `etb_triggers`,
`planeswalker_support`, ...) but my `agent_intent_preservation_check_v1`
classifier uses a closed 13-theme vocabulary with limited alias
coverage. Most LLM-emitted themes don't map to my canonical themes,
producing 100% drift in profile_themes vs deck_mix comparison.

**Fix path** (small but not run in-session due to sweep-cost):
either (a) constrain B2's system prompt to emit only from a closed
canonical theme vocabulary, or (b) expand the alias map +
_THEME_PRIMITIVE_SIGNALS to cover open vocabulary. Path (a) is
preferred — close the LLM's emission set so downstream classifier
matches reliably.

**Criterion 12 - combo_space_expanded -33 vs >=500: FAIL (kickoff allows skip)**

Two issues:

1. The metric implementation: `merged_count - canonical_count`
   produces a NEGATIVE delta because Spellbook's canonical registry
   has internal duplicates by sorted-card-name pair-key (different
   variant_ids for the same card pair, different combo lines). My
   merger correctly dedupes these → merged_count < canonical_count.
   Correct metric: count external-source variants successfully
   landed in the merged registry. That count is 12 (all curated
   seed entries) — but well below 500.

2. The target 500 was per kickoff's "live extractor" path. I
   Tier-3-skipped the at-scale EDHRec/cEDHdb extractors (Cloudflare
   gating + time budget). The kickoff explicitly says: "allow Tier-3
   -skip if Phase 12 skipped, in which case this criterion is
   removed." Phase 12 SHIPPED a partial — 12 curated entries — so
   the criterion should be tagged TIER-3-PARTIAL-SKIP not FAIL.

## What shipped successfully (6 passing + Phases 1-12)

- creativity_delta 35.8 (>=35 target)
- cost $0.29 (well under $0.45 target)
- coverage_v1 93.4% (well above 90% target)
- Hellkite Charger absent on Ur-Dragon
- Pillar F ordering Yuriko > Krenko > Edgar ~ Ur-Dragon > Atraxa
- theme_profile structured (5/5 cases produced valid profiles)

All architectural Phase 1-12 modules shipped + unit-tested:
- Semantic-neighbor score boost + C2.2 priority guidance (Phase 1)
- C2.1 prompt trim — input 16k->7k, output 5k->3k, latency 51s->38s
  on Yuriko but Atraxa C2.1 anomaly noted (Phase 2)
- Pillar C ontology v1 + 17 rules_modifier tags + LLM extractor
  module; corpus coverage 83.8% -> 90.8% on cards-with-abilities (Phase 3)
- Voyage rules+rulings embedding pipeline + schema migration (Phase 4)
- B2 structured weighted theme profile + 4-mode inference (Phase 5)
- Theme profile cascading through C2.1/C2.2/D2 (Phase 6)
- Theme-aware Pillar E target counts via blended matrix (Phase 7)
- User-intent-preservation validation module (Phase 8)
- Aggressive Pillar E mana-base reconciliation (any-delta gate) (Phase 9)
- Mana-cost-aware Voyage downgrade pass module (Phase 10)
- Functional diversity prompt-engineering with Pillar E targets (Phase 11)
- Combo DB merger + 12-entry curated external seed (Phase 12)

## User options on resumption (mirrors iter 3 option (c) / iter 4 option (a))

**Option (a)** — Revise criteria + minor in-session fixes, accept
revised pass count, proceed to Phase 14. Concretely:
- Revise criterion 5 (wallclock 110s -> 120s) — close miss with
  documented Atraxa anomaly worth a follow-up but not iter-5 blocker.
- Revise criterion 6 (voyage_semantic 4 -> 2) — Phase 1 substrate is
  correct; further selection improvement is iter 6 work.
- Revise criterion 11 (intent_drift 0.3 -> 0.5 AND constrain B2 to
  closed vocabulary) — single small B2 system-prompt change closes
  the gap properly.
- Mark criterion 12 TIER-3-SKIPPED per kickoff clause.
- Investigate + fix Yuriko iter1 fail (likely the Phase 2 output
  budget) — small fix.
- Re-running the sweep costs ~$1.50 + 12 min if desired.

**Option (b)** — Tier-2 fix-without-resweep:
- Patch the B2 vocabulary constraint + alias expansion in-place.
- Patch the combo_space metric to count external additions.
- Document Yuriko iter1 fail + Atraxa C2.1 anomaly as iter 6
  follow-up items.
- Accept the current sweep numbers + proceed to Phase 14 final
  regression on the substrate ship state (architectural deliverables
  shipped; tuning the iter-5 sweep targets is the open item).

**Option (c)** — Authorize a re-sweep after the small fixes
(estimated 1-2 criteria flip from FAIL to PASS — Yuriko iter1 +
intent_drift via vocab constraint). Cost: $1.50 + 12 min.