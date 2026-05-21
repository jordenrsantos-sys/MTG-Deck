# Pillar D Iteration 4 - Validation Report

Generated: 2026-05-21 09:38:14
Snapshot: `20260217_190902_tagpass_20260222`

## Headline

**Passed: 7 / 10 success criteria.**

- [PASS] **iter1_structural_pass_5_of_5** — value `True`
- [PASS] **mean_creativity_delta_geq_35** — value `37.8` (threshold `35`)
- [PASS] **mean_novel_combo_geq_5** — value `5.2` (threshold `5`)
- [PASS] **mean_cost_usd_leq_0_45** — value `0.3102` (threshold `0.45`)
- [FAIL] **mean_wallclock_s_leq_95** — value `129.3` (threshold `95`)
- [PASS] **ur_dragon_hellkite_charger_absent**
- [FAIL] **voyage_semantic_contribution_avg_geq_5** — value `1.8` (threshold `5`)
- [FAIL] **pillar_c_primitive_coverage_geq_95pct** — value `83.8` (threshold `95`)
- [PASS] **pillar_f_winrate_ordering_sane** — value `{'yuriko': 0.68, 'krenko': 0.549, 'edgar': 0.282, 'ur_dragon': 0.321, 'atraxa': 0.162}`
- [PASS] **atraxa_archetype_is_counters_matter** — value `counters_matter`

## Per-case summary

| Case | iter1 | wall (s) | cost ($) | calls | creativity | novel | semantic | coverage | archetype | pod_winrate |
|---|---|---|---|---|---|---|---|---|---|---|
| edgar_b3_vampire_tribal | PASS | 141.3 | $0.2991 | 8 | 36 | 6 | 3 | 83.9% | tribal | 0.282 |
| krenko_b4_goblin_combo | PASS | 126.0 | $0.3078 | 8 | 37 | 5 | 1 | 77.8% | tribal | 0.549 |
| atraxa_b2_proliferate | PASS | 124.0 | $0.3538 | 8 | 42 | 5 | 2 | 79.4% | counters_matter | 0.162 |
| yuriko_b5_ninja_tempo | PASS | 126.4 | $0.2972 | 8 | 34 | 3 | 1 | 91.9% | combo | 0.68 |
| ur_dragon_b3_dragon_tribal | PASS | 128.7 | $0.2929 | 8 | 40 | 7 | 2 | 85.9% | tribal | 0.321 |

## Means

- creativity_delta: 37.8
- novel_combo: 5.2
- cost_usd: 0.3102
- wallclock_s: 129.3
- voyage_semantic_contribution: 1.8
- pillar_c_primitive_coverage_pct: 83.8

## Outer-chain parallelization (Phase 3)

| Case | C2.1 latency (s) | C2.2 latency (s) | parallel window (s) | serial baseline (s) | savings (s) |
|---|---|---|---|---|---|
| edgar_b3_vampire_tribal | 57.3 | 20.0 | 57.3 | 77.4 | 20.0 |
| krenko_b4_goblin_combo | 51.7 | 20.7 | 51.7 | 72.4 | 20.7 |
| atraxa_b2_proliferate | 47.0 | 23.0 | 47.0 | 70.0 | 23.0 |
| yuriko_b5_ninja_tempo | 52.1 | 24.4 | 52.1 | 76.5 | 24.4 |
| ur_dragon_b3_dragon_tribal | 49.4 | 19.7 | 49.4 | 69.1 | 19.7 |

## Halt analysis - 7/10 passed, hard halt condition #6 triggered

Per kickoff hard halt condition #6 ("Phase 7 iter 4 final validation
fails on >= 2 of 10 criteria. Halt; don't proceed to Phase 8 final
regression on a broken iter 4"), this report is the halt event.

3 of 10 criteria missed. Honest analysis of each:

**Criterion 5 - mean_wallclock_s_leq_95: 129.3s vs 95s target.**

Iter 3 baseline was 139.8s. Iter 4 dropped to 129.3s (-7.5%). The
Phase 3 outer-chain parallelization is working as designed - per-case
savings 19.7-24.4s of C2.2 latency overlap (table above). The
architectural floor with the current chain is:

    B2 (~26s) + max(C2.1, C2.2) (~52s) + D2 max-batch (~30s) + 
    Pillar E mana_base critique (~5s) + Pillar E card_advantage 
    critique (~8s) = ~121s

Iter 4 hit ~129s mean - within 8s of the architectural floor. The 95s
kickoff target assumed deeper parallelization that wasn't in scope
(e.g. B2 in parallel with the C2.1/C2.2 pair, or trimming individual
call latency). Iter 5 options:

  - Parallelize B2 against the wide-pool build step (~10-15s saving).
  - Trim C2.1 prompt to reduce its 47-57s ceiling.
  - Accept ~120-130s as the realistic floor; revise target.

**Criterion 7 - voyage_semantic_contribution_avg_geq_5: 1.8 vs 5 target.**

Phase 1 Edgar smoke had 72 semantic neighbors injected into C2.2 wide
pool and 2 reached the final deck. The 5-case sweep saw 1-3 per case
(3 Edgar, 1 Krenko, 2 Atraxa, 1 Yuriko, 2 Ur-Dragon) = mean 1.8.

Diagnosis: semantic neighbors ARE landing in the C2.2 pool reliably
(verified in Phase 1 smoke), but C2.2's LLM doesn't consistently pick
them over higher-corpus-score candidates. Voyage neighbors have score
= cosine similarity (~0.7-0.85) which is much lower than theme-
matched candidates' score (~10+ per theme primitive). The 5/case
target would require either:

  - Boosting semantic neighbors' score in the wide pool (currently
    they get raw similarity; theme cards get 10x).
  - Explicit "consider these semantic neighbors" prompt fragment for
    C2.2.
  - Filtering wide pool to surface MORE semantic-neighbor diversity.

The mechanism is correct; the magnitude is below target.

**Criterion 8 - pillar_c_primitive_coverage_geq_95pct: 83.8% vs 95%.**

Sweep deck cards (commander + 99) with non-empty primitives_v1_json
average 83.8% across the 5 cases (range 77.8-91.9%). 

The Phase 5 extractor is correct (golden test 50/50, Spellbook 49/49)
but the ontology v0 has narrow coverage by design (combo-relevant
mechanics only). Lands without taps, vanilla creatures, generic stat-
boost equipment, etc. don't match any ontology pattern. The 95%
target assumed ontology coverage broader than the v0 spec aims for.

Iter 5 options:

  - Layer an LLM extractor over ambiguous cards (per kickoff's
    explicit authorization).
  - Extend ontology v0 with ~10-15 more tags for vanilla creatures,
    equipment stat-boost, lands, etc. (would be ontology v1).
  - Accept ~85% as the realistic ceiling for regex-only extraction.

## What shipped successfully (the 7 passing criteria + Phase 1-6)

- Iter 1 structural pass: 5/5
- Creativity_delta: 37.8 (well above 35 target, on par with iter 3's 37.6)
- Novel combo: 5.2 (above 5 target, on par with iter 3's 5.8)
- Cost: $0.31 (well under $0.45 target; matches iter 3's $0.30 + 
  small overhead from card_advantage critique)
- Hellkite Charger absent from Ur-Dragon: PASS (Phase 2 guard holds)
- Pillar F winrate ordering: Yuriko 0.68 > Krenko 0.55 > 
  Ur-Dragon 0.32 ~ Edgar 0.28 > Atraxa 0.16 (sane)
- Atraxa archetype: counters_matter (Phase 2 fix confirmed live)

Phase 1-6 deliverables all shipped + tested:
- Voyage AI index live (30,395 vectors, ~$1.62 one-time cost)
- counters_matter archetype detector working live (Atraxa shows 
  "counters_matter" in C2_2 archetype field this sweep)
- Outer-chain parallel saving 20-24s per case (table above)
- Pillar E v0.2 card_advantage block populated in summary; critique 
  fires when significant
- Pillar C primitive extractor built; 100% golden tests; 60.4% 
  corpus coverage / 100% Spellbook combo pair coverage
- Pillar F primitive-grounded win-paths (18 total, +6 new)

## Iter 4 -> 5 hand-off questions (kickoff)

1. **Did Voyage semantic retrieval surface novel synergies?** Yes - 
   2 of Edgar's 3 semantic-source cards were Elenda the Dusk Rose and
   Mavren Fein, both legitimate vampire-tribal cards the corpus prior
   under-weighted. Magnitude is below target (1.8 vs 5/case mean) but
   the mechanism produces signal. Iter 5 should boost semantic score
   in C2.2 wide pool.

2. **Did outer-chain parallelization save ~40-50s?** No - saved 
   ~20s/case on average (the parallel window of max(C2.1, C2.2) 
   ranged 47-57s vs serial 70-77s). The kickoff's 40-50s estimate 
   over-counted; the actual C2.2 latency is ~20s so overlap saves at
   most that. To save more, parallelize B2 against the wide-pool 
   build OR trim C2.1 directly.

3. **Did Pillar C cover the 6 dimensions evenly?** Backfill showed
   all 64 tags appearing at least once. Per-dimension coverage on
   sweep decks varies - combo_role and win_condition_role are well-
   covered; tempo and mana_valuation have gaps (e.g. tap-symbol 
   notation `{T}` was patched but other modern-text quirks remain).

4. **Did Pillar F orderings change vs iter 3?** Yes - Edgar, Krenko, 
   Ur-Dragon all gained pod_winrate (0.20 -> 0.28-0.55) because new 
   win-paths (mass_token_anthem, tutor_combo_assembly, extra_turn_
   chain) match their full decks. Yuriko went 0.56 -> 0.68 because 
   tutor_combo_assembly armed via Demonic Consultation + Thassa's
   Oracle. Ordering preserved.

5. **Did Pillar E v0.2 fire critique passes consistently?** Yes - 
   every case fired E_card_advantage_critique (8 LLM calls per case 
   vs iter 3's 7). Per-call cost ~$0.006-0.010.

6. **Orphan ontology tags?** All 64 appeared at least once in the
   backfill. None orphaned.

7. **Most plausible iter 5 priority?** Three candidates:
   - **Trim C2.1 latency** (~52s currently). Drop to 30-35s would 
     bring sweep mean to ~110s, much closer to a "fast" cEDH target.
   - **Boost semantic-neighbor score in C2.2 wide pool** to drive 
     voyage_semantic_contribution from 1.8 to 4-5 per case.
   - **Ontology v1 extension** - add tags for vanilla creatures, 
     equipment stat-boost, etc. to push corpus coverage above 90%.

   I'd prioritize C2.1 trim - largest impact for least architectural
   work.