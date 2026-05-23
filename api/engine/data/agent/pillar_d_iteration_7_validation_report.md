# Pillar D Iteration 7 — Validation Report (mega-task v6 Phase 11)

Generated: 2026-05-22 22:39:52
Snapshot: `20260217_190902_tagpass_20260222`

## Headline

**Passed: 10 / 14 success criteria** (kickoff target ≥12).

- [PASS] **iter1_structural_pass_5_of_5** — value `True`
- [PASS] **mean_creativity_delta_geq_35** — value `37.6` (threshold `35`)
- [PASS] **mean_novel_combo_geq_5** — value `5.4` (threshold `5`)
- [PASS] **mean_cost_usd_leq_0_50** — value `0.3111` (threshold `0.5`)
- [PASS] **mean_wallclock_s_leq_130** — value `111.1` (threshold `130`)
- [FAIL] **voyage_semantic_avg_geq_3** — value `2.2` (threshold `3`)
- [FAIL] **intent_drift_per_case_below_threshold_4_of_5** — value `3/5 (mean drift 0.546)`
- [FAIL] **pillar_e_v0_4_interaction_within_4_of_5** — value `0/5`
- [PASS] **pillar_e_v0_3_curve_check_5_of_5** — value `5/5`
- [PASS] **graduated_playtest_5_of_5** — value `5/5 (5 tier0 predictions)`
- [PASS] **ui_e2e_build_renders_5_of_5** — value `5/5`
- [FAIL] **win_con_coherence_5_of_5** — value `0/5`
- [PASS] **anti_meta_recommendations_5_of_5** — value `5/5`
- [PASS] **voyage_rules_query_geq_1_per_build_5_of_5** — value `5/5`

## Per-case summary

| Case | iter1 | wall (s) | cost ($) | creativity | novel | semantic | inj | drift | E v0.4 ok | wcc | amr | rules | dgr |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| edgar_b3_vampire_tribal | PASS | 115.0 | $0.2956 | 36 | 6 | 3 | 1 | 0.579 | n | n | y | 1 | n |
| krenko_b4_goblin_combo | PASS | 107.5 | $0.3114 | 37 | 5 | 1 | 1 | 0.476 | n | n | y | 1 | y |
| atraxa_b2_proliferate | PASS | 107.6 | $0.3574 | 41 | 6 | 3 | 0 | 0.485 | n | n | y | 1 | n |
| yuriko_b5_ninja_tempo | PASS | 115.2 | $0.2976 | 34 | 4 | 1 | 1 | 0.512 | n | n | y | 1 | y |
| ur_dragon_b3_dragon_tribal | PASS | 110.3 | $0.2935 | 40 | 6 | 3 | 0 | 0.679 | n | n | y | 1 | n |

## Iter 7 → iter 8 hand-off

Pillar E v0.1-v0.6 complete (mana base, card advantage, curve smoother,
interaction designer, win-con coherence, anti-meta hate). Phase 11 final
report + hand-off questions land in `mega_task_v6_final_report.md` at Phase 12.
