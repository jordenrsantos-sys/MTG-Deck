# Pillar D Iteration 6 — Validation Report

Generated: 2026-05-22 15:13:31
Snapshot: `20260217_190902_tagpass_20260222`

## Headline

**Passed: 9 / 12 success criteria.**

- [PASS] **iter1_structural_pass_5_of_5** — value `True`
- [PASS] **mean_creativity_delta_geq_35** — value `37.6` (threshold `35`)
- [PASS] **mean_novel_combo_geq_5** — value `5.6` (threshold `5`)
- [PASS] **mean_cost_usd_leq_0_45** — value `0.3096` (threshold `0.45`)
- [PASS] **mean_wallclock_s_leq_120** — value `112.1` (threshold `120`)
- [FAIL] **voyage_semantic_avg_geq_3** — value `1.4` (threshold `3`)
- [FAIL] **intent_drift_per_case_below_threshold_4_of_5** — value `2/5 (mean drift 0.628)`
- [PASS] **atraxa_c2_1_latency_gt_0** — value `36407`
- [PASS] **pillar_e_v0_3_curve_check_5_of_5** — value `5/5`
- [FAIL] **pillar_e_v0_4_interaction_within_4_of_5** — value `0/5`
- [PASS] **graduated_playtest_5_of_5** — value `5/5 (5 tier0 predictions)`
- [PASS] **ui_equivalent_build_path_5_of_5** — value `5/5`

## Per-case summary

| Case | iter1 | wall (s) | cost ($) | creativity | novel | semantic | drift | C2.1 (ms) | E v0.3 | E v0.4 ok | GP |
|---|---|---|---|---|---|---|---|---|---|---|---|
| edgar_b3_vampire_tribal | PASS | 118.2 | $0.2950 | 36 | 6 | 2 | 0.471 | 43885 | y | n | y |
| krenko_b4_goblin_combo | PASS | 114.5 | $0.3118 | 37 | 5 | 0 | 0.594 | 40366 | y | n | y |
| atraxa_b2_proliferate | PASS | 109.4 | $0.3565 | 41 | 6 | 3 | 0.882 | 36407 | y | n | y |
| yuriko_b5_ninja_tempo | PASS | 114.9 | $0.2974 | 34 | 4 | 0 | 0.512 | 37868 | y | n | y |
| ur_dragon_b3_dragon_tribal | PASS | 103.5 | $0.2872 | 40 | 7 | 2 | 0.679 | 36515 | y | n | y |

## Iter 6 → iter 7 hand-off

See mega_task_v5_progress_log.md Phase 14 for the full hand-off summary.
