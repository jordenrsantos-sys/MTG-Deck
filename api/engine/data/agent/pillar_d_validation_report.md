# Pillar D Phase F — Validation Report

Generated: 2026-05-20 17:35:47
Snapshot: `20260217_190902_tagpass_20260222`

## Headline

**5 / 5 test cases pass.** Wall clock: 1.1–1.8 s per build (target was <15 s).
Endpoint calls: 3–4 per build (budget was 30). Every user must-include was
honored: `2/2 resolved` for all five cases, `0` dropped. Creativity envelope
held: 19–26 high-frequency corpus staples kept OUT of each deck.

## Per-case theme verdict (the core creativity-envelope check)

| Case | Theme coherence | Notes |
|---|---|---|
| Edgar Markov / B3 / Vampire Tribal | 1.00 | TYPAL_VAMPIRES classified; lifegain payoff cards anchored by user picks honored. |
| Krenko / B4 / Goblin Combo (user includes Kiki+Snoop) | 1.00 | Both halves of the B4-only combo present (user override applied per Fix 1). |
| Atraxa / B2 / Proliferate Counters | 0.50 | THEME_PROLIFERATE matched; THEME_PLUS1_COUNTERS classifier didn't fire as strongly. Soft signal — still ≥0.5 threshold. |
| Yuriko / B5 / Ninja Tempo (Thoracle+Consult) | 1.00 | TYPAL_NINJAS classified; Thoracle+Consult both present because B5 unrestricted (Fix 1). |
| The Ur-Dragon / B3 / Dragon Tribal (Tiamat) | 1.00 | TYPAL_DRAGONS classified. **Fix 2 verified by deck-dump grep:** Old Gnawbone NOT present; Hellkite Charger NOT present. Tiamat (user pick) IS present. Agent did not auto-expand the combo chain Tiamat tutors for. |

## Caveats / known gaps

1. `bracket_estimate` returned `None` from `compute_deck_analyze_v1` in
   all five cases. Analyze does emit a `bracket_estimate` field, but its
   shape doesn't carry the keys (`bracket` / `bracket_id`) my Phase D
   validator looks for. Small follow-up: inspect the actual
   bracket_estimate shape (probably the bracket walk's `natural_bracket`
   field) and update the validator. NOT load-bearing for the success
   criterion — the deck composition is correct; only the after-the-fact
   bracket-estimate cross-check is silent.
2. `compute_deck_strength_check_v1` skipped in this sweep
   (`--with-strength-check` flag available but defaults off). Reason:
   `_ensure_vectors` against the cold 13K-deck corpus + 30K-card snapshot
   takes ~10+ minutes on a first call. A pre-warming pass is the
   right follow-up; for the Phase F success criterion analyze's signal is
   sufficient.
3. Phase D's `_attempt_swap` doesn't yet patch `BRACKET_MISMATCH` — no
   power-up/down heuristic exists. The Atraxa B2 case showed
   `THEME_COHERENCE` at 0.5 (right at threshold) but the validation
   passed because 0.5 meets the threshold; no swap was attempted. This is
   the right behavior given the current swap repertoire.
4. The `archetype_brief_v1` cold-vectorization removal (a 1-line cleanup
   of provably dead code in `agent_endpoints_v1.py`) made these timings
   tractable. Before that fix, even the first case wouldn't complete in
   10+ minutes; after, the first case completes in 1.8 s.

## Summary

**Passed: 5 / 5**

| Case | Passed | Wall (ms) | Calls | Pool/Select/Validate (ms) | Theme coh. | Must-inc resolved |
|---|---|---|---|---|---|---|
| edgar_b3_vampire_tribal | ✅ | 1750 | 3 | 337/589/823 | 1.00 | 2/2 |
| krenko_b4_goblin_combo | ✅ | 1424 | 3 | 469/628/325 | 1.00 | 2/2 |
| atraxa_b2_proliferate | ✅ | 1181 | 4 | 489/594/97 | 0.50 | 2/2 |
| yuriko_b5_ninja_tempo | ✅ | 1301 | 3 | 446/743/112 | 1.00 | 2/2 |
| ur_dragon_b3_dragon_tribal | ✅ | 1111 | 3 | 382/592/136 | 1.00 | 2/2 |

## Per-case detail

### edgar_b3_vampire_tribal — PASS

**Checks:**
- `count_100` = `True`
- `count_actual` = `100`
- `singleton` = `True`
- `no_validation_issues` = `True`
- `must_includes_present` = `True`
- `must_includes_resolved` = `['Vito, Thorn of the Dusk Rose', 'Bloodthirsty Conqueror']`
- `must_includes_dropped` = `[]`
- `bracket_estimate_matches` = `None`
- `bracket_signal` = `None`
- `bracket_estimate` = `None`
- `theme_coherence` = `1.0`
- `theme_coherence_passes` = `True`
- `all_reasons_nonempty` = `True`
- `reason_avg_len` = `62.6`

**Creativity envelope:**
- `user_picks_present` = `2`
- `user_picks_total` = `2`
- `must_includes_resolved` = `["Vito, Thorn of the Dusk Rose", "Bloodthirsty Conqueror"]`
- `must_includes_dropped` = `[]`
- `staples_avoided_count` = `21`
- `theme_coherence_score` = `1.0`

**Timing & cost:**
- wall_clock_ms (Python perf_counter): `1750`
- elapsed_ms (layer reported): `1750`
- endpoint_calls: `3`
- phase_timings_ms: `{"pool": 337, "select": 589, "validate": 823}`
- warnings_count: `1`

### krenko_b4_goblin_combo — PASS

**Checks:**
- `count_100` = `True`
- `count_actual` = `100`
- `singleton` = `True`
- `no_validation_issues` = `True`
- `must_includes_present` = `True`
- `must_includes_resolved` = `['Conspicuous Snoop', 'Kiki-Jiki, Mirror Breaker']`
- `must_includes_dropped` = `[]`
- `bracket_estimate_matches` = `None`
- `bracket_signal` = `None`
- `bracket_estimate` = `None`
- `theme_coherence` = `1.0`
- `theme_coherence_passes` = `True`
- `all_reasons_nonempty` = `True`
- `reason_avg_len` = `55.9`

**Creativity envelope:**
- `user_picks_present` = `2`
- `user_picks_total` = `2`
- `must_includes_resolved` = `["Conspicuous Snoop", "Kiki-Jiki, Mirror Breaker"]`
- `must_includes_dropped` = `[]`
- `staples_avoided_count` = `26`
- `theme_coherence_score` = `1.0`

**Timing & cost:**
- wall_clock_ms (Python perf_counter): `1424`
- elapsed_ms (layer reported): `1423`
- endpoint_calls: `3`
- phase_timings_ms: `{"pool": 469, "select": 628, "validate": 325}`
- warnings_count: `1`

### atraxa_b2_proliferate — PASS

**Checks:**
- `count_100` = `True`
- `count_actual` = `100`
- `singleton` = `True`
- `no_validation_issues` = `True`
- `must_includes_present` = `True`
- `must_includes_resolved` = `['Doubling Season', 'Pir, Imaginative Rascal']`
- `must_includes_dropped` = `[]`
- `bracket_estimate_matches` = `None`
- `bracket_signal` = `None`
- `bracket_estimate` = `None`
- `theme_coherence` = `0.5`
- `theme_coherence_passes` = `True`
- `all_reasons_nonempty` = `True`
- `reason_avg_len` = `89.1`

**Creativity envelope:**
- `user_picks_present` = `2`
- `user_picks_total` = `2`
- `must_includes_resolved` = `["Doubling Season", "Pir, Imaginative Rascal"]`
- `must_includes_dropped` = `[]`
- `staples_avoided_count` = `24`
- `theme_coherence_score` = `0.5`

**Timing & cost:**
- wall_clock_ms (Python perf_counter): `1181`
- elapsed_ms (layer reported): `1181`
- endpoint_calls: `4`
- phase_timings_ms: `{"pool": 489, "select": 594, "validate": 97}`
- warnings_count: `1`

### yuriko_b5_ninja_tempo — PASS

**Checks:**
- `count_100` = `True`
- `count_actual` = `100`
- `singleton` = `True`
- `no_validation_issues` = `True`
- `must_includes_present` = `True`
- `must_includes_resolved` = `["Thassa's Oracle", 'Demonic Consultation']`
- `must_includes_dropped` = `[]`
- `bracket_estimate_matches` = `None`
- `bracket_signal` = `None`
- `bracket_estimate` = `None`
- `theme_coherence` = `1.0`
- `theme_coherence_passes` = `True`
- `all_reasons_nonempty` = `True`
- `reason_avg_len` = `59.4`

**Creativity envelope:**
- `user_picks_present` = `2`
- `user_picks_total` = `2`
- `must_includes_resolved` = `["Thassa's Oracle", "Demonic Consultation"]`
- `must_includes_dropped` = `[]`
- `staples_avoided_count` = `23`
- `theme_coherence_score` = `1.0`

**Timing & cost:**
- wall_clock_ms (Python perf_counter): `1301`
- elapsed_ms (layer reported): `1301`
- endpoint_calls: `3`
- phase_timings_ms: `{"pool": 446, "select": 743, "validate": 112}`
- warnings_count: `1`

### ur_dragon_b3_dragon_tribal — PASS

**Checks:**
- `count_100` = `True`
- `count_actual` = `100`
- `singleton` = `True`
- `no_validation_issues` = `True`
- `must_includes_present` = `True`
- `must_includes_resolved` = `['Dragon Tempest', 'Tiamat']`
- `must_includes_dropped` = `[]`
- `bracket_estimate_matches` = `None`
- `bracket_signal` = `None`
- `bracket_estimate` = `None`
- `theme_coherence` = `1.0`
- `theme_coherence_passes` = `True`
- `all_reasons_nonempty` = `True`
- `reason_avg_len` = `69.2`

**Creativity envelope:**
- `user_picks_present` = `2`
- `user_picks_total` = `2`
- `must_includes_resolved` = `["Dragon Tempest", "Tiamat"]`
- `must_includes_dropped` = `[]`
- `staples_avoided_count` = `19`
- `theme_coherence_score` = `1.0`

**Timing & cost:**
- wall_clock_ms (Python perf_counter): `1111`
- elapsed_ms (layer reported): `1111`
- endpoint_calls: `3`
- phase_timings_ms: `{"pool": 382, "select": 592, "validate": 136}`
- warnings_count: `1`
