# MPA Calibration Report — 2026-05-20T02:05:49Z

- Matrix: `calibration_matrix_v1`
- Snapshot: `20260217_190902`
- Games per matchup: `20` (n_game_pairs=10, mirrored)
- Max turns/game: `50`
- Total elapsed: `26.6s`

## Summary

| Matchup | N | Actual WR | Expected | In Band | Drift | Draw Rate | Seat 0/1 wins |
|---|---|---|---|---|---|---|---|
| B5_vs_B2 | 20 | 0.900 | 0.85-0.95 | ✓ | 0.000 | 0.100 | 10/8 |
| B4_vs_B2 | 20 | 0.000 | 0.70-0.85 | ✗ | 0.700 | 0.250 | 8/7 |
| B4_vs_B3 | 20 | 0.050 | 0.55-0.70 | ✗ | 0.500 | 0.550 | 2/7 |
| B3_vs_B2 | 20 | 0.500 | 0.55-0.70 | ✗ | 0.050 | 0.150 | 7/10 |
| B5_vs_B4 | 20 | 0.800 | 0.55-0.70 | ✗ | 0.100 | 0.100 | 10/8 |
| mirror_B3_pillow_fort | 20 | 0.550 | 0.45-0.55 | ✓ | 0.000 | 0.050 | 9/10 |

## Headline aggregates

- **Matchups outside expected band (drift count):** `4` of `6`
- **Overall draw rate (all matchups):** `0.2` (24 draws / 120 games)
- **Anti-bias mirror check:** seat 0 won `9`, seat 1 won `10` (seat 0 share = 0.474 of decisive games; draws=1)

## Per-matchup details

### B5_vs_B2

- **deck_a:** `B5-cedh-combo` → `edhrec_ezio_auditore_da_firenze_b5_edhrec_ezio_auditore_da_firenze_cedh_rank1_20260518T003102_13`
- **deck_b:** `B2-precon-draconic-domination` → `edhrec_the_ur-dragon_b2_edhrec_precon_draconic_domination_20260517T185258_0`
- **rationale:** Largest power gap in the matrix. If B5 doesn't crush B2, either MPA can't pilot the combo (policy limitation) or B2 precons are stronger than the bracket assumes.
- **expected:** 0.85-0.95 (wide)
- **actual:** deck_a winrate = 0.900 over 20 games
- **in band:** True; drift amount: 0.000
- **draws:** 2 (0.100 draw rate)
- **seat wins:** seat0=10, seat1=8
- **elapsed:** 4.5s

| # | swap | outcome | turns | loss_reason |
|---|---|---|---|---|
| 1 | False | A_wins | 47 | thoracle_consult_combo |
| 2 | True | A_wins | 28 | thoracle_consult_combo |
| 3 | False | A_wins | 33 | thoracle_consult_combo |
| 4 | True | draw | 50 | turn_cap_reached |
| 5 | False | A_wins | 13 | thoracle_consult_combo |
| 6 | True | A_wins | 14 | thoracle_consult_combo |
| 7 | False | A_wins | 15 | thoracle_consult_combo |
| 8 | True | draw | 50 | turn_cap_reached |
| 9 | False | A_wins | 47 | thoracle_consult_combo |
| 10 | True | A_wins | 14 | thoracle_consult_combo |
| 11 | False | A_wins | 13 | thoracle_consult_combo |
| 12 | True | A_wins | 18 | thoracle_consult_combo |
| 13 | False | A_wins | 5 | thoracle_consult_combo |
| 14 | True | A_wins | 18 | thoracle_consult_combo |
| 15 | False | A_wins | 7 | thoracle_consult_combo |
| 16 | True | A_wins | 32 | thoracle_consult_combo |
| 17 | False | A_wins | 29 | thoracle_consult_combo |
| 18 | True | A_wins | 34 | thoracle_consult_combo |
| 19 | False | A_wins | 17 | thoracle_consult_combo |
| 20 | True | A_wins | 12 | thoracle_consult_combo |

### B4_vs_B2

- **deck_a:** `B4-optimized-tribal-goodstuff` → `edhrec_the_ur-dragon_b4_edhrec_urdragon_optimized_rank3_20260517T185258_13`
- **deck_b:** `B2-precon-breed-lethality` → `edhrec_atraxa_praetors'_voice_b2_edhrec_precon_breed_lethality_20260517T192054_0`
- **rationale:** B4 should win decisively but Atraxa-precon is one of the strongest precons (5-color, +1/+1 counters synergy).
- **expected:** 0.70-0.85 (wide)
- **actual:** deck_a winrate = 0.000 over 20 games
- **in band:** False; drift amount: 0.700
- **draws:** 5 (0.250 draw rate)
- **seat wins:** seat0=8, seat1=7
- **elapsed:** 4.6s

| # | swap | outcome | turns | loss_reason |
|---|---|---|---|---|
| 1 | False | B_wins | 32 | life_to_zero |
| 2 | True | B_wins | 41 | life_to_zero |
| 3 | False | B_wins | 48 | life_to_zero |
| 4 | True | B_wins | 29 | life_to_zero |
| 5 | False | B_wins | 50 | life_to_zero |
| 6 | True | B_wins | 27 | life_to_zero |
| 7 | False | B_wins | 44 | life_to_zero |
| 8 | True | B_wins | 33 | life_to_zero |
| 9 | False | B_wins | 32 | life_to_zero |
| 10 | True | draw | 50 | turn_cap_reached |
| 11 | False | B_wins | 42 | life_to_zero |
| 12 | True | B_wins | 35 | life_to_zero |
| 13 | False | draw | 50 | turn_cap_reached |
| 14 | True | draw | 50 | turn_cap_reached |
| 15 | False | draw | 50 | turn_cap_reached |
| 16 | True | B_wins | 29 | life_to_zero |
| 17 | False | B_wins | 28 | life_to_zero |
| 18 | True | B_wins | 29 | life_to_zero |
| 19 | False | draw | 50 | turn_cap_reached |
| 20 | True | B_wins | 27 | life_to_zero |

### B4_vs_B3

- **deck_a:** `B4-optimized-graveyard-combo` → `edhrec_muldrotha_the_gravetide_b4_edhrec_muldrotha_the_gravetide_upgraded_rank1_20260518T000808_9`
- **deck_b:** `B3-upgraded-vampire-tribal` → `edhrec_edgar_markov_b3_edhrec_edgar_upgraded_rank1_20260517T183601_6`
- **rationale:** B4 advantage should be real but small. Muldrotha is heavy on engine-build turns; if MPA can't pilot the graveyard engine the advantage collapses.
- **expected:** 0.55-0.70 (medium)
- **actual:** deck_a winrate = 0.050 over 20 games
- **in band:** False; drift amount: 0.500
- **draws:** 11 (0.550 draw rate)
- **seat wins:** seat0=2, seat1=7
- **elapsed:** 4.4s

| # | swap | outcome | turns | loss_reason |
|---|---|---|---|---|
| 1 | False | draw | 50 | turn_cap_reached |
| 2 | True | B_wins | 49 | life_to_zero |
| 3 | False | B_wins | 40 | life_to_zero |
| 4 | True | draw | 50 | turn_cap_reached |
| 5 | False | draw | 50 | turn_cap_reached |
| 6 | True | B_wins | 25 | life_to_zero |
| 7 | False | B_wins | 50 | life_to_zero |
| 8 | True | A_wins | 36 | life_to_zero |
| 9 | False | draw | 50 | turn_cap_reached |
| 10 | True | draw | 50 | turn_cap_reached |
| 11 | False | B_wins | 34 | life_to_zero |
| 12 | True | draw | 50 | turn_cap_reached |
| 13 | False | B_wins | 30 | life_to_zero |
| 14 | True | draw | 50 | turn_cap_reached |
| 15 | False | B_wins | 48 | life_to_zero |
| 16 | True | draw | 50 | turn_cap_reached |
| 17 | False | B_wins | 24 | life_to_zero |
| 18 | True | draw | 50 | turn_cap_reached |
| 19 | False | draw | 50 | turn_cap_reached |
| 20 | True | draw | 50 | turn_cap_reached |

### B3_vs_B2

- **deck_a:** `B3-upgraded-aristocrats-combo` → `edhrec_the_necrobloom_b3_edhrec_the_necrobloom_upgraded_rank2_20260518T000314_9`
- **deck_b:** `B2-precon-elven-empire` → `edhrec_lathril_blade_of_the_elves_b2_edhrec_precon_elven-empire_20260517T213330_0`
- **rationale:** B3 upgrade-tier should beat B2 precon but not crush. Necrobloom's combo lines may require >25 turns; if so, expect more draws.
- **expected:** 0.55-0.70 (medium)
- **actual:** deck_a winrate = 0.500 over 20 games
- **in band:** False; drift amount: 0.050
- **draws:** 3 (0.150 draw rate)
- **seat wins:** seat0=7, seat1=10
- **elapsed:** 4.5s

| # | swap | outcome | turns | loss_reason |
|---|---|---|---|---|
| 1 | False | B_wins | 24 | life_to_zero |
| 2 | True | A_wins | 36 | life_to_zero |
| 3 | False | A_wins | 23 | life_to_zero |
| 4 | True | A_wins | 30 | life_to_zero |
| 5 | False | A_wins | 31 | life_to_zero |
| 6 | True | A_wins | 18 | life_to_zero |
| 7 | False | B_wins | 16 | life_to_zero |
| 8 | True | B_wins | 35 | life_to_zero |
| 9 | False | B_wins | 38 | life_to_zero |
| 10 | True | A_wins | 18 | life_to_zero |
| 11 | False | draw | 50 | turn_cap_reached |
| 12 | True | A_wins | 44 | life_to_zero |
| 13 | False | A_wins | 25 | life_to_zero |
| 14 | True | A_wins | 48 | life_to_zero |
| 15 | False | B_wins | 48 | life_to_zero |
| 16 | True | B_wins | 45 | life_to_zero |
| 17 | False | A_wins | 31 | life_to_zero |
| 18 | True | draw | 50 | turn_cap_reached |
| 19 | False | draw | 50 | turn_cap_reached |
| 20 | True | B_wins | 23 | life_to_zero |

### B5_vs_B4

- **deck_a:** `B5-cedh-tempo` → `edhrec_yuriko_the_tiger's_shadow_b5_edhrec_yuriko_the_tigers_shadow_cedh_rank1_20260517T232550_12`
- **deck_b:** `B4-optimized-stax` → `edhrec_sauron_the_dark_lord_b4_edhrec_sauron_optimized_rank1_20260517T201526_11`
- **rationale:** Yuriko tempo should outpace Sauron stax, but stax can stall games long enough to favor draws on the MPA's heuristic policy.
- **expected:** 0.55-0.70 (medium)
- **actual:** deck_a winrate = 0.800 over 20 games
- **in band:** False; drift amount: 0.100
- **draws:** 2 (0.100 draw rate)
- **seat wins:** seat0=10, seat1=8
- **elapsed:** 4.5s

| # | swap | outcome | turns | loss_reason |
|---|---|---|---|---|
| 1 | False | A_wins | 23 | life_to_zero |
| 2 | True | A_wins | 48 | thoracle_consult_combo |
| 3 | False | A_wins | 15 | thoracle_consult_combo |
| 4 | True | A_wins | 22 | thoracle_consult_combo |
| 5 | False | A_wins | 27 | life_to_zero |
| 6 | True | B_wins | 49 | life_to_zero |
| 7 | False | A_wins | 15 | thoracle_consult_combo |
| 8 | True | A_wins | 36 | life_to_zero |
| 9 | False | A_wins | 45 | life_to_zero |
| 10 | True | A_wins | 32 | thoracle_consult_combo |
| 11 | False | B_wins | 24 | life_to_zero |
| 12 | True | A_wins | 8 | thoracle_consult_combo |
| 13 | False | A_wins | 39 | thoracle_consult_combo |
| 14 | True | draw | 50 | turn_cap_reached |
| 15 | False | A_wins | 17 | thoracle_consult_combo |
| 16 | True | draw | 50 | turn_cap_reached |
| 17 | False | A_wins | 37 | life_to_zero |
| 18 | True | A_wins | 16 | thoracle_consult_combo |
| 19 | False | A_wins | 29 | thoracle_consult_combo |
| 20 | True | A_wins | 26 | thoracle_consult_combo |

### mirror_B3_pillow_fort

- **deck_a:** `B3-upgraded-pillow-fort` → `edhrec_heliod_the_radiant_dawn_b3_edhrec_heliod_the_radiant_dawn_upgraded_rank1_20260518T033807_8`
- **deck_b:** `B3-upgraded-pillow-fort` → `edhrec_heliod_the_radiant_dawn_b3_edhrec_heliod_the_radiant_dawn_upgraded_rank1_20260518T033807_8`
- **rationale:** Pillow-fort variant chosen because it's defensive and unlikely to race; mirror match isolates seat bias from deck asymmetry. Tight band — anti-bias rule 1.4 violation if outside.
- **expected:** 0.45-0.55 (tight)
- **actual:** deck_a winrate = 0.550 over 20 games
- **in band:** True; drift amount: 0.000
- **draws:** 1 (0.050 draw rate)
- **seat wins:** seat0=9, seat1=10
- **elapsed:** 4.0s

| # | swap | outcome | turns | loss_reason |
|---|---|---|---|---|
| 1 | False | A_wins | 19 | life_to_zero |
| 2 | True | B_wins | 49 | life_to_zero |
| 3 | False | A_wins | 29 | life_to_zero |
| 4 | True | A_wins | 48 | life_to_zero |
| 5 | False | A_wins | 41 | life_to_zero |
| 6 | True | B_wins | 31 | life_to_zero |
| 7 | False | B_wins | 40 | life_to_zero |
| 8 | True | A_wins | 18 | life_to_zero |
| 9 | False | A_wins | 21 | life_to_zero |
| 10 | True | A_wins | 32 | life_to_zero |
| 11 | False | draw | 50 | turn_cap_reached |
| 12 | True | A_wins | 22 | life_to_zero |
| 13 | False | B_wins | 48 | life_to_zero |
| 14 | True | B_wins | 25 | life_to_zero |
| 15 | False | B_wins | 30 | life_to_zero |
| 16 | True | A_wins | 46 | life_to_zero |
| 17 | False | A_wins | 33 | life_to_zero |
| 18 | True | A_wins | 36 | life_to_zero |
| 19 | False | B_wins | 32 | life_to_zero |
| 20 | True | B_wins | 33 | life_to_zero |

## Recommendations

- 4 matchup(s) outside expected bands. Inspect per-matchup details — drift may indicate policy gaps or expected-band miscalibration.
