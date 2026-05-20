# MPA Calibration Report — 2026-05-20T01:12:16Z

- Matrix: `calibration_matrix_v1`
- Snapshot: `20260217_190902`
- Games per matchup: `10` (n_game_pairs=5, mirrored)
- Max turns/game: `50`
- Total elapsed: `21.7s`

## Summary

| Matchup | N | Actual WR | Expected | In Band | Drift | Draw Rate | Seat 0/1 wins |
|---|---|---|---|---|---|---|---|
| B5_vs_B2 | 10 | 0.000 | 0.85-0.95 | ✗ | 0.850 | 0.600 | 3/1 |
| B4_vs_B2 | 10 | 0.000 | 0.70-0.85 | ✗ | 0.700 | 0.200 | 4/4 |
| B4_vs_B3 | 10 | 0.100 | 0.55-0.70 | ✗ | 0.450 | 0.500 | 2/3 |
| B3_vs_B2 | 10 | 0.600 | 0.55-0.70 | ✓ | 0.000 | 0.100 | 3/6 |
| B5_vs_B4 | 10 | 0.400 | 0.55-0.70 | ✗ | 0.150 | 0.300 | 5/2 |
| mirror_B3_pillow_fort | 10 | 0.700 | 0.45-0.55 | ✗ | 0.150 | 0.000 | 6/4 |

## Headline aggregates

- **Matchups outside expected band (drift count):** `5` of `6`
- **Overall draw rate (all matchups):** `0.2833` (17 draws / 60 games)
- **Anti-bias mirror check:** seat 0 won `6`, seat 1 won `4` (seat 0 share = 0.600 of decisive games; draws=0)

## Per-matchup details

### B5_vs_B2

- **deck_a:** `B5-cedh-combo` → `edhrec_ezio_auditore_da_firenze_b5_edhrec_ezio_auditore_da_firenze_cedh_rank1_20260518T003102_13`
- **deck_b:** `B2-precon-draconic-domination` → `edhrec_the_ur-dragon_b2_edhrec_precon_draconic_domination_20260517T185258_0`
- **rationale:** Largest power gap in the matrix. If B5 doesn't crush B2, either MPA can't pilot the combo (policy limitation) or B2 precons are stronger than the bracket assumes.
- **expected:** 0.85-0.95 (wide)
- **actual:** deck_a winrate = 0.000 over 10 games
- **in band:** False; drift amount: 0.850
- **draws:** 6 (0.600 draw rate)
- **seat wins:** seat0=3, seat1=1
- **elapsed:** 3.7s

| # | swap | outcome | turns | loss_reason |
|---|---|---|---|---|
| 1 | False | draw | 50 | turn_cap_reached |
| 2 | True | B_wins | 47 | life_to_zero |
| 3 | False | draw | 50 | turn_cap_reached |
| 4 | True | draw | 50 | turn_cap_reached |
| 5 | False | B_wins | 42 | life_to_zero |
| 6 | True | B_wins | 29 | life_to_zero |
| 7 | False | draw | 50 | turn_cap_reached |
| 8 | True | draw | 50 | turn_cap_reached |
| 9 | False | draw | 50 | turn_cap_reached |
| 10 | True | B_wins | 43 | life_to_zero |

### B4_vs_B2

- **deck_a:** `B4-optimized-tribal-goodstuff` → `edhrec_the_ur-dragon_b4_edhrec_urdragon_optimized_rank3_20260517T185258_13`
- **deck_b:** `B2-precon-breed-lethality` → `edhrec_atraxa_praetors'_voice_b2_edhrec_precon_breed_lethality_20260517T192054_0`
- **rationale:** B4 should win decisively but Atraxa-precon is one of the strongest precons (5-color, +1/+1 counters synergy).
- **expected:** 0.70-0.85 (wide)
- **actual:** deck_a winrate = 0.000 over 10 games
- **in band:** False; drift amount: 0.700
- **draws:** 2 (0.200 draw rate)
- **seat wins:** seat0=4, seat1=4
- **elapsed:** 3.5s

| # | swap | outcome | turns | loss_reason |
|---|---|---|---|---|
| 1 | False | B_wins | 36 | life_to_zero |
| 2 | True | B_wins | 41 | life_to_zero |
| 3 | False | draw | 50 | turn_cap_reached |
| 4 | True | B_wins | 27 | life_to_zero |
| 5 | False | B_wins | 50 | life_to_zero |
| 6 | True | B_wins | 27 | life_to_zero |
| 7 | False | B_wins | 44 | life_to_zero |
| 8 | True | B_wins | 33 | life_to_zero |
| 9 | False | B_wins | 32 | life_to_zero |
| 10 | True | draw | 50 | turn_cap_reached |

### B4_vs_B3

- **deck_a:** `B4-optimized-graveyard-combo` → `edhrec_muldrotha_the_gravetide_b4_edhrec_muldrotha_the_gravetide_upgraded_rank1_20260518T000808_9`
- **deck_b:** `B3-upgraded-vampire-tribal` → `edhrec_edgar_markov_b3_edhrec_edgar_upgraded_rank1_20260517T183601_6`
- **rationale:** B4 advantage should be real but small. Muldrotha is heavy on engine-build turns; if MPA can't pilot the graveyard engine the advantage collapses.
- **expected:** 0.55-0.70 (medium)
- **actual:** deck_a winrate = 0.100 over 10 games
- **in band:** False; drift amount: 0.450
- **draws:** 5 (0.500 draw rate)
- **seat wins:** seat0=2, seat1=3
- **elapsed:** 3.6s

| # | swap | outcome | turns | loss_reason |
|---|---|---|---|---|
| 1 | False | draw | 50 | turn_cap_reached |
| 2 | True | B_wins | 49 | life_to_zero |
| 3 | False | B_wins | 40 | life_to_zero |
| 4 | True | draw | 50 | turn_cap_reached |
| 5 | False | draw | 50 | turn_cap_reached |
| 6 | True | B_wins | 25 | life_to_zero |
| 7 | False | B_wins | 48 | life_to_zero |
| 8 | True | A_wins | 38 | life_to_zero |
| 9 | False | draw | 50 | turn_cap_reached |
| 10 | True | draw | 50 | turn_cap_reached |

### B3_vs_B2

- **deck_a:** `B3-upgraded-aristocrats-combo` → `edhrec_the_necrobloom_b3_edhrec_the_necrobloom_upgraded_rank2_20260518T000314_9`
- **deck_b:** `B2-precon-elven-empire` → `edhrec_lathril_blade_of_the_elves_b2_edhrec_precon_elven-empire_20260517T213330_0`
- **rationale:** B3 upgrade-tier should beat B2 precon but not crush. Necrobloom's combo lines may require >25 turns; if so, expect more draws.
- **expected:** 0.55-0.70 (medium)
- **actual:** deck_a winrate = 0.600 over 10 games
- **in band:** True; drift amount: 0.000
- **draws:** 1 (0.100 draw rate)
- **seat wins:** seat0=3, seat1=6
- **elapsed:** 3.6s

| # | swap | outcome | turns | loss_reason |
|---|---|---|---|---|
| 1 | False | B_wins | 24 | life_to_zero |
| 2 | True | A_wins | 36 | life_to_zero |
| 3 | False | A_wins | 23 | life_to_zero |
| 4 | True | A_wins | 30 | life_to_zero |
| 5 | False | A_wins | 31 | life_to_zero |
| 6 | True | A_wins | 18 | life_to_zero |
| 7 | False | B_wins | 18 | life_to_zero |
| 8 | True | B_wins | 35 | life_to_zero |
| 9 | False | draw | 50 | turn_cap_reached |
| 10 | True | A_wins | 18 | life_to_zero |

### B5_vs_B4

- **deck_a:** `B5-cedh-tempo` → `edhrec_yuriko_the_tiger's_shadow_b5_edhrec_yuriko_the_tigers_shadow_cedh_rank1_20260517T232550_12`
- **deck_b:** `B4-optimized-stax` → `edhrec_sauron_the_dark_lord_b4_edhrec_sauron_optimized_rank1_20260517T201526_11`
- **rationale:** Yuriko tempo should outpace Sauron stax, but stax can stall games long enough to favor draws on the MPA's heuristic policy.
- **expected:** 0.55-0.70 (medium)
- **actual:** deck_a winrate = 0.400 over 10 games
- **in band:** False; drift amount: 0.150
- **draws:** 3 (0.300 draw rate)
- **seat wins:** seat0=5, seat1=2
- **elapsed:** 3.7s

| # | swap | outcome | turns | loss_reason |
|---|---|---|---|---|
| 1 | False | A_wins | 23 | life_to_zero |
| 2 | True | draw | 50 | turn_cap_reached |
| 3 | False | A_wins | 45 | life_to_zero |
| 4 | True | A_wins | 38 | life_to_zero |
| 5 | False | draw | 50 | turn_cap_reached |
| 6 | True | B_wins | 49 | life_to_zero |
| 7 | False | B_wins | 30 | life_to_zero |
| 8 | True | B_wins | 49 | life_to_zero |
| 9 | False | A_wins | 45 | life_to_zero |
| 10 | True | draw | 50 | turn_cap_reached |

### mirror_B3_pillow_fort

- **deck_a:** `B3-upgraded-pillow-fort` → `edhrec_heliod_the_radiant_dawn_b3_edhrec_heliod_the_radiant_dawn_upgraded_rank1_20260518T033807_8`
- **deck_b:** `B3-upgraded-pillow-fort` → `edhrec_heliod_the_radiant_dawn_b3_edhrec_heliod_the_radiant_dawn_upgraded_rank1_20260518T033807_8`
- **rationale:** Pillow-fort variant chosen because it's defensive and unlikely to race; mirror match isolates seat bias from deck asymmetry. Tight band — anti-bias rule 1.4 violation if outside.
- **expected:** 0.45-0.55 (tight)
- **actual:** deck_a winrate = 0.700 over 10 games
- **in band:** False; drift amount: 0.150
- **draws:** 0 (0.000 draw rate)
- **seat wins:** seat0=6, seat1=4
- **elapsed:** 3.5s

| # | swap | outcome | turns | loss_reason |
|---|---|---|---|---|
| 1 | False | A_wins | 25 | life_to_zero |
| 2 | True | B_wins | 49 | life_to_zero |
| 3 | False | A_wins | 31 | life_to_zero |
| 4 | True | A_wins | 48 | life_to_zero |
| 5 | False | A_wins | 35 | life_to_zero |
| 6 | True | B_wins | 19 | life_to_zero |
| 7 | False | B_wins | 44 | life_to_zero |
| 8 | True | A_wins | 18 | life_to_zero |
| 9 | False | A_wins | 21 | life_to_zero |
| 10 | True | A_wins | 32 | life_to_zero |

## Recommendations

- 5 matchup(s) outside expected bands. Inspect per-matchup details — drift may indicate policy gaps or expected-band miscalibration.
