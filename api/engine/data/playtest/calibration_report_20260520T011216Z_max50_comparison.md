# MPA Calibration — max_turns=25 vs max_turns=50 Comparison

- 25-turn run: `calibration_report_20260520T010446Z.md` (snapshot 20260217_190902, N=10/matchup)
- 50-turn run: `calibration_report_20260520T011216Z.md` (snapshot 20260217_190902, N=10/matchup)
- Same matrix, same opposition decks, only `max_turns_per_game` changed.

## Side-by-side summary

| Matchup | WR 25T | WR 50T | Δ WR | Draw rate 25T | Draw rate 50T | Δ draws | Expected | In band 50T |
|---|---|---|---|---|---|---|---|---|
| B5_vs_B2 | 0.000 | 0.000 | +0.000 | 1.000 | 0.600 | −0.400 | 0.85-0.95 | ✗ |
| B4_vs_B2 | 0.000 | 0.000 | +0.000 | 1.000 | 0.200 | −0.800 | 0.70-0.85 | ✗ |
| B4_vs_B3 | 0.000 | 0.100 | +0.100 | 0.900 | 0.500 | −0.400 | 0.55-0.70 | ✗ |
| B3_vs_B2 | 0.300 | 0.600 | +0.300 | 0.500 | 0.100 | −0.400 | 0.55-0.70 | ✓ |
| B5_vs_B4 | 0.100 | 0.400 | +0.300 | 0.900 | 0.300 | −0.600 | 0.55-0.70 | ✗ |
| mirror_B3_pillow_fort | 0.300 | 0.700 | +0.400 | 0.600 | 0.000 | −0.600 | 0.45-0.55 | ✗ |

## Headline aggregates compared

|  | 25T run | 50T run |
|---|---|---|
| Overall draw rate | **0.817** (49/60) | **0.283** (17/60) |
| Matchups in band | 0 / 6 | **1 / 6** (B3_vs_B2) |
| Matchups with 0% winrate | 5 / 6 | 2 / 6 |
| Mirror seat-0 share of decisive games | 0.750 (3/4) | **0.600 (6/10)** |
| Decisive games in mirror | 4 | **10** |

## What changed dramatically

- **Draw rate plummeted 81.7% → 28.3%** — below the user's 50% blocker threshold.
- **B4_vs_B2 went from 10/10 turn-cap draws → 8/10 decisive games** (largest single improvement). At 25 turns nobody could finish; at 50 turns games resolve.
- **Mirror match now has 0 draws** (was 6). Anti-bias check has usable signal: seat 0 = 60% of decisive games, seat 1 = 40%. Sample of 10 is still small but the directional read is clearer.
- **B3_vs_B2 entered its expected band** (0.600, expected 0.55-0.70). One matchup now correctly calibrated.

## What did NOT improve

- **B5_vs_B2 and B4_vs_B2 still 0% deck_a winrate**, but for a different reason now. At 25 turns: all draws. At 50 turns: deck_b wins decisively (B5_vs_B2 = 4 B-wins / 0 A-wins / 6 draws; B4_vs_B2 = 8 B-wins / 0 A-wins / 2 draws). **The higher-bracket deck is losing the games it does finish.**
- **B4_vs_B3 went from 0 wins → 1 win out of 10** — Muldrotha B4 wins 1, Edgar Markov B3 wins 4, 5 draws. Drift narrowed (0.550 → 0.450) but still outside band.
- **B5_vs_B4 = 4 A-wins / 3 B-wins / 3 draws** — closer to its 0.55-0.70 band but still below. Yuriko tempo doesn't pull away from Sauron stax the way the bracket gap predicts.

## Mirror match anti-bias check (now with usable N)

Seat 0 won 6/10, seat 1 won 4/10. **Seat 0 share = 60%** of decisive games. Still inside ±10pp of fair (50%), but at N=10 the binomial 95% CI is roughly ±30pp — sample is too small to confirm whether this is real first-player bias or noise. Re-run at N=30+ would tighten it; current data is suggestive of mild seat-0 advantage but not conclusive.

## Interpretation: which of the three scenarios does the data support?

**Scenario C: Mixed signal — leaning toward "policy plays simple decks better than complex ones."**

The data partially supports each interpretation but lands strongest on **a sharper variant of "mixed signal"** than the kickoff anticipated:

- **Not "policy is slow but correct" (A)** — if it were, raising max_turns would have pulled winrates toward expected bands monotonically. Instead, the higher-bracket decks systematically lose at higher decisiveness. B5_vs_B2 at 25 turns was 0% winrate from draws; at 50 turns it's 0% winrate from *losses*. More games finishing made the inversion visible.

- **Not "policy is genuinely stuck" (B)** — finish-rate is now manageable (28.3% draws, B3_vs_B2 produced 9 decisive games out of 10, B4_vs_B2 produced 8). The policy *can* close games.

- **"Mixed signal" (C), specialized**: The policy systematically favors deck shapes its heuristic understands — simple aggressive creatures + commander + ramp. Higher-bracket decks (Ezio combo, Muldrotha graveyard engine, Ur-Dragon B4 goodstuff, Yuriko tempo, Sauron stax) all rely on win conditions the policy can't recognize. Lower-bracket precons that just play creatures and attack win on default heuristic alone. This isn't generic "needs more turns" — it's a structural inversion: the bracket gap is inverted in the policy's evaluation because the policy plays the wrong cards correctly.

Specific evidence of the inversion:
- B4_vs_B2: Atraxa precon (B2) wins 8-0-2 against Ur-Dragon B4 goodstuff. Atraxa just plays creatures + counters; Ur-Dragon B4 wants to assemble extra-combat-step chains the policy doesn't reach.
- B5_vs_B2: Ezio Auditore (B5 combo) wins 0-4-6 vs Ur-Dragon B2 precon. Same pattern — combo deck loses to creature-attack deck because policy can't fire the combo.
- B3_vs_B2 *did* land in band — Necrobloom is aristocrats with creature deaths, the policy's wheelhouse. **The one matchup that worked is the one matchup the heuristic understands.**

## Recommended next direction

Don't pursue Phase 5b.4 (scenarios) as originally planned — scripted scenarios on this policy will mostly assert "policy doesn't fire combos / doesn't recognize value engines," and we already know that from this run. The valuable scenarios become writeable *after* the policy gains the capability the scenarios are testing.

Likely highest-leverage policy work, in rough order:
1. **Win-condition recognition** — combos (e.g. Ezio's "exile from graveyard" lines, Muldrotha's recursion engine, Necrobloom's land-sac loops). Without this, B5/B4 combo decks default to "attack with creatures" which they're bad at.
2. **Cost-aware spell priority** — current policy casts highest-CMC spell. For tempo decks (Yuriko) the right play is often cheap tempo + commander damage, not biggest body.
3. **Removal / interaction** — currently zero. Policy ignores instants entirely. Any deck whose plan relies on interaction (Sauron stax, Yuriko bounce, Muldrotha graveyard hate) plays without its toolbox.

A focused policy upgrade pass on (1) — combo recognition — would likely move B5_vs_B2 toward 80%+ at the next calibration run. That's the single intervention with the biggest expected bracket-spread impact.

**Phase C (scripted scenarios) becomes valuable as the *acceptance suite* for the policy upgrade**, not as the next ship. Scenarios like "Thoracle + Consultation in hand → fire Consultation" can't pass on today's policy because today's policy doesn't even consider Consultation as castable in the "what should I cast" branch. Once combo recognition lands, those scenarios become the regression suite.

Awaiting decision on direction: (i) policy upgrade pass focused on combo recognition, (ii) Phase C scenarios anyway (with the understanding most will fail and serve as a "what's missing" catalog), or (iii) something else entirely.
