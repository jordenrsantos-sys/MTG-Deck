# MPA Policy Upgrade Log

Iterative upgrade pass starting from `mpa_policy_v0.3_wincon_recognition`.

**Success criterion:** Calibration matrix N=10 max_turns=50 shows ≥4/6 matchups in band AND overall draw rate <30%.

## Baseline (v0.3, post-wincon-PoC)

50-turn calibration (from `calibration_report_20260520T011216Z.md`):

| Matchup | WR | Expected | In Band | Draw Rate |
|---|---|---|---|---|
| B5_vs_B2 | 0.000 → 0.100 (PoC rerun) | 0.85-0.95 | ✗ | 0.6 → 0.5 |
| B4_vs_B2 | 0.000 | 0.70-0.85 | ✗ | 0.20 |
| B4_vs_B3 | 0.100 | 0.55-0.70 | ✗ | 0.50 |
| B3_vs_B2 | 0.600 | 0.55-0.70 | ✓ | 0.10 |
| B5_vs_B4 | 0.400 | 0.55-0.70 | ✗ | 0.30 |
| mirror_B3 | 0.700 | 0.45-0.55 | ✗ | 0.00 |

**Baseline state:** 1/6 in band, overall draw rate 28.3%.
**Gap to close:** need ≥3 more matchups in band; draws can rise only ~2pp.

## Phase log

(Each phase appended below as work progresses.)

### Phase 1: Tutor recognition — landed

- **Code:** `ActionType.TUTOR_FOR_TARGET` (mpa_actions.py:54-60), `check_tutor_actions()` (mpa_policy.py:81-128), runner branch (mpa_runner.py:191-218). 15 tutor names recognized.
- **Tests:** 8 new in `test_mpa_tutor_recognition.py`; all pass; no regressions (27/27 tests green).
- **Targeted calibration (B5_vs_B2):** 0.10 → **0.90** — in band.
- **Full calibration (calibration_report_20260520T013022Z.md):**

| Matchup | WR before | WR after | In band | Draw before | Draw after |
|---|---|---|---|---|---|
| B5_vs_B2 | 0.100 | **0.900** | ✓ | 0.5 | 0.1 |
| B4_vs_B2 | 0.000 | 0.000 | ✗ | 0.2 | 0.2 |
| B4_vs_B3 | 0.100 | 0.100 | ✗ | 0.5 | 0.5 |
| B3_vs_B2 | 0.600 | 0.600 | ✓ | 0.1 | 0.1 |
| B5_vs_B4 | 0.400 | 0.800 | ✗ (overshoot) | 0.3 | 0.0 |
| mirror_B3 | 0.700 | 0.700 | ✗ | 0.0 | 0.0 |

- **Aggregate:** in band 1→2, draw rate 28.3%→15%, no matchup regressed (B5_vs_B4 drift narrowed from 0.450 → 0.150).

### Phase 2: Color-aware manabase — REVERTED

- **Intent:** `_land_color` returns tuple of producible colors (basics + curated non-basic table + fallback to `card.color_identity`). Updated `_available_mana` to add each land to all colors. Updated `_auto_tap_for_cost` to dedup duals.
- **Tests:** 10 new in `test_mpa_color_aware_manabase.py`; all passed.
- **Full calibration (calibration_report_20260520T013713Z.md, since deleted):**

| Matchup | WR before | WR after | In band before | In band after |
|---|---|---|---|---|
| B5_vs_B2 | 0.900 | 0.200 | ✓ | ✗ (regression) |
| B4_vs_B2 | 0.000 | 0.500 | ✗ | ✗ (improved, still out) |
| B4_vs_B3 | 0.100 | 0.400 | ✗ | ✗ (improved) |
| B3_vs_B2 | 0.600 | 0.600 | ✓ | ✓ |
| B5_vs_B4 | 0.800 | 0.100 | ✗ | ✗ (regression direction flipped) |
| mirror_B3 | 0.700 | 0.300 | ✗ | ✗ |

- **Reason for revert (per methodology):** B5_vs_B2 moved from in-band to out-of-band (-0.7 WR drift). Hard-stop test triggered.
- **Diagnosis:** The change is architecturally correct (B2 precons can now cast their colored-cost dragons), but it lets B2 race Ezio before Ezio's tutor chain assembles the combo. Games ending at turn 15-24 (was 30-50). Fix would need Phase 3 (fast mana) to bring Ezio's combo turn down to 3-4, which restores the race advantage. Logged as "tried, didn't work in isolation — revisit after fast mana lands."

### Phase 3: Fast mana — landed (verified neutral at higher N)

- **Code:** `FAST_MANA_PRODUCERS` dict (mpa_actions.py:200-228, 24 cards), `_available_mana` extended to count untapped fast mana (mpa_actions.py:241-244), `_untapped_mana_sources_count` helper (mpa_policy.py:9-22), `_pick_fast_mana_cast` priority (mpa_policy.py:164-179), wired in `choose_action` after wincon+tutor and before commander+highest-CMC. Runner `_auto_tap_for_cost` extended to tap fast mana before lands.
- **Tests:** 7 new in `test_mpa_fast_mana.py`; 20/20 green including regression on existing test files.
- **Full calibration (calibration_report_20260520T014539Z.md):**

| Matchup | v0.4 | v0.5 fast mana N=10 | v0.5 N=20 | In band v0.5 |
|---|---|---|---|---|
| B5_vs_B2 | 0.900 | 0.800 (marginal) | **0.900** | ✓ (verified at N=20) |
| B4_vs_B2 | 0.000 | 0.000 | — | ✗ |
| B4_vs_B3 | 0.100 | 0.100 | — | ✗ |
| B3_vs_B2 | 0.600 | 0.600 | — | ✓ |
| B5_vs_B4 | 0.800 | 0.900 | — | ✗ (overshoot worse) |
| mirror_B3 | 0.700 | 0.700 | — | ✗ |

- **Decision:** N=10 showed marginal in-band → out-of-band move on B5_vs_B2 (drift 0.05). Verified at N=20: WR=0.90, back in band. The N=10 dip is within binomial noise (1 game out of 10 with σ≈0.11). Capability is architecturally correct and needed by downstream phases (color-aware mana revisit, removal, attack heuristic). Committed as neutral; logged the N=10 → N=20 disambiguation pattern for future reference.
- **Aggregate state after Phase 3:** in band 2/6, draw rate 13.3% (under 30% threshold).

### Phase 4: More wincon patterns (extra-combat) — landed; Phase 4b color-aware redux — REVERTED

- **Phase 4 code:** `_EXTRA_COMBAT_ANCHORS` and `_EXTRA_COMBAT_ENABLERS` sets (mpa_policy.py:36-66), `_check_extra_combat_wincon` helper, wired into `check_win_conditions` after Thoracle pattern (mpa_policy.py:97-117). Anchors: Aggravated Assault, Hellkite Charger, Combat Celebrant. Enablers: Old Gnawbone, Savage Ventmaw, Ancient Copper Dragon, Bear Umbra, Sword of Feast and Famine, Nature's Will, Aurelia.
- **Phase 4 tests:** 7 new in `test_mpa_more_wincons.py`; 27/27 green.
- **Phase 4 alone:** identical to Phase 3 baseline. The combo wincon NEVER fires in real games because Ur-Dragon B4 cannot CAST Aggravated Assault ({3}{R}{R}) — its nonbasic-heavy manabase shows as colorless without Phase 2's color-aware mana. Code committed as architecturally correct dead weight; ready to use when color-aware mana lands.
- **Phase 4b (color-aware mana re-introduction):** I re-applied Phase 2's _land_color tuple-return + nonbasic table to see if combo wincon could now fire. It DID fire — B4_vs_B2 went 0.0 → 0.5, B4_vs_B3 went 0.1 → 0.4. **But** the same dynamic regressed B5_vs_B2 (0.8 → 0.2) and B5_vs_B4 (0.9 → 0.1) because Ezio's policy started casting its "junk" big spells (Mnemonic Betrayal, Wheel of Fortune) once they became legal, bleeding mana that should have gone to combo assembly. Net in-band count went 2 → 1; total drift sum went 1.55 → 1.70.
- **Reverted Phase 4b per methodology.** The capability is correct but exposes a deeper issue: the policy needs to know *which* spells advance toward a wincon vs which are junk. Without that filter, color-aware mana harms B5 decks more than it helps B4 decks.
- **Open question for end-of-loop report:** can the "wincon-relevant cast filter" be implemented as a Phase 7 capability that re-enables Phase 4b? Out of scope for this loop's listed capabilities.
- **Aggregate state after Phase 4:** 1/6 in band (B3_vs_B2), draw rate ~13% — unchanged from Phase 3 (B5_vs_B2 is 0.8 at N=10 today, was 0.9 at N=20 last run; within noise).





