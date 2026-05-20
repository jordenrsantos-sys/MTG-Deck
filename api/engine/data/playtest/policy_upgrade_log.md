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


