"""Mega-task v5 Phase 12 — Graduated playtest tests.

Validates:
  - run_graduated_sweep returns a GraduationReport with the expected
    tier_results structure.
  - Advancement gates work: winrate ≥ threshold → next tier runs;
    winrate < threshold → stops.
  - Per-tier effective bracket adjustment (tier 0 = -1, tier 1 = 0,
    tier 2 = +1).
  - Missing opposition entries don't crash — the tier is marked
    "no_opposition_in_registry".
  - Suggested tweaks fire on stalled tiers.
"""
from __future__ import annotations

import unittest
from unittest.mock import patch

from api.engine.layers.agent_graduated_playtest_v1 import (
    GRADUATED_PLAYTEST_VERSION, TIER_ADVANCE_THRESHOLD,
    GraduationReport, TierResult,
    _effective_bracket, _build_opposition_for_tier,
    run_graduated_sweep,
)
from api.engine.layers.agent_statistical_approximator_v1 import (
    PodWinrateReport, DeckDecomposition,
)


def _make_pod_report(winrate: float) -> PodWinrateReport:
    """Construct a fake PodWinrateReport with a fixed pod_winrate."""
    return PodWinrateReport(
        pod_winrate=winrate,
        per_opponent_winrate={"FakeOpp (B3)": winrate},
        decomposition=DeckDecomposition(
            win_paths=[], speed_score=10.0, interaction_density=5,
            resilience_score=2, vulnerability_to=[],
        ),
    )


class EffectiveBracketTest(unittest.TestCase):
    def test_tier_0_steps_down_one_bracket(self) -> None:
        self.assertEqual(_effective_bracket("B3", 0), "B2")
        self.assertEqual(_effective_bracket("B4", 0), "B3")

    def test_tier_1_keeps_bracket(self) -> None:
        for b in ("B1", "B2", "B3", "B4", "B5"):
            self.assertEqual(_effective_bracket(b, 1), b)

    def test_tier_2_steps_up_one_bracket(self) -> None:
        self.assertEqual(_effective_bracket("B3", 2), "B4")
        self.assertEqual(_effective_bracket("B5", 2), "B5")  # already capped

    def test_tier_0_floor_at_b1(self) -> None:
        self.assertEqual(_effective_bracket("B1", 0), "B1")  # clamps at floor

    def test_unknown_bracket_defaults_to_b3_then_adjusts(self) -> None:
        # Defensive fallback if someone passes a garbage bracket.
        self.assertEqual(_effective_bracket("X", 1), "B3")


class BuildOppositionForTierTest(unittest.TestCase):
    def test_returns_up_to_3_entries_per_tier(self) -> None:
        # Real registry — B3 tier 0 should have at least 3 entries.
        opps = _build_opposition_for_tier("B3", 0)
        self.assertEqual(len(opps), 3)
        for o in opps:
            self.assertEqual(o.get("bracket"), "B2")  # tier 0 effective
            self.assertEqual(o.get("bracket_displayed"), "B3")

    def test_empty_for_unsupported_bracket_tier(self) -> None:
        # B1 tier 0 has no entries per kickoff (B1 stayed mid-only).
        self.assertEqual(_build_opposition_for_tier("B1", 0), [])

    def test_each_tier_isolates_correctly(self) -> None:
        for t in (0, 1, 2):
            opps = _build_opposition_for_tier("B4", t)
            self.assertEqual(len(opps), 3)
            for o in opps:
                self.assertEqual(int(o.get("opposition_tier", -1)), t)


class RunGraduatedSweepTest(unittest.TestCase):
    def test_returns_graduation_report(self) -> None:
        with patch(
            "api.engine.layers.agent_graduated_playtest_v1.approximate_pod_winrate"
        ) as mock:
            mock.return_value = _make_pod_report(0.40)
            r = run_graduated_sweep(deck=[{"card_name": "X"}], bracket="B3")
        self.assertIsInstance(r, GraduationReport)
        self.assertEqual(r.version, GRADUATED_PLAYTEST_VERSION)

    def test_stalls_tier_0_when_below_threshold(self) -> None:
        with patch(
            "api.engine.layers.agent_graduated_playtest_v1.approximate_pod_winrate"
        ) as mock:
            mock.return_value = _make_pod_report(0.40)
            r = run_graduated_sweep(deck=[{"card_name": "X"}], bracket="B3")
        self.assertEqual(len(r.tier_results), 1,
                         "should stop after tier 0 fails")
        self.assertEqual(r.tier_results[0].tier, 0)
        self.assertFalse(r.tier_results[0].advanced)
        self.assertEqual(r.overall_status, "stalled_tier_0")
        self.assertEqual(r.final_tier_reached, 0)

    def test_advances_to_tier_1_when_tier_0_passes(self) -> None:
        # Tier 0 = 0.70 (pass), Tier 1 = 0.30 (fail).
        side_effects = [_make_pod_report(0.70), _make_pod_report(0.30)]
        with patch(
            "api.engine.layers.agent_graduated_playtest_v1.approximate_pod_winrate",
            side_effect=side_effects,
        ):
            r = run_graduated_sweep(deck=[{"card_name": "X"}], bracket="B3")
        self.assertEqual(len(r.tier_results), 2)
        self.assertTrue(r.tier_results[0].advanced)
        self.assertFalse(r.tier_results[1].advanced)
        self.assertEqual(r.overall_status, "stalled_tier_1")
        self.assertEqual(r.final_tier_reached, 1)

    def test_full_graduation_when_all_three_tiers_pass(self) -> None:
        side_effects = [_make_pod_report(0.80), _make_pod_report(0.70),
                        _make_pod_report(0.60)]
        with patch(
            "api.engine.layers.agent_graduated_playtest_v1.approximate_pod_winrate",
            side_effect=side_effects,
        ):
            r = run_graduated_sweep(deck=[{"card_name": "X"}], bracket="B3")
        self.assertEqual(len(r.tier_results), 3)
        for t in r.tier_results:
            self.assertTrue(t.advanced)
        self.assertEqual(r.overall_status, "graduated")
        self.assertEqual(r.final_tier_reached, 2)

    def test_tier_2_failure_marks_graduated_partial(self) -> None:
        # Pass T0 and T1, fail T2.
        side_effects = [_make_pod_report(0.80), _make_pod_report(0.65),
                        _make_pod_report(0.30)]
        with patch(
            "api.engine.layers.agent_graduated_playtest_v1.approximate_pod_winrate",
            side_effect=side_effects,
        ):
            r = run_graduated_sweep(deck=[{"card_name": "X"}], bracket="B3")
        self.assertEqual(len(r.tier_results), 3)
        self.assertTrue(r.tier_results[0].advanced)
        self.assertTrue(r.tier_results[1].advanced)
        self.assertFalse(r.tier_results[2].advanced)
        self.assertEqual(r.overall_status, "graduated_partial")
        self.assertEqual(r.final_tier_reached, 2)

    def test_threshold_is_settable(self) -> None:
        # At a loose 0.20 threshold, even a 0.30 winrate advances.
        side_effects = [_make_pod_report(0.30), _make_pod_report(0.30),
                        _make_pod_report(0.30)]
        with patch(
            "api.engine.layers.agent_graduated_playtest_v1.approximate_pod_winrate",
            side_effect=side_effects,
        ):
            r = run_graduated_sweep(
                deck=[{"card_name": "X"}], bracket="B3",
                tier_advance_threshold=0.20,
            )
        self.assertEqual(len(r.tier_results), 3)
        for t in r.tier_results:
            self.assertTrue(t.advanced)
        self.assertEqual(r.threshold_used, 0.20)

    def test_missing_opposition_tier_marks_no_data(self) -> None:
        # B1 tier 0 doesn't exist in the current registry.
        with patch(
            "api.engine.layers.agent_graduated_playtest_v1.approximate_pod_winrate"
        ) as mock:
            mock.return_value = _make_pod_report(0.99)
            r = run_graduated_sweep(deck=[{"card_name": "X"}], bracket="B1")
        # First tier should bail with no_opposition_in_registry.
        self.assertEqual(r.tier_results[0].reason, "no_opposition_in_registry")
        self.assertFalse(r.tier_results[0].advanced)

    def test_suggested_tweaks_fire_on_stall(self) -> None:
        with patch(
            "api.engine.layers.agent_graduated_playtest_v1.approximate_pod_winrate"
        ) as mock:
            mock.return_value = _make_pod_report(0.30)
            r = run_graduated_sweep(deck=[{"card_name": "X"}], bracket="B3")
        self.assertGreater(len(r.suggested_tweaks), 0,
                           "stalled deck should get tweak suggestions")
        # Tweak should reference the tier or a pillar_e check.
        self.assertTrue(any("tier 0" in t.lower() or "pillar_e" in t.lower()
                            for t in r.suggested_tweaks))

    def test_no_tweaks_on_full_graduation(self) -> None:
        side_effects = [_make_pod_report(0.80), _make_pod_report(0.70),
                        _make_pod_report(0.60)]
        with patch(
            "api.engine.layers.agent_graduated_playtest_v1.approximate_pod_winrate",
            side_effect=side_effects,
        ):
            r = run_graduated_sweep(deck=[{"card_name": "X"}], bracket="B3")
        # Final tier advanced → no stall → no tweaks.
        self.assertEqual(r.suggested_tweaks, [])


class GraduationReportToDictTest(unittest.TestCase):
    def test_to_dict_round_trip(self) -> None:
        with patch(
            "api.engine.layers.agent_graduated_playtest_v1.approximate_pod_winrate"
        ) as mock:
            mock.return_value = _make_pod_report(0.30)
            r = run_graduated_sweep(deck=[{"card_name": "X"}], bracket="B3")
        d = r.to_dict()
        self.assertIn("tier_results", d)
        self.assertIn("overall_status", d)
        self.assertIn("threshold_used", d)
        self.assertEqual(d["version"], GRADUATED_PLAYTEST_VERSION)


if __name__ == "__main__":
    unittest.main()
