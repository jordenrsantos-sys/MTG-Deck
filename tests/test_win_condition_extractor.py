"""Tests for tools/playtest/win_condition_extractor.py (Phase 5b stage 5b.7).

Critical safety #8 stage: must distinguish Heliod from Varis at high-level
granularity (OQ-6's combat / combo / mill / alt_win / value_grind axis).
"""
from __future__ import annotations

import unittest

from tools.playtest.win_condition_extractor import (
    DeckSignals,
    WIN_CONDITION_EXTRACTOR_V1_VERSION,
    WIN_LINE_COMBAT,
    WIN_LINE_COMBO,
    WIN_LINE_VALUE_GRIND,
    envelope_for_deck_dict,
    extract_envelope,
)


def _make_signals(**overrides) -> DeckSignals:
    defaults = dict(
        deck_id="X",
        gc=0,
        combo_count=0,
        cheap_tutor=0,
        fast_mana=0,
        interaction_staple=0,
        value_staple=0,
        engine_assigned_bracket=None,
        self_reported_bracket=None,
    )
    defaults.update(overrides)
    return DeckSignals(**defaults)


class WinConditionEnvelopeBasicTests(unittest.TestCase):
    def test_zero_signal_deck_returns_combat_primary(self) -> None:
        env = extract_envelope(_make_signals())
        self.assertEqual(env["primary_type"], WIN_LINE_COMBAT)
        self.assertEqual(env["win_line_count"], 1)

    def test_combo_with_tutors_returns_combo_primary(self) -> None:
        # Combo + cheap_tutor >= 2 -> combo is the assembled win line.
        env = extract_envelope(_make_signals(combo_count=2, cheap_tutor=3, gc=9))
        self.assertEqual(env["primary_type"], WIN_LINE_COMBO)
        self.assertGreater(env["primary_win_lines"][0]["redundancy_count"], 0)
        self.assertGreater(env["primary_win_lines"][0]["enabler_density"], 0)

    def test_combo_without_tutors_returns_combat_primary_combo_secondary(self) -> None:
        # Combo present but cheap_tutor < 2 -> combat is primary (combo is
        # opportunistic, not the plan).
        env = extract_envelope(_make_signals(combo_count=2, cheap_tutor=0, gc=9))
        self.assertEqual(env["primary_type"], WIN_LINE_COMBAT)
        secondary_types = [s["type"] for s in env["secondary_win_lines"]]
        self.assertIn(WIN_LINE_COMBO, secondary_types)

    def test_value_grind_detected_for_low_gc_high_value_staple_no_combo(self) -> None:
        env = extract_envelope(_make_signals(combo_count=0, gc=0, value_staple=4))
        secondary_types = [s["type"] for s in env["secondary_win_lines"]]
        self.assertIn(WIN_LINE_VALUE_GRIND, secondary_types)


class WinConditionHeliodVsVarisDisambiguationTests(unittest.TestCase):
    """Safety #8: Heliod (user=B4) vs Varis (user=B5) must produce different
    win-condition envelopes at high-level granularity per OQ-6.

    Per [[CALIBRATION_DRIFT_REPORT_2026-05-08-post-5a62]]:
    - Heliod (ARCHIDEKT_18520785, self=B4): gc=9, combos=2, diag c0 f1 i2 v2
    - Varis (ARCHIDEKT_1971524, self=B5): gc=9, combos=2, diag c3 f1 i1 v0

    The cheap_tutor axis (c=0 vs c=3) is the disambiguation signal.
    """

    def test_heliod_envelope_is_combat_primary(self) -> None:
        # Heliod: combo present (count=2) but cheap_tutor=0 -> combat primary.
        heliod = _make_signals(
            deck_id="ARCHIDEKT_18520785",
            gc=9,
            combo_count=2,
            cheap_tutor=0,
            fast_mana=1,
            interaction_staple=2,
            value_staple=2,
            self_reported_bracket="B4",
            engine_assigned_bracket="B5",
        )
        env = extract_envelope(heliod)
        self.assertEqual(env["primary_type"], WIN_LINE_COMBAT)

    def test_varis_envelope_is_combo_primary(self) -> None:
        # Varis: combo present (count=2) + cheap_tutor=3 -> combo primary.
        varis = _make_signals(
            deck_id="ARCHIDEKT_1971524",
            gc=9,
            combo_count=2,
            cheap_tutor=3,
            fast_mana=1,
            interaction_staple=1,
            value_staple=0,
            self_reported_bracket="B5",
            engine_assigned_bracket="B5",
        )
        env = extract_envelope(varis)
        self.assertEqual(env["primary_type"], WIN_LINE_COMBO)

    def test_heliod_and_varis_produce_different_primary_types(self) -> None:
        heliod = _make_signals(
            deck_id="HELIOD",
            gc=9, combo_count=2, cheap_tutor=0, fast_mana=1,
            interaction_staple=2, value_staple=2,
        )
        varis = _make_signals(
            deck_id="VARIS",
            gc=9, combo_count=2, cheap_tutor=3, fast_mana=1,
            interaction_staple=1, value_staple=0,
        )
        env_h = extract_envelope(heliod)
        env_v = extract_envelope(varis)
        # SAFETY #8: high-level granularity MUST distinguish them.
        self.assertNotEqual(env_h["primary_type"], env_v["primary_type"])


class WinConditionLiveCorpusTests(unittest.TestCase):
    """Run extractor against the live 26-deck corpus."""

    def test_envelope_for_deck_dict_with_signals_override(self) -> None:
        # Synthetic deck dict with signals injected.
        deck = {
            "deck_id": "TEST",
            "engine_assigned_bracket": "B5",
            "self_reported_bracket": "B5",
            "power_staple_diagnostic": {
                "cheap_tutor": 3, "fast_mana": 1,
                "interaction_staple": 1, "value_staple": 0,
            },
            "signals": {"gc": 9, "combo_count": 2},
        }
        env = envelope_for_deck_dict(deck)
        self.assertEqual(env["primary_type"], WIN_LINE_COMBO)

    def test_envelope_signature_contains_version_and_signals(self) -> None:
        deck = {
            "deck_id": "X",
            "power_staple_diagnostic": {},
            "signals": {"gc": 0, "combo_count": 0},
        }
        env = envelope_for_deck_dict(deck)
        self.assertEqual(env["version"], WIN_CONDITION_EXTRACTOR_V1_VERSION)
        self.assertIn("signals_used", env)


if __name__ == "__main__":
    unittest.main()
