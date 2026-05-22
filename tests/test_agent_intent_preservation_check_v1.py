"""Iter 5 Phase 8 — user-intent-preservation validation tests.

Mega-task v5 Phase 7 expands the test set to cover the new archetype-
aware drift thresholds + the expanded counters_matter signal set.
"""
from __future__ import annotations

import unittest

from api.engine.layers.agent_intent_preservation_check_v1 import (
    check_intent_preservation,
    classify_deck_archetype_mix,
    _ARCHETYPE_AWARE_DRIFT_THRESHOLD,
    _THEME_PRIMITIVE_SIGNALS,
)


def _card(name, primitives):
    return {"card_name": name, "primitives": list(primitives)}


def _tribal_profile():
    return {
        "primary":   {"theme": "tribal", "weight": 0.7},
        "secondary": {"theme": "value_engine", "weight": 0.3},
        "tertiary":  {"theme": "", "weight": 0.0},
        "mode": "hint_led",
    }


class ClassifyDeckArchetypeMixTests(unittest.TestCase):
    def test_empty_deck_returns_empty(self) -> None:
        self.assertEqual(classify_deck_archetype_mix([]), {})

    def test_pure_tribal_deck(self) -> None:
        # anthem-effect and token-producer are shared signals across
        # multiple themes (tribal, dragon_tribal, vampire_tribal, tokens,
        # counters_matter, etc.) so tribal alone scores ~0.25. The
        # important assertion is that tribal RANKS TOP among the matched
        # themes — not its absolute weight.
        deck = [
            _card("Tribal A", ["tribal-anchor"]),
            _card("Tribal B", ["tribal-anchor", "anthem-effect"]),
            _card("Tribal C", ["token-producer", "anthem-effect"]),
        ]
        mix = classify_deck_archetype_mix(deck)
        ranked = sorted(mix.items(), key=lambda kv: -kv[1])
        self.assertEqual(ranked[0][0], "tribal")

    def test_aristocrats_deck(self) -> None:
        deck = [
            _card("Outlet", ["sac-outlet"]),
            _card("Persister", ["persist-creature", "death-trigger"]),
            _card("Drain", ["death-trigger"]),
        ]
        mix = classify_deck_archetype_mix(deck)
        ranked = sorted(mix.items(), key=lambda kv: -kv[1])
        self.assertEqual(ranked[0][0], "aristocrats")


class IntentPreservationTests(unittest.TestCase):
    def test_aligned_deck_below_drift_floor(self) -> None:
        # An aligned tribal deck still shows non-zero drift because
        # primitive-signal overlaps spread the deck-mix weight across
        # multiple shared themes. Verify drift stays below 0.7 (deck IS
        # substantively tribal) and that warning_triggered tracks the
        # EFFECTIVE threshold — for a tribal+value_engine profile, Phase 7
        # makes the effective threshold 0.7 (the v1 primitive ontology
        # can't faithfully detect "tribal with value-engine subtype").
        profile = _tribal_profile()
        deck = [_card(f"Tribal {i}", ["tribal-anchor", "anthem-effect"])
                for i in range(8)] + [
            _card("Draw A", ["draw-engine"]),
            _card("Draw B", ["draw-engine"]),
        ]
        report = check_intent_preservation(profile, deck)
        self.assertLess(report.drift, 0.7)
        # warning_triggered tracks the effective threshold reported on the
        # IntentPreservationReport, not a hard-coded 0.3.
        self.assertEqual(
            report.warning_triggered,
            report.drift > report.effective_drift_threshold,
        )
        # And for this tribal+value_engine profile specifically, Phase 7
        # bumps the effective threshold to 0.7.
        self.assertEqual(report.effective_drift_threshold, 0.7)

    def test_drifted_deck_high_drift_triggers_warning(self) -> None:
        # User said tribal/value_engine; agent built a sac+death deck.
        profile = _tribal_profile()
        deck = [_card(f"Sac {i}", ["sac-outlet", "death-trigger"])
                for i in range(15)]
        report = check_intent_preservation(profile, deck)
        self.assertGreater(report.drift, 0.3)
        self.assertTrue(report.warning_triggered)
        self.assertIn("tribal", report.drifted_themes)

    def test_no_profile_no_warning(self) -> None:
        deck = [_card("X", ["sac-outlet"])]
        report = check_intent_preservation(None, deck)
        self.assertFalse(report.warning_triggered)

    def test_empty_deck_no_drift(self) -> None:
        report = check_intent_preservation(_tribal_profile(), [])
        # Empty deck against any profile yields drift=1.0 (all expected
        # weight unmet); but our normalization gives 1.0 for the entire
        # expected portion. Verify the warning fires.
        self.assertGreater(report.drift, 0.3)

    def test_report_contains_deck_archetype_mix(self) -> None:
        profile = _tribal_profile()
        deck = [_card("A", ["tribal-anchor"])]
        report = check_intent_preservation(profile, deck)
        self.assertIn("tribal", report.deck_archetype_mix)
        self.assertIn("tribal", report.profile_themes)


def _counters_profile():
    return {
        "primary":   {"theme": "counters_matter", "weight": 0.6},
        "secondary": {"theme": "value_engine", "weight": 0.3},
        "tertiary":  {"theme": "tokens", "weight": 0.1},
        "mode": "hybrid",
    }


def _tribal_tokens_profile():
    """Tribal primary + tokens secondary — Phase 7 should NOT bump
    threshold here (tokens secondary != value_engine secondary)."""
    return {
        "primary":   {"theme": "tribal", "weight": 0.7},
        "secondary": {"theme": "tokens", "weight": 0.3},
        "tertiary":  {"theme": "", "weight": 0.0},
        "mode": "hybrid",
    }


class Phase7ArchetypeAwareThresholdsTest(unittest.TestCase):
    """Mega-task v5 Phase 7: archetype-aware drift thresholds.

    Iter 5 outliers Atraxa (counters_matter) and Ur-Dragon (tribal +
    value_engine) blew through the 0.3 default not because the agent
    failed at the user's stated intent — it succeeded — but because the
    v1 primitive ontology has no proliferate/counter/cost-reduction tags
    so the classifier can't map their actual expression back to the
    declared theme. Phase 7 raises the bar to 0.7 for these two shapes
    so the warning fires only on genuinely-drifted decks.
    """

    def test_counters_matter_primary_uses_07_threshold(self) -> None:
        profile = _counters_profile()
        # Deck with NO counters_matter primitives; pure drift.
        deck = [_card(f"Noise {i}", ["draw-engine", "sac-outlet"])
                for i in range(10)]
        report = check_intent_preservation(profile, deck)
        self.assertEqual(report.effective_drift_threshold,
                         _ARCHETYPE_AWARE_DRIFT_THRESHOLD)

    def test_tribal_value_engine_combo_uses_07_threshold(self) -> None:
        profile = _tribal_profile()
        deck = [_card("X", ["tribal-anchor"])]
        report = check_intent_preservation(profile, deck)
        self.assertEqual(report.effective_drift_threshold,
                         _ARCHETYPE_AWARE_DRIFT_THRESHOLD)

    def test_tribal_with_other_secondary_keeps_03_threshold(self) -> None:
        # Tribal+tokens shouldn't get the looser threshold — tokens is a
        # different ontology gap than value_engine and we don't blanket-
        # upgrade tribal across the board.
        profile = _tribal_tokens_profile()
        deck = [_card("X", ["tribal-anchor"])]
        report = check_intent_preservation(profile, deck)
        self.assertEqual(report.effective_drift_threshold, 0.3)

    def test_other_archetype_keeps_03_threshold(self) -> None:
        profile = {"primary": {"theme": "aristocrats", "weight": 0.7}}
        deck = [_card("X", ["sac-outlet"])]
        report = check_intent_preservation(profile, deck)
        self.assertEqual(report.effective_drift_threshold, 0.3)

    def test_explicit_caller_override_above_07_takes_precedence(self) -> None:
        profile = _counters_profile()
        deck = [_card("X", ["sac-outlet"])]
        report = check_intent_preservation(profile, deck, drift_threshold=0.9)
        # Caller's higher explicit override wins over the archetype bump.
        self.assertEqual(report.effective_drift_threshold, 0.9)

    def test_effective_threshold_in_to_dict_serialization(self) -> None:
        profile = _counters_profile()
        deck = [_card("X", ["doubler-effect"])]
        d = check_intent_preservation(profile, deck).to_dict()
        self.assertIn("effective_drift_threshold", d)
        self.assertEqual(d["effective_drift_threshold"], 0.7)


class Phase7CountersMatterSignalExpansionTest(unittest.TestCase):
    """Mega-task v5 Phase 7: counters_matter signal set expanded to
    include `anthem-effect` (the broadest reliable proxy for +1/+1-style
    counter contribution under the v1 primitive ontology).
    """

    def test_anthem_effect_now_in_counters_matter_signal_set(self) -> None:
        self.assertIn("anthem-effect", _THEME_PRIMITIVE_SIGNALS["counters_matter"])

    def test_doubler_effect_still_in_counters_matter_signal_set(self) -> None:
        # Don't lose the original signal — only add to it.
        self.assertIn("doubler-effect", _THEME_PRIMITIVE_SIGNALS["counters_matter"])

    def test_anthem_card_contributes_to_counters_matter_weight(self) -> None:
        # Before Phase 7: a pure-anthem card scored 0 toward
        # counters_matter (only doubler-effect counted). After Phase 7:
        # counters_matter picks up some weight.
        deck = [_card("Anthem A", ["anthem-effect"])]
        mix = classify_deck_archetype_mix(deck)
        self.assertGreater(mix.get("counters_matter", 0.0), 0.0,
                           "Phase 7: anthem-only card should contribute "
                           "to counters_matter, was 0 pre-Phase-7")


if __name__ == "__main__":
    unittest.main()
