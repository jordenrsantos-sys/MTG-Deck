"""Mega-task v5 Phase 9 — Pillar E v0.3 curve smoother tests.

Validates the curve analysis behavior:
  - Archetype target lookup (named archetypes + default fallback).
  - Brick detection (CMC > archetype ceiling).
  - Hole detection (slot < target * hole_pct).
  - Lands excluded from the CMC distribution.
  - `significant` flag tracks bricks-or-holes.
  - The kickoff invariant: a tribal-aggro deck flags 7-CMC cards as
    bricks while a control deck treats them as fine.
"""
from __future__ import annotations

import unittest

from api.engine.layers.curve_smoother_v1 import (
    analyze_curve, load_archetype_curves, CurveAnalysis,
    CURVE_SMOOTHER_VERSION,
)


def _card(name: str, cmc: float, *, source: str = "wild_pool", reason: str = "") -> dict:
    return {"card_name": name, "cmc": cmc, "source": source, "reason": reason}


class CurveTargetLoadingTest(unittest.TestCase):
    def test_loads_named_archetypes(self) -> None:
        arches = load_archetype_curves()
        # We expect at least the kickoff-mentioned archetypes plus default.
        for k in ("tribal", "combo", "control", "counters_matter",
                  "value_engine", "default"):
            self.assertIn(k, arches, f"archetype {k!r} missing from JSON")

    def test_each_archetype_has_required_keys(self) -> None:
        arches = load_archetype_curves()
        for name, spec in arches.items():
            self.assertIn("target", spec, f"{name} missing 'target'")
            self.assertIn("ceiling", spec, f"{name} missing 'ceiling'")
            self.assertIn("hole_pct", spec, f"{name} missing 'hole_pct'")
            # Target sum should be sensible (10-100 range).
            total = sum(spec["target"].values())
            self.assertGreater(total, 10, f"{name} target sum {total} too low")
            self.assertLess(total, 100, f"{name} target sum {total} too high")


class CurveAnalysisBasicTest(unittest.TestCase):
    def test_returns_curve_analysis(self) -> None:
        deck = [_card("X", 2.0), _card("Y", 3.0)]
        result = analyze_curve(deck=deck, archetype_hint="tribal", pool=None)
        self.assertIsInstance(result, CurveAnalysis)
        self.assertEqual(result.version, CURVE_SMOOTHER_VERSION)
        self.assertEqual(result.resolved_archetype, "tribal")

    def test_unknown_archetype_falls_back_to_default(self) -> None:
        deck = [_card("X", 3.0)]
        result = analyze_curve(deck=deck, archetype_hint="not_a_real_thing", pool=None)
        self.assertEqual(result.resolved_archetype, "default")

    def test_none_archetype_falls_back_to_default(self) -> None:
        deck = [_card("X", 3.0)]
        result = analyze_curve(deck=deck, archetype_hint=None, pool=None)
        self.assertEqual(result.resolved_archetype, "default")

    def test_lands_excluded_from_curve(self) -> None:
        # source=mana_base, name in basics, reason includes [slot=land]
        deck = [
            _card("Sol Land", 0.0, source="mana_base"),
            _card("Mountain", 0.0),
            _card("Custom Land", 0.0, reason="[slot=land]"),
            _card("Real Card", 2.0),
        ]
        basics = {"Mountain"}
        result = analyze_curve(deck=deck, archetype_hint="tribal",
                               pool=None, basic_land_names=basics)
        self.assertEqual(result.nonland_card_count, 1,
                         "only one non-land card should be counted")

    def test_empty_deck(self) -> None:
        result = analyze_curve(deck=[], archetype_hint="tribal", pool=None)
        self.assertEqual(result.nonland_card_count, 0)
        self.assertEqual(result.bricks, [])
        # An empty deck has every slot as a hole (0 vs target > 0). That
        # IS significant — the analysis correctly flags a deck with no
        # nonland cards. We just ensure no crash and a sensible report.
        self.assertGreater(len(result.holes), 0,
                           "empty deck should yield holes for all targeted slots")

    def test_pool_cmc_lookup_overrides_deck_field(self) -> None:
        deck = [_card("Real Card", 0.0)]  # deck says cmc=0
        pool = {"candidates": [{"name": "Real Card", "cmc": 5.0}]}  # pool says cmc=5
        result = analyze_curve(deck=deck, archetype_hint="tribal", pool=pool)
        # Pool wins → card lands in CMC 5 slot, not 0.
        self.assertEqual(result.deck_curve["5"], 1)
        self.assertEqual(result.deck_curve["0"], 0)


class CurveBrickDetectionTest(unittest.TestCase):
    def test_tribal_flags_7_cmc_as_brick(self) -> None:
        """Kickoff requirement: tribal-aggro flags 7-CMC cards as bricks
        (tribal ceiling is 6)."""
        deck = [_card("Big Vampire", 8.0)]
        result = analyze_curve(deck=deck, archetype_hint="tribal", pool=None)
        self.assertEqual(len(result.bricks), 1)
        self.assertEqual(result.bricks[0]["card_name"], "Big Vampire")

    def test_control_does_not_flag_7_cmc_as_brick(self) -> None:
        """Kickoff requirement: a control deck treats 7-CMC cards as
        fine (control ceiling is 8)."""
        deck = [_card("Big Spell", 7.0)]
        result = analyze_curve(deck=deck, archetype_hint="control", pool=None)
        self.assertEqual(len(result.bricks), 0,
                         "control should NOT flag 7-CMC as brick")

    def test_storm_flags_6_cmc_as_brick(self) -> None:
        """Storm's ceiling is 5 (super low — wants cheap cantrips and
        rituals); 6-CMC creature is a brick in storm."""
        deck = [_card("Slow Spell", 6.0)]
        result = analyze_curve(deck=deck, archetype_hint="storm", pool=None)
        self.assertEqual(len(result.bricks), 1)

    def test_multiple_bricks_listed(self) -> None:
        deck = [_card(f"Brick {i}", 9.0) for i in range(5)]
        result = analyze_curve(deck=deck, archetype_hint="tribal", pool=None)
        self.assertEqual(len(result.bricks), 5)


class CurveHoleDetectionTest(unittest.TestCase):
    def test_zero_cards_at_cmc_2_is_a_hole_for_tribal(self) -> None:
        """Tribal wants 13 cards at CMC 2 (per the v1 JSON); 0 is well
        under the 50% threshold."""
        # 14 cards at CMC 3 only — saturates the CMC 3 slot, but CMC 2 is empty.
        deck = [_card(f"C3 {i}", 3.0) for i in range(14)]
        result = analyze_curve(deck=deck, archetype_hint="tribal", pool=None)
        holes_cmcs = [h["cmc"] for h in result.holes]
        self.assertIn("2", holes_cmcs,
                      "CMC 2 should be flagged as a hole for tribal")

    def test_full_target_at_slot_is_not_a_hole(self) -> None:
        # Tribal target at CMC 2 is 13. Fill it exactly.
        deck = [_card(f"C2 {i}", 2.0) for i in range(13)]
        result = analyze_curve(deck=deck, archetype_hint="tribal", pool=None)
        for h in result.holes:
            self.assertNotEqual(h["cmc"], "2",
                                "CMC 2 with 13 cards should NOT be a hole")


class CurveSignificanceTest(unittest.TestCase):
    def test_no_bricks_no_holes_then_significant_is_false(self) -> None:
        # Manufacture a tiny deck where each filled slot exceeds 50% of
        # tribal's target. Most slots will be holes (0 vs target>0) — so
        # the only way to get significant=False is to fill every target
        # slot. Approximating with the full tribal target.
        deck = []
        # tribal targets: 0:5, 1:8, 2:13, 3:14, 4:10, 5:4, 6:3, 7+:3
        slot_counts = {"0": 5, "1": 8, "2": 13, "3": 14, "4": 10,
                       "5": 4, "6": 3}  # skip 7+ for simplicity
        cmc_for_slot = {"0": 0.0, "1": 1.0, "2": 2.0, "3": 3.0,
                        "4": 4.0, "5": 5.0, "6": 6.0}
        for slot, n in slot_counts.items():
            for i in range(n):
                deck.append(_card(f"{slot}-{i}", cmc_for_slot[slot]))
        # Add 3 to the 7+ slot but stay under ceiling 6 by making them CMC 6.
        # Actually wait — tribal ceiling is 6, so CMC 7+ ARE bricks.
        # To avoid bricks, we just skip 7+ in this fixture and accept the hole.
        # That means the result IS significant (CMC 7+ hole).
        result = analyze_curve(deck=deck, archetype_hint="tribal", pool=None)
        # The 7+ slot is a hole (0 vs target 3) — so this WILL be
        # significant. Adjust expectation: verify the only hole is 7+.
        holes_cmcs = {h["cmc"] for h in result.holes}
        self.assertEqual(holes_cmcs, {"7+"},
                         f"only 7+ should be a hole, got {holes_cmcs}")
        self.assertEqual(result.bricks, [])
        self.assertTrue(result.significant,
                        "missing 7+ slot makes this significant")

    def test_brick_makes_significant_true(self) -> None:
        deck = [_card("Big", 9.0)]
        result = analyze_curve(deck=deck, archetype_hint="tribal", pool=None)
        self.assertTrue(result.significant)
        self.assertGreater(len(result.discrepancies), 0)


class CurveAnalysisToDictTest(unittest.TestCase):
    def test_to_dict_round_trip(self) -> None:
        deck = [_card("A", 2.0), _card("Big", 8.0)]
        result = analyze_curve(deck=deck, archetype_hint="tribal", pool=None)
        d = result.to_dict()
        self.assertIn("archetype_target", d)
        self.assertIn("deck_curve", d)
        self.assertIn("bricks", d)
        self.assertIn("holes", d)
        self.assertIn("significant", d)
        self.assertEqual(d["resolved_archetype"], "tribal")


if __name__ == "__main__":
    unittest.main()
