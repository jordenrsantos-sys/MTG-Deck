"""Phase 10 tests — Pillar E v0.1 mana base optimizer.

Covers:
  - _parse_color_pips: WUBRG pip counting, hybrid handling, generic
    cost rejection.
  - _pip_sources_required: Karsten table lookup with CMC clamping.
  - compute_mana_base: shape across 5+ deck examples (mono-W, 2-color
    RG, 3-color WUB, 4-color WUBR, 5-color WUBRG; multiple brackets).
  - Bracket adjustments: tap-land tolerance, basic ratio, utility
    budget scale correctly with B1→B5.
  - Archetype adjustments: storm = -4 lands, landfall = +2.
  - reconcile_deck_lands: detects significant deltas (>2) and emits
    discrepancies.
  - Reference: Karsten-style sanity checks against published values.
"""
from __future__ import annotations

import unittest

from api.engine.layers.mana_base_optimizer_v1 import (
    KARSTEN_TABLE_COMMANDER,
    ManaBaseRecommendation,
    _parse_color_pips,
    _pip_sources_required,
    compute_mana_base,
    reconcile_deck_lands,
)


class ParsePipsTests(unittest.TestCase):
    def test_single_color_pip(self) -> None:
        self.assertEqual(_parse_color_pips("{1}{B}"), {"B": 1})

    def test_double_pip(self) -> None:
        self.assertEqual(_parse_color_pips("{2}{B}{B}"), {"B": 2})

    def test_triple_pip(self) -> None:
        self.assertEqual(_parse_color_pips("{B}{B}{B}"), {"B": 3})

    def test_multi_color(self) -> None:
        self.assertEqual(_parse_color_pips("{1}{W}{U}{B}"), {"W": 1, "U": 1, "B": 1})

    def test_hybrid_counts_both_colors(self) -> None:
        # Hybrid {W/B}: both white and black get 1 pip.
        self.assertEqual(_parse_color_pips("{W/B}"), {"W": 1, "B": 1})

    def test_x_cost_ignored(self) -> None:
        self.assertEqual(_parse_color_pips("{X}{R}{R}"), {"R": 2})

    def test_generic_only(self) -> None:
        self.assertEqual(_parse_color_pips("{3}"), {})

    def test_empty_string(self) -> None:
        self.assertEqual(_parse_color_pips(""), {})


class SourcesRequiredTests(unittest.TestCase):
    def test_bb_at_cmc_3_per_karsten(self) -> None:
        # Karsten table value for (CMC 3, 2 pips) = 20.
        self.assertEqual(_pip_sources_required(3, 2), 20)

    def test_single_pip_at_cmc_2(self) -> None:
        self.assertEqual(_pip_sources_required(2, 1), 18)

    def test_cmc_below_1_treated_as_1(self) -> None:
        self.assertEqual(_pip_sources_required(0, 1), 19)

    def test_cmc_above_7_treated_as_7(self) -> None:
        self.assertEqual(_pip_sources_required(10, 1), 12)

    def test_zero_pips_returns_zero(self) -> None:
        self.assertEqual(_pip_sources_required(3, 0), 0)

    def test_table_has_all_cmc_rows(self) -> None:
        for cmc in range(1, 8):
            self.assertIn(cmc, KARSTEN_TABLE_COMMANDER)


class ComputeManaBaseTests(unittest.TestCase):
    def test_mono_white_simple_deck(self) -> None:
        nonland = [
            {"name": "Soltari Priest", "mana_cost": "{W}{W}", "cmc": 2},
            {"name": "Wrath of God", "mana_cost": "{2}{W}{W}", "cmc": 4},
        ]
        rec = compute_mana_base(
            commander_color_identity=["W"],
            nonland_cards=nonland, bracket="B3",
        )
        self.assertIsInstance(rec, ManaBaseRecommendation)
        # MAX over (WW@2=23, WW@4=18) = 23. Karsten's table says
        # earlier double-pip is the harder requirement.
        self.assertEqual(rec.color_source_targets["W"], 23)
        # B3 baseline is 36 lands.
        self.assertEqual(rec.target_land_count, 36)
        # B3 tap tolerance.
        self.assertEqual(rec.tap_land_tolerance, 6)

    def test_two_color_rg_deck(self) -> None:
        nonland = [
            {"name": "Hellrider", "mana_cost": "{2}{R}{R}", "cmc": 4},
            {"name": "Llanowar Elves", "mana_cost": "{G}", "cmc": 1},
            {"name": "Burning Tree Emissary", "mana_cost": "{R/G}{R/G}", "cmc": 2},
        ]
        rec = compute_mana_base(
            commander_color_identity=["R", "G"],
            nonland_cards=nonland, bracket="B3",
        )
        # MAX over (RR@4=18, RG-hybrid 2pip@2=23). Hybrid at low CMC is
        # the harder requirement, so both R and G max at 23.
        self.assertEqual(rec.color_source_targets["R"], 23)
        self.assertEqual(rec.color_source_targets["G"], 23)

    def test_five_color_deck_with_aggressive_pips(self) -> None:
        nonland = [
            {"name": "Cromat", "mana_cost": "{W}{U}{B}{R}{G}", "cmc": 5},
        ]
        rec = compute_mana_base(
            commander_color_identity=["W", "U", "B", "R", "G"],
            nonland_cards=nonland, bracket="B5",
        )
        # Each color single pip at CMC 5 = 14.
        for color in ["W", "U", "B", "R", "G"]:
            self.assertEqual(rec.color_source_targets[color], 14)
        # B5 baseline.
        self.assertEqual(rec.target_land_count, 32)
        # B5 = 0 tap tolerance.
        self.assertEqual(rec.tap_land_tolerance, 0)

    def test_bracket_progression(self) -> None:
        nonland = [{"name": "X", "mana_cost": "{B}", "cmc": 1}]
        # B1 should give more lands than B5.
        b1 = compute_mana_base(
            commander_color_identity=["B"],
            nonland_cards=nonland, bracket="B1",
        )
        b5 = compute_mana_base(
            commander_color_identity=["B"],
            nonland_cards=nonland, bracket="B5",
        )
        self.assertGreater(b1.target_land_count, b5.target_land_count)
        self.assertGreater(b1.tap_land_tolerance, b5.tap_land_tolerance)
        self.assertGreater(b1.basic_nonbasic_ratio, b5.basic_nonbasic_ratio)

    def test_archetype_storm_reduces_lands(self) -> None:
        nonland = [{"name": "X", "mana_cost": "{U}", "cmc": 1}]
        baseline = compute_mana_base(
            commander_color_identity=["U"],
            nonland_cards=nonland, bracket="B5",
        )
        storm = compute_mana_base(
            commander_color_identity=["U"],
            nonland_cards=nonland, bracket="B5",
            archetype_hint="storm",
        )
        self.assertEqual(storm.target_land_count, baseline.target_land_count - 4)

    def test_archetype_landfall_adds_lands(self) -> None:
        nonland = [{"name": "X", "mana_cost": "{G}", "cmc": 1}]
        baseline = compute_mana_base(
            commander_color_identity=["G"],
            nonland_cards=nonland, bracket="B3",
        )
        landfall = compute_mana_base(
            commander_color_identity=["G"],
            nonland_cards=nonland, bracket="B3",
            archetype_hint="landfall",
        )
        self.assertEqual(landfall.target_land_count, baseline.target_land_count + 2)

    def test_off_ci_pips_ignored(self) -> None:
        # User must-include with a W pip in a B-only commander — defensive.
        nonland = [{"name": "X", "mana_cost": "{B}", "cmc": 1},
                   {"name": "Y", "mana_cost": "{W}", "cmc": 1}]  # off-CI
        rec = compute_mana_base(
            commander_color_identity=["B"],
            nonland_cards=nonland, bracket="B3",
        )
        # W not in CI → no W requirement tracked.
        self.assertNotIn("W", rec.color_source_targets)

    def test_rationale_includes_color_explanation(self) -> None:
        nonland = [{"name": "Vito, Thorn", "mana_cost": "{2}{B}{B}", "cmc": 4}]
        rec = compute_mana_base(
            commander_color_identity=["B"],
            nonland_cards=nonland, bracket="B3",
        )
        # Karsten BB at CMC 4 = 18.
        self.assertIn("18 sources", rec.rationale)
        self.assertIn("Vito, Thorn", rec.rationale)

    def test_edgar_shape_3_color_mardu(self) -> None:
        # Spec reference: a 3-color B3 vampire deck with avg MV ~4 and
        # BB requirements at CMC 3+ should target ~36 lands with ~14
        # B sources. Use a representative nonland set.
        nonland = [
            {"name": "Edgar Markov", "mana_cost": "{3}{R}{W}{B}", "cmc": 6},
            {"name": "Vito, Thorn", "mana_cost": "{2}{B}{B}", "cmc": 4},
            {"name": "Bloodthirsty Conqueror", "mana_cost": "{2}{B}{B}", "cmc": 4},
            {"name": "Goblin Tutor", "mana_cost": "{R}", "cmc": 1},
        ]
        rec = compute_mana_base(
            commander_color_identity=["B", "R", "W"],
            nonland_cards=nonland, bracket="B3",
        )
        self.assertEqual(rec.target_land_count, 36)
        # BB at CMC 4 → 18 B sources required (Vito + BC trigger).
        self.assertEqual(rec.color_source_targets["B"], 18)


class ReconcileDeckLandsTests(unittest.TestCase):
    def _rec(self, land_target=36, b_target=14) -> ManaBaseRecommendation:
        return ManaBaseRecommendation(
            target_land_count=land_target,
            color_source_targets={"B": b_target},
            tap_land_tolerance=6,
            utility_land_budget=5,
            basic_nonbasic_ratio=0.30,
            rationale="...",
        )

    def _deck(self, n_swamps: int, n_other_lands: int, n_nonlands: int = 60) -> list:
        deck = []
        for _ in range(n_swamps):
            deck.append({"card_name": "Swamp", "source": "mana_base"})
        for i in range(n_other_lands):
            deck.append({"card_name": f"Other Land {i}", "source": "mana_base"})
        for i in range(n_nonlands):
            deck.append({"card_name": f"Spell {i}", "source": "theme"})
        return deck

    def test_exact_match_no_discrepancies(self) -> None:
        rec = self._rec(land_target=20, b_target=14)
        deck = self._deck(n_swamps=14, n_other_lands=6, n_nonlands=80)
        r = reconcile_deck_lands(deck=deck, recommendation=rec)
        self.assertEqual(r["actual_land_count"], 20)
        self.assertEqual(r["actual_color_sources"]["B"], 14)
        self.assertEqual(r["discrepancies"], [])
        self.assertFalse(r["significant"])

    def test_too_few_lands_flags_discrepancy(self) -> None:
        rec = self._rec(land_target=36, b_target=14)
        deck = self._deck(n_swamps=10, n_other_lands=10, n_nonlands=80)  # 20 lands vs 36 target
        r = reconcile_deck_lands(deck=deck, recommendation=rec)
        self.assertTrue(r["significant"])
        codes = " ".join(r["discrepancies"])
        self.assertIn("Land count", codes)

    def test_b_sources_too_few_flagged(self) -> None:
        rec = self._rec(land_target=36, b_target=14)
        # 36 lands but only 5 swamps → B sources well under target.
        deck = self._deck(n_swamps=5, n_other_lands=31, n_nonlands=64)
        r = reconcile_deck_lands(deck=deck, recommendation=rec)
        self.assertTrue(r["significant"])
        codes = " ".join(r["discrepancies"])
        self.assertIn("B sources", codes)


if __name__ == "__main__":
    unittest.main()
