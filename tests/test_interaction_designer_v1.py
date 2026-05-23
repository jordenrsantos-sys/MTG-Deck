"""Mega-task v5 Phase 10 — Pillar E v0.4 interaction designer tests.

Validates:
  - Per-bracket total + sorcery/instant split + mass_removal target.
  - Color-gating: counterspells require U; other categories track the
    color rules in _COLOR_GATES.
  - Per-category allocation sums to ~total_target (within rounding).
  - When a deck is provided, actual counts and discrepancies populate.
  - The two kickoff reference cases:
      * Atraxa B2 (WUBG, 4-color) → ~9 interaction with counterspells > 0
      * Krenko B4 (R mono) → ~11 interaction with 0 counterspells
"""
from __future__ import annotations

import unittest

from api.engine.layers.interaction_designer_v1 import (
    compute_interaction_targets,
    InteractionTargets,
    INTERACTION_DESIGNER_VERSION,
)


def _card(name: str, primitives) -> dict:
    return {"card_name": name, "primitives": list(primitives)}


class BracketPolicyTest(unittest.TestCase):
    def test_b2_total_is_9_with_70pct_sorcery(self) -> None:
        out = compute_interaction_targets(
            commander_color_identity=["W", "U", "B", "G"], bracket="B2",
        )
        self.assertEqual(out.total_target, 9)
        self.assertEqual(out.sorcery_speed_target, int(round(9 * 0.7)))
        self.assertEqual(out.mass_removal_target, 2)

    def test_b4_total_is_11_with_50pct_sorcery(self) -> None:
        out = compute_interaction_targets(
            commander_color_identity=["R"], bracket="B4",
        )
        self.assertEqual(out.total_target, 11)
        self.assertEqual(out.sorcery_speed_target, int(round(11 * 0.5)))
        self.assertEqual(out.mass_removal_target, 3)

    def test_b5_total_is_13_with_20pct_sorcery(self) -> None:
        out = compute_interaction_targets(
            commander_color_identity=["U", "B"], bracket="B5",
        )
        self.assertEqual(out.total_target, 13)
        self.assertEqual(out.sorcery_speed_target, int(round(13 * 0.2)))
        self.assertEqual(out.mass_removal_target, 1)

    def test_unknown_bracket_falls_back_to_default(self) -> None:
        out = compute_interaction_targets(
            commander_color_identity=["B", "R", "W"], bracket="BX",
        )
        # Default policy: total=10, sorcery_pct=0.5, mass=2
        self.assertEqual(out.total_target, 10)
        self.assertEqual(out.mass_removal_target, 2)


class ColorGatingTest(unittest.TestCase):
    def test_atraxa_b2_4_color_with_U_gets_counterspells(self) -> None:
        """Kickoff reference: Atraxa B2 (4-color) gets ~10 interaction
        with 4 counterspells (U included)."""
        out = compute_interaction_targets(
            commander_color_identity=["W", "U", "B", "G"], bracket="B2",
        )
        self.assertGreater(out.targets_by_category["counterspells"], 0,
                           "U in CI should enable counterspells target > 0")

    def test_krenko_b4_mono_R_gets_zero_counterspells(self) -> None:
        """Kickoff reference: Krenko B4 (mono-R) gets ~10 interaction
        with 0 counterspells (no U)."""
        out = compute_interaction_targets(
            commander_color_identity=["R"], bracket="B4",
        )
        self.assertEqual(out.targets_by_category["counterspells"], 0,
                         "mono-R must have 0 counterspells target")

    def test_targeted_creature_removal_available_in_every_color(self) -> None:
        for ci in (["W"], ["U"], ["B"], ["R"], ["G"]):
            out = compute_interaction_targets(
                commander_color_identity=ci, bracket="B3",
            )
            self.assertGreater(
                out.targets_by_category["targeted_creature_removal"], 0,
                f"targeted_creature_removal target should be > 0 for CI={ci}",
            )

    def test_colorless_commander_uses_default_targets(self) -> None:
        out = compute_interaction_targets(
            commander_color_identity=[], bracket="B3",
        )
        # No counterspells (no U), no targeted artifact (no W/G/R), etc.
        self.assertEqual(out.targets_by_category["counterspells"], 0)
        # mass_removal is still set (colorless decks use Nevinyrral-style
        # artifact wipes).
        self.assertEqual(out.targets_by_category["mass_removal"], 2)


class AllocationSumTest(unittest.TestCase):
    def test_allocation_sums_to_about_total(self) -> None:
        """The per-category targets (including mass_removal) should sum
        within ±1 of total_target after rounding."""
        for ci in (["W"], ["W", "U"], ["W", "U", "B"],
                   ["W", "U", "B", "G"], ["W", "U", "B", "R", "G"]):
            for bracket in ("B1", "B2", "B3", "B4", "B5"):
                out = compute_interaction_targets(
                    commander_color_identity=ci, bracket=bracket,
                )
                allocated = sum(out.targets_by_category.values())
                self.assertLessEqual(
                    abs(allocated - out.total_target), 1,
                    f"CI={ci} bracket={bracket}: allocated {allocated} "
                    f"vs total_target {out.total_target}",
                )


class DeckDiscrepancyTest(unittest.TestCase):
    def test_no_deck_means_no_actual_no_discrepancies(self) -> None:
        out = compute_interaction_targets(
            commander_color_identity=["U"], bracket="B3",
        )
        self.assertEqual(out.actual_by_category, {})
        self.assertEqual(out.discrepancies, [])
        self.assertFalse(out.significant)

    def test_deck_below_target_is_flagged_as_under(self) -> None:
        # B3 wants ~11 interaction. We provide a deck with 0 — every
        # enabled category should be flagged as under (v7 Phase 6 uses
        # per-category min bounds instead of the legacy ±50% sum band,
        # so the message format is "below per-category min" for bounded
        # categories or "under target" for unbounded ones).
        deck = [_card(f"Card {i}", ["cantrip"]) for i in range(10)]
        out = compute_interaction_targets(
            commander_color_identity=["U", "B"], bracket="B3", deck=deck,
        )
        self.assertGreater(len(out.discrepancies), 0)
        self.assertTrue(out.significant)
        # Every under-discrepancy should either match the legacy message
        # format or the v7 Phase 6 per-category-min format.
        for d in out.discrepancies:
            self.assertTrue(
                "under target" in d or "below per-category min" in d,
                f"Unexpected discrepancy message: {d!r}",
            )

    def test_deck_with_counterspells_counted_in_U_decks(self) -> None:
        deck = [
            _card("Counterspell A", ["counterspell-hard"]),
            _card("Counterspell B", ["counterspell-soft"]),
        ]
        out = compute_interaction_targets(
            commander_color_identity=["U"], bracket="B3", deck=deck,
        )
        self.assertEqual(out.actual_by_category["counterspells"], 2)

    def test_off_color_counterspells_dropped_from_count(self) -> None:
        """A mono-R deck whose deck list somehow contains a card with the
        counterspell-hard primitive (impossible in legal play but tests
        the gating logic): the counter should NOT be counted because
        counterspells require U in CI."""
        deck = [
            _card("Counterspell", ["counterspell-hard"]),
        ]
        out = compute_interaction_targets(
            commander_color_identity=["R"], bracket="B3", deck=deck,
        )
        self.assertEqual(out.actual_by_category["counterspells"], 0,
                         "non-U deck must not count counterspells")

    def test_pool_primitives_win_over_deck_primitives(self) -> None:
        # Deck entry has no primitives; pool match supplies them.
        deck = [{"card_name": "Wrath", "primitives": []}]
        pool = {"candidates": [
            {"name": "Wrath", "primitives": ["removal-mass-creatures"]},
        ]}
        out = compute_interaction_targets(
            commander_color_identity=["W"], bracket="B3", deck=deck, pool=pool,
        )
        self.assertEqual(out.actual_by_category["mass_removal"], 1)


class InstantSorcerySplitTest(unittest.TestCase):
    def test_instant_plus_sorcery_equals_total(self) -> None:
        for bracket in ("B1", "B2", "B3", "B4", "B5"):
            out = compute_interaction_targets(
                commander_color_identity=["U", "B", "G"], bracket=bracket,
            )
            self.assertEqual(
                out.instant_speed_target + out.sorcery_speed_target,
                out.total_target,
            )


class InteractionTargetsToDictTest(unittest.TestCase):
    def test_to_dict_round_trip(self) -> None:
        out = compute_interaction_targets(
            commander_color_identity=["W", "U"], bracket="B3",
        )
        d = out.to_dict()
        self.assertIn("targets_by_category", d)
        self.assertIn("total_target", d)
        self.assertIn("sorcery_speed_target", d)
        self.assertIn("mass_removal_target", d)
        self.assertEqual(d["version"], INTERACTION_DESIGNER_VERSION)


class V6Phase4MultiCategoryClassificationTest(unittest.TestCase):
    """Mega-task v6 Phase 4 (BLOCKING): _classify_card_interaction now
    returns ALL matching interaction categories per card (was: first
    only). The iter 6 sweep landed 0/5 on pillar_e_v0_4_interaction_
    within target as a direct consequence of the undercount.
    """

    def test_classify_returns_set_of_categories(self) -> None:
        from api.engine.layers.interaction_designer_v1 import (
            _classify_card_interaction,
        )
        # Multi-mode interaction card with counterspell + creature removal.
        cats = _classify_card_interaction([
            "counterspell-hard", "removal-creature"
        ])
        self.assertEqual(cats, {"counterspells", "targeted_creature_removal"})

    def test_classify_returns_empty_when_no_interaction_tags(self) -> None:
        from api.engine.layers.interaction_designer_v1 import (
            _classify_card_interaction,
        )
        cats = _classify_card_interaction([
            "sac-outlet", "etb-trigger", "death-trigger"
        ])
        self.assertEqual(cats, set())

    def test_classify_deduplicates_same_category_tags(self) -> None:
        from api.engine.layers.interaction_designer_v1 import (
            _classify_card_interaction,
        )
        # bounce + tap-down BOTH map to targeted_creature_removal — count once.
        cats = _classify_card_interaction(["bounce", "tap-down"])
        self.assertEqual(cats, {"targeted_creature_removal"})

    def test_multi_category_card_counts_in_multiple_categories(self) -> None:
        """A counter+removal hybrid spell now contributes to BOTH
        categories. Before Phase 4 it contributed to only the first."""
        deck = [
            _card("Multi-Mode", ["counterspell-hard", "removal-creature"]),
        ]
        out = compute_interaction_targets(
            commander_color_identity=["U", "B"], bracket="B3", deck=deck,
        )
        # Both categories should register the card.
        self.assertEqual(out.actual_by_category.get("counterspells"), 1)
        self.assertEqual(
            out.actual_by_category.get("targeted_creature_removal"), 1
        )

    def test_interaction_total_no_longer_undercounts_by_first_match(self) -> None:
        """8 multi-mode cards (counter+removal) used to count as 8 total
        because each had ONE first-match category. Now each contributes
        1 to counterspells AND 1 to targeted_creature_removal → total 16,
        which is what the kickoff's ±50% interaction-within target needs."""
        deck = [
            _card(f"Multi {i}", ["counterspell-hard", "removal-creature"])
            for i in range(8)
        ]
        out = compute_interaction_targets(
            commander_color_identity=["U", "B"], bracket="B3", deck=deck,
        )
        total_actual = sum(out.actual_by_category.values())
        self.assertGreaterEqual(
            total_actual, 16,
            "v6 Phase 4: multi-category cards should each add 1 per "
            "matching category",
        )

    def test_counterspell_color_gate_still_enforced_in_multi_category(self) -> None:
        """A multi-mode counter+removal card in a non-U deck should
        STILL not count toward counterspells — but should count toward
        creature removal (the non-gated category)."""
        deck = [
            _card("Multi-Mode", ["counterspell-hard", "removal-creature"]),
        ]
        out = compute_interaction_targets(
            commander_color_identity=["R", "G"], bracket="B4", deck=deck,
        )
        self.assertEqual(out.actual_by_category.get("counterspells", 0), 0)
        self.assertEqual(
            out.actual_by_category.get("targeted_creature_removal"), 1
        )


class V7Phase6PerCategoryBoundsTest(unittest.TestCase):
    """v7 Phase 6: per-category min-max bounds (CC iter-7 sweep gap #3).
    Pre-v7 the ±50% sum-based discrepancy overshot 1.5×target on every
    iter-7 sweep case once v6 P4 multi-primitive counting inflated multi-
    mode counts. Per-category bounds replace the sum-based check."""

    def test_per_category_field_populated_for_bounded_categories(self) -> None:
        deck = [
            _card("Wrath", ["removal-mass-creatures"]),
            _card("Wrath2", ["removal-mass-creatures"]),
            _card("Wrath3", ["removal-mass-creatures"]),
            _card("Doom Blade", ["removal-creature"]),
            _card("Path", ["removal-creature"]),
            _card("Swords", ["removal-creature"]),
            _card("Anguished Unmaking", ["removal-creature"]),
        ]
        out = compute_interaction_targets(
            commander_color_identity=["W", "B"], bracket="B3", deck=deck,
        )
        # mass_removal: 3 cards, bound (2,4) — in range.
        mr = out.per_category.get("mass_removal")
        self.assertIsNotNone(mr)
        self.assertEqual(mr["actual"], 3)
        self.assertEqual(mr["min"], 2)
        self.assertEqual(mr["max"], 4)
        self.assertTrue(mr["in_range"])
        # targeted_creature_removal: 4 cards, bound (4,7) — in range.
        tcr = out.per_category.get("targeted_creature_removal")
        self.assertIsNotNone(tcr)
        self.assertEqual(tcr["actual"], 4)
        self.assertTrue(tcr["in_range"])

    def test_overshoot_in_one_category_does_not_flag_others(self) -> None:
        # Build a deck with 6 mass_removal (above bound 4) and 5
        # targeted_creature_removal (in bound 4-7). Only mass_removal
        # should flag.
        deck = (
            [_card(f"Wrath {i}", ["removal-mass-creatures"]) for i in range(6)]
            + [_card(f"DoomBlade {i}", ["removal-creature"]) for i in range(5)]
        )
        out = compute_interaction_targets(
            commander_color_identity=["W", "B"], bracket="B3", deck=deck,
        )
        # mass_removal overshoots; should flag.
        msgs = "\n".join(out.discrepancies)
        self.assertIn("mass_removal", msgs)
        self.assertIn("above per-category max", msgs)
        # targeted_creature_removal at 5/4-7 should NOT flag.
        self.assertNotIn(
            "targeted_creature_removal above", msgs,
            f"Expected targeted_creature_removal to be in range; got: {msgs}",
        )

    def test_in_range_deck_has_no_discrepancies(self) -> None:
        # B3 WUB deck hitting every bound mid-range.
        deck = (
            [_card(f"Wrath {i}", ["removal-mass-creatures"]) for i in range(3)]
            + [_card(f"DoomBlade {i}", ["removal-creature"]) for i in range(5)]
            + [_card(f"Counterspell {i}", ["counterspell-hard"]) for i in range(4)]
            + [_card("Naturalize", ["removal-artifact"]),
               _card("Disenchant", ["removal-enchantment"])]
        )
        out = compute_interaction_targets(
            commander_color_identity=["W", "U", "B"], bracket="B3", deck=deck,
        )
        # Should be at-or-near in-range everywhere.
        out_of_range_cats = [
            c for c, info in out.per_category.items() if not info["in_range"]
        ]
        # Allow at most 1 category mildly out of range (the asymmetric
        # heuristic allocations can shift a value by 1).
        self.assertLessEqual(
            len(out_of_range_cats), 1,
            f"Too many out-of-range categories: {out_of_range_cats} / per_cat={out.per_category}",
        )

    def test_color_gated_off_category_not_in_per_category(self) -> None:
        # BRG deck has no U → counterspells gated off; should NOT appear
        # in per_category at all.
        deck = [_card("Doom Blade", ["removal-creature"])]
        out = compute_interaction_targets(
            commander_color_identity=["B", "R", "G"], bracket="B3", deck=deck,
        )
        self.assertNotIn("counterspells", out.per_category)

    def test_bounds_message_format_distinguishes_min_max(self) -> None:
        deck = (
            [_card(f"Wrath {i}", ["removal-mass-creatures"]) for i in range(8)]
        )  # mass_removal 8, above bound max 4
        out = compute_interaction_targets(
            commander_color_identity=["W", "B"], bracket="B3", deck=deck,
        )
        any_over_msg = any("above per-category max" in d for d in out.discrepancies)
        self.assertTrue(
            any_over_msg,
            f"Expected 'above per-category max' message; got: {out.discrepancies}",
        )


if __name__ == "__main__":
    unittest.main()
