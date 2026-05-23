"""Mega-task v6 Phase 9 — Pillar E v0.5 win-condition coherence tests.

Verifies the checker:
  - Identifies a clear primary plan when one pattern dominates.
  - Identifies a backup plan when a secondary path clears the floor.
  - Flags the "75% pile of good cards" anti-pattern when no plan clears.
  - Respects per-bracket primary-plan floors.
  - Returns a stable report shape (`to_dict()`).
"""
from __future__ import annotations

import unittest
from typing import Any, Dict, List

from api.engine.layers.win_con_coherence_v1 import (
    WIN_CON_COHERENCE_VERSION,
    check_win_con_coherence,
)


def _card(name: str, primitives: List[str]) -> Dict[str, Any]:
    return {"card_name": name, "primitives": list(primitives), "reason": "", "source": "test"}


class PrimaryPlanIdentificationTests(unittest.TestCase):
    def test_combo_deck_identifies_combo_primary(self) -> None:
        # B5 needs 4 combo-assembly enablers — feed 5.
        deck = [
            _card(f"Combo {i}", ["combo-assembly"])
            for i in range(5)
        ] + [_card("Filler", ["mana-fixing-utility"])]
        report = check_win_con_coherence(deck, None, "B5")
        self.assertIsNotNone(report.primary_plan)
        self.assertEqual(report.primary_plan["pattern_id"], "combo_win")
        self.assertGreaterEqual(report.primary_plan["count"], 5)

    def test_go_wide_anthem_deck_identifies_correct_primary(self) -> None:
        # B3 needs 6 enablers.
        deck = (
            [_card(f"Tokenmaker {i}", ["token-producer", "anthem-effect"])
             for i in range(7)]
            + [_card("Filler", ["mana-fixing-utility"])]
        )
        report = check_win_con_coherence(deck, None, "B3")
        self.assertIsNotNone(report.primary_plan)
        self.assertEqual(report.primary_plan["pattern_id"], "go_wide_anthem")

    def test_counters_proliferate_deck_identifies_v6_dim8_signal(self) -> None:
        # v6 Phase 3 ontology v2 dim-8 primitives should flow through.
        deck = (
            [_card(f"Proliferate {i}", ["proliferate-trigger"]) for i in range(5)]
            + [_card("Counter Payoff A", ["plus1plus1-counter-payoff",
                                          "plus1plus1-counter-distributor"])]
            + [_card("Counter Payoff B", ["plus1plus1-counter-payoff",
                                          "plus1plus1-counter-distributor"])]
        )
        report = check_win_con_coherence(deck, None, "B4")  # B4 floor=5
        self.assertIsNotNone(report.primary_plan)
        self.assertEqual(report.primary_plan["pattern_id"], "counters_proliferate")


class BackupPlanIdentificationTests(unittest.TestCase):
    def test_backup_plan_set_when_secondary_path_clears_floor(self) -> None:
        deck = (
            [_card(f"Combo {i}", ["combo-assembly"]) for i in range(6)]  # primary
            + [_card(f"Reanimate {i}", ["recursion-graveyard"]) for i in range(4)]  # backup
        )
        report = check_win_con_coherence(deck, None, "B4")  # B4 floor=5
        self.assertIsNotNone(report.primary_plan)
        self.assertEqual(report.primary_plan["pattern_id"], "combo_win")
        self.assertIsNotNone(report.backup_plan)
        self.assertEqual(report.backup_plan["pattern_id"], "reanimator")

    def test_no_backup_when_only_primary_clears_floor(self) -> None:
        deck = (
            [_card(f"Combo {i}", ["combo-assembly"]) for i in range(7)]
            + [_card("Lone reanimate", ["recursion-graveyard"])]  # only 1, < backup floor
        )
        report = check_win_con_coherence(deck, None, "B3")
        self.assertIsNotNone(report.primary_plan)
        self.assertIsNone(report.backup_plan)


class FlagSeventyFivePctPileTests(unittest.TestCase):
    def test_flagged_when_no_pattern_reaches_primary_floor(self) -> None:
        # A bunch of cards with diffuse primitives — no clear pattern.
        deck = [
            _card("X1", ["mana-fixing-utility"]),
            _card("X2", ["etb-trigger"]),
            _card("X3", ["card-draw-burst"]),
            _card("X4", ["removal-creature"]),
            _card("X5", ["counterspell-soft"]),
            _card("X6", ["lifegain-payoff"]),
        ]
        report = check_win_con_coherence(deck, None, "B3")
        self.assertTrue(report.flagged_75pct_pile)
        self.assertIsNone(report.primary_plan)
        self.assertIsNone(report.backup_plan)
        self.assertIn("primary floor", report.flag_reason or "")

    def test_flagged_when_only_a_couple_cards_per_pattern(self) -> None:
        # Mega-task v6 Phase 11: bracket floors recalibrated (B3 primary
        # floor now 3). With only 2 recursion cards the primary doesn't
        # clear (need 3), no other pattern has cards, so backup stays
        # None → flagged_75pct_pile fires.
        deck = [_card(f"R {i}", ["recursion-graveyard"]) for i in range(2)]
        report = check_win_con_coherence(deck, None, "B3")  # floor=3
        self.assertTrue(report.flagged_75pct_pile)
        self.assertIsNone(report.primary_plan)
        self.assertIsNone(report.backup_plan)

    def test_clean_when_primary_clears_even_if_no_backup(self) -> None:
        deck = [_card(f"Combo {i}", ["combo-assembly"]) for i in range(7)]
        report = check_win_con_coherence(deck, None, "B3")
        self.assertFalse(report.flagged_75pct_pile)
        self.assertIsNotNone(report.primary_plan)


class BracketSensitivityTests(unittest.TestCase):
    def test_b5_floor_lower_than_b1(self) -> None:
        # 4 combo cards: clears B5 (floor 4), not B1 (floor 8).
        deck = [_card(f"Combo {i}", ["combo-assembly"]) for i in range(4)]
        rb5 = check_win_con_coherence(deck, None, "B5")
        rb1 = check_win_con_coherence(deck, None, "B1")
        self.assertIsNotNone(rb5.primary_plan)
        self.assertIsNone(rb1.primary_plan)
        self.assertTrue(rb1.flagged_75pct_pile)


class ReportShapeTests(unittest.TestCase):
    def test_to_dict_round_trip(self) -> None:
        report = check_win_con_coherence([], None, "B3")
        d = report.to_dict()
        self.assertEqual(d["version"], WIN_CON_COHERENCE_VERSION)
        self.assertIn("primary_plan", d)
        self.assertIn("backup_plan", d)
        self.assertIn("pattern_scores", d)
        self.assertIn("flagged_75pct_pile", d)
        self.assertIn("primary_floor", d)
        self.assertIn("backup_floor", d)

    def test_pool_primitives_win_over_deck_primitives(self) -> None:
        deck = [_card(f"Combo {i}", ["mana-fixing-utility"]) for i in range(6)]
        pool = {"candidates": [
            {"name": f"Combo {i}", "primitives": ["combo-assembly"]}
            for i in range(6)
        ]}
        report = check_win_con_coherence(deck, None, "B3", pool=pool)
        self.assertIsNotNone(report.primary_plan)
        self.assertEqual(report.primary_plan["pattern_id"], "combo_win")


class V7Phase7DBPrimitiveHydrationTests(unittest.TestCase):
    """v7 Phase 7: db_snapshot_id triggers DB hydration for deck cards
    not covered by pool/inlined. Closes CC iter-7 sweep gap #4
    (win_con_coherence 0/5 because only ~30 of 100 deck cards' primitives
    were visible to the pattern matcher)."""

    def test_db_hydration_path_uses_mocked_find_card_by_name(self) -> None:
        # 8 deck cards with no inlined primitives + no pool entries.
        # Without hydration: 0 primitives visible → 75pct_pile.
        # With hydration via a mocked find_card_by_name: combo pattern
        # fires.
        deck = [_card(f"Mystery Combo {i}", []) for i in range(8)]

        import engine.db as _eng_db
        original = _eng_db.find_card_by_name

        def _mock(snap, name):
            return {"primitives": ["combo-assembly"], "name": name}

        try:
            _eng_db.find_card_by_name = _mock
            report = check_win_con_coherence(
                deck, None, "B3", pool=None, db_snapshot_id="any",
            )
        finally:
            _eng_db.find_card_by_name = original

        self.assertIsNotNone(report.primary_plan)
        self.assertEqual(report.primary_plan["pattern_id"], "combo_win")
        self.assertGreaterEqual(report.primary_plan["count"], 3)
        self.assertFalse(report.flagged_75pct_pile)

    def test_db_hydration_skipped_when_db_snapshot_id_is_none(self) -> None:
        deck = [_card(f"Mystery {i}", []) for i in range(8)]
        report = check_win_con_coherence(
            deck, None, "B3", pool=None, db_snapshot_id=None,
        )
        self.assertTrue(report.flagged_75pct_pile)

    def test_db_hydration_skipped_for_basic_lands(self) -> None:
        # Basic lands should NEVER be DB-queried — they have no useful
        # primitives. Verified by checking that find_card_by_name was
        # never called with a basic-land name.
        deck = [_card(name, []) for name in ["Plains", "Island", "Swamp"]] * 4

        import engine.db as _eng_db
        original = _eng_db.find_card_by_name
        called_with: List[str] = []

        def _mock(snap, name):
            called_with.append(name)
            return None

        try:
            _eng_db.find_card_by_name = _mock
            _ = check_win_con_coherence(
                deck, None, "B3", pool=None, db_snapshot_id="any",
            )
        finally:
            _eng_db.find_card_by_name = original

        for name in called_with:
            self.assertNotIn(name, ("Plains", "Island", "Swamp"))

    def test_db_hydration_does_not_override_pool_primitives(self) -> None:
        # Pool has rich primitives for some cards; DB should NOT be
        # queried for those (precedence: pool > inlined > DB).
        deck = [_card(f"Mystery {i}", []) for i in range(8)]
        pool = {"candidates": [
            {"name": "Mystery 0", "primitives": ["counterspell-hard"]},
            {"name": "Mystery 1", "primitives": ["counterspell-hard"]},
        ]}

        import engine.db as _eng_db
        original = _eng_db.find_card_by_name
        called_with: List[str] = []

        def _mock(snap, name):
            called_with.append(name)
            return None

        try:
            _eng_db.find_card_by_name = _mock
            _ = check_win_con_coherence(
                deck, None, "B3", pool=pool, db_snapshot_id="any",
            )
        finally:
            _eng_db.find_card_by_name = original

        # Pool-covered cards should NOT have been DB-queried; only the
        # un-pool-covered ones (Mystery 2 through 7) should.
        self.assertNotIn("Mystery 0", called_with)
        self.assertNotIn("Mystery 1", called_with)
        self.assertIn("Mystery 2", called_with)


if __name__ == "__main__":
    unittest.main()
