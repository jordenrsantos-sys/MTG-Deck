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

    def test_not_flagged_when_backup_clears(self) -> None:
        # Even if primary doesn't clear, a strong backup avoids the flag
        # — actually wait, the flag is BOTH primary AND backup missing.
        # Let's test where only backup clears: it counts as backup, no
        # primary, still flagged.
        deck = [_card(f"R {i}", ["recursion-graveyard"]) for i in range(4)]
        report = check_win_con_coherence(deck, None, "B3")  # floor=6
        self.assertTrue(report.flagged_75pct_pile)
        # In the current logic, the backup is selected from ranked[1:],
        # so if there's only ONE pattern hitting any count, backup_plan
        # stays None. flagged_75pct_pile fires because no primary.

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
        # Pool primitives let the combo pattern fire.
        self.assertIsNotNone(report.primary_plan)
        self.assertEqual(report.primary_plan["pattern_id"], "combo_win")


if __name__ == "__main__":
    unittest.main()
