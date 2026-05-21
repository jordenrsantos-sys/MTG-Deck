"""Iter 5 Phase 9 — aggressive Pillar E mana base reconciliation tests.

Per `feedback_mana_base_serves_spells_not_reverse`, reconciliation
fires on ANY discrepancy now (was iter-3's ">2 delta").

Verifies:
  - delta of 1 land triggers significant=True (was False in iter 3)
  - delta of 1 color source triggers significant=True
  - 0 delta produces no discrepancies + significant=False
  - reconciliation result includes policy="aggressive_recompute_fresh"
"""
from __future__ import annotations

import unittest

from api.engine.layers.mana_base_optimizer_v1 import (
    ManaBaseRecommendation, reconcile_deck_lands,
)


def _mk_rec(lands=36, sources=None):
    """Minimal ManaBaseRecommendation for testing reconciliation."""
    return ManaBaseRecommendation(
        target_land_count=lands,
        color_source_targets=sources or {"B": 20, "R": 13, "W": 13},
        tap_land_tolerance=6,
        utility_land_budget=5,
        basic_nonbasic_ratio=0.30,
        rationale="Test",
    )


class AggressiveReconciliationTests(unittest.TestCase):
    def test_delta_of_one_land_triggers_significant(self) -> None:
        # Build a deck with 35 basics (1 short of 36 target).
        deck = (
            [{"card_name": "Swamp", "source": "mana_base"}] * 20
            + [{"card_name": "Mountain", "source": "mana_base"}] * 8
            + [{"card_name": "Plains", "source": "mana_base"}] * 7
        )
        rec = _mk_rec(lands=36)
        result = reconcile_deck_lands(deck=deck, recommendation=rec)
        # 35 actual vs 36 target = delta -1. Iter 3 would NOT flag this.
        self.assertEqual(result["land_count_delta"], -1)
        self.assertTrue(result["significant"])
        self.assertTrue(any("Land count" in d for d in result["discrepancies"]))

    def test_delta_of_zero_no_discrepancies(self) -> None:
        deck = (
            [{"card_name": "Swamp", "source": "mana_base"}] * 20
            + [{"card_name": "Mountain", "source": "mana_base"}] * 13
            + [{"card_name": "Plains", "source": "mana_base"}] * 3
        )
        rec = _mk_rec(lands=36, sources={"B": 20, "R": 13, "W": 3})
        result = reconcile_deck_lands(deck=deck, recommendation=rec)
        self.assertEqual(result["land_count_delta"], 0)
        self.assertFalse(result["significant"])
        self.assertEqual(result["discrepancies"], [])

    def test_color_source_delta_of_one_triggers_significant(self) -> None:
        # 36 lands but 1 fewer black source than target.
        deck = (
            [{"card_name": "Swamp", "source": "mana_base"}] * 19  # target 20
            + [{"card_name": "Mountain", "source": "mana_base"}] * 14
            + [{"card_name": "Plains", "source": "mana_base"}] * 3
        )
        rec = _mk_rec(lands=36, sources={"B": 20, "R": 13, "W": 3})
        result = reconcile_deck_lands(deck=deck, recommendation=rec)
        self.assertEqual(result["color_source_deltas"]["B"], -1)
        self.assertTrue(result["significant"])

    def test_policy_field_present(self) -> None:
        deck = [{"card_name": "Swamp", "source": "mana_base"}] * 36
        rec = _mk_rec(lands=36, sources={"B": 36})
        result = reconcile_deck_lands(deck=deck, recommendation=rec)
        self.assertEqual(result.get("policy"), "aggressive_recompute_fresh")


if __name__ == "__main__":
    unittest.main()
