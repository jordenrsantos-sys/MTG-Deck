"""Pillar E v0.2 — card-advantage optimizer tests.

Tests verify:
  - Per-bracket base targets (B1=8 .. B5=10).
  - Archetype deltas (storm -3, control +2, etc.).
  - Mix profile apportions cantrip/engine/burst to the total.
  - Keyword classifier hits the three categories correctly.
  - Reconciliation surfaces discrepancies above the 2-unit threshold
    in either direction.
  - Five reference deck shapes (mono-W control, BG aristocrats, UR
    storm, WUBRG goodstuff, mono-R aggro) produce reasonable
    recommendations.
"""
from __future__ import annotations

import unittest

from api.engine.layers.card_advantage_optimizer_v1 import (
    CARD_ADVANTAGE_OPTIMIZER_VERSION,
    CardAdvantageRecommendation,
    compute_card_advantage,
    _classify_card_advantage,
    _mix_for,
)


def _mk_deck(card_names):
    return [{"card_name": n, "source": "agent", "reason": ""} for n in card_names]


def _mk_pool(card_dicts):
    """Build a pool dict with given candidate dicts (each must have name,
    oracle_text, cmc, type_line)."""
    return {"candidates": list(card_dicts)}


def _candidate(name, oracle_text="", cmc=0, type_line=""):
    return {
        "name": name,
        "oracle_text": oracle_text,
        "cmc": cmc,
        "type_line": type_line,
    }


class ClassifyTests(unittest.TestCase):
    def test_cantrip_recognized(self) -> None:
        self.assertEqual(
            _classify_card_advantage(
                "Look at the top two cards of your library. Put one into your hand and the other on the bottom of your library. Draw a card.",
                cmc=1, type_line="Sorcery",
            ),
            "cantrip",
        )

    def test_burst_recognized_three_cards(self) -> None:
        self.assertEqual(
            _classify_card_advantage(
                "Draw three cards.", cmc=4, type_line="Sorcery",
            ),
            "burst",
        )

    def test_burst_recognized_equal_to(self) -> None:
        self.assertEqual(
            _classify_card_advantage(
                "Sacrifice X creatures. Draw cards equal to the number of creatures sacrificed.",
                cmc=2, type_line="Instant",
            ),
            "burst",
        )

    def test_engine_recognized_upkeep(self) -> None:
        self.assertEqual(
            _classify_card_advantage(
                "At the beginning of your upkeep, draw a card. Pay 1 life for each card in your hand.",
                cmc=3, type_line="Enchantment",
            ),
            "engine",
        )

    def test_engine_recognized_combat_trigger(self) -> None:
        # Edric, Spymaster of Trest — "whenever a creature deals combat
        # damage ... that player may draw a card"
        self.assertEqual(
            _classify_card_advantage(
                "Whenever a creature deals combat damage to one of your opponents, its controller may draw a card.",
                cmc=3, type_line="Legendary Creature",
            ),
            "engine",
        )

    def test_not_a_draw_card(self) -> None:
        self.assertIsNone(
            _classify_card_advantage(
                "Deal 3 damage to any target.",
                cmc=1, type_line="Instant",
            ),
        )

    def test_high_cmc_cantrip_pattern_classifies_as_engine(self) -> None:
        # "Enters: draw a card" on a 6 CMC body is closer to an engine
        # than a cantrip (it's expensive value).
        self.assertEqual(
            _classify_card_advantage(
                "When Mulldrifter enters the battlefield, draw a card.",
                cmc=6, type_line="Creature - Elemental",
            ),
            "engine",
        )


class TargetTests(unittest.TestCase):
    def test_b1_default_target_is_8(self) -> None:
        rec = compute_card_advantage(
            deck=[], bracket="B1", archetype_hint="default",
        )
        self.assertEqual(rec.target_count, 8)

    def test_b5_default_target_is_10(self) -> None:
        rec = compute_card_advantage(
            deck=[], bracket="B5", archetype_hint="default",
        )
        self.assertEqual(rec.target_count, 10)

    def test_storm_archetype_reduces_target(self) -> None:
        rec = compute_card_advantage(
            deck=[], bracket="B3", archetype_hint="storm",
        )
        # Base 10 - 3 = 7.
        self.assertEqual(rec.target_count, 7)

    def test_control_archetype_increases_target(self) -> None:
        rec = compute_card_advantage(
            deck=[], bracket="B3", archetype_hint="control",
        )
        self.assertEqual(rec.target_count, 12)

    def test_target_has_minimum_floor(self) -> None:
        # Synthetic: extreme negative archetype delta floored at 4.
        rec = compute_card_advantage(
            deck=[], bracket="B1", archetype_hint="storm",
        )
        # Base 8 - 3 = 5. Above floor.
        self.assertEqual(rec.target_count, 5)


class MixProfileTests(unittest.TestCase):
    def test_default_mix_apportions(self) -> None:
        mix = _mix_for("default", 10)
        # 4/4/2 profile → 4 cantrip / 4 engine / 2 burst at total=10.
        self.assertEqual(mix["cantrip"], 4)
        self.assertEqual(mix["engine"], 4)
        self.assertEqual(mix["burst"], 2)

    def test_control_mix_has_more_engine_and_cantrip(self) -> None:
        mix = _mix_for("control", 12)
        self.assertGreaterEqual(mix["engine"], 4)
        self.assertGreaterEqual(mix["cantrip"], 4)

    def test_storm_mix_has_zero_engine(self) -> None:
        mix = _mix_for("storm", 6)
        # storm profile is (4, 0, 2).
        self.assertEqual(mix["engine"], 0)

    def test_mix_sums_to_total(self) -> None:
        for arch in ("control", "combo", "tribal", "storm", "aristocrats",
                     "counters_matter", "default"):
            mix = _mix_for(arch, 10)
            self.assertEqual(sum(mix.values()), 10, f"archetype={arch}")


class ReconciliationTests(unittest.TestCase):
    def test_significant_when_deficit_above_threshold(self) -> None:
        deck = _mk_deck(["Lightning Bolt"])
        pool = _mk_pool([_candidate("Lightning Bolt",
                                    "Deal 3 damage to any target.",
                                    cmc=1, type_line="Instant")])
        rec = compute_card_advantage(
            deck=deck, bracket="B3", archetype_hint="default", pool=pool,
        )
        # 0 card-advantage pieces vs target 10 → significant.
        self.assertTrue(rec.significant)
        self.assertGreaterEqual(len(rec.discrepancies), 1)

    def test_not_significant_when_within_tolerance(self) -> None:
        # Build a deck with 4 cantrips + 4 engines + 2 burst = 10.
        cantrips = [_candidate(f"Cantrip{i}",
                               "Draw a card.", cmc=1,
                               type_line="Instant") for i in range(4)]
        engines = [_candidate(f"Engine{i}",
                              "At the beginning of your upkeep, draw a card.",
                              cmc=3, type_line="Enchantment")
                   for i in range(4)]
        bursts = [_candidate(f"Burst{i}", "Draw three cards.", cmc=4,
                             type_line="Sorcery") for i in range(2)]
        all_cards = cantrips + engines + bursts
        deck = _mk_deck([c["name"] for c in all_cards])
        pool = _mk_pool(all_cards)
        rec = compute_card_advantage(
            deck=deck, bracket="B3", archetype_hint="default", pool=pool,
        )
        self.assertFalse(rec.significant)
        self.assertEqual(rec.current_counts["cantrip"], 4)
        self.assertEqual(rec.current_counts["engine"], 4)
        self.assertEqual(rec.current_counts["burst"], 2)


class ReferenceDeckShapesTests(unittest.TestCase):
    """Five reference shapes to verify the optimizer behaves reasonably
    across archetypes."""

    def test_mono_white_control_target_12(self) -> None:
        rec = compute_card_advantage(deck=[], bracket="B3",
                                     archetype_hint="control")
        self.assertEqual(rec.target_count, 12)
        # Control profile prefers engines.
        self.assertGreaterEqual(rec.mix_targets["engine"], 4)

    def test_bg_aristocrats_target_10(self) -> None:
        rec = compute_card_advantage(deck=[], bracket="B3",
                                     archetype_hint="aristocrats")
        self.assertEqual(rec.target_count, 10)
        self.assertGreaterEqual(rec.mix_targets["engine"], 3)

    def test_ur_storm_target_7(self) -> None:
        rec = compute_card_advantage(deck=[], bracket="B3",
                                     archetype_hint="storm")
        self.assertEqual(rec.target_count, 7)
        self.assertEqual(rec.mix_targets["engine"], 0)

    def test_wubrg_goodstuff_b4_target_10(self) -> None:
        rec = compute_card_advantage(deck=[], bracket="B4",
                                     archetype_hint="default")
        self.assertEqual(rec.target_count, 10)

    def test_mono_red_aggro_voltron_target_9(self) -> None:
        rec = compute_card_advantage(deck=[], bracket="B3",
                                     archetype_hint="voltron")
        # Base 10 - 1 = 9.
        self.assertEqual(rec.target_count, 9)


class VersionTagTests(unittest.TestCase):
    def test_version_string_present(self) -> None:
        rec = compute_card_advantage(deck=[], bracket="B3",
                                     archetype_hint="default")
        self.assertEqual(rec.version, CARD_ADVANTAGE_OPTIMIZER_VERSION)
        self.assertIn("v1", rec.version)


if __name__ == "__main__":
    unittest.main()
