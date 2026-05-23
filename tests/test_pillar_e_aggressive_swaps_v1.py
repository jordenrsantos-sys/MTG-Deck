"""
Tests for pillar_e_aggressive_swaps_v1 — Mega-task v7 Phase 3.

Validates the deterministic swap engine that ACTS on Pillar E
optimizer flags. Each test feeds a synthetic optimizer block + a deck
+ a pool and asserts the resulting swaps close the flagged gap (or
skip with a documented reason).
"""
from __future__ import annotations

import unittest
from typing import Any, Dict, List

from api.engine.layers.pillar_e_aggressive_swaps_v1 import (
    compute_pillar_e_aggressive_swaps,
    TOTAL_SWAP_BUDGET,
)


def _make_deck(*, commander: str, fillers: int = 30, basics: int = 68) -> List[Dict[str, str]]:
    deck: List[Dict[str, str]] = [
        {"card_name": commander, "reason": "Commander (locked).", "source": "user_intent"},
    ]
    deck += [
        {"card_name": f"Filler {i}", "reason": "Theme fill.", "source": "theme:TYPAL_TEST"}
        for i in range(1, fillers + 1)
    ]
    deck += [
        {"card_name": "Swamp", "reason": "Mana base", "source": "mana_base"}
    ] * basics
    return deck


def _ramp_pool_entry(name: str, cmc: float = 2.0) -> Dict[str, Any]:
    return {
        "name": name, "type_line": "Artifact",
        "primitives": ["MANA_ROCK", "RAMP_MANA"], "color_identity": [],
        "cmc": cmc, "source": "archetype_staple",
    }


def _draw_pool_entry(name: str, cmc: float = 3.0) -> Dict[str, Any]:
    return {
        "name": name, "type_line": "Enchantment",
        "primitives": ["CARD_DRAW"], "color_identity": ["B"],
        "cmc": cmc, "source": "archetype_staple",
    }


def _removal_pool_entry(name: str, cmc: float = 1.0) -> Dict[str, Any]:
    return {
        "name": name, "type_line": "Instant",
        "primitives": ["REMOVAL_SINGLE", "TARGETED_REMOVAL_CREATURE"],
        "color_identity": ["W"], "cmc": cmc, "source": "archetype_staple",
    }


class ManaBaseSwapTests(unittest.TestCase):
    def test_surplus_lands_get_swapped_for_ramp(self) -> None:
        deck = _make_deck(commander="Edgar Markov", fillers=30, basics=68)
        pool = {
            "color_identity": ["B", "R", "W"],
            "candidates": [_ramp_pool_entry("Sol Ring", 1.0),
                           _ramp_pool_entry("Arcane Signet", 2.0)],
        }
        mb = {
            "active": True,
            "reconciliation": {"significant": True, "actual_land_count": 68,
                               "actual_color_sources": {}, "discrepancies": ["surplus"]},
            "recommendation": {"target_land_count": 36, "color_source_targets": {},
                               "tap_land_tolerance": 0.3, "rationale": "test"},
        }
        result = compute_pillar_e_aggressive_swaps(
            deck=deck, pool=pool, db_snapshot_id="test",
            commander_color_identity=["B", "R", "W"],
            must_include_lower=set(), forbidden_set=set(),
            mana_base_block=mb,
        )
        swaps = result["applied_swaps"]
        self.assertEqual(len(swaps), 2)
        for s in swaps:
            self.assertEqual(s["category"], "mana_base")
            self.assertEqual(s["card_out"], "Swamp")
        # Resulting deck contains Sol Ring and Arcane Signet, has 2 fewer Swamps.
        new_names = [c["card_name"] for c in result["new_deck"]]
        self.assertIn("Sol Ring", new_names)
        self.assertIn("Arcane Signet", new_names)
        self.assertEqual(new_names.count("Swamp"), 66)

    def test_surplus_swap_skipped_when_no_ramp_in_pool(self) -> None:
        deck = _make_deck(commander="Edgar Markov", fillers=30, basics=68)
        pool = {"color_identity": ["B", "R", "W"], "candidates": []}
        mb = {
            "active": True,
            "reconciliation": {"significant": True, "actual_land_count": 68,
                               "actual_color_sources": {}, "discrepancies": []},
            "recommendation": {"target_land_count": 36, "color_source_targets": {},
                               "tap_land_tolerance": 0.3, "rationale": "test"},
        }
        result = compute_pillar_e_aggressive_swaps(
            deck=deck, pool=pool, db_snapshot_id="test",
            commander_color_identity=["B", "R", "W"],
            must_include_lower=set(), forbidden_set=set(),
            mana_base_block=mb,
        )
        # No swaps applied because pool has nothing to swap in.
        self.assertEqual(result["applied_swaps"], [])
        # And no skip records either — the loop just doesn't iterate.

    def test_must_include_cards_never_swapped_out(self) -> None:
        deck = _make_deck(commander="Edgar Markov", fillers=30, basics=68)
        deck.append({"card_name": "Bloodthirsty Conqueror",
                     "reason": "User must-include.", "source": "user_intent"})
        pool = {
            "color_identity": ["B", "R", "W"],
            "candidates": [_ramp_pool_entry("Sol Ring", 1.0)],
        }
        mb = {
            "active": True,
            "reconciliation": {"significant": True, "actual_land_count": 68,
                               "actual_color_sources": {}, "discrepancies": []},
            "recommendation": {"target_land_count": 36, "color_source_targets": {},
                               "tap_land_tolerance": 0.3, "rationale": "test"},
        }
        must_inc = {"bloodthirsty conqueror"}
        result = compute_pillar_e_aggressive_swaps(
            deck=deck, pool=pool, db_snapshot_id="test",
            commander_color_identity=["B", "R", "W"],
            must_include_lower=must_inc, forbidden_set=set(),
            mana_base_block=mb,
        )
        # Bloodthirsty Conqueror is never swapped out.
        for s in result["applied_swaps"]:
            self.assertNotEqual(s["card_out"], "Bloodthirsty Conqueror")
        final_names = [c["card_name"] for c in result["new_deck"]]
        self.assertIn("Bloodthirsty Conqueror", final_names)


class CardAdvantageSwapTests(unittest.TestCase):
    def test_draw_deficit_triggers_swap(self) -> None:
        deck = _make_deck(commander="Edgar Markov", fillers=30, basics=68)
        pool = {
            "color_identity": ["B", "R", "W"],
            "candidates": [
                _draw_pool_entry("Phyrexian Arena"),
                _draw_pool_entry("Necropotence", 3.0),
                _draw_pool_entry("Greed", 3.0),
            ],
        }
        ca = {
            "active": True,
            "recommendation": {
                "significant": True, "target_count": 10,
                "mix_targets": {"cantrip": 3, "engine": 4, "burst": 3},
                "current_counts": {"cantrip": 0, "engine": 1, "burst": 0},
            },
        }
        result = compute_pillar_e_aggressive_swaps(
            deck=deck, pool=pool, db_snapshot_id="test",
            commander_color_identity=["B", "R", "W"],
            must_include_lower=set(), forbidden_set=set(),
            card_advantage_block=ca,
        )
        # Should apply up to per-category cap (4) or available draw cards (3).
        self.assertGreaterEqual(len(result["applied_swaps"]), 1)
        self.assertLessEqual(len(result["applied_swaps"]), 4)
        for s in result["applied_swaps"]:
            self.assertEqual(s["category"], "card_advantage")
            self.assertIn(s["card_in"], {"Phyrexian Arena", "Necropotence", "Greed"})


class InteractionSwapTests(unittest.TestCase):
    def test_removal_deficit_per_category_triggers_swap(self) -> None:
        deck = _make_deck(commander="Edgar Markov", fillers=30, basics=68)
        pool = {
            "color_identity": ["B", "R", "W"],
            "candidates": [
                _removal_pool_entry("Swords to Plowshares"),
                _removal_pool_entry("Path to Exile"),
            ],
        }
        ix = {
            "active": True,
            "analysis": {
                "significant": True,
                "per_category": {
                    "targeted_creature_removal": {"target": 4, "actual": 0},
                },
            },
        }
        result = compute_pillar_e_aggressive_swaps(
            deck=deck, pool=pool, db_snapshot_id="test",
            commander_color_identity=["B", "R", "W"],
            must_include_lower=set(), forbidden_set=set(),
            interaction_designer_block=ix,
        )
        self.assertGreaterEqual(len(result["applied_swaps"]), 1)
        names_in = {s["card_in"] for s in result["applied_swaps"]}
        self.assertTrue(names_in & {"Swords to Plowshares", "Path to Exile"})


class GuardrailsTests(unittest.TestCase):
    def test_total_swap_budget_caps_combined_swaps(self) -> None:
        deck = _make_deck(commander="Edgar Markov", fillers=30, basics=68)
        # Build a pool with way more candidates than budget allows.
        pool = {
            "color_identity": ["B", "R", "W"],
            "candidates": (
                [_ramp_pool_entry(f"Ramp{i}", float(i)) for i in range(20)]
                + [_draw_pool_entry(f"Draw{i}") for i in range(20)]
                + [_removal_pool_entry(f"Removal{i}") for i in range(20)]
            ),
        }
        mb = {
            "active": True,
            "reconciliation": {"significant": True, "actual_land_count": 68,
                               "actual_color_sources": {}, "discrepancies": []},
            "recommendation": {"target_land_count": 36, "color_source_targets": {},
                               "tap_land_tolerance": 0.3, "rationale": "test"},
        }
        ca = {
            "active": True,
            "recommendation": {
                "significant": True, "target_count": 20,
                "mix_targets": {"cantrip": 7, "engine": 7, "burst": 6},
                "current_counts": {"cantrip": 0, "engine": 0, "burst": 0},
            },
        }
        ix = {
            "active": True,
            "analysis": {
                "significant": True,
                "per_category": {
                    "targeted_creature_removal": {"target": 10, "actual": 0},
                },
            },
        }
        result = compute_pillar_e_aggressive_swaps(
            deck=deck, pool=pool, db_snapshot_id="test",
            commander_color_identity=["B", "R", "W"],
            must_include_lower=set(), forbidden_set=set(),
            mana_base_block=mb, card_advantage_block=ca,
            interaction_designer_block=ix,
        )
        self.assertLessEqual(len(result["applied_swaps"]), TOTAL_SWAP_BUDGET)

    def test_color_identity_violation_is_skipped(self) -> None:
        # Edgar BRW; pool offers a green card.
        deck = _make_deck(commander="Edgar Markov", fillers=30, basics=68)
        pool = {
            "color_identity": ["B", "R", "W"],
            "candidates": [
                {"name": "Cultivate", "type_line": "Sorcery",
                 "primitives": ["RAMP_LAND"], "color_identity": ["G"],
                 "cmc": 3.0, "source": "archetype_staple"},
            ],
        }
        mb = {
            "active": True,
            "reconciliation": {"significant": True, "actual_land_count": 68,
                               "actual_color_sources": {}, "discrepancies": []},
            "recommendation": {"target_land_count": 36, "color_source_targets": {},
                               "tap_land_tolerance": 0.3, "rationale": "test"},
        }
        result = compute_pillar_e_aggressive_swaps(
            deck=deck, pool=pool, db_snapshot_id="test",
            commander_color_identity=["B", "R", "W"],
            must_include_lower=set(), forbidden_set=set(),
            mana_base_block=mb,
        )
        # No swap should pick up Cultivate (green not in BRW).
        for s in result["applied_swaps"]:
            self.assertNotEqual(s["card_in"], "Cultivate")
        # And there should be a skip record.
        skip_reasons = [s.get("skip_reason", "") for s in result["skipped_swaps"]]
        self.assertTrue(
            any("color identity" in r for r in skip_reasons),
            f"Expected color-identity skip; got: {skip_reasons}",
        )

    def test_singleton_rule_blocks_duplicate_swap_in(self) -> None:
        # Pool has Sol Ring; deck already contains Sol Ring.
        deck = _make_deck(commander="Edgar Markov", fillers=30, basics=67)
        deck.append({"card_name": "Sol Ring", "reason": "Already there.",
                     "source": "archetype_staple"})
        pool = {
            "color_identity": ["B", "R", "W"],
            "candidates": [_ramp_pool_entry("Sol Ring", 1.0),
                           _ramp_pool_entry("Arcane Signet", 2.0)],
        }
        mb = {
            "active": True,
            "reconciliation": {"significant": True, "actual_land_count": 67,
                               "actual_color_sources": {}, "discrepancies": []},
            "recommendation": {"target_land_count": 36, "color_source_targets": {},
                               "tap_land_tolerance": 0.3, "rationale": "test"},
        }
        result = compute_pillar_e_aggressive_swaps(
            deck=deck, pool=pool, db_snapshot_id="test",
            commander_color_identity=["B", "R", "W"],
            must_include_lower=set(), forbidden_set=set(),
            mana_base_block=mb,
        )
        # The pool filter excludes already-in-deck cards before any
        # proposal is constructed, so Sol Ring is never offered as a
        # swap target. Only Arcane Signet should appear among the
        # applied swaps.
        applied_card_ins = {s["card_in"] for s in result["applied_swaps"]}
        self.assertNotIn("Sol Ring", applied_card_ins)
        self.assertIn("Arcane Signet", applied_card_ins)

    def test_returns_empty_when_no_optimizer_flagged(self) -> None:
        deck = _make_deck(commander="Edgar Markov", fillers=30, basics=68)
        result = compute_pillar_e_aggressive_swaps(
            deck=deck, pool={"color_identity": ["B"], "candidates": []},
            db_snapshot_id="test",
            commander_color_identity=["B"],
            must_include_lower=set(), forbidden_set=set(),
            # All optimizer blocks omitted.
        )
        self.assertEqual(result["applied_swaps"], [])
        self.assertEqual(result["skipped_swaps"], [])


if __name__ == "__main__":
    unittest.main()
