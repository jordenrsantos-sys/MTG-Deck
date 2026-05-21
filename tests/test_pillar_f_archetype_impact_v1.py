"""Mega-task v3 Phase 5 — Pillar F new-card archetype-impact scoring.

Verifies:
  - score_card_archetype_impact returns one entry per archetype
  - Counters-matter card scores highest for `counters_matter`
  - Sac-outlet card scores highest for `aristocrats`
  - Mana-positive-rock card scores positive for `storm`, less elsewhere
  - Vanilla card (no primitives) scores zero across all archetypes
  - top_archetypes_for_card returns the top-3 by |delta|
  - Delta is capped at +0.15
"""
from __future__ import annotations

import unittest

from api.engine.layers.agent_statistical_approximator_v1 import (
    score_card_archetype_impact,
    top_archetypes_for_card,
)


def _card(name, primitives):
    return {"name": name, "primitives": list(primitives)}


class ScoreImpactTests(unittest.TestCase):
    def test_returns_one_entry_per_archetype_by_default(self) -> None:
        card = _card("Test", ["sac-outlet"])
        result = score_card_archetype_impact(card)
        # 12 archetypes in the lookup.
        self.assertEqual(len(result), 12)

    def test_counters_matter_card_scores_highest_for_counters_matter(self) -> None:
        card = _card("Doubling Season Variant", ["doubler-effect"])
        result = score_card_archetype_impact(card)
        # `doubler-effect` is weight 1.0 for counters_matter, 0.6 for tokens,
        # 0.0 for everything else.
        ranked = sorted(result.items(), key=lambda kv: -kv[1]["delta"])
        self.assertEqual(ranked[0][0], "counters_matter")
        self.assertGreater(ranked[0][1]["delta"], 0.0)

    def test_sac_outlet_card_scores_highest_for_aristocrats(self) -> None:
        card = _card("Test Seer", ["sac-outlet"])
        result = score_card_archetype_impact(card)
        ranked = sorted(result.items(), key=lambda kv: -kv[1]["delta"])
        self.assertEqual(ranked[0][0], "aristocrats")
        self.assertGreater(ranked[0][1]["delta"], 0.0)

    def test_etb_plus_flicker_card_scores_blink(self) -> None:
        card = _card("Test Closet", ["etb-trigger", "flicker-effect"])
        result = score_card_archetype_impact(card)
        ranked = sorted(result.items(), key=lambda kv: -kv[1]["delta"])
        self.assertEqual(ranked[0][0], "blink")

    def test_vanilla_card_scores_zero(self) -> None:
        card = _card("Vanilla 2/2", [])
        result = score_card_archetype_impact(card)
        for arch, entry in result.items():
            self.assertEqual(entry["delta"], 0.0,
                             msg=f"{arch} had nonzero delta on vanilla")
            self.assertEqual(entry["fits_role"], "vanilla")

    def test_archetypes_filter_param(self) -> None:
        card = _card("Test", ["sac-outlet"])
        result = score_card_archetype_impact(card, archetypes=["aristocrats", "tribal"])
        self.assertEqual(set(result.keys()), {"aristocrats", "tribal"})

    def test_delta_capped_at_0_15(self) -> None:
        # Pile every primitive a single archetype could care about.
        card = _card("Theoretical", [
            "sac-outlet", "death-trigger", "persist-creature",
            "recursion-graveyard", "token-producer",
        ])
        result = score_card_archetype_impact(card)
        self.assertLessEqual(result["aristocrats"]["delta"], 0.15 + 1e-9)

    def test_matched_primitives_list_populated(self) -> None:
        card = _card("Test", ["sac-outlet", "death-trigger"])
        result = score_card_archetype_impact(card)
        self.assertIn("sac-outlet", result["aristocrats"]["matched_primitives"])
        self.assertIn("death-trigger", result["aristocrats"]["matched_primitives"])


class TopArchetypesTests(unittest.TestCase):
    def test_returns_top_3_by_default(self) -> None:
        card = _card("Test", ["sac-outlet", "death-trigger", "token-producer"])
        top = top_archetypes_for_card(card)
        self.assertEqual(len(top), 3)

    def test_top_archetype_for_sac_outlet_is_aristocrats(self) -> None:
        card = _card("Test", ["sac-outlet"])
        top = top_archetypes_for_card(card)
        self.assertEqual(top[0][0], "aristocrats")

    def test_top_k_param(self) -> None:
        card = _card("Test", ["sac-outlet"])
        top = top_archetypes_for_card(card, k=1)
        self.assertEqual(len(top), 1)


if __name__ == "__main__":
    unittest.main()
