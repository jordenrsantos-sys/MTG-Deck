"""Mega-task v6 Phase 10 — Pillar E v0.6 anti-meta hate optimizer tests.

Verifies:
  - Per-bracket flat targets resolve correctly (B1 minimal, B5 cEDH heavy).
  - Meta-conditional bumps fire on reanimator / combo / artifacts /
    storm / control themes.
  - Output shape includes expected_meta, targets_by_category,
    suggested_candidates, rationale.
  - Loads opposition_decks_v1.json on disk (integration smoke).
"""
from __future__ import annotations

import unittest
from typing import Any, Dict, List

from api.engine.layers.anti_meta_hate_v1 import (
    ANTI_META_HATE_VERSION,
    AntiMetaRecommendations,
    recommend_anti_meta_hate,
)


def _opp(commander: str, bracket: str, archetype_hint: str, tier: int = 1) -> Dict[str, Any]:
    return {
        "corpus_id": f"test_{commander.replace(' ', '_').lower()}",
        "commander": commander,
        "bracket": bracket,
        "archetype_hint": archetype_hint,
        "role_tag": f"{bracket}-test",
        "opposition_tier": tier,
    }


class BracketFlatTargetsTests(unittest.TestCase):
    def test_b1_minimal_hate(self) -> None:
        rec = recommend_anti_meta_hate([], "B1", opposition_data=[])
        self.assertEqual(rec.targets_by_category.get("graveyard_hate"), 0)
        self.assertEqual(rec.targets_by_category.get("artifact_hate"), 0)
        self.assertEqual(rec.targets_by_category.get("stax_tax"), 0)

    def test_b2_one_grave_hate(self) -> None:
        rec = recommend_anti_meta_hate([], "B2", opposition_data=[])
        self.assertEqual(rec.targets_by_category.get("graveyard_hate"), 1)

    def test_b4_includes_stax_and_counter(self) -> None:
        rec = recommend_anti_meta_hate([], "B4", opposition_data=[])
        self.assertEqual(rec.targets_by_category.get("stax_tax"), 1)
        self.assertEqual(rec.targets_by_category.get("counterspell_density"), 1)
        self.assertEqual(rec.targets_by_category.get("graveyard_hate"), 2)

    def test_b5_cedh_counter_density(self) -> None:
        rec = recommend_anti_meta_hate([], "B5", opposition_data=[])
        self.assertEqual(rec.targets_by_category.get("counterspell_density"), 2)


class MetaConditionalBumpsTests(unittest.TestCase):
    def test_reanimator_in_meta_bumps_graveyard_hate(self) -> None:
        opp = [
            _opp("Meren", "B3", "Reanimator graveyard combo"),
            _opp("Karador", "B3", "Reanimator value engine"),
        ]
        rec = recommend_anti_meta_hate([], "B3", opposition_data=opp)
        self.assertIn("reanimator", rec.expected_meta)
        self.assertGreaterEqual(rec.targets_by_category.get("graveyard_hate", 0), 2)
        self.assertTrue(any("graveyard_hate" in r for r in rec.rationale))

    def test_combo_meta_ensures_artifact_hate(self) -> None:
        opp = [
            _opp("Yidris", "B3", "Storm spellslinger combo"),
        ]
        rec = recommend_anti_meta_hate([], "B3", opposition_data=opp)
        self.assertGreaterEqual(rec.targets_by_category.get("artifact_hate", 0), 1)

    def test_b5_control_meta_bumps_counterspell_density_to_3(self) -> None:
        opp = [
            _opp("Talrand", "B5", "Control counter-heavy combo"),
        ]
        rec = recommend_anti_meta_hate([], "B5", opposition_data=opp)
        # Control theme in B5 bumps to >=3.
        self.assertGreaterEqual(rec.targets_by_category.get("counterspell_density", 0), 3)


class SuggestedCandidatesTests(unittest.TestCase):
    def test_b4_offers_specific_candidates_per_target_category(self) -> None:
        rec = recommend_anti_meta_hate([], "B4", opposition_data=[])
        self.assertIn("graveyard_hate", rec.suggested_candidates)
        self.assertIn("artifact_hate", rec.suggested_candidates)
        self.assertIn("stax_tax", rec.suggested_candidates)
        # Some well-known examples present in each category list.
        self.assertIn("Rest in Peace", rec.suggested_candidates["graveyard_hate"])

    def test_b1_offers_no_candidates_for_zero_target_categories(self) -> None:
        rec = recommend_anti_meta_hate([], "B1", opposition_data=[])
        self.assertNotIn("stax_tax", rec.suggested_candidates)
        self.assertNotIn("graveyard_hate", rec.suggested_candidates)


class IntegrationTests(unittest.TestCase):
    def test_loads_real_opposition_file(self) -> None:
        # No opposition_data → loads opposition_decks_v1.json from disk.
        rec = recommend_anti_meta_hate([], "B3")
        # Should produce a non-empty expected_meta from the registered
        # B3 opposition entries.
        self.assertIsInstance(rec.expected_meta, list)
        self.assertGreater(len(rec.expected_meta), 0)


class ReportShapeTests(unittest.TestCase):
    def test_to_dict_round_trip(self) -> None:
        rec = recommend_anti_meta_hate([], "B3", opposition_data=[])
        d = rec.to_dict()
        self.assertEqual(d["version"], ANTI_META_HATE_VERSION)
        self.assertIn("bracket", d)
        self.assertIn("expected_meta", d)
        self.assertIn("targets_by_category", d)
        self.assertIn("suggested_candidates", d)
        self.assertIn("rationale", d)


if __name__ == "__main__":
    unittest.main()
