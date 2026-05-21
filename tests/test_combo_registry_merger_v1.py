"""Iter 5 Phase 12 — combo registry merger tests."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from api.engine.layers import combo_registry_merger_v1 as mg


class LoadMergedRegistryTests(unittest.TestCase):
    def test_merges_canonical_plus_external(self) -> None:
        # Use the real on-disk files (Spellbook + Phase 12 seed).
        result = mg.load_merged_registry()
        self.assertGreater(result["canonical_count"], 1000,
                           msg="Spellbook canonical should have thousands of variants")
        self.assertGreater(result["external_count"], 5,
                           msg="External-sources seed should have >5 curated entries")
        self.assertGreater(result["merged_count"], 1000)

    def test_external_only_pair_lands_in_merged(self) -> None:
        result = mg.load_merged_registry()
        names_to_find = {"niv-mizzet, parun", "curiosity"}
        found = False
        for v in result["merged_variants"]:
            normalized = {(c or "").strip().lower() for c in (v.get("card_names") or [])}
            if names_to_find.issubset(normalized):
                found = True
                break
        self.assertTrue(found,
                        msg="Niv-Mizzet + Curiosity (external seed) should be in merged")

    def test_spellbook_precedence_on_bracket_conflict(self) -> None:
        # Synthesize a small conflict: one Spellbook entry + one external
        # entry for the same pair with different brackets.
        with tempfile.TemporaryDirectory() as td:
            canonical_path = Path(td) / "canonical.json"
            external_path = Path(td) / "external.json"
            canonical_path.write_text(json.dumps({
                "by_variant_id": {
                    "v1": {
                        "card_names": ["A", "B"],
                        "brackets_allowed": ["B5"],  # Spellbook says B5
                        "color_identity": ["R"],
                        "combo_size": 2,
                        "category": "creature-combo",
                    }
                }
            }), encoding="utf-8")
            external_path.write_text(json.dumps({
                "discovered": [
                    {
                        "card_names": ["A", "B"],
                        "brackets_allowed": ["B3"],  # External says B3
                        "source": "hand_curated",
                        "outcome": "test outcome",
                    }
                ]
            }), encoding="utf-8")
            with patch.object(mg, "CANONICAL_REGISTRY_PATH", canonical_path), \
                 patch.object(mg, "EXTERNAL_SOURCES_REGISTRY_PATH", external_path):
                result = mg.load_merged_registry()
            # Find the merged entry.
            entry = next(v for v in result["merged_variants"]
                         if set(v.get("card_names")) == {"A", "B"})
            # Spellbook brackets won.
            self.assertEqual(entry["brackets_allowed"], ["B5"])
            # Conflict logged.
            self.assertEqual(len(result["bracket_conflicts"]), 1)
            self.assertEqual(result["bracket_conflicts"][0]["spellbook_brackets"], ["B5"])
            self.assertEqual(result["bracket_conflicts"][0]["external_brackets"], ["B3"])


class CombosAssemblyNamesMergedTests(unittest.TestCase):
    def test_includes_external_only_names(self) -> None:
        names = mg.load_combo_assembly_names_merged()
        # Niv-Mizzet + Curiosity from the external seed should appear.
        self.assertIn("curiosity", names)
        # Spellbook canonical names still present (e.g., Sol Ring is in
        # many Spellbook combos).
        self.assertIn("sol ring", names)


if __name__ == "__main__":
    unittest.main()
