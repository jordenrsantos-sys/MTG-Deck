from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import api.engine.combos.commander_spellbook_variants_v1 as spellbook


class CommanderSpellbookVariantsV1Tests(unittest.TestCase):
    def test_loader_missing_file_raises_explicit_error_code(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            missing_path = Path(tmp_dir) / "commander_spellbook_variants_v1_missing.json"
            with patch.object(spellbook, "_SPELLBOOK_VARIANTS_V1_FILE", missing_path):
                with self.assertRaises(RuntimeError) as raised:
                    spellbook.load_commander_spellbook_variants_v1()

        self.assertTrue(str(raised.exception).startswith("SPELLBOOK_VARIANTS_V1_MISSING:"))

    def test_loader_maps_curated_manifest_errors_to_missing_code(self) -> None:
        with patch.object(
            spellbook,
            "resolve_pack_file_path",
            side_effect=RuntimeError("CURATED_PACK_MANIFEST_V1_MISSING: test"),
        ):
            with self.assertRaises(RuntimeError) as raised:
                spellbook.load_commander_spellbook_variants_v1()

        self.assertTrue(str(raised.exception).startswith("SPELLBOOK_VARIANTS_V1_MISSING:"))

    def test_loader_invalid_payload_raises_explicit_error_code(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            invalid_path = Path(tmp_dir) / "commander_spellbook_variants_v1.json"
            invalid_path.write_text("[]", encoding="utf-8")

            with patch.object(spellbook, "_SPELLBOOK_VARIANTS_V1_FILE", invalid_path):
                with self.assertRaises(RuntimeError) as raised:
                    spellbook.load_commander_spellbook_variants_v1()

        self.assertIn("SPELLBOOK_VARIANTS_V1_INVALID", str(raised.exception))

    def test_loader_normalizes_and_sorts_variants_deterministically(self) -> None:
        payload = {
            "version": "commander_spellbook_variants_v1",
            "source": "commander_spellbook_api",
            "generated_from": "/variants/",
            "variants": [
                {
                    "variant_id": "Z2",
                    "cards": [" Card B ", "card a", "card b", "CARD   A"],
                    "result": " Infinite Mana ",
                    "tags": ["combo", "", "combo", "engine"],
                },
                {
                    "variant_id": "A1",
                    "cards": ["Beta", "Alpha", "alpha"],
                },
            ],
        }

        with TemporaryDirectory() as tmp_dir:
            payload_path = Path(tmp_dir) / "commander_spellbook_variants_v1.json"
            payload_path.write_text(json.dumps(payload), encoding="utf-8")

            with patch.object(spellbook, "_SPELLBOOK_VARIANTS_V1_FILE", payload_path):
                first = spellbook.load_commander_spellbook_variants_v1()
                second = spellbook.load_commander_spellbook_variants_v1()

        self.assertEqual(first, second)
        self.assertEqual(first.get("version"), spellbook.SPELLBOOK_VARIANTS_V1_VERSION)
        self.assertEqual(first.get("source"), "commander_spellbook_api")
        self.assertEqual(first.get("generated_from"), "/variants/")

        variants = first.get("variants") if isinstance(first.get("variants"), list) else []
        self.assertEqual([row.get("variant_id") for row in variants], ["A1", "Z2"])
        self.assertEqual(variants[0].get("cards"), ["alpha", "beta"])
        self.assertNotIn("tags", variants[0])

        self.assertEqual(variants[1].get("cards"), ["card a", "card b"])
        self.assertEqual(variants[1].get("result"), "Infinite Mana")
        self.assertEqual(variants[1].get("tags"), ["combo", "engine"])

    def test_loader_rejects_duplicate_variant_id(self) -> None:
        payload = {
            "version": "commander_spellbook_variants_v1",
            "source": "commander_spellbook_api",
            "generated_from": "/variants/",
            "variants": [
                {"variant_id": "DUP", "cards": ["A", "B"]},
                {"variant_id": "DUP", "cards": ["C", "D"]},
            ],
        }

        with TemporaryDirectory() as tmp_dir:
            payload_path = Path(tmp_dir) / "commander_spellbook_variants_v1.json"
            payload_path.write_text(json.dumps(payload), encoding="utf-8")

            with patch.object(spellbook, "_SPELLBOOK_VARIANTS_V1_FILE", payload_path):
                with self.assertRaises(RuntimeError) as raised:
                    spellbook.load_commander_spellbook_variants_v1()

        self.assertIn("SPELLBOOK_VARIANTS_V1_INVALID", str(raised.exception))
        self.assertIn("duplicate variant_id 'DUP'", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
