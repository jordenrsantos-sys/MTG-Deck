from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import tools.update_combo_packs_from_spellbook as updater


class UpdateComboPacksFromSpellbookTests(unittest.TestCase):
    def test_main_dry_run_does_not_write_files(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)

            with patch.object(
                updater,
                "_fetch_json",
                return_value={
                    "results": [
                        {
                            "variant_id": "V1",
                            "cards": ["Combo Piece B", "Combo Piece A"],
                        }
                    ],
                    "next": None,
                },
            ):
                exit_code = updater.main(
                    [
                        "--api-base",
                        "https://example.test",
                        "--endpoint",
                        "/variants/",
                        "--output-dir",
                        str(output_dir),
                        "--dry-run",
                    ]
                )

        self.assertEqual(exit_code, 0)
        self.assertFalse((output_dir / "commander_spellbook_variants_v1.json").exists())
        self.assertFalse((output_dir / "two_card_combos_v2.json").exists())

    def test_main_writes_and_is_deterministic_across_repeated_runs(self) -> None:
        page_one = {
            "results": [
                {
                    "variant_id": "V2",
                    "cards": ["Combo Piece B", "Combo Piece A"],
                    "result": " Infinite Mana ",
                    "tags": ["engine", "combo", "combo"],
                }
            ],
            "next": "https://example.test/variants/?page=2",
        }
        page_two = {
            "results": [
                {
                    "variant_id": "V1",
                    "cards": ["combo piece a", "combo piece b"],
                },
                {
                    "variant_id": "V3",
                    "cards": ["Card C", "Card D", "Card E"],
                },
            ],
            "next": None,
        }

        responses_by_url = {
            "https://example.test/variants/": page_one,
            "https://example.test/variants/?page=2": page_two,
        }

        def _fetch_json_side_effect(url: str, *, timeout_seconds: int):
            _ = timeout_seconds
            return responses_by_url[url]

        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)

            with patch.object(updater, "_fetch_json", side_effect=_fetch_json_side_effect):
                first_exit = updater.main(
                    [
                        "--api-base",
                        "https://example.test",
                        "--endpoint",
                        "/variants/",
                        "--output-dir",
                        str(output_dir),
                    ]
                )

            variants_path = output_dir / "commander_spellbook_variants_v1.json"
            two_card_path = output_dir / "two_card_combos_v2.json"

            self.assertEqual(first_exit, 0)
            self.assertTrue(variants_path.is_file())
            self.assertTrue(two_card_path.is_file())

            first_variants_text = variants_path.read_text(encoding="utf-8")
            first_two_card_text = two_card_path.read_text(encoding="utf-8")

            variants_payload = json.loads(first_variants_text)
            two_card_payload = json.loads(first_two_card_text)

            self.assertEqual(variants_payload.get("version"), updater.SPELLBOOK_VARIANTS_V1_VERSION)
            self.assertEqual(variants_payload.get("generated_from"), "/variants/")
            self.assertEqual(
                variants_payload.get("variants"),
                [
                    {"cards": ["combo piece a", "combo piece b"], "variant_id": "V1"},
                    {
                        "cards": ["combo piece a", "combo piece b"],
                        "result": "Infinite Mana",
                        "tags": ["combo", "engine"],
                        "variant_id": "V2",
                    },
                    {"cards": ["card c", "card d", "card e"], "variant_id": "V3"},
                ],
            )
            self.assertEqual(two_card_payload.get("version"), updater.TWO_CARD_COMBOS_V2_VERSION)
            self.assertEqual(
                two_card_payload.get("pairs"),
                [
                    {
                        "a": "combo piece a",
                        "b": "combo piece b",
                        "variant_ids": ["V1", "V2"],
                    }
                ],
            )

            with patch.object(updater, "_fetch_json", side_effect=_fetch_json_side_effect):
                second_exit = updater.main(
                    [
                        "--api-base",
                        "https://example.test",
                        "--endpoint",
                        "/variants/",
                        "--output-dir",
                        str(output_dir),
                    ]
                )

            self.assertEqual(second_exit, 0)
            self.assertEqual(first_variants_text, variants_path.read_text(encoding="utf-8"))
            self.assertEqual(first_two_card_text, two_card_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
