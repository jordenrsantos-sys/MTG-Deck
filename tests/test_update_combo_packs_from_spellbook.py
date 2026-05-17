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
        self.assertFalse((output_dir / "combo_brackets_v1.json").exists())

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

            with patch.object(updater, "_fetch_json", side_effect=_fetch_json_side_effect), \
                 patch.object(updater, "_now_iso_utc", return_value="2026-05-17T00:00:00Z"):
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
            combo_brackets_path = output_dir / "combo_brackets_v1.json"

            self.assertEqual(first_exit, 0)
            self.assertTrue(variants_path.is_file())
            self.assertTrue(two_card_path.is_file())
            self.assertTrue(combo_brackets_path.is_file())

            first_variants_text = variants_path.read_text(encoding="utf-8")
            first_two_card_text = two_card_path.read_text(encoding="utf-8")
            first_combo_brackets_text = combo_brackets_path.read_text(encoding="utf-8")

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

            with patch.object(updater, "_fetch_json", side_effect=_fetch_json_side_effect), \
                 patch.object(updater, "_now_iso_utc", return_value="2026-05-17T00:00:00Z"):
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
            self.assertEqual(first_combo_brackets_text, combo_brackets_path.read_text(encoding="utf-8"))

    def test_main_handles_nested_card_uses_shape_from_live_api(self) -> None:
        # Regression for the 2026-05-05 ingest blocker: the live Commander Spellbook API
        # returns each uses[] element as {"card": {"oracleId": "...", "name": "..."}, ...},
        # not as a flat dict with oracle_id/name. The original normalizer dropped every
        # variant on len(cards) < 2 and silently emitted zero. This test pins the fixed
        # shape so a future regression on the same code path will fail loudly.
        page = {
            "results": [
                {
                    "id": "VARNESTED1",
                    "uses": [
                        {"card": {"oracleId": "AAAAAAAA-1111-2222-3333-444444444444", "name": "Card Alpha"}},
                        {"card": {"oracleId": "bbbbbbbb-5555-6666-7777-888888888888", "name": "Card Bravo"}},
                    ],
                    "produces": "infinite mana",
                }
            ],
            "next": None,
        }

        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)

            with patch.object(updater, "_fetch_json", return_value=page):
                exit_code = updater.main(
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

            self.assertEqual(exit_code, 0)
            self.assertTrue(variants_path.is_file())
            self.assertTrue(two_card_path.is_file())

            variants_payload = json.loads(variants_path.read_text(encoding="utf-8"))
            two_card_payload = json.loads(two_card_path.read_text(encoding="utf-8"))

            self.assertEqual(
                variants_payload.get("variants"),
                [
                    {
                        "cards": [
                            "aaaaaaaa-1111-2222-3333-444444444444",
                            "bbbbbbbb-5555-6666-7777-888888888888",
                        ],
                        "result": "infinite mana",
                        "variant_id": "VARNESTED1",
                    }
                ],
            )
            self.assertEqual(
                two_card_payload.get("pairs"),
                [
                    {
                        "a": "aaaaaaaa-1111-2222-3333-444444444444",
                        "b": "bbbbbbbb-5555-6666-7777-888888888888",
                        "variant_ids": ["VARNESTED1"],
                    }
                ],
            )

    def test_default_max_pages_is_500(self) -> None:
        args = updater._parse_args([])
        self.assertEqual(args.max_pages, 500)

    def test_default_request_delay_seconds_is_half(self) -> None:
        args = updater._parse_args([])
        self.assertEqual(args.request_delay_seconds, 0.5)

    def test_normalization_drops_all_rows_raises(self) -> None:
        # Pins the new guard: API returned data, but every row dropped during
        # normalization. Previously this silently emitted an empty pack — exactly
        # the failure mode of the 2026-05-05 pass 2 ingest blocker.
        with self.assertRaisesRegex(RuntimeError, "Normalization dropped all rows"):
            updater._build_variants_payload(
                [{"of": [], "uses": []}],
                generated_from="/variants/",
            )

    def test_extract_rows_handles_bulk_dump_variants_key(self) -> None:
        # Regression for the 2026-05-06 pass 8 blocker: the bulk JSON dump at
        # https://json.commanderspellbook.com/variants.json returns a top-level
        # dict shaped {"aliases": [...], "timestamp": "...", "variants": [...],
        # "version": "..."} — variants are under the "variants" key, not
        # "results" or "data". Earlier the extractor returned zero rows.
        payload = {
            "variants": [{"id": 1}, {"id": 2}],
            "version": "v1",
            "timestamp": "2026-05-06T00:00:00Z",
        }
        rows, next_url = updater._extract_rows_and_next(payload)
        self.assertEqual(rows, [{"id": 1}, {"id": 2}])
        self.assertIsNone(next_url)

    def test_extract_rows_raises_on_unknown_dict_shape(self) -> None:
        # Mirror of the normalization-side guard at the extraction layer:
        # if the payload is a non-empty dict but no recognized rows-bearing
        # key exists, raise loudly so a future schema drift is surfaced
        # instead of silently producing zero rows.
        with self.assertRaisesRegex(RuntimeError, "response shape may have changed"):
            updater._extract_rows_and_next({"unknown_key": [1, 2]})

    def test_combo_brackets_payload_maps_letters_and_filters_non_commander_legal(self) -> None:
        rows = [
            # R (Ruthless) -> early / ["B4","B5"]; commander-legal -> KEEP
            {
                "id": "742-1295",
                "bracketTag": "R",
                "identity": "UB",
                "popularity": 140515,
                "manaNeeded": "{B}{U}{U}",
                "manaValueNeeded": 3,
                "description": "Win immediately.",
                "easyPrerequisites": "",
                "notablePrerequisites": "",
                "nonCardPrerequisiteCount": 0,
                "uses": [
                    {"card": {"name": "Demonic Consultation"}},
                    {"card": {"name": "Thassa's Oracle"}},
                ],
                "produces": [{"feature": {"name": "Win the game"}}],
                "requires": [],
                "legalities": {"commander": True, "vintage": True, "modern": False},
            },
            # E (Exhibition) -> universal; commander-legal -> KEEP
            {
                "id": "X-Y",
                "bracketTag": "E",
                "identity": "WUB",
                "popularity": 0,
                "manaValueNeeded": 5,
                "nonCardPrerequisiteCount": 1,  # has_extra_prerequisite True
                "uses": [
                    {"card": {"name": "Card One"}},
                    {"card": {"name": "Card Two"}},
                ],
                "produces": [],
                "requires": [{"template": {"name": "Any sac outlet"}}],
                "legalities": {"commander": True},
            },
            # B (Banned) -> banned bucket; NOT commander-legal -> DROP
            {
                "id": "BANNED-1",
                "bracketTag": "B",
                "identity": "R",
                "uses": [{"card": {"name": "A"}}, {"card": {"name": "B"}}],
                "legalities": {"commander": False},
            },
        ]

        payload = updater._build_combo_brackets_payload(rows, scraped_at="2026-05-17T00:00:00Z")

        self.assertEqual(payload["version"], "combo_brackets_v1.0")
        self.assertEqual(payload["scraped_at"], "2026-05-17T00:00:00Z")
        self.assertEqual(payload["total_variants"], 2)
        self.assertEqual(set(payload["by_variant_id"].keys()), {"742-1295", "X-Y"})

        thoracle = payload["by_variant_id"]["742-1295"]
        self.assertEqual(thoracle["category"], "early")
        self.assertEqual(thoracle["brackets_allowed"], ["B4", "B5"])
        self.assertEqual(thoracle["bracket_tag_raw"], "R")
        self.assertEqual(thoracle["card_names"], ["Demonic Consultation", "Thassa's Oracle"])
        self.assertEqual(thoracle["color_identity"], ["B", "U"])
        self.assertEqual(thoracle["combo_size"], 2)
        self.assertFalse(thoracle["has_extra_prerequisite"])
        self.assertEqual(thoracle["results"], ["Win the game"])
        self.assertEqual(thoracle["source_url"], "https://commanderspellbook.com/combo/742-1295")

        universal = payload["by_variant_id"]["X-Y"]
        self.assertEqual(universal["category"], "universal")
        self.assertEqual(universal["brackets_allowed"], ["B1", "B2", "B3", "B4", "B5"])
        self.assertTrue(universal["has_extra_prerequisite"])
        self.assertEqual(universal["requires_text"], ["Any sac outlet"])

    def test_combo_brackets_payload_raises_on_unknown_bracket_tag(self) -> None:
        # Halt criterion: if Spellbook adds a new tier (e.g. "Z"), we must
        # raise rather than silently misclassify.
        rows = [
            {
                "id": "NEW-TAG",
                "bracketTag": "Z",
                "identity": "W",
                "uses": [{"card": {"name": "A"}}, {"card": {"name": "B"}}],
                "legalities": {"commander": True},
            },
        ]
        with self.assertRaisesRegex(RuntimeError, "Unknown bracketTag"):
            updater._build_combo_brackets_payload(rows, scraped_at="2026-05-17T00:00:00Z")


if __name__ == "__main__":
    unittest.main()
