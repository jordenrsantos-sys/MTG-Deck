from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import api.engine.combos.two_card_combos_v2 as combos_v2


class TwoCardCombosV2Tests(unittest.TestCase):
    def test_loader_missing_file_raises_explicit_error_code(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            missing_path = Path(tmp_dir) / "two_card_combos_v2_missing.json"
            with patch.object(combos_v2, "_TWO_CARD_COMBOS_V2_FILE", missing_path):
                with self.assertRaises(RuntimeError) as raised:
                    combos_v2.load_two_card_combos_v2()

        self.assertTrue(str(raised.exception).startswith("TWO_CARD_COMBOS_V2_MISSING:"))

    def test_loader_normalizes_and_sorts_pairs_deterministically(self) -> None:
        payload = {
            "version": "two_card_combos_v2",
            "pairs": [
                {"a": "gamma", "b": "omega", "variant_ids": ["Z", "", "A", "A"]},
                {"a": "alpha", "b": "beta", "variant_ids": ["V2", "V1", "V1"]},
            ],
        }

        with TemporaryDirectory() as tmp_dir:
            payload_path = Path(tmp_dir) / "two_card_combos_v2.json"
            payload_path.write_text(json.dumps(payload), encoding="utf-8")

            with patch.object(combos_v2, "_TWO_CARD_COMBOS_V2_FILE", payload_path):
                first = combos_v2.load_two_card_combos_v2()
                second = combos_v2.load_two_card_combos_v2()

        self.assertEqual(first, second)
        self.assertEqual(first.get("version"), combos_v2.TWO_CARD_COMBOS_V2_VERSION)

        pairs = first.get("pairs") if isinstance(first.get("pairs"), list) else []
        self.assertEqual(
            pairs,
            [
                {"a": "alpha", "b": "beta", "variant_ids": ["V1", "V2"]},
                {"a": "gamma", "b": "omega", "variant_ids": ["A", "Z"]},
            ],
        )

    def test_derive_two_card_combos_from_variants_aggregates_variant_ids(self) -> None:
        variants_payload = {
            "version": "commander_spellbook_variants_v1",
            "source": "commander_spellbook_api",
            "generated_from": "/variants/",
            "variants": [
                {"variant_id": "V3", "cards": ["B", "A"]},
                {"variant_id": "V2", "cards": ["A", "B", "C"]},
                {"variant_id": "V1", "cards": ["a", "b"]},
                {"variant_id": "V4", "cards": ["x", "y"]},
                {"variant_id": "V5", "cards": ["x", "x"]},
            ],
        }

        payload = combos_v2.derive_two_card_combos_v2_from_variants(variants_payload)

        self.assertEqual(payload.get("version"), combos_v2.TWO_CARD_COMBOS_V2_VERSION)
        self.assertEqual(
            payload.get("pairs"),
            [
                {"a": "a", "b": "b", "variant_ids": ["V1", "V3"]},
                {"a": "x", "b": "y", "variant_ids": ["V4"]},
            ],
        )

    def test_load_prefer_v2_falls_back_to_v1_only_when_v2_missing(self) -> None:
        legacy_payload = {
            "version": "two_card_combos_v1",
            "mode": "pairs_only",
            "pairs": [
                {"id": "LEGACY_02", "a": "Card B", "b": "Card A"},
                {"id": "LEGACY_01", "a": "card a", "b": "card b"},
            ],
        }

        with (
            patch.object(combos_v2, "load_two_card_combos_v2", side_effect=RuntimeError("TWO_CARD_COMBOS_V2_MISSING: test")),
            patch.object(combos_v2, "load_two_card_combos_v1", return_value=legacy_payload),
        ):
            payload = combos_v2.load_two_card_combos_prefer_v2()

        self.assertEqual(payload.get("version"), combos_v2.TWO_CARD_COMBOS_V1_VERSION)
        self.assertEqual(
            payload.get("pairs"),
            [
                {
                    "a": "card a",
                    "b": "card b",
                    "variant_ids": ["LEGACY_01", "LEGACY_02"],
                }
            ],
        )

    def test_detect_unavailable_returns_supported_false_with_error_code(self) -> None:
        with patch.object(
            combos_v2,
            "load_two_card_combos_prefer_v2",
            side_effect=RuntimeError("TWO_CARD_COMBOS_V2_INVALID: test"),
        ):
            payload = combos_v2.detect_two_card_combos(["Alpha", "Beta"])

        self.assertEqual(payload.get("supported"), False)
        self.assertEqual(payload.get("version"), None)
        self.assertEqual(payload.get("count"), None)
        self.assertEqual(payload.get("matches"), [])
        self.assertEqual(payload.get("error_code"), "TWO_CARD_COMBOS_V2_INVALID")

    def test_detect_supports_mapping_input_and_bounds_matches(self) -> None:
        combos_payload = {
            "version": "two_card_combos_v2",
            "pairs": [
                {"a": "alpha", "b": "beta", "variant_ids": ["V2", "", "V1", "V1"]},
                {"a": "gamma", "b": "omega", "variant_ids": ["V9"]},
            ],
        }

        with patch.object(combos_v2, "load_two_card_combos_prefer_v2", return_value=combos_payload):
            payload = combos_v2.detect_two_card_combos(
                {
                    "S1": " Alpha ",
                    "S2": {"name": "BETA"},
                    "S3": {"oracle_id": "Gamma"},
                    "S4": "omega",
                    "S5": None,
                },
                max_matches=1,
            )

        self.assertEqual(payload.get("supported"), True)
        self.assertEqual(payload.get("version"), "two_card_combos_v2")
        self.assertEqual(payload.get("count"), 2)
        self.assertEqual(
            payload.get("matches"),
            [
                {"a": "alpha", "b": "beta", "variant_ids": ["V1", "V2"]},
            ],
        )

    def test_detect_negative_max_matches_clamps_to_zero(self) -> None:
        combos_payload = {
            "version": "two_card_combos_v2",
            "pairs": [
                {"a": "alpha", "b": "beta", "variant_ids": ["V1"]},
            ],
        }

        with patch.object(combos_v2, "load_two_card_combos_prefer_v2", return_value=combos_payload):
            payload = combos_v2.detect_two_card_combos(["alpha", "beta"], max_matches=-5)

        self.assertEqual(payload.get("supported"), True)
        self.assertEqual(payload.get("count"), 1)
        self.assertEqual(payload.get("matches"), [])


if __name__ == "__main__":
    unittest.main()
