from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import api.engine.two_card_combos as combos


class TwoCardCombosV1Tests(unittest.TestCase):
    def test_loader_missing_file_raises_deterministic_runtime_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            missing_path = Path(tmp_dir) / "two_card_combos_v1_missing.json"
            with patch.object(combos, "_TWO_CARD_COMBOS_FILE", missing_path):
                with self.assertRaises(RuntimeError) as ctx:
                    combos.load_two_card_combos_v1()

        self.assertTrue(str(ctx.exception).startswith("TWO_CARD_COMBOS_V1_MISSING:"))

    def test_loader_maps_curated_manifest_errors_to_missing_code(self) -> None:
        with patch.object(
            combos,
            "resolve_pack_file_path",
            side_effect=RuntimeError("CURATED_PACK_MANIFEST_V1_MISSING: test"),
        ):
            with self.assertRaises(RuntimeError) as raised:
                combos.load_two_card_combos_v1()

        self.assertTrue(str(raised.exception).startswith("TWO_CARD_COMBOS_V1_MISSING:"))

    def test_detect_two_card_combos_matches_when_both_cards_present(self) -> None:
        payload = combos.detect_two_card_combos(["Combo Piece A", "Combo Piece B"])

        self.assertEqual(payload.get("version"), "two_card_combos_v1")
        self.assertEqual(payload.get("count"), 1)
        self.assertEqual(
            payload.get("matches"),
            [
                {
                    "id": "TCC_0001",
                    "a": "combo piece a",
                    "b": "combo piece b",
                }
            ],
        )

    def test_detect_two_card_combos_returns_zero_when_partial_pair_present(self) -> None:
        payload = combos.detect_two_card_combos(["Combo Piece A"])

        self.assertEqual(payload.get("version"), "two_card_combos_v1")
        self.assertEqual(payload.get("count"), 0)
        self.assertEqual(payload.get("matches"), [])

    def test_detect_two_card_combos_is_stable_for_multiple_matches(self) -> None:
        deck_cards = [
            "  COMBO PIECE A ",
            "combo piece b",
            "Missing Card B",
            "missing card a",
            "omega piece a",
            "OMEGA PIECE B",
            "combo piece a",
        ]

        first = combos.detect_two_card_combos(deck_cards)
        second = combos.detect_two_card_combos(deck_cards)

        expected_matches = [
            {"id": "TCC_0001", "a": "combo piece a", "b": "combo piece b"},
            {"id": "TCC_0002", "a": "missing card a", "b": "missing card b"},
            {"id": "TCC_0003", "a": "omega piece a", "b": "omega piece b"},
        ]

        self.assertEqual(first.get("count"), 3)
        self.assertEqual(first.get("matches"), expected_matches)
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
