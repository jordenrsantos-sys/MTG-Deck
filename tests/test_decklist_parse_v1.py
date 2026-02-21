from __future__ import annotations

import json
import unittest

from api.engine.decklist_parse_v1 import DECKLIST_PARSE_VERSION, parse_decklist_text


class DecklistParseV1Tests(unittest.TestCase):
    def test_parse_supports_counts_defaults_comments_and_headers(self) -> None:
        raw = """
# comment line
Commander:
1 Krenko, Mob Boss
Deck
2x Sol Ring
Arcane   Signet
// another comment
Sideboard
1 Lightning Bolt
"""

        payload = parse_decklist_text(raw)

        self.assertEqual(payload.get("version"), DECKLIST_PARSE_VERSION)
        items = payload.get("items") if isinstance(payload.get("items"), list) else []
        self.assertEqual(
            items,
            [
                {
                    "count": 1,
                    "name_raw": "Krenko, Mob Boss",
                    "name_norm": "krenko, mob boss",
                    "line_no": 4,
                    "section": "commander",
                },
                {
                    "count": 2,
                    "name_raw": "Sol Ring",
                    "name_norm": "sol ring",
                    "line_no": 6,
                    "section": "mainboard",
                },
                {
                    "count": 1,
                    "name_raw": "Arcane   Signet",
                    "name_norm": "arcane signet",
                    "line_no": 7,
                    "section": "mainboard",
                },
                {
                    "count": 1,
                    "name_raw": "Lightning Bolt",
                    "name_norm": "lightning bolt",
                    "line_no": 10,
                    "section": "sideboard",
                },
            ],
        )

        totals = payload.get("totals") if isinstance(payload.get("totals"), dict) else {}
        self.assertEqual(totals.get("items_total"), 4)
        self.assertEqual(totals.get("card_count_total"), 5)
        self.assertEqual(totals.get("ignored_line_total"), 6)

    def test_parse_is_deterministic_for_same_input(self) -> None:
        raw = "Commander\nKrenko, Mob Boss\nDeck\n2x Sol Ring\nArcane Signet\n"

        first = parse_decklist_text(raw)
        second = parse_decklist_text(raw)

        self.assertEqual(first, second)

        first_json = json.dumps(first, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        second_json = json.dumps(second, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        self.assertEqual(first_json, second_json)

        normalized_sha = first.get("normalized_sha256")
        self.assertIsInstance(normalized_sha, str)
        self.assertEqual(len(normalized_sha), 64)


if __name__ == "__main__":
    unittest.main()
