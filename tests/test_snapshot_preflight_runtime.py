from __future__ import annotations

import sqlite3
import unittest
from unittest.mock import patch

from api.engine.deck_completion_v0 import generate_deck_completion_v0
from engine.db_tags import ensure_tag_tables


class DeckCompletionPreflightRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.con = sqlite3.connect(":memory:")
        self.con.row_factory = sqlite3.Row
        ensure_tag_tables(self.con)

    def tearDown(self) -> None:
        self.con.close()

    def test_preflight_runs_and_blocks_on_empty_compiled_tables(self) -> None:
        commander_card = {
            "name": "Test Commander",
            "oracle_id": "oid-test-commander",
            "type_line": "Legendary Creature — Test",
            "mana_cost": "{1}{U}",
            "color_identity": ["U"],
            "legalities": {"commander": "legal"},
            "primitives_json": "[]",
            "primitives": [],
        }

        with (
            patch("api.engine.deck_completion_v0.cards_db_connect", return_value=self.con),
            patch("api.engine.deck_completion_v0.find_card_by_name", return_value=commander_card),
            patch("api.engine.deck_completion_v0.resolve_runtime_taxonomy_version", return_value="taxonomy_v_test"),
            patch("api.engine.deck_completion_v0.resolve_runtime_ruleset_version", return_value="ruleset_v_test"),
        ):
            out = generate_deck_completion_v0(
                commander="Test Commander",
                anchors=[],
                profile_id="focused",
                bracket_id="B2",
                max_iters=1,
                target_deck_size=100,
                seed_package=None,
                validate_each_iter=False,
                db_snapshot_id="snap-test",
            )

        self.assertEqual(out.get("status"), "TAGS_NOT_COMPILED")
        unknowns = out.get("unknowns") if isinstance(out, dict) else []
        unknown = unknowns[0] if isinstance(unknowns, list) and unknowns else {}
        self.assertEqual(unknown.get("code"), "TAGS_NOT_COMPILED")
        self.assertIn("card_tags rows missing", str(unknown.get("reason") or ""))


if __name__ == "__main__":
    unittest.main()
