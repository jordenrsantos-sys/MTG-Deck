from __future__ import annotations

import sqlite3
import unittest
from unittest.mock import patch

from api.engine.constants import MIN_PRIMITIVE_COVERAGE, MIN_PRIMITIVE_TO_CARDS
from api.engine.deck_completion_v0 import generate_deck_completion_v0
from api.engine.snapshot_preflight_v1 import SnapshotPreflightError, run_snapshot_preflight
from engine.db_tags import ensure_tag_tables


class DeckCompletionPreflightRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.con = sqlite3.connect(":memory:")
        self.con.row_factory = sqlite3.Row
        ensure_tag_tables(self.con)

    def tearDown(self) -> None:
        self.con.close()

    def _insert_card_tag(
        self,
        *,
        oracle_id: str,
        snapshot_id: str,
        taxonomy_version: str,
        ruleset_version: str,
        primitive_ids_json: str,
        facets_json: str,
    ) -> None:
        self.con.execute(
            """
            INSERT INTO card_tags (
              oracle_id,
              snapshot_id,
              taxonomy_version,
              ruleset_version,
              primitive_ids_json,
              equiv_class_ids_json,
              facets_json,
              evidence_json,
              created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                oracle_id,
                snapshot_id,
                taxonomy_version,
                ruleset_version,
                primitive_ids_json,
                "[]",
                facets_json,
                "[]",
                "2026-01-01T00:00:00+00:00",
            ),
        )

    def _ensure_primitive_to_cards_table(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS primitive_to_cards (
              primitive_id TEXT NOT NULL,
              oracle_id TEXT NOT NULL,
              snapshot_id TEXT NOT NULL,
              taxonomy_version TEXT NOT NULL
            )
            """
        )

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

    def test_preflight_rejects_stale_partial_compile_by_thresholds(self) -> None:
        snapshot_id = "snap-partial"
        taxonomy_version = "taxonomy_v_partial"
        ruleset_version = "ruleset_v_partial"
        commander_oracle_id = "oid-commander"
        total_cards = 100
        cards_with_any_primitive = 10

        self._ensure_primitive_to_cards_table()

        for idx in range(total_cards):
            oracle_id = commander_oracle_id if idx == 0 else f"oid-{idx:03d}"
            primitive_ids_json = '["RAMP_MANA"]' if idx < cards_with_any_primitive else "[]"
            facets_json = '{"commander_eligible":["true"]}' if idx == 0 else '{"is_creature":["false"]}'
            self._insert_card_tag(
                oracle_id=oracle_id,
                snapshot_id=snapshot_id,
                taxonomy_version=taxonomy_version,
                ruleset_version=ruleset_version,
                primitive_ids_json=primitive_ids_json,
                facets_json=facets_json,
            )

        primitive_to_cards_rows = max(1, MIN_PRIMITIVE_TO_CARDS - 1)
        primitive_to_cards_payload = [
            (f"P{idx % 5}", f"oid-ptc-{idx:05d}", snapshot_id, taxonomy_version)
            for idx in range(primitive_to_cards_rows)
        ]
        self.con.executemany(
            """
            INSERT INTO primitive_to_cards (primitive_id, oracle_id, snapshot_id, taxonomy_version)
            VALUES (?, ?, ?, ?)
            """,
            primitive_to_cards_payload,
        )
        self.con.commit()

        with self.assertRaises(SnapshotPreflightError) as err:
            run_snapshot_preflight(
                db=self.con,
                db_snapshot_id=snapshot_id,
                taxonomy_version=taxonomy_version,
                ruleset_version=ruleset_version,
                commander_oracle_id=commander_oracle_id,
            )

        report = err.exception.to_unknown()
        self.assertEqual(report.get("status"), "TAGS_NOT_COMPILED")

        failures = report.get("failures") if isinstance(report.get("failures"), list) else []
        self.assertTrue(
            any("primitive_to_cards rows below stale-compilation threshold" in str(f) for f in failures)
        )
        self.assertTrue(
            any("cards_with_any_primitive_rate below stale-compilation threshold" in str(f) for f in failures)
        )

        # This fixture would pass the old checks (rows/facets/commander/table all present).
        self.assertFalse(any("card_tags rows missing" in str(f) for f in failures))
        self.assertFalse(any("facets_json appears empty" in str(f) for f in failures))
        self.assertFalse(any("commander oracle_id missing" in str(f) for f in failures))
        self.assertFalse(any("commander card_tags facets_json is empty" in str(f) for f in failures))
        self.assertFalse(any("primitive_to_cards table is missing" in str(f) for f in failures))
        self.assertFalse(any("primitive_to_cards rows missing" in str(f) for f in failures))

        counts = report.get("counts") if isinstance(report.get("counts"), dict) else {}
        rates = report.get("rates") if isinstance(report.get("rates"), dict) else {}
        thresholds = report.get("thresholds") if isinstance(report.get("thresholds"), dict) else {}

        self.assertEqual(counts.get("card_tags_rows_snapshot_taxonomy_ruleset"), total_cards)
        self.assertEqual(
            counts.get("cards_with_any_primitive_rows_snapshot_taxonomy_ruleset"),
            cards_with_any_primitive,
        )
        self.assertEqual(counts.get("primitive_to_cards_rows_snapshot_taxonomy"), primitive_to_cards_rows)
        self.assertEqual(rates.get("cards_with_any_primitive_rate"), 0.1)
        self.assertEqual(thresholds.get("min_primitive_to_cards"), MIN_PRIMITIVE_TO_CARDS)
        self.assertEqual(thresholds.get("min_primitive_coverage"), MIN_PRIMITIVE_COVERAGE)


if __name__ == "__main__":
    unittest.main()
