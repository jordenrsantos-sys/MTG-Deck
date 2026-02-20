from __future__ import annotations

import sqlite3
import unittest
from unittest.mock import patch

from engine.db_tags import TagSnapshotMissingError, bulk_get_card_tags, ensure_tag_tables, get_deck_tag_count
from snapshot_build.tag_snapshot import get_tag_status, get_unknowns_report


class DbTagsRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.con = sqlite3.connect(":memory:")
        self.con.row_factory = sqlite3.Row
        ensure_tag_tables(self.con)

    def tearDown(self) -> None:
        self.con.close()

    def _insert_card_tag(
        self,
        oracle_id: str,
        snapshot_id: str,
        taxonomy_version: str,
        ruleset_version: str,
        primitive_ids_json: str,
        equiv_class_ids_json: str,
        facets_json: str,
        evidence_json: str,
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
                equiv_class_ids_json,
                facets_json,
                evidence_json,
                "2026-01-01T00:00:00+00:00",
            ),
        )
        self.con.commit()

    def test_bulk_get_card_tags_returns_all_requested(self) -> None:
        self._insert_card_tag(
            oracle_id="oid-1",
            snapshot_id="snap-a",
            taxonomy_version="taxonomy_v1",
            ruleset_version="taxonomy_v1",
            primitive_ids_json='["RAMP_MANA"]',
            equiv_class_ids_json='["EQ_1"]',
            facets_json='{"role":["ramp"]}',
            evidence_json='[{"rule_id":"R1"}]',
        )
        self._insert_card_tag(
            oracle_id="oid-2",
            snapshot_id="snap-a",
            taxonomy_version="taxonomy_v1",
            ruleset_version="taxonomy_v1",
            primitive_ids_json='["CARD_DRAW"]',
            equiv_class_ids_json='["EQ_2"]',
            facets_json='{"role":["draw"]}',
            evidence_json='[{"rule_id":"R2"}]',
        )

        payload = bulk_get_card_tags(
            conn=self.con,
            oracle_ids=["oid-2", "oid-1"],
            snapshot_id="snap-a",
            taxonomy_version="taxonomy_v1",
        )

        self.assertEqual(sorted(payload.keys()), ["oid-1", "oid-2"])
        self.assertEqual(payload["oid-1"]["primitive_ids"], ["RAMP_MANA"])
        self.assertEqual(payload["oid-1"]["equiv_ids"], ["EQ_1"])
        self.assertIsInstance(payload["oid-1"]["facets"], dict)
        self.assertIsInstance(payload["oid-1"]["evidence"], dict)

    def test_bulk_get_card_tags_raises_when_missing_oracle_id(self) -> None:
        self._insert_card_tag(
            oracle_id="oid-present",
            snapshot_id="snap-b",
            taxonomy_version="taxonomy_v1",
            ruleset_version="taxonomy_v1",
            primitive_ids_json='["REMOVAL_SINGLE"]',
            equiv_class_ids_json='["EQ_3"]',
            facets_json='{}',
            evidence_json='[]',
        )

        with self.assertRaises(TagSnapshotMissingError) as err:
            bulk_get_card_tags(
                conn=self.con,
                oracle_ids=["oid-present", "oid-missing"],
                snapshot_id="snap-b",
                taxonomy_version="taxonomy_v1",
            )

        self.assertEqual(err.exception.snapshot_id, "snap-b")
        self.assertEqual(err.exception.taxonomy_version, "taxonomy_v1")
        self.assertIn("oid-missing", err.exception.missing_oracle_ids)

    def test_get_deck_tag_count_counts_distinct_slots_with_tag(self) -> None:
        primitive_index_by_slot = {
            "S0": ["mass_land_denial", "RAMP_MANA"],
            "S1": ["CARD_DRAW"],
            "S2": ["mass_land_denial"],
            "S3": "invalid",
        }
        deck_slot_ids = ["S2", "S0", "S0", "S1", "", "S3"]

        count = get_deck_tag_count(
            primitive_index_by_slot=primitive_index_by_slot,
            deck_slot_ids=deck_slot_ids,
            tag_id="mass_land_denial",
        )

        self.assertEqual(count, 2)


class TagSnapshotReportingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.con = sqlite3.connect(":memory:")
        self.con.row_factory = sqlite3.Row
        self.con.execute(
            """
            CREATE TABLE card_tags (
              oracle_id TEXT NOT NULL,
              snapshot_id TEXT NOT NULL,
              taxonomy_version TEXT NOT NULL,
              ruleset_version TEXT NOT NULL,
              primitive_ids_json TEXT NOT NULL,
              equiv_class_ids_json TEXT NOT NULL,
              facets_json TEXT NOT NULL,
              evidence_json TEXT NOT NULL,
              created_at TEXT NOT NULL,
              PRIMARY KEY (oracle_id, snapshot_id, taxonomy_version)
            )
            """
        )
        self.con.execute(
            """
            CREATE TABLE unknowns_queue (
              id INTEGER PRIMARY KEY,
              oracle_id TEXT,
              snapshot_id TEXT,
              taxonomy_version TEXT,
              rule_id TEXT,
              reason TEXT,
              snippet TEXT,
              created_at TEXT
            )
            """
        )
        self.con.commit()

    def tearDown(self) -> None:
        self.con.close()

    def test_status_and_unknowns_report_run(self) -> None:
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
                "oid-10",
                "snap-report",
                "taxonomy_v_report",
                "taxonomy_v_report",
                '["RAMP_MANA"]',
                '["EQ_A"]',
                '{}',
                '[]',
                "2026-01-01T00:00:00+00:00",
            ),
        )
        self.con.executemany(
            """
            INSERT INTO unknowns_queue (
              oracle_id,
              snapshot_id,
              taxonomy_version,
              rule_id,
              reason,
              snippet,
              created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "oid-10",
                    "snap-report",
                    "taxonomy_v_report",
                    "R_X",
                    "MATCH_WITHOUT_PRIMITIVE",
                    "draw a card",
                    "2026-01-01T00:00:00+00:00",
                ),
                (
                    "oid-11",
                    "snap-report",
                    "taxonomy_v_report",
                    "R_X",
                    "MATCH_WITHOUT_PRIMITIVE",
                    "draw two cards",
                    "2026-01-01T00:00:01+00:00",
                ),
            ],
        )
        self.con.commit()

        with patch("snapshot_build.tag_snapshot.connect", return_value=self.con):
            status = get_tag_status(
                snapshot_id="snap-report",
                taxonomy_version="taxonomy_v_report",
                taxonomy_pack_folder=None,
            )
            report = get_unknowns_report(
                snapshot_id="snap-report",
                taxonomy_version="taxonomy_v_report",
            )

        self.assertTrue(status["tags_exist"])
        self.assertEqual(status["card_tags_count"], 1)
        self.assertEqual(status["unknowns_count"], 2)
        self.assertEqual(report["total_unknowns"], 2)
        self.assertGreaterEqual(len(report["top_rules"]), 1)


if __name__ == "__main__":
    unittest.main()
