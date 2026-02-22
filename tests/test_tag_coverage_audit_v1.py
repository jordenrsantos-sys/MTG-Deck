from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from snapshot_build.tag_coverage_audit_v1 import VERSION, build_tag_coverage_audit_v1


class TagCoverageAuditV1Tests(unittest.TestCase):
    def _create_fixture_db(self, db_path: Path) -> None:
        con = sqlite3.connect(str(db_path))
        con.executescript(
            """
            CREATE TABLE snapshots (
              snapshot_id TEXT PRIMARY KEY,
              created_at TEXT,
              source TEXT,
              scryfall_bulk_updated_at TEXT,
              manifest_json TEXT
            );

            CREATE TABLE cards (
              snapshot_id TEXT NOT NULL,
              oracle_id TEXT NOT NULL,
              name TEXT NOT NULL,
              mana_cost TEXT,
              cmc REAL,
              type_line TEXT,
              oracle_text TEXT,
              colors TEXT,
              color_identity TEXT,
              produced_mana TEXT,
              keywords TEXT,
              legalities_json TEXT,
              primitives_json TEXT,
              PRIMARY KEY (snapshot_id, oracle_id)
            );

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
            );
            """
        )

        con.execute(
            "INSERT INTO snapshots (snapshot_id, created_at, source, scryfall_bulk_updated_at, manifest_json) VALUES (?, ?, ?, ?, ?)",
            (
                "snap_audit",
                "2026-02-22T00:00:00+00:00",
                "unit-test",
                None,
                "{}",
            ),
        )

        cards_rows = [
            (
                "snap_audit",
                "oid-a",
                "Arcane Signet",
                "{2}",
                2.0,
                "Artifact",
                "{T}: Add one mana of any color.",
                "[]",
                "[]",
                "[]",
                "[]",
                "{}",
                "[]",
            ),
            (
                "snap_audit",
                "oid-b",
                "Opt",
                "{U}",
                1.0,
                "Instant",
                "Scry 1, then draw a card.",
                "[\"U\"]",
                "[\"U\"]",
                "[]",
                "[]",
                "{}",
                "[]",
            ),
            (
                "snap_audit",
                "oid-c",
                "Bala Ged Recovery // Bala Ged Sanctuary",
                "{2}{G}",
                3.0,
                "Sorcery // Land",
                "Return target card from your graveyard to your hand.",
                "[\"G\"]",
                "[\"G\"]",
                "[]",
                "[]",
                "{}",
                "[]",
            ),
            (
                "snap_audit",
                "oid-d",
                "Brightclimb Pathway // Grimclimb Pathway",
                None,
                0.0,
                "Land // Land",
                "",
                "[]",
                "[\"W\", \"B\"]",
                "[]",
                "[]",
                "{}",
                "[]",
            ),
            (
                "snap_audit",
                "oid-e",
                "Elvish Mystic",
                "{G}",
                1.0,
                "Creature — Elf Druid",
                "{T}: Add {G}.",
                "[\"G\"]",
                "[\"G\"]",
                "[]",
                "[]",
                "{}",
                "[]",
            ),
            (
                "snap_audit",
                "oid-f",
                "Wrath of God",
                "{2}{W}{W}",
                4.0,
                "Sorcery",
                "Destroy all creatures. They can't be regenerated.",
                "[\"W\"]",
                "[\"W\"]",
                "[]",
                "[]",
                "{}",
                "[]",
            ),
        ]
        con.executemany(
            """
            INSERT INTO cards (
              snapshot_id,
              oracle_id,
              name,
              mana_cost,
              cmc,
              type_line,
              oracle_text,
              colors,
              color_identity,
              produced_mana,
              keywords,
              legalities_json,
              primitives_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            cards_rows,
        )

        tag_rows = [
            (
                "oid-a",
                "snap_audit",
                "taxonomy_test_v1",
                "taxonomy_test_v1",
                "[\"MANA_RAMP_ARTIFACT_ROCK\", \"MANA_RAMP_ARTIFACT_ROCK\"]",
                "[]",
                "{}",
                "[]",
                "2026-02-22T00:00:00+00:00",
            ),
            (
                "oid-c",
                "snap_audit",
                "taxonomy_test_v1",
                "taxonomy_test_v1",
                "[\"EXTRA_LAND_DROPS\"]",
                "[]",
                "{}",
                "[]",
                "2026-02-22T00:00:00+00:00",
            ),
            (
                "oid-e",
                "snap_audit",
                "taxonomy_test_v1",
                "taxonomy_test_v1",
                "[]",
                "[]",
                "{}",
                "[]",
                "2026-02-22T00:00:00+00:00",
            ),
            (
                "oid-f",
                "snap_audit",
                "taxonomy_test_v1",
                "taxonomy_test_v1",
                "[\"BOARDWIPE_CREATURES\"]",
                "[]",
                "{}",
                "[]",
                "2026-02-22T00:00:00+00:00",
            ),
        ]
        con.executemany(
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
            tag_rows,
        )

        con.commit()
        con.close()

    def test_audit_report_shape_counts_and_sorting(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "audit_fixture.sqlite"
            self._create_fixture_db(db_path)

            report = build_tag_coverage_audit_v1(
                snapshot_id="snap_audit",
                db_path=str(db_path),
            )

        self.assertEqual(report.get("version"), VERSION)
        self.assertEqual(report.get("snapshot_id"), "snap_audit")
        self.assertEqual(report.get("taxonomy_version"), "taxonomy_test_v1")

        self.assertEqual(int(report.get("total_cards") or 0), 6)
        self.assertEqual(int(report.get("cards_with_any_primitives") or 0), 3)
        self.assertEqual(int(report.get("cards_with_zero_primitives") or 0), 3)
        self.assertEqual(float(report.get("pct_with_any_primitives") or 0.0), 0.5)

        primitive_distribution = report.get("primitive_distribution")
        self.assertIsInstance(primitive_distribution, list)
        primitive_ids = [row.get("primitive_id") for row in primitive_distribution if isinstance(row, dict)]
        self.assertEqual(
            primitive_ids,
            [
                "BOARDWIPE_CREATURES",
                "EXTRA_LAND_DROPS",
                "MANA_RAMP_ARTIFACT_ROCK",
            ],
        )

        top_missing = report.get("top_missing") if isinstance(report.get("top_missing"), dict) else {}
        self.assertEqual(top_missing.get("strategy"), "cmc_and_color_identity")

        top_by_cmc = top_missing.get("top_200_missing_by_cmc") if isinstance(top_missing.get("top_200_missing_by_cmc"), list) else []
        self.assertGreaterEqual(len(top_by_cmc), 1)
        self.assertEqual(top_by_cmc[0].get("oracle_id"), "oid-d")

        color_buckets = (
            top_missing.get("top_200_missing_by_color_identity")
            if isinstance(top_missing.get("top_200_missing_by_color_identity"), list)
            else []
        )
        self.assertGreaterEqual(len(color_buckets), 1)
        self.assertEqual(color_buckets[0].get("color_identity_bucket"), "BW")

        dfc = report.get("dfc_coverage_sanity") if isinstance(report.get("dfc_coverage_sanity"), dict) else {}
        self.assertEqual(int(dfc.get("dfc_cards_total") or 0), 2)
        self.assertEqual(int(dfc.get("dfc_cards_tagged") or 0), 1)
        self.assertEqual(int(dfc.get("dfc_cards_untagged") or 0), 1)
        self.assertEqual(float(dfc.get("pct_dfc_with_any_primitives") or 0.0), 0.5)

    def test_audit_deterministic_repeat(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "audit_fixture.sqlite"
            self._create_fixture_db(db_path)

            first = build_tag_coverage_audit_v1(snapshot_id="snap_audit", db_path=str(db_path))
            second = build_tag_coverage_audit_v1(snapshot_id="snap_audit", db_path=str(db_path))

        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
