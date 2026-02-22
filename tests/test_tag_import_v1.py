from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from snapshot_build.tag_coverage_audit_v1 import build_tag_coverage_audit_v1
from snapshot_build.tag_import_v1 import VERSION, run_tag_import_v1
from taxonomy.pack_manifest import build_manifest, stable_json_dumps, write_manifest
from taxonomy.taxonomy_pack_v1 import build_taxonomy_pack_v1


def _write_taxonomy_pack(
    *,
    pack_dir: Path,
    taxonomy_version: str,
) -> Path:
    pack_dir.mkdir(parents=True, exist_ok=True)

    payloads = {
        "rulespec_rules.json": [],
        "rulespec_facets.json": [],
        "qa_rules.json": [],
        "primitives.json": [
            {
                "primitive_id": "MANA_RAMP_LAND_SEARCH",
                "category": "ENGINE",
                "description": "Land search and fetch style mana development",
                "engine_primitive": True,
            }
        ],
    }

    payloads["taxonomy_pack_v1.json"] = build_taxonomy_pack_v1(
        {
            "taxonomy_source_id": taxonomy_version,
            "tag_taxonomy_version": taxonomy_version,
            "generator_version": "test_tag_import_v1",
            "rulespec_rules": payloads["rulespec_rules.json"],
            "rulespec_facets": payloads["rulespec_facets.json"],
            "primitives": payloads["primitives.json"],
        }
    )

    for file_name, payload in payloads.items():
        (pack_dir / file_name).write_text(stable_json_dumps(payload), encoding="utf-8")

    manifest = build_manifest(
        taxonomy_version=taxonomy_version,
        pack_folder=pack_dir,
        file_names=payloads.keys(),
        generated_at="2026-02-22T00:00:00+00:00",
    )
    write_manifest(pack_folder=pack_dir, manifest=manifest)
    return pack_dir


class TagImportV1Tests(unittest.TestCase):
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
            """
        )

        con.execute(
            "INSERT INTO snapshots (snapshot_id, created_at, source, scryfall_bulk_updated_at, manifest_json) VALUES (?, ?, ?, ?, ?)",
            (
                "snap_import",
                "2026-02-22T00:00:00+00:00",
                "unit-test",
                None,
                stable_json_dumps({"tags_compiled": False}),
            ),
        )

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
            [
                (
                    "snap_import",
                    "oid_target",
                    "Prismatic Vista",
                    None,
                    0.0,
                    "Land",
                    "{T}, Pay 1 life, Sacrifice Prismatic Vista: Search your library for a basic land card.",
                    "[]",
                    "[]",
                    "[]",
                    "[]",
                    "{}",
                    "[]",
                ),
                (
                    "snap_import",
                    "oid_control",
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
            ],
        )

        con.commit()
        con.close()

    def test_import_adds_tag_and_coverage_not_worse(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "import_fixture.sqlite"
            self._create_fixture_db(db_path)

            taxonomy_pack_dir = _write_taxonomy_pack(
                pack_dir=tmp_path / "taxonomy_pack",
                taxonomy_version="taxonomy_unit_import_v1",
            )

            import_pack_path = tmp_path / "tag_import_pack.json"
            import_pack_path.write_text(
                stable_json_dumps(
                    {
                        "version": "tag_import_test_pass_v1",
                        "entries": [
                            {
                                "oracle_id": "oid_target",
                                "name": "Prismatic Vista",
                                "primitive_ids": ["MANA_RAMP_LAND_SEARCH"],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            before_report = build_tag_coverage_audit_v1(
                snapshot_id="snap_import",
                db_path=str(db_path),
            )

            summary = run_tag_import_v1(
                snapshot_id="snap_import",
                taxonomy_pack_folder=str(taxonomy_pack_dir),
                import_pack_path=str(import_pack_path),
                db_path=str(db_path),
                build_indices=False,
                dry_run=False,
            )

            after_report = build_tag_coverage_audit_v1(
                snapshot_id="snap_import",
                db_path=str(db_path),
                taxonomy_version="taxonomy_unit_import_v1",
            )

            con = sqlite3.connect(str(db_path))
            con.row_factory = sqlite3.Row
            row = con.execute(
                """
                SELECT primitive_ids_json
                FROM card_tags
                WHERE snapshot_id = ?
                  AND taxonomy_version = ?
                  AND oracle_id = ?
                LIMIT 1
                """,
                ("snap_import", "taxonomy_unit_import_v1", "oid_target"),
            ).fetchone()
            con.close()

        self.assertEqual(summary.get("version"), VERSION)
        self.assertEqual(int(summary.get("patch_rows_planned") or 0), 1)

        compile_summary = summary.get("compile_summary") if isinstance(summary.get("compile_summary"), dict) else {}
        self.assertEqual(int(compile_summary.get("patches_written") or 0), 1)

        self.assertIsNotNone(row)
        primitives = json.loads(row["primitive_ids_json"]) if isinstance(row["primitive_ids_json"], str) else []
        self.assertIn("MANA_RAMP_LAND_SEARCH", primitives)

        before_with_any = int(before_report.get("cards_with_any_primitives") or 0)
        after_with_any = int(after_report.get("cards_with_any_primitives") or 0)
        self.assertGreaterEqual(after_with_any, before_with_any)

    def test_dry_run_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "import_fixture.sqlite"
            self._create_fixture_db(db_path)

            taxonomy_pack_dir = _write_taxonomy_pack(
                pack_dir=tmp_path / "taxonomy_pack",
                taxonomy_version="taxonomy_unit_import_v1",
            )

            import_pack_path = tmp_path / "tag_import_pack.json"
            import_pack_path.write_text(
                stable_json_dumps(
                    {
                        "version": "tag_import_test_pass_v1",
                        "entries": [
                            {
                                "oracle_id": "oid_target",
                                "name": "Prismatic Vista",
                                "primitive_ids": ["MANA_RAMP_LAND_SEARCH"],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            first = run_tag_import_v1(
                snapshot_id="snap_import",
                taxonomy_pack_folder=str(taxonomy_pack_dir),
                import_pack_path=str(import_pack_path),
                db_path=str(db_path),
                dry_run=True,
            )
            second = run_tag_import_v1(
                snapshot_id="snap_import",
                taxonomy_pack_folder=str(taxonomy_pack_dir),
                import_pack_path=str(import_pack_path),
                db_path=str(db_path),
                dry_run=True,
            )

        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
