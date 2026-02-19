from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch
from xml.sax.saxutils import escape

from snapshot_build.tag_snapshot import compile_snapshot_tags
from taxonomy.exporter import export_workbook_to_pack
from taxonomy.loader import load
from taxonomy.pack_manifest import build_manifest, stable_json_dumps, write_manifest

MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"


def _excel_col_name(index_1_based: int) -> str:
    n = int(index_1_based)
    out = []
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out.append(chr(ord("A") + rem))
    return "".join(reversed(out))


def _worksheet_xml(rows: list[list[object]]) -> str:
    row_nodes: list[str] = []
    for row_idx, row in enumerate(rows, start=1):
        cell_nodes: list[str] = []
        for col_idx, value in enumerate(row, start=1):
            if value is None:
                continue
            cell_ref = f"{_excel_col_name(col_idx)}{row_idx}"
            cell_text = escape(str(value))
            cell_nodes.append(
                f'<c r="{cell_ref}" t="inlineStr"><is><t>{cell_text}</t></is></c>'
            )
        row_nodes.append(f'<row r="{row_idx}">{"".join(cell_nodes)}</row>')

    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<worksheet xmlns="{MAIN_NS}"><sheetData>'
        f'{"".join(row_nodes)}'
        "</sheetData></worksheet>"
    )


def _write_minimal_xlsx(workbook_path: Path, sheets: dict[str, list[list[object]]]) -> None:
    workbook_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(workbook_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        workbook_sheet_nodes: list[str] = []
        rel_nodes: list[str] = []

        for idx, (sheet_name, rows) in enumerate(sheets.items(), start=1):
            rel_id = f"rId{idx}"
            target = f"worksheets/sheet{idx}.xml"
            workbook_sheet_nodes.append(
                f'<sheet name="{escape(sheet_name)}" sheetId="{idx}" r:id="{rel_id}"/>'
            )
            rel_nodes.append(
                f'<Relationship Id="{rel_id}" '
                'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
                f'Target="{target}"/>'
            )
            zf.writestr(f"xl/{target}", _worksheet_xml(rows))

        workbook_xml = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            f'<workbook xmlns="{MAIN_NS}" xmlns:r="{REL_NS}">'  # noqa: E501
            f'<sheets>{"".join(workbook_sheet_nodes)}</sheets>'
            "</workbook>"
        )
        rels_xml = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            f'<Relationships xmlns="{PKG_REL_NS}">{"".join(rel_nodes)}</Relationships>'
        )

        zf.writestr("xl/workbook.xml", workbook_xml)
        zf.writestr("xl/_rels/workbook.xml.rels", rels_xml)


def _write_taxonomy_pack(
    pack_dir: Path,
    taxonomy_version: str,
    rulespec_rules: list[dict[str, object]],
    rulespec_facets: list[dict[str, object]] | None = None,
    qa_rules: list[dict[str, object]] | None = None,
) -> Path:
    pack_dir.mkdir(parents=True, exist_ok=True)
    payloads = {
        "rulespec_rules.json": rulespec_rules,
        "rulespec_facets.json": rulespec_facets or [],
        "qa_rules.json": qa_rules or [],
    }

    for file_name, payload in payloads.items():
        (pack_dir / file_name).write_text(stable_json_dumps(payload), encoding="utf-8")

    manifest = build_manifest(
        taxonomy_version=taxonomy_version,
        pack_folder=pack_dir,
        file_names=payloads.keys(),
    )
    write_manifest(pack_folder=pack_dir, manifest=manifest)
    return pack_dir


class TaxonomyExporterLoaderTests(unittest.TestCase):
    def test_exporter_creates_pack_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            workbook_path = tmp_path / "MTG_Tag_Taxonomy_v1_23_full.xlsx"
            out_dir = tmp_path / "packs"

            _write_minimal_xlsx(
                workbook_path,
                {
                    "Rulespec Rules": [
                        ["rule_id", "primitive_id", "pattern", "field", "rule_type", "priority"],
                        ["R1", "TOKEN_PRODUCTION", "create", "oracle_text", "substring", "1"],
                    ],
                    "Rulespec Facets": [
                        ["facet_key", "facet_value"],
                        ["strategy", "tokens"],
                    ],
                    "QA Rules": [
                        ["rule_id", "note"],
                        ["Q1", "smoke"],
                    ],
                },
            )

            pack_dir = export_workbook_to_pack(
                workbook_path=workbook_path,
                out_dir=out_dir,
                taxonomy_version="taxonomy_test_v1",
            )

            self.assertTrue(pack_dir.exists())
            self.assertTrue((pack_dir / "rulespec_rules.json").exists())
            self.assertTrue((pack_dir / "rulespec_facets.json").exists())
            self.assertTrue((pack_dir / "qa_rules.json").exists())
            self.assertTrue((pack_dir / "pack_manifest.json").exists())

            manifest_obj = json.loads((pack_dir / "pack_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest_obj.get("taxonomy_version"), "taxonomy_test_v1")
            manifest_files = {entry.get("file_name") for entry in manifest_obj.get("files", [])}
            self.assertIn("rulespec_rules.json", manifest_files)
            self.assertIn("rulespec_facets.json", manifest_files)
            self.assertIn("qa_rules.json", manifest_files)

    def test_loader_rejects_manifest_hash_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            workbook_path = tmp_path / "MTG_Tag_Taxonomy_v1_23_full.xlsx"
            out_dir = tmp_path / "packs"

            _write_minimal_xlsx(
                workbook_path,
                {
                    "Rulespec Rules": [
                        ["rule_id", "primitive_id", "pattern"],
                        ["R1", "TOKEN_PRODUCTION", "create"],
                    ],
                    "Rulespec Facets": [["facet_key", "facet_value"]],
                    "QA Rules": [["rule_id", "note"]],
                },
            )

            pack_dir = export_workbook_to_pack(
                workbook_path=workbook_path,
                out_dir=out_dir,
                taxonomy_version="taxonomy_test_v2",
            )

            rules_path = pack_dir / "rulespec_rules.json"
            rules_path.write_text(rules_path.read_text(encoding="utf-8") + " ", encoding="utf-8")

            with self.assertRaises(ValueError) as err:
                load(pack_dir)

            self.assertIn("Manifest", str(err.exception))


class SnapshotCompilerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.con = sqlite3.connect(":memory:")
        self.con.row_factory = sqlite3.Row
        self.con.execute(
            """
            CREATE TABLE snapshots (
              snapshot_id TEXT PRIMARY KEY,
              created_at TEXT,
              source TEXT,
              scryfall_bulk_updated_at TEXT
            )
            """
        )
        self.con.execute(
            """
            CREATE TABLE cards (
              oracle_id TEXT,
              name TEXT,
              type_line TEXT,
              oracle_text TEXT,
              snapshot_id TEXT
            )
            """
        )
        self.con.commit()

    def tearDown(self) -> None:
        self.con.close()

    def _insert_card(
        self,
        snapshot_id: str,
        oracle_id: str,
        name: str,
        type_line: str,
        oracle_text: str,
    ) -> None:
        self.con.execute(
            "INSERT OR IGNORE INTO snapshots (snapshot_id, created_at, source, scryfall_bulk_updated_at) VALUES (?, ?, ?, ?)",
            (snapshot_id, "2026-01-01T00:00:00+00:00", "unit-test", None),
        )
        self.con.execute(
            "INSERT INTO cards (oracle_id, name, type_line, oracle_text, snapshot_id) VALUES (?, ?, ?, ?, ?)",
            (oracle_id, name, type_line, oracle_text, snapshot_id),
        )
        self.con.commit()

    def test_compile_snapshot_writes_card_tags(self) -> None:
        snapshot_id = "snap_tags"
        self._insert_card(
            snapshot_id=snapshot_id,
            oracle_id="oid-1",
            name="Goblin Rally",
            type_line="Sorcery",
            oracle_text="Create two 1/1 Goblin creature tokens.",
        )

        with tempfile.TemporaryDirectory() as tmp:
            pack_dir = _write_taxonomy_pack(
                pack_dir=Path(tmp) / "taxonomy_unit_v1",
                taxonomy_version="taxonomy_unit_v1",
                rulespec_rules=[
                    {
                        "rule_id": "R_TOKEN",
                        "primitive_id": "TOKEN_PRODUCTION",
                        "pattern": "create two 1/1 goblin creature tokens",
                        "field": "oracle_text",
                        "rule_type": "substring",
                        "priority": 1,
                    }
                ],
            )

            with patch("snapshot_build.tag_snapshot.connect", return_value=self.con), patch(
                "snapshot_build.tag_snapshot.snapshot_exists", return_value=True
            ):
                summary = compile_snapshot_tags(
                    snapshot_id=snapshot_id,
                    taxonomy_pack_folder=str(pack_dir),
                )

        self.assertEqual(summary["card_tags_written"], 1)
        self.assertEqual(summary["unknowns_written"], 0)

        row = self.con.execute(
            "SELECT primitive_ids_json, facets_json FROM card_tags WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(json.loads(row["primitive_ids_json"]), ["TOKEN_PRODUCTION"])
        self.assertEqual(
            json.loads(row["facets_json"]),
            {
                "commander_eligible": ["false"],
                "is_creature": ["false"],
                "is_legendary": ["false"],
                "is_legendary_creature": ["false"],
            },
        )

    def test_compile_snapshot_routes_unknown_match_without_primitive(self) -> None:
        snapshot_id = "snap_unknown"
        self._insert_card(
            snapshot_id=snapshot_id,
            oracle_id="oid-2",
            name="Mystery Draw",
            type_line="Instant",
            oracle_text="Draw a card.",
        )

        with tempfile.TemporaryDirectory() as tmp:
            pack_dir = _write_taxonomy_pack(
                pack_dir=Path(tmp) / "taxonomy_unit_v2",
                taxonomy_version="taxonomy_unit_v2",
                rulespec_rules=[
                    {
                        "rule_id": "R_UNKNOWN",
                        "pattern": "draw a card",
                        "field": "oracle_text",
                        "rule_type": "substring",
                        "priority": 1,
                    }
                ],
            )

            with patch("snapshot_build.tag_snapshot.connect", return_value=self.con), patch(
                "snapshot_build.tag_snapshot.snapshot_exists", return_value=True
            ):
                summary = compile_snapshot_tags(
                    snapshot_id=snapshot_id,
                    taxonomy_pack_folder=str(pack_dir),
                )

        self.assertEqual(summary["card_tags_written"], 1)
        self.assertEqual(summary["unknowns_written"], 1)

        tag_row = self.con.execute(
            "SELECT primitive_ids_json, facets_json, evidence_json FROM card_tags WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()
        self.assertIsNotNone(tag_row)
        self.assertEqual(json.loads(tag_row["primitive_ids_json"]), [])
        self.assertEqual(
            json.loads(tag_row["facets_json"]),
            {
                "commander_eligible": ["false"],
                "is_creature": ["false"],
                "is_legendary": ["false"],
                "is_legendary_creature": ["false"],
            },
        )
        self.assertEqual(
            json.loads(tag_row["evidence_json"]),
            [
                {
                    "field": "oracle_text",
                    "rule_id": "R_UNKNOWN",
                    "snippet": "Draw a card",
                    "span": [0, 11],
                }
            ],
        )

        unknown_row = self.con.execute(
            "SELECT reason, rule_id FROM unknowns_queue WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()
        self.assertIsNotNone(unknown_row)
        self.assertEqual(unknown_row["reason"], "MATCH_WITHOUT_PRIMITIVE")
        self.assertEqual(unknown_row["rule_id"], "R_UNKNOWN")


if __name__ == "__main__":
    unittest.main()
