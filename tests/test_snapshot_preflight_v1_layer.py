from __future__ import annotations

import sqlite3
import unittest

from api.engine.layers.snapshot_preflight_v1 import SNAPSHOT_PREFLIGHT_V1_VERSION, run_snapshot_preflight_v1


class SnapshotPreflightV1LayerTests(unittest.TestCase):
    def _create_snapshots_table(self, con: sqlite3.Connection) -> None:
        con.execute(
            """
            CREATE TABLE snapshots (
              snapshot_id TEXT PRIMARY KEY,
              created_at TEXT NOT NULL,
              source TEXT NOT NULL,
              scryfall_bulk_uri TEXT NOT NULL,
              scryfall_bulk_updated_at TEXT,
              manifest_json TEXT NOT NULL
            )
            """
        )

    def _insert_snapshot(self, con: sqlite3.Connection, snapshot_id: str, manifest_json: str) -> None:
        con.execute(
            """
            INSERT INTO snapshots (
              snapshot_id,
              created_at,
              source,
              scryfall_bulk_uri,
              scryfall_bulk_updated_at,
              manifest_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_id,
                "2026-01-01T00:00:00+00:00",
                "unit-test",
                "local://unit-test/scryfall/default_cards",
                "2026-01-01T00:00:00+00:00",
                manifest_json,
            ),
        )

    def test_preflight_ok_with_manifest_and_tags_compiled_true(self) -> None:
        con = sqlite3.connect(":memory:")
        con.row_factory = sqlite3.Row
        try:
            self._create_snapshots_table(con)
            self._insert_snapshot(con, snapshot_id="TEST_SNAPSHOT_0001", manifest_json='{"tags_compiled": true}')
            con.commit()

            payload = run_snapshot_preflight_v1(db=con, snapshot_id="TEST_SNAPSHOT_0001")
        finally:
            con.close()

        self.assertEqual(payload.get("version"), SNAPSHOT_PREFLIGHT_V1_VERSION)
        self.assertEqual(payload.get("snapshot_id"), "TEST_SNAPSHOT_0001")
        self.assertEqual(payload.get("status"), "OK")
        self.assertEqual(payload.get("errors"), [])

        checks = payload.get("checks") if isinstance(payload.get("checks"), dict) else {}
        self.assertEqual(
            checks,
            {
                "snapshot_exists": True,
                "manifest_present": True,
                "tags_compiled": True,
                "schema_ok": True,
            },
        )

    def test_preflight_error_when_manifest_missing(self) -> None:
        con = sqlite3.connect(":memory:")
        con.row_factory = sqlite3.Row
        try:
            self._create_snapshots_table(con)
            self._insert_snapshot(con, snapshot_id="TEST_SNAPSHOT_0001", manifest_json="")
            con.commit()

            payload = run_snapshot_preflight_v1(db=con, snapshot_id="TEST_SNAPSHOT_0001")
        finally:
            con.close()

        self.assertEqual(payload.get("status"), "ERROR")
        checks = payload.get("checks") if isinstance(payload.get("checks"), dict) else {}
        self.assertIs(checks.get("manifest_present"), False)
        self.assertIsNone(checks.get("tags_compiled"))

        errors = payload.get("errors") if isinstance(payload.get("errors"), list) else []
        error_codes = [err.get("code") for err in errors if isinstance(err, dict)]
        self.assertEqual(error_codes, ["SNAPSHOT_MANIFEST_MISSING"])

    def test_preflight_error_when_tags_compiled_not_truthy(self) -> None:
        con = sqlite3.connect(":memory:")
        con.row_factory = sqlite3.Row
        try:
            self._create_snapshots_table(con)
            self._insert_snapshot(con, snapshot_id="TEST_SNAPSHOT_0001", manifest_json='{"tags_compiled": 0}')
            con.commit()

            payload = run_snapshot_preflight_v1(db=con, snapshot_id="TEST_SNAPSHOT_0001")
        finally:
            con.close()

        self.assertEqual(payload.get("status"), "ERROR")
        checks = payload.get("checks") if isinstance(payload.get("checks"), dict) else {}
        self.assertIs(checks.get("manifest_present"), True)
        self.assertIs(checks.get("tags_compiled"), False)

        errors = payload.get("errors") if isinstance(payload.get("errors"), list) else []
        error_codes = [err.get("code") for err in errors if isinstance(err, dict)]
        self.assertEqual(error_codes, ["SNAPSHOT_TAGS_NOT_COMPILED"])

    def test_preflight_error_when_snapshots_schema_missing_required_columns(self) -> None:
        con = sqlite3.connect(":memory:")
        con.row_factory = sqlite3.Row
        try:
            con.execute(
                """
                CREATE TABLE snapshots (
                  snapshot_id TEXT PRIMARY KEY,
                  manifest_json TEXT
                )
                """
            )
            con.execute(
                "INSERT INTO snapshots (snapshot_id, manifest_json) VALUES (?, ?)",
                ("TEST_SNAPSHOT_0001", '{"tags_compiled": true}'),
            )
            con.commit()

            payload = run_snapshot_preflight_v1(db=con, snapshot_id="TEST_SNAPSHOT_0001")
        finally:
            con.close()

        self.assertEqual(payload.get("status"), "ERROR")
        checks = payload.get("checks") if isinstance(payload.get("checks"), dict) else {}
        self.assertIs(checks.get("schema_ok"), False)

        errors = payload.get("errors") if isinstance(payload.get("errors"), list) else []
        error_codes = [err.get("code") for err in errors if isinstance(err, dict)]
        self.assertIn("SNAPSHOTS_SCHEMA_INVALID", error_codes)

    def test_preflight_is_deterministic_for_same_input(self) -> None:
        con = sqlite3.connect(":memory:")
        con.row_factory = sqlite3.Row
        try:
            self._create_snapshots_table(con)
            self._insert_snapshot(con, snapshot_id="TEST_SNAPSHOT_0001", manifest_json='{"tags_compiled": true}')
            con.commit()

            first = run_snapshot_preflight_v1(db=con, snapshot_id="TEST_SNAPSHOT_0001")
            second = run_snapshot_preflight_v1(db=con, snapshot_id="TEST_SNAPSHOT_0001")
        finally:
            con.close()

        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
