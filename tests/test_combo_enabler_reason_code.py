"""v1.7 Stage 2 — COMBO_ENABLER reason-code emission test.

This test exercises the new `combo_enabler_reasons_v1` engine layer that
post-processes `added_cards_v1` and annotates each proposed-add with a
COMBO_ENABLER reason whenever the proposed card forms a known 2-card
combo with an existing card in the deck (or commander).

Test fixture pair (Commander Spellbook variant 1622-3542):
  - Palinchron        oracle_id 0023888e-7bec-43e0-8dee-d1a4eb94b372
  - Minion Reflector  oracle_id 0124977f-06e6-430a-a874-f7d05883a97c
  - outcome label: "Infinite creature tokens; Infinite ETB; Infinite LTB; ..."

Deck contains Palinchron; the engine proposes adding Minion Reflector;
the layer must annotate the added card with a COMBO_ENABLER reason whose
structured payload carries Palinchron's oracle_id, resolved name, and
the outcome label sourced from the Stage 1.5 outcome pack.

The reason is encoded as a tagged string `"COMBO_ENABLER:<json>"` so it
flows through api/main.py's strict `reasons_v1: List[str]` filter.
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


FIXTURE_SNAPSHOT_ID = "COMBO_ENABLER_TEST_SNAPSHOT"

PALINCHRON_ORACLE_ID = "0023888e-7bec-43e0-8dee-d1a4eb94b372"
MINION_REFLECTOR_ORACLE_ID = "0124977f-06e6-430a-a874-f7d05883a97c"
EXPECTED_VARIANT_ID = "1622-3542"


def _create_combo_fixture_db(tmp_dir: Path) -> Path:
    tmp_dir.mkdir(parents=True, exist_ok=True)
    db_path = (tmp_dir / "combo_enabler_fixture.sqlite").resolve()
    schema_sql = (
        Path(__file__).resolve().parents[1] / "schemas" / "schema.sql"
    ).read_text(encoding="utf-8")
    con = sqlite3.connect(str(db_path))
    try:
        con.executescript(schema_sql)
        con.execute(
            "INSERT INTO snapshots (snapshot_id, created_at, source, "
            "scryfall_bulk_uri, scryfall_bulk_updated_at, manifest_json) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                FIXTURE_SNAPSHOT_ID,
                "2026-01-01T00:00:00+00:00",
                "pytest_combo_enabler",
                "local://pytest/combo_enabler",
                "2026-01-01T00:00:00+00:00",
                "{}",
            ),
        )
        rows = [
            (FIXTURE_SNAPSHOT_ID, PALINCHRON_ORACLE_ID, "Palinchron",
             "{4}{U}", 5.0, "Creature — Illusion", "[\"U\"]", "[\"U\"]", "{}", "[]"),
            (FIXTURE_SNAPSHOT_ID, MINION_REFLECTOR_ORACLE_ID, "Minion Reflector",
             "{4}", 4.0, "Artifact", "[]", "[]", "{}", "[]"),
            (FIXTURE_SNAPSHOT_ID, "11111111-1111-1111-1111-111111111111",
             "Talrand, Sky Summoner", "{2}{U}{U}", 4.0,
             "Legendary Creature — Merfolk Wizard", "[\"U\"]", "[\"U\"]", "{}", "[]"),
        ]
        con.executemany(
            "INSERT INTO cards (snapshot_id, oracle_id, name, mana_cost, cmc, "
            "type_line, colors, color_identity, legalities_json, primitives_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        con.commit()
    finally:
        con.close()
    return db_path


@contextmanager
def _set_db_env(db_path: Path) -> Iterator[None]:
    previous = os.environ.get("MTG_ENGINE_DB_PATH")
    os.environ["MTG_ENGINE_DB_PATH"] = str(db_path.resolve())
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("MTG_ENGINE_DB_PATH", None)
        else:
            os.environ["MTG_ENGINE_DB_PATH"] = previous


class ComboEnablerReasonCodeTests(unittest.TestCase):
    _tmp_dir_ctx: tempfile.TemporaryDirectory[str] | None = None
    _db_env_ctx = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._tmp_dir_ctx = tempfile.TemporaryDirectory()
        db_path = _create_combo_fixture_db(Path(cls._tmp_dir_ctx.name))
        cls._db_env_ctx = _set_db_env(db_path)
        cls._db_env_ctx.__enter__()

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            if cls._db_env_ctx is not None:
                cls._db_env_ctx.__exit__(None, None, None)
                cls._db_env_ctx = None
        finally:
            if cls._tmp_dir_ctx is not None:
                cls._tmp_dir_ctx.cleanup()
                cls._tmp_dir_ctx = None
            super().tearDownClass()

    def test_combo_enabler_reason_attached_to_added_partner(self) -> None:
        from api.engine.layers.combo_enabler_reasons_v1 import (
            attach_combo_enabler_reasons_v1,
        )

        added_cards_v1 = [
            {
                "name": "Minion Reflector",
                "reasons_v1": ["ADD_REDUNDANCY_SUPPORT", "COMPLETE_TO_TARGET_SIZE"],
                "primitives_added_v1": [],
            }
        ]

        result = attach_combo_enabler_reasons_v1(
            db_snapshot_id=FIXTURE_SNAPSHOT_ID,
            commander_names=["Talrand, Sky Summoner"],
            deck_cards=["Palinchron"],
            added_cards_v1=added_cards_v1,
        )

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        row = result[0]
        self.assertEqual(row["name"], "Minion Reflector")

        reasons = row.get("reasons_v1")
        self.assertIsInstance(reasons, list)
        self.assertIn("ADD_REDUNDANCY_SUPPORT", reasons)
        self.assertIn("COMPLETE_TO_TARGET_SIZE", reasons)

        combo_entries = [r for r in reasons if isinstance(r, str) and r.startswith("COMBO_ENABLER:")]
        self.assertEqual(
            len(combo_entries), 1,
            f"expected exactly one COMBO_ENABLER reason; got reasons={reasons!r}",
        )

        payload = json.loads(combo_entries[0][len("COMBO_ENABLER:"):])
        self.assertEqual(payload["partner_card_oracle_id"], PALINCHRON_ORACLE_ID)
        self.assertEqual(payload["partner_card_name"], "Palinchron")
        self.assertIn("Infinite", payload["combo_outcome_label"])
        self.assertIn("storm count", payload["combo_outcome_label"])

    def test_no_combo_reason_when_no_partner_in_deck(self) -> None:
        from api.engine.layers.combo_enabler_reasons_v1 import (
            attach_combo_enabler_reasons_v1,
        )

        added_cards_v1 = [
            {
                "name": "Minion Reflector",
                "reasons_v1": ["ADD_REDUNDANCY_SUPPORT"],
                "primitives_added_v1": [],
            }
        ]

        result = attach_combo_enabler_reasons_v1(
            db_snapshot_id=FIXTURE_SNAPSHOT_ID,
            commander_names=["Talrand, Sky Summoner"],
            deck_cards=[],
            added_cards_v1=added_cards_v1,
        )

        reasons = result[0].get("reasons_v1")
        combo_entries = [r for r in reasons if isinstance(r, str) and r.startswith("COMBO_ENABLER:")]
        self.assertEqual(combo_entries, [])


if __name__ == "__main__":
    unittest.main()
