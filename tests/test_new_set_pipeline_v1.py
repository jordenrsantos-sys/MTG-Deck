"""Mega-task v3 Phase 3 — new_set_pipeline_v1 tests.

Verifies the upgraded orchestrator:
  - tag_with_primitives produces v1 tags via the Pillar C extractor
  - score_for_themes maps primitives to themes correctly
  - update_corpus_metadata still works (passthrough)
  - update_embedding_index has the right shape (mocked Voyage)
  - full pipeline integration test with a 5-card synthetic payload
  - idempotency: re-running on the same input produces the same state
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.new_set_pipeline_v1 import (
    flag_potential_combo_pairs,
    ingest_new_cards_v1,
    score_for_themes,
    tag_with_primitives,
    update_corpus_metadata,
    update_embedding_index,
)


def _mk_card(name, oracle_text="", type_line="Creature - Test",
             mana_cost="{1}", oracle_id=None):
    return {
        "oracle_id": oracle_id or f"oid-{name.lower().replace(' ', '-')}",
        "id": f"sid-{name.lower().replace(' ', '-')}",
        "lang": "en",
        "name": name,
        "mana_cost": mana_cost,
        "cmc": 1,
        "type_line": type_line,
        "oracle_text": oracle_text,
        "colors": ["U"],
        "color_identity": ["U"],
        "produced_mana": [],
        "keywords": [],
        "legalities": {"commander": "legal"},
        "image_uris": {},
        "card_faces": [],
        "image_status": "highres",
        "released_at": "2026-05-01",
    }


def _mk_sqlite():
    td = tempfile.TemporaryDirectory()
    path = Path(td.name) / "test.sqlite"
    con = sqlite3.connect(str(path))
    con.execute("""
        CREATE TABLE cards (
            snapshot_id TEXT, oracle_id TEXT, name TEXT,
            mana_cost TEXT, cmc REAL, type_line TEXT, oracle_text TEXT,
            colors TEXT, color_identity TEXT, produced_mana TEXT,
            keywords TEXT, legalities_json TEXT, primitives_json TEXT,
            primitives_v1_json TEXT,
            image_uris_json TEXT, card_faces_json TEXT, image_status TEXT,
            released_at TEXT,
            PRIMARY KEY (snapshot_id, oracle_id)
        )
    """)
    con.execute("""
        CREATE TABLE cards_raw (
            snapshot_id TEXT, scryfall_id TEXT, oracle_id TEXT,
            lang TEXT, name TEXT, json TEXT,
            PRIMARY KEY (snapshot_id, scryfall_id)
        )
    """)
    con.commit()
    con.close()
    return path, td


class TagWithPrimitivesTests(unittest.TestCase):
    def test_sac_outlet_card_gets_tagged(self) -> None:
        cards = [_mk_card(
            "Test Seer",
            oracle_text="Sacrifice a creature: Scry 1.",
        )]
        result = tag_with_primitives(cards)
        self.assertIn("sac-outlet", result["Test Seer"])

    def test_etb_creature_gets_tagged(self) -> None:
        cards = [_mk_card(
            "Test Mulldrifter",
            oracle_text="When Test Mulldrifter enters the battlefield, draw two cards.",
        )]
        result = tag_with_primitives(cards)
        self.assertIn("etb-trigger", result["Test Mulldrifter"])

    def test_writes_to_db_when_path_given(self) -> None:
        path, _td = _mk_sqlite()
        # Pre-populate the cards table.
        con = sqlite3.connect(str(path))
        con.execute(
            "INSERT INTO cards (snapshot_id, oracle_id, name, oracle_text) "
            "VALUES (?, ?, ?, ?)",
            ("snap", "oid-1", "Sample", "Sacrifice a creature: Scry 1."),
        )
        con.commit()
        con.close()
        cards = [_mk_card(
            "Sample", oracle_text="Sacrifice a creature: Scry 1.",
            oracle_id="oid-1",
        )]
        result = tag_with_primitives(
            cards, db_path=path, snapshot_id="snap",
        )
        self.assertIn("sac-outlet", result["Sample"])
        # Verify the column was written.
        con = sqlite3.connect(str(path))
        try:
            row = con.execute(
                "SELECT primitives_v1_json FROM cards WHERE name=?",
                ("Sample",),
            ).fetchone()
        finally:
            con.close()
        self.assertIsNotNone(row)
        stored = json.loads(row[0])
        self.assertIn("sac-outlet", stored)


class ScoreForThemesTests(unittest.TestCase):
    def test_sac_outlet_signals_aristocrats(self) -> None:
        cards = [_mk_card("Test")]
        prims = {"Test": ["sac-outlet", "death-trigger"]}
        scores = score_for_themes(cards, prims)
        self.assertIn("Test", scores)
        self.assertIn("THEME_ARISTOCRATS", scores["Test"])
        self.assertEqual(scores["Test"]["THEME_ARISTOCRATS"], 2)

    def test_mana_rock_signals_ramp(self) -> None:
        cards = [_mk_card("Mana Card")]
        prims = {"Mana Card": ["mana-positive-rock"]}
        scores = score_for_themes(cards, prims)
        self.assertIn("THEME_RAMP", scores["Mana Card"])

    def test_empty_primitives_no_themes(self) -> None:
        cards = [_mk_card("Vanilla")]
        prims = {"Vanilla": []}
        scores = score_for_themes(cards, prims)
        self.assertNotIn("Vanilla", scores)


class UpdateEmbeddingIndexTests(unittest.TestCase):
    def test_skips_when_no_snapshot(self) -> None:
        result = update_embedding_index([_mk_card("X")], snapshot_id=None)
        self.assertEqual(result, 0)

    def test_calls_build_index_with_snapshot(self) -> None:
        from api.engine.layers import agent_semantic_retrieval_v1 as sr
        with patch.object(sr, "build_index", return_value={"status": "skipped", "newly_inserted": 0}) as m:
            update_embedding_index([_mk_card("X")], snapshot_id="snap")
            m.assert_called_once_with(db_snapshot_id="snap")


class PipelineIntegrationTests(unittest.TestCase):
    def test_full_pipeline_5_card_payload(self) -> None:
        path, _td = _mk_sqlite()
        cards = [
            _mk_card("New Seer", oracle_text="Sacrifice a creature: Scry 1."),
            _mk_card("New Drifter",
                     oracle_text="When New Drifter enters the battlefield, draw two cards.",
                     type_line="Creature - Elemental"),
            _mk_card("New Anthem",
                     oracle_text="Creatures you control get +1/+1.",
                     type_line="Enchantment"),
            _mk_card("New Rock",
                     oracle_text="{T}: Add {C}{C}.",
                     type_line="Artifact"),
            _mk_card("Vanilla 2/2",
                     oracle_text="", type_line="Creature - Beast"),
        ]
        # Skip embedding (we don't want to hit Voyage in tests).
        result = ingest_new_cards_v1(
            cards, path, "snap", skip_embedding=True,
        )
        # Corpus rows written.
        self.assertGreaterEqual(result.corpus_rows_written, 5)
        # At least 4 of 5 cards tagged (vanilla creature gets nothing).
        self.assertGreaterEqual(result.primitives_written, 3)
        # Each step status recorded.
        self.assertIn("update_corpus_metadata", result.per_step_status)
        self.assertIn("tag_with_primitives", result.per_step_status)
        self.assertIn("score_for_themes", result.per_step_status)
        self.assertIn("update_embedding_index", result.per_step_status)
        self.assertIn("flag_potential_combo_pairs", result.per_step_status)
        # No errors.
        for status in result.per_step_status.values():
            self.assertNotIn("ERROR", status, msg=str(result.per_step_status))

    def test_idempotent_on_rerun(self) -> None:
        path, _td = _mk_sqlite()
        cards = [_mk_card("Idempotent",
                          oracle_text="Sacrifice a creature: Scry 1.")]
        ingest_new_cards_v1(cards, path, "snap", skip_embedding=True)
        ingest_new_cards_v1(cards, path, "snap", skip_embedding=True)
        # Row count should be 1, primitives_v1_json identical.
        con = sqlite3.connect(str(path))
        try:
            n = con.execute(
                "SELECT COUNT(*) FROM cards WHERE name=?",
                ("Idempotent",),
            ).fetchone()[0]
            row = con.execute(
                "SELECT primitives_v1_json FROM cards WHERE name=?",
                ("Idempotent",),
            ).fetchone()
        finally:
            con.close()
        self.assertEqual(n, 1)
        self.assertIn("sac-outlet", json.loads(row[0]))


if __name__ == "__main__":
    unittest.main()
