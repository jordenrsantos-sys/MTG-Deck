"""Mega-task v4 Phase 4 — voyage_rules_embedding_v1 tests.

Verifies:
  - ensure_schema adds source_type / rule_id / ruling_card / raw_text columns
  - split_rules_into_sections parses the WotC Comprehensive Rules format
  - embed_comprehensive_rules + embed_scryfall_rulings happy paths
    (with mocked Voyage client) write to the DB correctly
  - query_rules returns top-k semantic matches (with mocked Voyage)
"""
from __future__ import annotations

import sqlite3
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from api.engine.layers import voyage_rules_embedding_v1 as vre


def _setup_card_embeddings_db(path: Path) -> None:
    """Build the same card_embeddings schema as the real index."""
    con = sqlite3.connect(str(path))
    try:
        con.execute("""
            CREATE TABLE card_embeddings (
                name TEXT PRIMARY KEY,
                color_identity TEXT NOT NULL DEFAULT '',
                type_line TEXT NOT NULL DEFAULT '',
                oracle_text TEXT NOT NULL DEFAULT '',
                cmc REAL,
                released_at TEXT NOT NULL DEFAULT '',
                vec BLOB NOT NULL
            )
        """)
        con.commit()
    finally:
        con.close()


class EnsureSchemaTests(unittest.TestCase):
    def test_adds_columns_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "emb.sqlite"
            _setup_card_embeddings_db(path)
            vre.ensure_schema(path)
            con = sqlite3.connect(str(path))
            try:
                cols = {r[1] for r in con.execute("PRAGMA table_info(card_embeddings)")}
                self.assertIn("source_type", cols)
                self.assertIn("rule_id", cols)
                self.assertIn("ruling_card", cols)
                self.assertIn("raw_text", cols)
            finally:
                con.close()

    def test_idempotent_on_rerun(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "emb.sqlite"
            _setup_card_embeddings_db(path)
            vre.ensure_schema(path)
            vre.ensure_schema(path)  # should not raise
            con = sqlite3.connect(str(path))
            try:
                cols = {r[1] for r in con.execute("PRAGMA table_info(card_embeddings)")}
                self.assertIn("source_type", cols)
            finally:
                con.close()


class SplitRulesTests(unittest.TestCase):
    def test_splits_into_sections(self) -> None:
        rules_text = (
            "100.1 Welcome.\nMagic the Gathering is a game...\n\n"
            "100.2 Components.\nA Magic game requires...\n\n"
            "601.2a When a player casts a spell...\n"
        )
        sections = vre.split_rules_into_sections(rules_text)
        ids = [s["rule_id"] for s in sections]
        self.assertIn("100.1", ids)
        self.assertIn("100.2", ids)
        self.assertIn("601.2a", ids)


class EmbedComprehensiveRulesTests(unittest.TestCase):
    def test_embeds_sections_and_writes_to_db(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "emb.sqlite"
            _setup_card_embeddings_db(path)
            rules_text = (
                "100.1 Welcome.\nIntro paragraph.\n\n"
                "100.2 Components.\nDeck composition.\n"
            )

            mock_resp = MagicMock()
            mock_resp.embeddings = [
                [1.0] + [0.0] * 1023,  # 1024-dim like voyage-3
                [0.0, 1.0] + [0.0] * 1022,
            ]
            mock_client = MagicMock()
            mock_client.embed.return_value = mock_resp

            with patch.object(vre, "_embed_batch",
                              return_value=mock_resp.embeddings):
                with patch("voyageai.Client", return_value=mock_client):
                    result = vre.embed_comprehensive_rules(
                        rules_text, path, batch_size=10,
                    )
            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["inserted"], 2)
            # Verify rows are in the DB.
            con = sqlite3.connect(str(path))
            try:
                n = con.execute(
                    "SELECT COUNT(*) FROM card_embeddings WHERE source_type='rule'"
                ).fetchone()[0]
            finally:
                con.close()
            self.assertEqual(n, 2)


class EmbedScryfallRulingsTests(unittest.TestCase):
    def test_embeds_rulings_grouped_by_card(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "emb.sqlite"
            _setup_card_embeddings_db(path)
            rulings_data = [
                {"card_name": "Sol Ring",
                 "comment": "Sol Ring's mana ability triggers on tap."},
                {"card_name": "Sol Ring",
                 "comment": "Mana Crypt's ability differs because..."},
                {"card_name": "Mana Crypt",
                 "comment": "Mana Crypt deals 3 damage on coin loss."},
            ]
            mock_resp = MagicMock()
            mock_resp.embeddings = [
                [1.0] + [0.0] * 1023,
                [0.0, 1.0] + [0.0] * 1022,
                [0.0, 0.0, 1.0] + [0.0] * 1021,
            ]
            with patch.object(vre, "_embed_batch",
                              return_value=mock_resp.embeddings):
                with patch("voyageai.Client", return_value=MagicMock()):
                    result = vre.embed_scryfall_rulings(
                        rulings_data, path, batch_size=10,
                    )
            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["inserted"], 3)
            con = sqlite3.connect(str(path))
            try:
                rows = con.execute(
                    "SELECT name, ruling_card FROM card_embeddings "
                    "WHERE source_type='ruling' ORDER BY name"
                ).fetchall()
            finally:
                con.close()
            # 2 Sol Ring rulings + 1 Mana Crypt ruling = 3 rows.
            self.assertEqual(len(rows), 3)
            self.assertIn(("ruling:Mana Crypt:0", "Mana Crypt"), rows)


class QueryRulesTests(unittest.TestCase):
    def test_returns_empty_when_db_missing(self) -> None:
        result = vre.query_rules("test query", db_path=Path("/nonexistent.sqlite"))
        self.assertEqual(result, [])

    def test_returns_top_k_matches(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "emb.sqlite"
            _setup_card_embeddings_db(path)
            vre.ensure_schema(path)
            # Insert 2 rule rows with known vectors.
            con = sqlite3.connect(str(path))
            try:
                con.execute(
                    "INSERT INTO card_embeddings "
                    "(name, source_type, rule_id, raw_text, vec) "
                    "VALUES (?, 'rule', ?, ?, ?)",
                    ("rule:100.1", "100.1", "Welcome paragraph.",
                     struct.pack("<4f", 1.0, 0.0, 0.0, 0.0)),
                )
                con.execute(
                    "INSERT INTO card_embeddings "
                    "(name, source_type, rule_id, raw_text, vec) "
                    "VALUES (?, 'rule', ?, ?, ?)",
                    ("rule:200.1", "200.1", "Other paragraph.",
                     struct.pack("<4f", 0.0, 1.0, 0.0, 0.0)),
                )
                con.commit()
            finally:
                con.close()

            mock_resp = MagicMock()
            mock_resp.embeddings = [[1.0, 0.0, 0.0, 0.0]]
            with patch("voyageai.Client") as mc:
                mc.return_value.embed.return_value = mock_resp
                result = vre.query_rules(
                    "Welcome", k=2, db_path=path,
                )
            self.assertEqual(len(result), 2)
            # Query vector matches rule 100.1 exactly → top result.
            self.assertEqual(result[0]["rule_id"], "100.1")
            self.assertGreater(result[0]["similarity"], 0.99)


if __name__ == "__main__":
    unittest.main()
