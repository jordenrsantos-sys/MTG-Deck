"""Iter 3 Phase 7 + Iter 4 Phase 1 — semantic retrieval activation tests.

Tests verify:
  - `is_available()` returns False when the index file is missing/empty.
  - `is_available()` returns True when populated.
  - `query_neighbors()` returns an empty list when not available.
  - `query_neighbors()` returns top-k results from an in-memory test index.
  - `query_neighbors()` honors color_identity_filter.
  - `build_index()` is idempotent on re-run (skip when meta matches).
  - `EMBEDDING_DB_PATH` resolves to the canonical
    `data/embeddings/card_embeddings_v1.sqlite` location.
"""
from __future__ import annotations

import sqlite3
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from api.engine.layers import agent_semantic_retrieval_v1 as sr


def _pack(vec):
    return struct.pack(f"<{len(vec)}f", *vec)


def _build_test_index(path: Path) -> None:
    """Create a tiny test index with 4 cards in a 4-dim space."""
    con = sr._ensure_schema(path)
    try:
        # Sol Ring close to Mana Crypt (both fast mana artifacts).
        con.execute(
            "INSERT INTO card_embeddings VALUES (?,?,?,?,?,?,?)",
            ("Sol Ring", "", "Artifact", "Tap: add {C}{C}.", 1, "1993-12-01",
             _pack([1.0, 0.0, 0.0, 0.0])),
        )
        con.execute(
            "INSERT INTO card_embeddings VALUES (?,?,?,?,?,?,?)",
            ("Mana Crypt", "", "Artifact", "Tap: add {C}{C}.", 0, "2010-01-01",
             _pack([0.99, 0.05, 0.0, 0.0])),
        )
        # Lightning Bolt — direct damage, orthogonal.
        con.execute(
            "INSERT INTO card_embeddings VALUES (?,?,?,?,?,?,?)",
            ("Lightning Bolt", "R", "Instant", "Deal 3 damage to any target.", 1,
             "1993-12-01", _pack([0.0, 1.0, 0.0, 0.0])),
        )
        # Vampire Nighthawk — black creature.
        con.execute(
            "INSERT INTO card_embeddings VALUES (?,?,?,?,?,?,?)",
            ("Vampire Nighthawk", "B", "Creature - Vampire Shaman", "Flying.", 3,
             "2009-10-01", _pack([0.0, 0.0, 1.0, 0.0])),
        )
        sr._meta_set(con, "snapshot_id", "test_snapshot")
        sr._meta_set(con, "model", "test-model")
        sr._meta_set(con, "card_count", "4")
        con.commit()
    finally:
        con.close()


class IsAvailableTests(unittest.TestCase):
    def setUp(self) -> None:
        # Force cache invalidation between tests.
        sr._CACHE["loaded_path"] = None

    def test_returns_false_when_db_file_missing(self) -> None:
        with patch.object(sr, "EMBEDDING_DB_PATH",
                          Path("/nonexistent/path/card_embeddings_v1.sqlite")):
            self.assertFalse(sr.is_available())

    def test_returns_false_when_db_file_empty(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
            path = Path(f.name)
        try:
            sqlite3.connect(str(path)).close()
            with patch.object(sr, "EMBEDDING_DB_PATH", path):
                self.assertFalse(sr.is_available())
        finally:
            path.unlink(missing_ok=True)

    def test_returns_true_when_db_has_data(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
            path = Path(f.name)
        try:
            _build_test_index(path)
            with patch.object(sr, "EMBEDDING_DB_PATH", path):
                self.assertTrue(sr.is_available())
        finally:
            path.unlink(missing_ok=True)


class QueryNeighborsTests(unittest.TestCase):
    def setUp(self) -> None:
        sr._CACHE["loaded_path"] = None

    def test_empty_when_not_available(self) -> None:
        with patch.object(sr, "is_available", return_value=False):
            self.assertEqual(sr.query_neighbors("Sol Ring", k=20), [])

    def test_color_identity_filter_signature(self) -> None:
        with patch.object(sr, "is_available", return_value=False):
            r = sr.query_neighbors(
                "Sol Ring", k=20, color_identity_filter=["B", "R"],
            )
            self.assertEqual(r, [])

    def test_returns_top_k_neighbors(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
            path = Path(f.name)
        try:
            _build_test_index(path)
            with patch.object(sr, "EMBEDDING_DB_PATH", path):
                sr._CACHE["loaded_path"] = None
                neighbors = sr.query_neighbors("Sol Ring", k=2)
                self.assertEqual(len(neighbors), 2)
                # Mana Crypt should be the nearest (highest similarity).
                self.assertEqual(neighbors[0]["name"], "Mana Crypt")
                self.assertGreater(neighbors[0]["similarity"], 0.9)
        finally:
            sr._CACHE["loaded_path"] = None
            path.unlink(missing_ok=True)

    def test_color_identity_filter_drops_off_color(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
            path = Path(f.name)
        try:
            _build_test_index(path)
            with patch.object(sr, "EMBEDDING_DB_PATH", path):
                sr._CACHE["loaded_path"] = None
                # Querying Sol Ring with a B-only filter — Lightning Bolt (R)
                # should be filtered out.
                neighbors = sr.query_neighbors(
                    "Sol Ring", k=3, color_identity_filter=["B"],
                )
                names = [n["name"] for n in neighbors]
                self.assertIn("Mana Crypt", names)  # colorless ⊆ B
                self.assertIn("Vampire Nighthawk", names)  # B ⊆ B
                self.assertNotIn("Lightning Bolt", names)  # R ⊄ B
        finally:
            sr._CACHE["loaded_path"] = None
            path.unlink(missing_ok=True)

    def test_unknown_card_returns_empty(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
            path = Path(f.name)
        try:
            _build_test_index(path)
            with patch.object(sr, "EMBEDDING_DB_PATH", path):
                sr._CACHE["loaded_path"] = None
                self.assertEqual(
                    sr.query_neighbors("Made Up Card Name", k=5), [],
                )
        finally:
            sr._CACHE["loaded_path"] = None
            path.unlink(missing_ok=True)


class BuildIndexIdempotencyTests(unittest.TestCase):
    def setUp(self) -> None:
        sr._CACHE["loaded_path"] = None

    def test_skipped_when_meta_matches(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
            path = Path(f.name)
        try:
            _build_test_index(path)
            with patch.object(sr, "EMBEDDING_DB_PATH", path), \
                 patch.object(sr, "_commander_legal_cards", return_value=[
                     {"name": "Sol Ring", "type_line": "", "oracle_text": "",
                      "cmc": 1, "color_identity": [], "released_at": ""},
                     {"name": "Mana Crypt", "type_line": "", "oracle_text": "",
                      "cmc": 0, "color_identity": [], "released_at": ""},
                     {"name": "Lightning Bolt", "type_line": "", "oracle_text": "",
                      "cmc": 1, "color_identity": ["R"], "released_at": ""},
                     {"name": "Vampire Nighthawk", "type_line": "", "oracle_text": "",
                      "cmc": 3, "color_identity": ["B"], "released_at": ""},
                 ]):
                # Skip the API-key gate by patching environ.
                with patch.dict("os.environ", {"VOYAGE_API_KEY": "test-key"}):
                    result = sr.build_index(
                        db_snapshot_id="test_snapshot",
                        model_name="test-model",
                    )
            self.assertEqual(result["status"], "skipped")
            self.assertEqual(result["card_count"], 4)
        finally:
            sr._CACHE["loaded_path"] = None
            path.unlink(missing_ok=True)

    def test_failed_when_no_api_key(self) -> None:
        env = dict()  # No VOYAGE_API_KEY.
        with patch.dict("os.environ", env, clear=True):
            result = sr.build_index(db_snapshot_id="test")
        self.assertEqual(result["status"], "failed")
        self.assertIn("VOYAGE_API_KEY", result["message"])


class EmbeddingDbPathTests(unittest.TestCase):
    def test_canonical_path_location(self) -> None:
        path_str = str(sr.EMBEDDING_DB_PATH).replace("\\", "/")
        self.assertIn("api/engine/data/embeddings/", path_str)
        self.assertTrue(path_str.endswith("card_embeddings_v1.sqlite"))


if __name__ == "__main__":
    unittest.main()
