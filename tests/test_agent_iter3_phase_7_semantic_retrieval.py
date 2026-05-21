"""Iter 3 Phase 7 tests — card-text semantic retrieval (scaffolded).

The module ships with a graceful no-op fallback when the embeddings
index isn't populated. Iter 4 will plug in the actual Voyage AI /
Anthropic embedding backend; iter 3 just stages the API surface so
B2 and C2.2 have stable integration points.

Tests verify:
  - `is_available()` returns False when the index file is missing.
  - `query_neighbors()` returns an empty list when not available.
  - `build_index()` returns a NOT_IMPLEMENTED status dict (placeholder
    for iter 4).
  - `EMBEDDING_DB_PATH` resolves to the canonical
    `data/embeddings/card_embeddings_v1.sqlite` location.
"""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from api.engine.layers import agent_semantic_retrieval_v1 as sr


class IsAvailableTests(unittest.TestCase):
    def test_returns_false_when_db_file_missing(self) -> None:
        with patch.object(sr, "EMBEDDING_DB_PATH", Path("/nonexistent/path/card_embeddings_v1.sqlite")):
            self.assertFalse(sr.is_available())

    def test_returns_false_when_db_file_empty(self) -> None:
        # Create an empty sqlite file (no card_embeddings table).
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
            path = Path(f.name)
        try:
            sqlite3.connect(str(path)).close()  # creates empty DB
            with patch.object(sr, "EMBEDDING_DB_PATH", path):
                self.assertFalse(sr.is_available())
        finally:
            path.unlink(missing_ok=True)

    def test_returns_true_when_db_has_data(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
            path = Path(f.name)
        try:
            con = sqlite3.connect(str(path))
            con.execute("CREATE TABLE card_embeddings (name TEXT, vec BLOB)")
            con.execute("INSERT INTO card_embeddings VALUES ('Sol Ring', X'00')")
            con.commit()
            con.close()
            with patch.object(sr, "EMBEDDING_DB_PATH", path):
                self.assertTrue(sr.is_available())
        finally:
            path.unlink(missing_ok=True)


class QueryNeighborsTests(unittest.TestCase):
    def test_empty_when_not_available(self) -> None:
        with patch.object(sr, "is_available", return_value=False):
            self.assertEqual(sr.query_neighbors("Sol Ring", k=20), [])

    def test_color_identity_filter_signature(self) -> None:
        # API accepts the filter even when result is empty.
        with patch.object(sr, "is_available", return_value=False):
            r = sr.query_neighbors(
                "Sol Ring", k=20, color_identity_filter=["B", "R"],
            )
            self.assertEqual(r, [])


class BuildIndexStubTests(unittest.TestCase):
    def test_returns_not_implemented_status(self) -> None:
        result = sr.build_index()
        self.assertEqual(result["status"], "NOT_IMPLEMENTED")
        self.assertIn("iter 4", result["message"].lower())


class EmbeddingDbPathTests(unittest.TestCase):
    def test_canonical_path_location(self) -> None:
        # Should live under api/engine/data/embeddings/
        path_str = str(sr.EMBEDDING_DB_PATH).replace("\\", "/")
        self.assertIn("api/engine/data/embeddings/", path_str)
        self.assertTrue(path_str.endswith("card_embeddings_v1.sqlite"))


if __name__ == "__main__":
    unittest.main()
