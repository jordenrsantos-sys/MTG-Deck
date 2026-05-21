"""Mega-task v3 Phase 2 — Scryfall set ingestion tests.

Verifies:
  - fetch_set_cards paginates via has_more/next_page
  - diff_against_corpus correctly buckets new/reprint/errata
  - ingest_new_set is idempotent on re-run (no duplicate rows)
  - ingest_new_set is atomic (rollback on mid-transaction failure)
  - 404 from Scryfall returns []
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from api.engine.integrations import scryfall_set_ingest_v1 as ingest


# ============================================================
# Fixtures.
# ============================================================


def _mk_card(oracle_id, name, oracle_text="", scryfall_id=None):
    return {
        "id": scryfall_id or f"sid-{oracle_id}",
        "oracle_id": oracle_id,
        "name": name,
        "lang": "en",
        "mana_cost": "{1}",
        "cmc": 1,
        "type_line": "Creature",
        "oracle_text": oracle_text,
        "colors": ["U"],
        "color_identity": ["U"],
        "produced_mana": [],
        "keywords": [],
        "legalities": {"commander": "legal"},
        "image_uris": {},
        "card_faces": [],
        "image_status": "highres_scan",
        "released_at": "2026-05-01",
    }


def _mk_sqlite():
    """Build an in-memory clone of the cards + cards_raw schema."""
    td = tempfile.TemporaryDirectory()
    path = Path(td.name) / "test.sqlite"
    con = sqlite3.connect(str(path))
    con.execute("""
        CREATE TABLE cards (
            snapshot_id TEXT, oracle_id TEXT, name TEXT,
            mana_cost TEXT, cmc REAL, type_line TEXT, oracle_text TEXT,
            colors TEXT, color_identity TEXT, produced_mana TEXT,
            keywords TEXT, legalities_json TEXT, primitives_json TEXT,
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


# ============================================================
# Tests.
# ============================================================


class FetchSetCardsTests(unittest.TestCase):
    def test_single_page(self) -> None:
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {
            "data": [_mk_card("o1", "Card 1"), _mk_card("o2", "Card 2")],
            "has_more": False,
        }
        resp.raise_for_status = MagicMock()
        result = ingest.fetch_set_cards(
            "tst", http_get=lambda url, **kw: resp,
        )
        self.assertEqual(len(result), 2)

    def test_paginates_via_next_page(self) -> None:
        page1 = MagicMock()
        page1.status_code = 200
        page1.json.return_value = {
            "data": [_mk_card("o1", "Card 1")],
            "has_more": True,
            "next_page": "https://api.scryfall.com/cards/page2",
        }
        page1.raise_for_status = MagicMock()
        page2 = MagicMock()
        page2.status_code = 200
        page2.json.return_value = {
            "data": [_mk_card("o2", "Card 2")],
            "has_more": False,
        }
        page2.raise_for_status = MagicMock()
        responses = [page1, page2]

        def http_get(url, **kw):
            return responses.pop(0)

        with patch("time.sleep"):
            result = ingest.fetch_set_cards("tst", http_get=http_get)
        self.assertEqual(len(result), 2)

    def test_404_returns_empty(self) -> None:
        resp = MagicMock()
        resp.status_code = 404
        result = ingest.fetch_set_cards(
            "nonexistent", http_get=lambda url, **kw: resp,
        )
        self.assertEqual(result, [])


class DiffAgainstCorpusTests(unittest.TestCase):
    def test_buckets_new_reprint_errata(self) -> None:
        path, _td = _mk_sqlite()
        # Pre-populate the cards table.
        con = sqlite3.connect(str(path))
        con.executemany(
            "INSERT INTO cards (snapshot_id, oracle_id, oracle_text) VALUES (?, ?, ?)",
            [("snap", "o-existing-same", "old text"),
             ("snap", "o-existing-errata", "old text")],
        )
        con.commit()
        con.close()
        cards = [
            _mk_card("o-new", "Brand New Card", "fresh text"),
            _mk_card("o-existing-same", "Reprint", "old text"),
            _mk_card("o-existing-errata", "Errata", "NEW errata text"),
        ]
        diff = ingest.diff_against_corpus(cards, path, "snap")
        self.assertEqual(len(diff["new_cards"]), 1)
        self.assertEqual(diff["new_cards"][0]["name"], "Brand New Card")
        self.assertEqual(len(diff["reprints"]), 1)
        self.assertEqual(diff["reprints"][0]["name"], "Reprint")
        self.assertEqual(len(diff["errata"]), 1)
        self.assertEqual(diff["errata"][0]["name"], "Errata")


class IngestNewSetTests(unittest.TestCase):
    def test_inserts_new_and_errata_rows(self) -> None:
        path, _td = _mk_sqlite()
        cards = [
            _mk_card("o-1", "Alpha", "alpha text"),
            _mk_card("o-2", "Beta", "beta text"),
        ]
        result = ingest.ingest_new_set(
            "tst", path, "snap", cards=cards, update_ledger=False,
        )
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["new_cards_count"], 2)
        self.assertEqual(result["cards_inserted"], 2)
        self.assertEqual(result["cards_raw_inserted"], 2)

    def test_idempotent_on_rerun(self) -> None:
        path, _td = _mk_sqlite()
        cards = [_mk_card("o-1", "Alpha", "text")]
        first = ingest.ingest_new_set(
            "tst", path, "snap", cards=cards, update_ledger=False,
        )
        second = ingest.ingest_new_set(
            "tst", path, "snap", cards=cards, update_ledger=False,
        )
        # First run inserts 1; second classifies as reprint, inserts 0.
        self.assertEqual(first["new_cards_count"], 1)
        self.assertEqual(second["new_cards_count"], 0)
        self.assertEqual(second["reprints_count"], 1)
        self.assertEqual(second["cards_inserted"], 0)

    def test_atomic_rollback_on_failure(self) -> None:
        path, _td = _mk_sqlite()
        cards = [_mk_card("o-1", "Alpha", "a")]
        # Patch _insert_cards_raw_rows to throw mid-transaction.
        with patch.object(
            ingest, "_insert_cards_raw_rows",
            side_effect=RuntimeError("forced failure"),
        ):
            with self.assertRaises(RuntimeError):
                ingest.ingest_new_set(
                    "tst", path, "snap", cards=cards, update_ledger=False,
                )
        # The cards insert should also have been rolled back.
        con = sqlite3.connect(str(path))
        try:
            n = con.execute("SELECT COUNT(*) FROM cards").fetchone()[0]
        finally:
            con.close()
        self.assertEqual(n, 0)


if __name__ == "__main__":
    unittest.main()
