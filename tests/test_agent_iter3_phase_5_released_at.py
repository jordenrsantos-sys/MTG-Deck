"""Iter 3 Phase 5 tests — released_at backfill + recent-set boost.

Two surfaces:
  1. `backfill_released_at` tool — idempotent migration + min-date
     backfill from cards_raw JSON.
  2. `agent_wide_candidate_pool_v1` — recent-set boost (+0.10 to score
     for cards whose released_at is within 24 months of today_iso).

Tests use synthetic in-memory sqlite DBs to keep them hermetic — no
dependency on the production mtg.sqlite path.
"""
from __future__ import annotations

import json
import sqlite3
import unittest
from unittest.mock import patch

import sys
from pathlib import Path

# Make tools/ importable.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from backfill_released_at import (  # type: ignore
    backfill,
    column_exists,
    ensure_column,
)

from api.engine.layers.agent_wide_candidate_pool_v1 import (
    RECENT_SET_BOOST,
    compute_agent_wide_candidate_pool_v1,
)


def _make_synthetic_db() -> sqlite3.Connection:
    """In-memory DB with the minimum schema the backfill + wide-pool
    code touches."""
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript("""
        CREATE TABLE snapshots (
            snapshot_id TEXT PRIMARY KEY,
            created_at TEXT
        );
        CREATE TABLE cards (
            snapshot_id TEXT,
            oracle_id TEXT,
            name TEXT,
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
            image_uris_json TEXT,
            card_faces_json TEXT,
            image_status TEXT,
            PRIMARY KEY (snapshot_id, oracle_id)
        );
        CREATE TABLE cards_raw (
            snapshot_id TEXT,
            scryfall_id TEXT,
            oracle_id TEXT,
            lang TEXT,
            name TEXT,
            json TEXT,
            PRIMARY KEY (snapshot_id, scryfall_id)
        );
    """)
    return con


# ============================================================
# Backfill tool tests.
# ============================================================


class BackfillReleasedAtTests(unittest.TestCase):
    def setUp(self) -> None:
        self.con = _make_synthetic_db()
        # Seed snapshot + a couple cards with cards_raw printings.
        self.con.execute("INSERT INTO snapshots VALUES (?, ?)", ("SNAP1", "2026-01-01"))
        self.con.executemany(
            "INSERT INTO cards (snapshot_id, oracle_id, name, color_identity, primitives_json, oracle_text, type_line, cmc) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("SNAP1", "oid-edgar", "Edgar Markov", '["B","R","W"]', '[]', "Vampires!", "Legendary Creature - Vampire Knight", 6.0),
                ("SNAP1", "oid-vito", "Vito, Thorn of the Dusk Rose", '["B"]', '[]', "Lifegain drain.", "Legendary Creature - Vampire Cleric", 3.0),
                ("SNAP1", "oid-old", "Sol Ring", '[]', '[]', "Cheap mana.", "Artifact", 1.0),
            ],
        )
        # cards_raw with released_at: Edgar = 2017-08-25, Vito = 2020-09-25,
        # Sol Ring = 1993-08-15. Vito's first printing date matters; later
        # printings (e.g. reprints) shouldn't override.
        self.con.executemany(
            "INSERT INTO cards_raw (snapshot_id, scryfall_id, oracle_id, lang, name, json) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                ("SNAP1", "sf-edgar-1", "oid-edgar", "en", "Edgar Markov",
                 json.dumps({"released_at": "2017-08-25", "name": "Edgar Markov"})),
                ("SNAP1", "sf-edgar-2", "oid-edgar", "en", "Edgar Markov (reprint)",
                 json.dumps({"released_at": "2021-04-23", "name": "Edgar Markov"})),
                ("SNAP1", "sf-vito-1", "oid-vito", "en", "Vito",
                 json.dumps({"released_at": "2020-09-25", "name": "Vito"})),
                ("SNAP1", "sf-sol-1", "oid-old", "en", "Sol Ring",
                 json.dumps({"released_at": "1993-08-15", "name": "Sol Ring"})),
            ],
        )
        self.con.commit()

    def tearDown(self) -> None:
        self.con.close()

    def test_ensure_column_adds_when_missing(self) -> None:
        self.assertFalse(column_exists(self.con, "cards", "released_at"))
        added = ensure_column(self.con)
        self.assertTrue(added)
        self.assertTrue(column_exists(self.con, "cards", "released_at"))

    def test_ensure_column_is_idempotent(self) -> None:
        ensure_column(self.con)
        added_again = ensure_column(self.con)
        self.assertFalse(added_again)

    def test_backfill_writes_earliest_release_per_oracle(self) -> None:
        ensure_column(self.con)
        result = backfill(self.con)
        self.assertGreater(result["seen"], 0)
        # Edgar has 2 printings — earliest is 2017-08-25, not 2021-04-23.
        row = self.con.execute(
            "SELECT released_at FROM cards WHERE oracle_id = ?", ("oid-edgar",),
        ).fetchone()
        self.assertEqual(row["released_at"], "2017-08-25")
        # Vito: single printing 2020-09-25.
        row = self.con.execute(
            "SELECT released_at FROM cards WHERE oracle_id = ?", ("oid-vito",),
        ).fetchone()
        self.assertEqual(row["released_at"], "2020-09-25")
        # Sol Ring (old).
        row = self.con.execute(
            "SELECT released_at FROM cards WHERE oracle_id = ?", ("oid-old",),
        ).fetchone()
        self.assertEqual(row["released_at"], "1993-08-15")

    def test_backfill_is_idempotent(self) -> None:
        ensure_column(self.con)
        first = backfill(self.con)
        second = backfill(self.con)
        # Re-running shouldn't shift dates (the predicate WHERE
        # released_at IS NULL OR released_at > ? guards regression).
        edgar = self.con.execute(
            "SELECT released_at FROM cards WHERE oracle_id = ?", ("oid-edgar",),
        ).fetchone()
        self.assertEqual(edgar["released_at"], "2017-08-25")
        self.assertEqual(first["seen"], second["seen"])


# ============================================================
# Wide-pool recent-set boost tests.
# ============================================================


class WidePoolRecentSetBoostTests(unittest.TestCase):
    def _patch_db_rows(self, rows):
        """Helper that patches engine.db.connect to yield a fake con
        whose execute() returns the given rows. Mirrors the existing
        test pattern in test_agent_build_deck_v1_phase_c2_2.py."""
        from contextlib import contextmanager
        from unittest.mock import MagicMock

        class _FakeRow:
            def __init__(self, d): self._d = d
            def __getitem__(self, k): return self._d.get(k)

        fake_rows = [_FakeRow(r) for r in rows]
        fake_cursor = MagicMock()
        fake_cursor.fetchall.return_value = fake_rows
        # PRAGMA table_info needs to indicate released_at is present.
        # The execute() call gets used for BOTH the pragma and the SELECT;
        # return pragma-shaped tuples for the pragma and rows for the SELECT.
        def _execute(query, *args, **kwargs):
            if "PRAGMA table_info" in query:
                # Return cid/name/type/notnull/dflt_value/pk tuples matching
                # what the wide-pool's `cols = [r[1] for r in ...]` expects.
                return [
                    (0, "snapshot_id", "TEXT", 1, None, 1),
                    (1, "oracle_id", "TEXT", 1, None, 2),
                    (2, "name", "TEXT", 0, None, 0),
                    (3, "type_line", "TEXT", 0, None, 0),
                    (4, "cmc", "REAL", 0, None, 0),
                    (5, "color_identity", "TEXT", 0, None, 0),
                    (6, "primitives_json", "TEXT", 0, None, 0),
                    (7, "oracle_text", "TEXT", 0, None, 0),
                    (8, "mana_cost", "TEXT", 0, None, 0),
                    (9, "released_at", "TEXT", 0, None, 0),
                ]
            return fake_cursor
        fake_con = MagicMock()
        fake_con.execute.side_effect = _execute
        fake_con.__enter__ = MagicMock(return_value=fake_con)
        fake_con.__exit__ = MagicMock(return_value=False)

        from engine import db as eng_db
        return patch.object(eng_db, "connect", return_value=fake_con)

    def _row(self, name, ci="B", released_at=None):
        return {
            "name": name,
            "type_line": "Creature",
            "cmc": 3.0,
            "color_identity": ci,
            "primitives_json": "[]",
            "oracle_text": "Text.",
            "mana_cost": "{2}{B}",
            "released_at": released_at,
        }

    def test_recent_card_gets_boost(self) -> None:
        rows = [
            self._row("Recent Vampire", released_at="2025-08-01"),  # within 24mo of 2026-05-20
            self._row("Old Vampire", released_at="2010-01-01"),     # well before cutoff
        ]
        with self._patch_db_rows(rows):
            r = compute_agent_wide_candidate_pool_v1(
                db_snapshot_id="snap", commander="X",
                color_identity=["B"], theme_primitives=None, pool_size=10,
                today_iso="2026-05-20",
            )
        # Find both candidates.
        recent = next(c for c in r["candidates"] if c["name"] == "Recent Vampire")
        old = next(c for c in r["candidates"] if c["name"] == "Old Vampire")
        # Recent gets RECENT_SET_BOOST = 0.10 added to its score (which
        # is 0.0 here because no theme overlap).
        self.assertAlmostEqual(recent["score"], 0.0 + RECENT_SET_BOOST, places=4)
        self.assertAlmostEqual(old["score"], 0.0, places=4)
        self.assertTrue(recent["is_recent_set"])
        self.assertFalse(old["is_recent_set"])

    def test_cutoff_around_24_months(self) -> None:
        # today = 2026-05-20; cutoff = 2024-05-20 (today - 730 days,
        # no leap-day adjustment because timedelta is pure days).
        # Cards on or after cutoff get the boost; one earlier doesn't.
        rows = [
            self._row("Boundary Card", released_at="2024-05-20"),
            self._row("Just Older", released_at="2024-05-19"),
        ]
        with self._patch_db_rows(rows):
            r = compute_agent_wide_candidate_pool_v1(
                db_snapshot_id="snap", commander="X",
                color_identity=["B"], pool_size=10,
                today_iso="2026-05-20",
            )
        boundary = next(c for c in r["candidates"] if c["name"] == "Boundary Card")
        older = next(c for c in r["candidates"] if c["name"] == "Just Older")
        self.assertTrue(boundary["is_recent_set"])
        self.assertFalse(older["is_recent_set"])

    def test_missing_released_at_no_boost(self) -> None:
        # released_at NULL → no boost.
        rows = [self._row("No Date", released_at=None)]
        with self._patch_db_rows(rows):
            r = compute_agent_wide_candidate_pool_v1(
                db_snapshot_id="snap", commander="X",
                color_identity=["B"], pool_size=10,
                today_iso="2026-05-20",
            )
        cand = next(c for c in r["candidates"] if c["name"] == "No Date")
        self.assertFalse(cand["is_recent_set"])
        self.assertAlmostEqual(cand["score"], 0.0, places=4)

    def test_boost_caps_correctly_with_theme_overlap(self) -> None:
        # Recent + 2 theme primitives → 20.0 (theme) + 0.10 (recent).
        rows = [{
            **self._row("Recent Theme Card", released_at="2025-08-01"),
            "primitives_json": '["LIFEGAIN_PAYOFF", "TYPAL_VAMPIRES"]',
        }]
        with self._patch_db_rows(rows):
            r = compute_agent_wide_candidate_pool_v1(
                db_snapshot_id="snap", commander="X",
                color_identity=["B"],
                theme_primitives=["LIFEGAIN_PAYOFF", "TYPAL_VAMPIRES"],
                pool_size=10, today_iso="2026-05-20",
            )
        c = r["candidates"][0]
        self.assertAlmostEqual(c["score"], 20.10, places=4)


if __name__ == "__main__":
    unittest.main()
