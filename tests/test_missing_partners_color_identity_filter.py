"""v1.7.4 — Color-identity filter for missing_partners_v1.

v1.7.3 Cowork browser-walk found `missing_partners_v1` listed
color-identity-illegal partners. Concrete example: mono-Red Krenko
deck with Storm-Kiln Artist alone → suggestions included Chain of
Acid + Chain of Smog (both Black) which the user can't legally add.

v1.7.4 applies the same color-identity gate that
`proactive_combo_completion_v1` already uses (CR 903.4): a candidate
partner is legal iff `candidate.color_identity ⊆ commander.color_identity`.
Filter is applied ONLY to `missing_partners_v1`; `detected_combos_v1`
is unaffected (cards in the deck are presumed legal by being there).

Three failing-first scenarios:
  (A) mono-R Krenko + Storm-Kiln Artist → only R-legal partners
      (Fury Storm + Haze of Rage + colorless Aetherflux Reservoir);
      Black-color partners filtered.
  (B) mono-B commander + Storm-Kiln Artist → only B-legal partners
      (the lone Black partner; red ones filtered).
  (C) 5-color rainbow commander → ALL 4 SKA partners surface
      (no false-positive filtering).
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


FIXTURE_SNAPSHOT_ID = "MP_COLOR_IDENTITY_FILTER_SNAPSHOT"

STORM_KILN_ARTIST_ORACLE = "a145ff8c-5812-4bcb-bd16-9839dc25121d"

# SKA partner mix for v1.7.4: deliberately CROSS-COLOR so the filter
# has work to do. Real partner names from the Spellbook pack; colors
# fabricated for filter-isolation purposes (the fixture DB is a test
# harness, not an authoritative card source).
SKA_PARTNERS = [
    # (oracle_id, name, color_identity_json)
    ("0b1a27bd-bb98-44f8-8357-666fabfeabf0", "Aetherflux Reservoir",   '[]'),       # colorless
    ("371fa9e3-5432-4f2f-89d4-55061b0b4e57", "Fury Storm",              '["R"]'),    # red
    ("ea14c26b-bf2f-48b4-b879-6e63069ded1f", "Chain of Acid",           '["B"]'),    # black (illegal under Krenko)
    ("f17d0fb8-c157-43b8-be26-f5ba4c6aed14", "Haze of Rage",            '["R"]'),    # red
]

# Three commanders for the three scenarios.
COMMANDERS = [
    # (oracle_id, name, color_identity_json)
    ("11111111-1111-1111-1111-111111111111", "Krenko, Mob Boss",         '["R"]'),
    ("22222222-2222-2222-2222-222222222222", "Test Black Commander",     '["B"]'),
    ("33333333-3333-3333-3333-333333333333", "Test 5C Commander",        '["W","U","B","R","G"]'),
]


def _create_fixture_db(tmp_dir: Path) -> Path:
    tmp_dir.mkdir(parents=True, exist_ok=True)
    db_path = (tmp_dir / "mp_color_identity_filter_fixture.sqlite").resolve()
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
            (FIXTURE_SNAPSHOT_ID, "2026-01-01T00:00:00+00:00",
             "pytest_mp_color_filter", "local://", "2026-01-01T00:00:00+00:00", "{}"),
        )
        rows = [
            # Storm-Kiln Artist (deck-side combo half) — red
            (FIXTURE_SNAPSHOT_ID, STORM_KILN_ARTIST_ORACLE, "Storm-Kiln Artist",
             "{1}{R}{R}", 3.0, "Creature — Devil Artificer", '["R"]', '["R"]', "{}", "[]"),
        ]
        for oid, name, ci in SKA_PARTNERS:
            rows.append((FIXTURE_SNAPSHOT_ID, oid, name,
                         "{1}", 1.0, "Sorcery", ci, ci, "{}", "[]"))
        for oid, name, ci in COMMANDERS:
            rows.append((FIXTURE_SNAPSHOT_ID, oid, name,
                         "{1}{R}", 2.0, "Legendary Creature — Test", ci, ci, "{}", "[]"))
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


class MissingPartnersColorIdentityFilterTests(unittest.TestCase):
    _tmp_dir_ctx: tempfile.TemporaryDirectory[str] | None = None
    _db_env_ctx = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._tmp_dir_ctx = tempfile.TemporaryDirectory()
        db_path = _create_fixture_db(Path(cls._tmp_dir_ctx.name))
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

    def _ska_missing_names(self, commander_name: str) -> list[str]:
        from api.engine.layers.deck_combo_insights_v1 import (
            compute_deck_combo_insights_v1,
        )
        result = compute_deck_combo_insights_v1(
            db_snapshot_id=FIXTURE_SNAPSHOT_ID,
            commander_names=[commander_name],
            deck_cards_after_completion=["Storm-Kiln Artist"],
        )
        missing = result.get("missing_partners_v1") or []
        return sorted(
            m.get("partner_card_name") for m in missing
            if isinstance(m, dict) and m.get("present_card_name") == "Storm-Kiln Artist"
        )

    def test_a_mono_red_krenko_filters_black_partners(self) -> None:
        names = self._ska_missing_names("Krenko, Mob Boss")
        # Expected: Aetherflux Reservoir (colorless) + Fury Storm (R) +
        # Haze of Rage (R) = 3 partners. Chain of Acid (B) filtered.
        self.assertIn("Fury Storm", names, f"Red partner Fury Storm should surface; got {names}")
        self.assertIn("Haze of Rage", names, f"Red partner Haze of Rage should surface; got {names}")
        self.assertIn(
            "Aetherflux Reservoir", names,
            f"Colorless partner Aetherflux Reservoir should be legal under any commander; got {names}",
        )
        self.assertNotIn(
            "Chain of Acid", names,
            f"Black partner Chain of Acid should be filtered under mono-Red Krenko; got {names}",
        )
        self.assertEqual(
            len(names), 3,
            f"Expected exactly 3 missing partners (3 R-legal); got {len(names)}: {names}",
        )

    def test_b_mono_black_commander_filters_red_partners(self) -> None:
        names = self._ska_missing_names("Test Black Commander")
        # Expected: Aetherflux Reservoir (colorless) + Chain of Acid (B) = 2.
        # Fury Storm + Haze of Rage (both R) filtered.
        self.assertIn(
            "Chain of Acid", names,
            f"Black partner Chain of Acid should surface under mono-Black commander; got {names}",
        )
        self.assertIn(
            "Aetherflux Reservoir", names,
            f"Colorless partner should be legal; got {names}",
        )
        self.assertNotIn(
            "Fury Storm", names,
            f"Red partner Fury Storm should be filtered under mono-Black commander; got {names}",
        )
        self.assertNotIn(
            "Haze of Rage", names,
            f"Red partner Haze of Rage should be filtered under mono-Black commander; got {names}",
        )
        self.assertEqual(
            len(names), 2,
            f"Expected exactly 2 missing partners (2 B-legal); got {len(names)}: {names}",
        )

    def test_c_five_color_commander_surfaces_all_partners(self) -> None:
        names = self._ska_missing_names("Test 5C Commander")
        # Expected: ALL 4 partners surface — no false-positive filtering.
        self.assertEqual(
            len(names), 4,
            f"5-color commander should not filter any partner; got {len(names)}: {names}",
        )
        for expected in ("Aetherflux Reservoir", "Chain of Acid", "Fury Storm", "Haze of Rage"):
            self.assertIn(expected, names, f"Partner {expected} missing under 5C commander; got {names}")


if __name__ == "__main__":
    unittest.main()
