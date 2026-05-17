"""v1.7.2 Stage 1 — Deck-combo insights engine layer.

v1.7 Stage 2 wired `combo_enabler_reasons_v1` to annotate engine-added
cards that complete a 2-card combo. The v1.7 Cowork browser-walk
(2026-05-16) found near-zero production coverage because the engine's
primitive-coverage-driven completion rarely picks a combo partner —
the post-hoc annotator has nothing to flag.

v1.7.2 closes the visibility gap with a NEW layer that scans the
FINAL completed deck (commander + initial deckText + engine adds)
against the v2 pair index + outcome pack and surfaces TWO insights:

  1. `detected_combos_v1` — pairs where BOTH halves are present in
     the final deck. UI message: "Your deck contains X + Y → outcome".
  2. `missing_partners_v1` — pairs where EXACTLY ONE half is present
     in the final deck AND the partner is NOT being added. UI message:
     "Add Y to enable a combo with X → outcome".

Fixture pair (confirmed against the Stage 1.5 outcome pack):
  - Storm-Kiln Artist  a145ff8c-5812-4bcb-bd16-9839dc25121d
  - Haze of Rage       f17d0fb8-c157-43b8-be26-f5ba4c6aed14
  - variant_id 3940-5195 → "Infinite colored mana; Infinitely powerful
    creatures…; Infinite magecraft triggers; Infinite storm count;
    Infinite Treasure tokens"

Storm-Kiln Artist has exactly 4 partners with outcomes in the pack
(variants 326-5195, 2655-5195, 3718-5195, 3940-5195) — the
missing-partner test asserts ≥4.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


FIXTURE_SNAPSHOT_ID = "DECK_COMBO_INSIGHTS_TEST_SNAPSHOT"

STORM_KILN_ARTIST_ORACLE = "a145ff8c-5812-4bcb-bd16-9839dc25121d"
HAZE_OF_RAGE_ORACLE = "f17d0fb8-c157-43b8-be26-f5ba4c6aed14"
TARGET_VARIANT_ID = "3940-5195"

# Storm-Kiln Artist's 4 known combo partners (from the outcome pack).
# Names are taken from the Spellbook API confirmation in Stage 0 audit;
# the test fixture only needs these rows in the DB so the partner-name
# resolution returns sensible strings (the test asserts on count, not
# on which specific names are returned).
SKA_PARTNERS = [
    ("0b1a27bd-bb98-44f8-8357-666fabfeabf0", "Aetherflux Reservoir"),  # variant 3718-5195
    ("371fa9e3-5432-4f2f-89d4-55061b0b4e57", "Fury Storm"),              # variant 2655-5195
    ("ea14c26b-bf2f-48b4-b879-6e63069ded1f", "Pyromancer Ascension"),    # variant 326-5195
    (HAZE_OF_RAGE_ORACLE, "Haze of Rage"),                                # variant 3940-5195
]


def _create_combo_insights_fixture_db(tmp_dir: Path) -> Path:
    tmp_dir.mkdir(parents=True, exist_ok=True)
    db_path = (tmp_dir / "deck_combo_insights_fixture.sqlite").resolve()
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
             "pytest_deck_combo_insights", "local://", "2026-01-01T00:00:00+00:00", "{}"),
        )
        rows = [
            # Storm-Kiln Artist (the deck-side combo half)
            (FIXTURE_SNAPSHOT_ID, STORM_KILN_ARTIST_ORACLE, "Storm-Kiln Artist",
             "{1}{R}{R}", 3.0, "Creature — Devil Artificer", "[\"R\"]", "[\"R\"]", "{}", "[]"),
        ]
        for oid, name in SKA_PARTNERS:
            rows.append((FIXTURE_SNAPSHOT_ID, oid, name,
                         "{R}", 1.0, "Sorcery", "[\"R\"]", "[\"R\"]", "{}", "[]"))
        # Commander (mono-red so all SKA partners are color-legal).
        rows.append((FIXTURE_SNAPSHOT_ID, "11111111-1111-1111-1111-111111111111",
                     "Krenko, Mob Boss", "{2}{R}{R}", 4.0,
                     "Legendary Creature — Goblin Warrior", "[\"R\"]", "[\"R\"]", "{}", "[]"))
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


class DeckComboInsightsV1Tests(unittest.TestCase):
    _tmp_dir_ctx: tempfile.TemporaryDirectory[str] | None = None
    _db_env_ctx = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._tmp_dir_ctx = tempfile.TemporaryDirectory()
        db_path = _create_combo_insights_fixture_db(Path(cls._tmp_dir_ctx.name))
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

    def test_detected_combo_when_both_halves_present(self) -> None:
        from api.engine.layers.deck_combo_insights_v1 import (
            compute_deck_combo_insights_v1,
        )

        result = compute_deck_combo_insights_v1(
            db_snapshot_id=FIXTURE_SNAPSHOT_ID,
            commander_names=["Krenko, Mob Boss"],
            deck_cards_after_completion=["Storm-Kiln Artist", "Haze of Rage"],
        )

        self.assertIsInstance(result, dict)
        detected = result.get("detected_combos_v1") or []
        missing = result.get("missing_partners_v1") or []

        # The detected combo for variant 3940-5195 must surface.
        ska_haze_entries = [
            e for e in detected
            if isinstance(e, dict)
            and {e.get("card_a_name"), e.get("card_b_name")}
                == {"Storm-Kiln Artist", "Haze of Rage"}
        ]
        self.assertEqual(
            len(ska_haze_entries), 1,
            f"Expected exactly one detected_combos entry for the SKA + Haze pair; "
            f"got {len(ska_haze_entries)}. detected_combos_v1={detected!r}",
        )
        entry = ska_haze_entries[0]
        self.assertEqual(entry.get("variant_id"), TARGET_VARIANT_ID)
        self.assertIn("Infinite", entry.get("combo_outcome_label") or "")
        self.assertIn("storm count", entry.get("combo_outcome_label") or "")

        # When BOTH halves are in the deck, the SKA+Haze pair must NOT
        # appear in missing_partners_v1 (it's not missing — it's there).
        for m in missing:
            if not isinstance(m, dict):
                continue
            self.assertNotEqual(
                {m.get("present_card_name"), m.get("partner_card_name")},
                {"Storm-Kiln Artist", "Haze of Rage"},
                f"SKA+Haze should not be in missing_partners_v1 when both halves present; "
                f"missing entry = {m!r}",
            )

    def test_missing_partners_when_only_one_half_present(self) -> None:
        from api.engine.layers.deck_combo_insights_v1 import (
            compute_deck_combo_insights_v1,
        )

        result = compute_deck_combo_insights_v1(
            db_snapshot_id=FIXTURE_SNAPSHOT_ID,
            commander_names=["Krenko, Mob Boss"],
            deck_cards_after_completion=["Storm-Kiln Artist"],
        )

        detected = result.get("detected_combos_v1") or []
        missing = result.get("missing_partners_v1") or []

        # No combo half pairing yet (only Storm-Kiln Artist present).
        self.assertEqual(
            detected, [],
            f"With only one half present, detected_combos_v1 should be empty. got={detected!r}",
        )

        # Storm-Kiln Artist should surface ALL 4 of its known partners
        # (the outcome pack carries exactly 4 SKA variants).
        ska_missing = [
            m for m in missing
            if isinstance(m, dict)
            and m.get("present_card_name") == "Storm-Kiln Artist"
        ]
        self.assertGreaterEqual(
            len(ska_missing), 4,
            f"Expected ≥4 SKA missing-partner entries; got {len(ska_missing)}. "
            f"missing entries: {[m.get('partner_card_name') for m in ska_missing]}",
        )

        # Each missing entry carries the structured payload the UI panel
        # needs (variant_id + outcome label + partner identifiers).
        for m in ska_missing:
            self.assertIn("variant_id", m)
            self.assertIn("combo_outcome_label", m)
            self.assertIn("partner_card_oracle_id", m)
            self.assertIn("partner_card_name", m)
            self.assertIn("present_card_oracle_id", m)
            self.assertEqual(m.get("present_card_name"), "Storm-Kiln Artist")


if __name__ == "__main__":
    unittest.main()
