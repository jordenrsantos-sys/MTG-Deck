"""v1.7.3 Stage 1 — Proactive combo completion engine layer.

v1.7 Stage 2's `combo_enabler_reasons_v1` only annotates rows the
engine already added (near-zero production coverage per the 2026-05-16
Cowork browser-walk). v1.7.3 closes the loop: when the deck contains
one half of a known 2-card combo, this layer proactively adds the
partner — bracket-gated so B1/B2 never see proactive adds (combos
DISALLOW per bracket_rules_v2.json policy).

Test fixture: Krenko, Mob Boss (mono-red) commander + Storm-Kiln
Artist (`a145ff8c-5812-4bcb-bd16-9839dc25121d`) in the deck. Storm-Kiln
Artist has 4 v2 partners with outcomes in the Stage 1.5 pack:

    326-5195   → Pyromancer Ascension (red)
    2655-5195  → Fury Storm           (red)
    3718-5195  → Aetherflux Reservoir (colorless)
    3940-5195  → Haze of Rage         (red)

All 4 are color-legal under mono-red Krenko (3 red + 1 colorless).

Bracket scaling:
    B1 → 0 (cap)
    B2 → 0 (cap)
    B3 → 1
    B4 → min(2, 4) = 2
    B5 → min(3, 4) = 3
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


FIXTURE_SNAPSHOT_ID = "PROACTIVE_COMBO_TEST_SNAPSHOT"

KRENKO_ORACLE = "11111111-1111-1111-1111-111111111111"
STORM_KILN_ARTIST_ORACLE = "a145ff8c-5812-4bcb-bd16-9839dc25121d"

# Storm-Kiln Artist's 4 partners (oracle_ids confirmed via spellbook
# variants endpoint during v1.7.2 + v1.7.3 Stage 0 audit). Color
# identity = red ("R") so all 4 are color-legal under mono-red Krenko.
SKA_PARTNERS = [
    ("0b1a27bd-bb98-44f8-8357-666fabfeabf0", "Aetherflux Reservoir"),  # variant 3718-5195
    ("371fa9e3-5432-4f2f-89d4-55061b0b4e57", "Fury Storm"),             # variant 2655-5195
    ("ea14c26b-bf2f-48b4-b879-6e63069ded1f", "Pyromancer Ascension"),   # variant 326-5195
    ("f17d0fb8-c157-43b8-be26-f5ba4c6aed14", "Haze of Rage"),           # variant 3940-5195
]


def _create_proactive_combo_fixture_db(tmp_dir: Path) -> Path:
    tmp_dir.mkdir(parents=True, exist_ok=True)
    db_path = (tmp_dir / "proactive_combo_fixture.sqlite").resolve()
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
             "pytest_proactive_combo", "local://", "2026-01-01T00:00:00+00:00", "{}"),
        )
        rows = [
            # Mono-red Krenko commander
            (FIXTURE_SNAPSHOT_ID, KRENKO_ORACLE, "Krenko, Mob Boss",
             "{2}{R}{R}", 4.0, "Legendary Creature — Goblin Warrior",
             "[\"R\"]", "[\"R\"]", "{}", "[]"),
            # Storm-Kiln Artist (red deck half)
            (FIXTURE_SNAPSHOT_ID, STORM_KILN_ARTIST_ORACLE, "Storm-Kiln Artist",
             "{1}{R}{R}", 3.0, "Creature — Devil Artificer",
             "[\"R\"]", "[\"R\"]", "{}", "[]"),
        ]
        for oid, name in SKA_PARTNERS:
            rows.append((FIXTURE_SNAPSHOT_ID, oid, name,
                         "{R}", 1.0, "Sorcery", "[\"R\"]", "[\"R\"]", "{}", "[]"))
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


class ProactiveComboCompletionV1Tests(unittest.TestCase):
    _tmp_dir_ctx: tempfile.TemporaryDirectory[str] | None = None
    _db_env_ctx = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._tmp_dir_ctx = tempfile.TemporaryDirectory()
        db_path = _create_proactive_combo_fixture_db(Path(cls._tmp_dir_ctx.name))
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

    def _propose(self, bracket_id: str) -> list[dict]:
        from api.engine.layers.proactive_combo_completion_v1 import (
            propose_proactive_combo_partners_v1,
        )
        return propose_proactive_combo_partners_v1(
            db_snapshot_id=FIXTURE_SNAPSHOT_ID,
            commander_names=["Krenko, Mob Boss"],
            deck_cards=["Storm-Kiln Artist"],
            current_added_cards_v1=[],
            bracket_id=bracket_id,
            commander_color_identity={"R"},
        )

    def test_b1_emits_zero_proactive_combo_partners(self) -> None:
        proposals = self._propose("B1")
        self.assertEqual(
            proposals, [],
            f"B1 bracket cap (combos DISALLOW); expected 0 proactive proposals; got {len(proposals)}",
        )

    def test_b2_emits_zero_proactive_combo_partners(self) -> None:
        proposals = self._propose("B2")
        self.assertEqual(
            proposals, [],
            f"B2 bracket cap (combos DISALLOW); expected 0 proactive proposals; got {len(proposals)}",
        )

    def test_b3_emits_at_least_one_proactive_combo_partner(self) -> None:
        proposals = self._propose("B3")
        self.assertGreaterEqual(
            len(proposals), 1,
            f"B3 should propose ≥1 proactive partner for SKA; got {len(proposals)}. "
            f"proposals={proposals!r}",
        )
        first = proposals[0]
        self.assertEqual(first.get("present_card_name"), "Storm-Kiln Artist")
        self.assertEqual(first.get("present_card_oracle_id"), STORM_KILN_ARTIST_ORACLE)
        self.assertIn("partner_card_name", first)
        self.assertIn("partner_card_oracle_id", first)
        self.assertIn("variant_id", first)
        self.assertIn("combo_outcome_label", first)
        # Each proposed partner must be one of SKA's 4 known partners.
        known_partner_oids = {oid for oid, _ in SKA_PARTNERS}
        self.assertIn(first["partner_card_oracle_id"], known_partner_oids)

    def test_b4_emits_at_least_two_proactive_combo_partners(self) -> None:
        proposals = self._propose("B4")
        self.assertGreaterEqual(
            len(proposals), 2,
            f"B4 should propose ≥2 proactive partners (cap=min(2, n_candidates=4)); got {len(proposals)}",
        )
        # All proposals must be unique partner oracle_ids.
        partner_oids = [p.get("partner_card_oracle_id") for p in proposals]
        self.assertEqual(
            len(partner_oids), len(set(partner_oids)),
            f"Proposals must dedupe by partner_card_oracle_id; got duplicates: {partner_oids!r}",
        )

    def test_b5_emits_at_least_three_proactive_combo_partners(self) -> None:
        proposals = self._propose("B5")
        self.assertGreaterEqual(
            len(proposals), 3,
            f"B5 should propose ≥3 proactive partners (cap=min(3, n_candidates=4)); got {len(proposals)}",
        )
        # Deterministic sort by variant_id ascending (no popularity
        # field in outcome pack — see Stage 0 audit). The first three
        # SKA variants by lex ascending are: 2655-5195, 326-5195, 3718-5195.
        # NOTE: lex string sort puts "2655" before "326" because '2' < '3'.
        variant_ids = [p.get("variant_id") for p in proposals]
        self.assertEqual(sorted(variant_ids), variant_ids,
            f"Proposals must be sorted ascending by variant_id; got {variant_ids!r}")


if __name__ == "__main__":
    unittest.main()
