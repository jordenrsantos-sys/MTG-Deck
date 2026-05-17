"""v1.7.3 Stage 2 — Pipeline integration: proactive combo cascade.

When the proactive layer adds a combo partner:
  1. The row lands in `added_cards_v1` with `reasons_v1` including
     `PROACTIVE_COMBO_TARGET`.
  2. The existing `attach_combo_enabler_reasons_v1` then appends a
     `COMBO_ENABLER:<json>` tagged-string reason to the SAME row
     (since the proactive partner now satisfies a v2 combo with
     a card present in the deck).
  3. The existing `compute_deck_combo_insights_v1` then surfaces the
     pair in `detected_combos_v1` (both halves present) and does NOT
     surface it in `missing_partners_v1` (no longer missing).

This test exercises the full cascade end-to-end via
`run_deck_complete_engine_v1` against the same Krenko + Storm-Kiln
Artist fixture used by Stage 1.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


FIXTURE_SNAPSHOT_ID = "PROACTIVE_COMBO_PIPELINE_TEST_SNAPSHOT"

KRENKO_ORACLE = "11111111-1111-1111-1111-111111111111"
STORM_KILN_ARTIST_ORACLE = "a145ff8c-5812-4bcb-bd16-9839dc25121d"
SKA_PARTNERS = [
    ("0b1a27bd-bb98-44f8-8357-666fabfeabf0", "Aetherflux Reservoir"),
    ("371fa9e3-5432-4f2f-89d4-55061b0b4e57", "Fury Storm"),
    ("ea14c26b-bf2f-48b4-b879-6e63069ded1f", "Pyromancer Ascension"),
    ("f17d0fb8-c157-43b8-be26-f5ba4c6aed14", "Haze of Rage"),
]
COMBO_ENABLER_REASON_PREFIX = "COMBO_ENABLER:"
PROACTIVE_COMBO_REASON_CODE = "PROACTIVE_COMBO_TARGET"


def _create_pipeline_fixture_db(tmp_dir: Path) -> Path:
    tmp_dir.mkdir(parents=True, exist_ok=True)
    db_path = (tmp_dir / "proactive_pipeline_fixture.sqlite").resolve()
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
             "pytest_proactive_pipeline", "local://", "2026-01-01T00:00:00+00:00", "{}"),
        )
        rows = [
            (FIXTURE_SNAPSHOT_ID, KRENKO_ORACLE, "Krenko, Mob Boss",
             "{2}{R}{R}", 4.0, "Legendary Creature — Goblin Warrior",
             "[\"R\"]", "[\"R\"]", "{}", "[]"),
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


def _baseline_build_result() -> dict:
    """Minimal OK-status baseline so run_deck_complete_engine_v1
    proceeds past its early-return guards. The proactive layer does
    NOT depend on the baseline's primitive counts — it scans the
    final added_cards + deck."""
    return {
        "status": "OK",
        "deck_size_total": 1,
        "result": {
            "deck_cards_canonical_input_order": [
                {"slot_id": "C0", "resolved_name": "Krenko, Mob Boss",
                 "resolved_oracle_id": KRENKO_ORACLE, "status": "PLAYABLE"},
                {"slot_id": "S0", "resolved_name": "Storm-Kiln Artist",
                 "resolved_oracle_id": STORM_KILN_ARTIST_ORACLE, "status": "PLAYABLE"},
            ],
            "primitive_index_by_slot": {
                "C0": ["COMMANDER_ENGINE"], "S0": [],
            },
            "structural_snapshot_v1": {
                "dead_slot_ids_v1": [], "missing_primitives_v1": [],
                "primitive_counts_by_id": {},
                "primitive_concentration_index_v1": 0.0,
                "structural_health_summary_v1": {
                    "dead_slot_count": 0, "missing_required_count": 0,
                },
            },
            "required_primitives_v0": [],
            "redundancy_index_v1": {"per_requirement": []},
            "resilience_math_engine_v1": {"metrics": {
                "engine_continuity_after_removal": 0.5, "rebuild_after_wipe": 0.5,
            }},
            "engine_coherence_v1": {"metrics": {"overlap_score": 0.1}},
            "profile_bracket_enforcement_v1": {"counts": {"game_changers_in_deck": 0}},
        },
    }


class ProactiveComboPipelineCascadeTests(unittest.TestCase):
    _tmp_dir_ctx: tempfile.TemporaryDirectory[str] | None = None
    _db_env_ctx = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._tmp_dir_ctx = tempfile.TemporaryDirectory()
        db_path = _create_pipeline_fixture_db(Path(cls._tmp_dir_ctx.name))
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

    def test_b3_cascade_proactive_then_combo_enabler_then_insights(self) -> None:
        from api.engine.deck_complete_engine_v1 import run_deck_complete_engine_v1

        payload = run_deck_complete_engine_v1(
            canonical_deck_input={
                "db_snapshot_id": FIXTURE_SNAPSHOT_ID,
                "profile_id": "focused",
                "bracket_id": "B3",
                "format": "commander",
                "commander": "Krenko, Mob Boss",
                "cards": ["Storm-Kiln Artist"],
                "engine_patches_v0": [],
            },
            baseline_build_result=_baseline_build_result(),
            db_snapshot_id=FIXTURE_SNAPSHOT_ID,
            bracket_id="B3",
            profile_id="focused",
            mulligan_model_id="NORMAL",
            target_deck_size=5,  # give the engine enough slots to
                                 # reach the post-backfill pipeline
                                 # tail where proactive runs (the
                                 # slots_needed<=0 early-return at
                                 # line 651 would otherwise skip it)
            max_adds=10,
            allow_basic_lands=False,
            land_target_mode="OFF",
        )
        added = payload.get("added_cards_v1") or []
        # 1) PROACTIVE_COMBO_TARGET appears on at least one added row.
        proactive_rows = [
            r for r in added
            if isinstance(r, dict) and PROACTIVE_COMBO_REASON_CODE in (r.get("reasons_v1") or [])
        ]
        self.assertGreaterEqual(
            len(proactive_rows), 1,
            f"B3 cascade should emit ≥1 PROACTIVE_COMBO_TARGET row; got {len(proactive_rows)}. "
            f"added_cards_v1 names = {[r.get('name') for r in added]}",
        )

        # 2) Combo enabler then chips the same row with COMBO_ENABLER:<json>.
        chipped = [
            r for r in proactive_rows
            if any(
                isinstance(s, str) and s.startswith(COMBO_ENABLER_REASON_PREFIX)
                for s in (r.get("reasons_v1") or [])
            )
        ]
        self.assertGreaterEqual(
            len(chipped), 1,
            f"Proactive rows must be re-annotated by combo_enabler_reasons_v1; "
            f"proactive_rows reasons = {[r.get('reasons_v1') for r in proactive_rows]}",
        )

        # 3) detected_combos_v1 contains the pair; missing_partners_v1 does NOT
        #    contain SKA+partner anymore.
        detected = payload.get("detected_combos_v1") or []
        missing = payload.get("missing_partners_v1") or []
        ska_in_detected = any(
            isinstance(e, dict)
            and "Storm-Kiln Artist" in {e.get("card_a_name"), e.get("card_b_name")}
            for e in detected
        )
        self.assertTrue(
            ska_in_detected,
            f"After proactive partner add, SKA's combo should be in detected_combos_v1. "
            f"detected={detected!r}",
        )
        ska_in_missing = [
            m for m in missing
            if isinstance(m, dict) and m.get("present_card_name") == "Storm-Kiln Artist"
        ]
        # All SKA partners should be NOT missing (the proactive add resolved
        # at least the chosen one). For B3 cap=1, 3 other SKA partners
        # are STILL missing → still in missing_partners_v1. So we assert
        # `len(missing for SKA) == 4 - cap = 3` (defensive ≥0 + ≤3).
        self.assertLessEqual(
            len(ska_in_missing), 3,
            f"After proactive cap=1 add, SKA should have ≤3 remaining missing partners; "
            f"got {len(ska_in_missing)}. missing for SKA = "
            f"{[m.get('partner_card_name') for m in ska_in_missing]}",
        )

    def test_b2_cascade_emits_no_proactive_target(self) -> None:
        """Bracket cap sentinel: B2 must NOT receive proactive combo adds
        even when partner candidates exist."""
        from api.engine.deck_complete_engine_v1 import run_deck_complete_engine_v1

        payload = run_deck_complete_engine_v1(
            canonical_deck_input={
                "db_snapshot_id": FIXTURE_SNAPSHOT_ID,
                "profile_id": "focused",
                "bracket_id": "B2",
                "format": "commander",
                "commander": "Krenko, Mob Boss",
                "cards": ["Storm-Kiln Artist"],
                "engine_patches_v0": [],
            },
            baseline_build_result=_baseline_build_result(),
            db_snapshot_id=FIXTURE_SNAPSHOT_ID,
            bracket_id="B2",
            profile_id="focused",
            mulligan_model_id="NORMAL",
            target_deck_size=5,  # match B3 — engine reaches pipeline tail
            max_adds=10,
            allow_basic_lands=False,
            land_target_mode="OFF",
        )
        added = payload.get("added_cards_v1") or []
        proactive_rows = [
            r for r in added
            if isinstance(r, dict) and PROACTIVE_COMBO_REASON_CODE in (r.get("reasons_v1") or [])
        ]
        self.assertEqual(
            len(proactive_rows), 0,
            f"B2 bracket cap (combos DISALLOW); got {len(proactive_rows)} proactive rows: "
            f"{[r.get('name') for r in proactive_rows]}",
        )


if __name__ == "__main__":
    unittest.main()
