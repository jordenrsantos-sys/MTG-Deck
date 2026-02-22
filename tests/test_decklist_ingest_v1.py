from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from api.engine.decklist_ingest_v1 import (
    DECKLIST_INGEST_VERSION,
    build_canonical_deck_input_v1,
    compute_request_hash_v1,
    ingest_decklist,
)
from tests.decklist_fixture_harness import (
    DECKLIST_FIXTURE_SNAPSHOT_ID,
    create_decklist_fixture_db,
    set_decklist_fixture_env,
)


class DecklistIngestV1Tests(unittest.TestCase):
    _tmp_dir_ctx: tempfile.TemporaryDirectory[str] | None = None
    _db_env_ctx = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._tmp_dir_ctx = tempfile.TemporaryDirectory()
        db_path = create_decklist_fixture_db(Path(cls._tmp_dir_ctx.name))
        cls._db_env_ctx = set_decklist_fixture_env(db_path)
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

    def test_ingest_detects_single_commander_section_and_expands_counts(self) -> None:
        payload = ingest_decklist(
            raw_text="""
Commander
1 Krenko, Mob Boss
Deck
1 Sol Ring
Arcane Signet
""",
            db_snapshot_id=DECKLIST_FIXTURE_SNAPSHOT_ID,
            format="commander",
        )

        self.assertEqual(payload.get("version"), DECKLIST_INGEST_VERSION)
        self.assertEqual(payload.get("status"), "OK")
        self.assertEqual(payload.get("unknowns"), [])
        self.assertEqual(payload.get("violations_v1"), [])

        canonical = payload.get("canonical_deck_input") if isinstance(payload.get("canonical_deck_input"), dict) else {}
        self.assertEqual(canonical.get("format"), "commander")
        self.assertEqual(canonical.get("commander"), "Krenko, Mob Boss")
        self.assertEqual(canonical.get("cards"), ["Sol Ring", "Arcane Signet"])

    def test_ingest_reports_commander_missing_when_not_labeled(self) -> None:
        payload = ingest_decklist(
            raw_text="""
Sol Ring
Arcane Signet
""",
            db_snapshot_id=DECKLIST_FIXTURE_SNAPSHOT_ID,
            format="commander",
        )

        self.assertEqual(payload.get("status"), "UNKNOWN_PRESENT")
        unknowns = payload.get("unknowns") if isinstance(payload.get("unknowns"), list) else []
        reason_codes = [entry.get("reason_code") for entry in unknowns if isinstance(entry, dict)]
        self.assertIn("COMMANDER_MISSING", reason_codes)
        self.assertEqual(payload.get("violations_v1"), [])

    def test_ingest_supports_commander_override(self) -> None:
        payload = ingest_decklist(
            raw_text="""
1 Sol Ring
""",
            db_snapshot_id=DECKLIST_FIXTURE_SNAPSHOT_ID,
            format="commander",
            commander_name_override="Krenko, Mob Boss",
        )

        self.assertEqual(payload.get("status"), "OK")
        canonical = payload.get("canonical_deck_input") if isinstance(payload.get("canonical_deck_input"), dict) else {}
        self.assertEqual(canonical.get("commander"), "Krenko, Mob Boss")
        self.assertEqual(canonical.get("cards"), ["Sol Ring"])
        self.assertEqual(payload.get("violations_v1"), [])

    def test_ingest_supports_partner_commander_section(self) -> None:
        payload = ingest_decklist(
            raw_text="""
Commander
1 Esior, Wardwing Familiar
1 Ishai, Ojutai Dragonspeaker
Deck
1 Sol Ring
""",
            db_snapshot_id=DECKLIST_FIXTURE_SNAPSHOT_ID,
            format="commander",
        )

        self.assertEqual(payload.get("status"), "OK")
        self.assertEqual(payload.get("unknowns"), [])
        canonical = payload.get("canonical_deck_input") if isinstance(payload.get("canonical_deck_input"), dict) else {}
        self.assertEqual(canonical.get("commander"), "Esior, Wardwing Familiar")
        self.assertEqual(
            canonical.get("commander_list_v1"),
            ["Esior, Wardwing Familiar", "Ishai, Ojutai Dragonspeaker"],
        )
        self.assertEqual(canonical.get("cards"), ["Sol Ring"])

    def test_ingest_duplicate_basic_land_is_allowed(self) -> None:
        payload = ingest_decklist(
            raw_text="""
Commander
1 Krenko, Mob Boss
Deck
2x Plains
""",
            db_snapshot_id=DECKLIST_FIXTURE_SNAPSHOT_ID,
            format="commander",
        )

        self.assertEqual(payload.get("status"), "OK")
        self.assertEqual(payload.get("unknowns"), [])
        self.assertEqual(payload.get("violations_v1"), [])

    def test_ingest_duplicate_snow_covered_basic_land_is_allowed(self) -> None:
        payload = ingest_decklist(
            raw_text="""
Commander
1 Krenko, Mob Boss
Deck
2x Snow-Covered Plains
""",
            db_snapshot_id=DECKLIST_FIXTURE_SNAPSHOT_ID,
            format="commander",
        )

        self.assertEqual(payload.get("status"), "OK")
        self.assertEqual(payload.get("unknowns"), [])
        self.assertEqual(payload.get("violations_v1"), [])

    def test_ingest_duplicate_nonbasic_emits_violation(self) -> None:
        payload = ingest_decklist(
            raw_text="""
Commander
1 Krenko, Mob Boss
Deck
2x Sol Ring
""",
            db_snapshot_id=DECKLIST_FIXTURE_SNAPSHOT_ID,
            format="commander",
        )

        self.assertEqual(payload.get("status"), "OK")
        self.assertEqual(payload.get("unknowns"), [])
        self.assertEqual(
            payload.get("violations_v1"),
            [
                {
                    "code": "COMMANDER_DUPLICATE_NONBASIC",
                    "card_name": "Sol Ring",
                    "count": 2,
                    "line_nos": [5],
                    "message": "Commander duplicates are only allowed for basic lands (including snow-covered basics).",
                }
            ],
        )

    def test_ingest_override_resolves_ambiguous_name(self) -> None:
        payload = ingest_decklist(
            raw_text="""
Commander
1 Krenko, Mob Boss
Deck
1 Twin Name
""",
            db_snapshot_id=DECKLIST_FIXTURE_SNAPSHOT_ID,
            format="commander",
            name_overrides_v1=[
                {
                    "name_raw": "Twin Name",
                    "resolved_oracle_id": "ORA_AMB_001",
                }
            ],
        )

        self.assertEqual(payload.get("status"), "OK")
        self.assertEqual(payload.get("unknowns"), [])
        canonical = payload.get("canonical_deck_input") if isinstance(payload.get("canonical_deck_input"), dict) else {}
        self.assertEqual(canonical.get("cards"), ["Twin Name"])

    def test_ingest_invalid_override_marks_unknown_with_override_invalid(self) -> None:
        payload = ingest_decklist(
            raw_text="""
Commander
1 Krenko, Mob Boss
Deck
1 Twin Name
""",
            db_snapshot_id=DECKLIST_FIXTURE_SNAPSHOT_ID,
            format="commander",
            name_overrides_v1=[
                {
                    "name_raw": "Twin Name",
                    "resolved_oracle_id": "ORA_DOES_NOT_EXIST",
                }
            ],
        )

        self.assertEqual(payload.get("status"), "UNKNOWN_PRESENT")
        unknowns = payload.get("unknowns") if isinstance(payload.get("unknowns"), list) else []
        self.assertEqual(len(unknowns), 1)
        self.assertEqual(unknowns[0].get("name_raw"), "Twin Name")
        self.assertEqual(unknowns[0].get("reason_code"), "OVERRIDE_INVALID")

    def test_request_hash_changes_when_name_override_changes(self) -> None:
        without_override = build_canonical_deck_input_v1(
            db_snapshot_id=DECKLIST_FIXTURE_SNAPSHOT_ID,
            profile_id="focused",
            bracket_id="B2",
            format="commander",
            commander="Krenko, Mob Boss",
            cards=["Twin Name"],
            engine_patches_v0=[],
            name_overrides_v1=[],
        )
        with_override = build_canonical_deck_input_v1(
            db_snapshot_id=DECKLIST_FIXTURE_SNAPSHOT_ID,
            profile_id="focused",
            bracket_id="B2",
            format="commander",
            commander="Krenko, Mob Boss",
            cards=["Twin Name"],
            engine_patches_v0=[],
            name_overrides_v1=[
                {
                    "name_raw": "Twin Name",
                    "resolved_oracle_id": "ORA_AMB_001",
                }
            ],
        )

        self.assertNotEqual(compute_request_hash_v1(without_override), compute_request_hash_v1(with_override))

    def test_request_hash_same_input_is_stable(self) -> None:
        canonical = build_canonical_deck_input_v1(
            db_snapshot_id=DECKLIST_FIXTURE_SNAPSHOT_ID,
            profile_id="focused",
            bracket_id="B2",
            format="commander",
            commander="Krenko, Mob Boss",
            cards=["Sol Ring", "Arcane Signet"],
            engine_patches_v0=[],
        )

        first = compute_request_hash_v1(canonical)
        second = compute_request_hash_v1(canonical)

        self.assertEqual(first, second)
        self.assertEqual(len(first), 64)

    def test_request_hash_changes_when_card_order_changes(self) -> None:
        first = build_canonical_deck_input_v1(
            db_snapshot_id=DECKLIST_FIXTURE_SNAPSHOT_ID,
            profile_id="focused",
            bracket_id="B2",
            format="commander",
            commander="Krenko, Mob Boss",
            cards=["Sol Ring", "Arcane Signet"],
            engine_patches_v0=[],
        )
        second = build_canonical_deck_input_v1(
            db_snapshot_id=DECKLIST_FIXTURE_SNAPSHOT_ID,
            profile_id="focused",
            bracket_id="B2",
            format="commander",
            commander="Krenko, Mob Boss",
            cards=["Arcane Signet", "Sol Ring"],
            engine_patches_v0=[],
        )

        self.assertNotEqual(compute_request_hash_v1(first), compute_request_hash_v1(second))


if __name__ == "__main__":
    unittest.main()
