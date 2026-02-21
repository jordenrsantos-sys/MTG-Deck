from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from api.engine.decklist_parse_v1 import parse_decklist_text
from api.engine.decklist_resolve_v1 import DECKLIST_RESOLVE_VERSION, resolve_parsed_decklist
from tests.decklist_fixture_harness import (
    DECKLIST_FIXTURE_SNAPSHOT_ID,
    create_decklist_fixture_db,
    set_decklist_fixture_env,
)


class DecklistResolveV1Tests(unittest.TestCase):
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

    def test_resolve_exact_and_normalized_and_alias(self) -> None:
        parsed = parse_decklist_text(
            """
1 Sol Ring
1 Arcane   Signet
1 Signet of Arcana
"""
        )

        payload = resolve_parsed_decklist(parsed, DECKLIST_FIXTURE_SNAPSHOT_ID)

        self.assertEqual(payload.get("version"), DECKLIST_RESOLVE_VERSION)
        self.assertEqual(payload.get("status"), "OK")
        self.assertEqual(payload.get("unknowns"), [])
        self.assertEqual(
            payload.get("resolved_cards"),
            [
                {
                    "oracle_id": "ORA_RING_001",
                    "name": "Sol Ring",
                    "count": 1,
                    "source_line_no": 2,
                },
                {
                    "oracle_id": "ORA_SIGNET_001",
                    "name": "Arcane Signet",
                    "count": 1,
                    "source_line_no": 3,
                },
                {
                    "oracle_id": "ORA_SIGNET_001",
                    "name": "Arcane Signet",
                    "count": 1,
                    "source_line_no": 4,
                },
            ],
        )

    def test_resolve_ambiguous_and_missing_with_stable_candidate_order(self) -> None:
        parsed = parse_decklist_text(
            """
1 Twin Name
1 Missing Card
"""
        )

        first = resolve_parsed_decklist(parsed, DECKLIST_FIXTURE_SNAPSHOT_ID)
        second = resolve_parsed_decklist(parsed, DECKLIST_FIXTURE_SNAPSHOT_ID)

        self.assertEqual(first, second)
        self.assertEqual(first.get("status"), "UNKNOWN_PRESENT")
        self.assertEqual(first.get("resolved_cards"), [])

        unknowns = first.get("unknowns") if isinstance(first.get("unknowns"), list) else []
        self.assertEqual(len(unknowns), 2)

        self.assertEqual(
            unknowns[0],
            {
                "name_raw": "Twin Name",
                "name_norm": "twin name",
                "count": 1,
                "line_no": 2,
                "reason_code": "CARD_NAME_AMBIGUOUS",
                "candidates": [
                    {"oracle_id": "ORA_AMB_001", "name": "Twin Name"},
                    {"oracle_id": "ORA_AMB_002", "name": "twin name"},
                ],
            },
        )
        self.assertEqual(
            unknowns[1],
            {
                "name_raw": "Missing Card",
                "name_norm": "missing card",
                "count": 1,
                "line_no": 3,
                "reason_code": "CARD_NOT_FOUND",
                "candidates": [],
            },
        )


if __name__ == "__main__":
    unittest.main()
