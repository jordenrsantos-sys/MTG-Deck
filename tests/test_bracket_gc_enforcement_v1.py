from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from api.engine.bracket_gc_enforcement_v1 import (
    UNKNOWN_BRACKET_RULES,
    VERSION,
    count_game_changers_v1,
    would_violate_gc_limit_v1,
)
from api.engine.constants import GAME_CHANGERS_SET
from tests.guardrails_fixture_harness import (
    GUARDRAILS_FIXTURE_SNAPSHOT_ID,
    create_guardrails_fixture_db,
    set_guardrails_fixture_env,
)


class BracketGcEnforcementV1Tests(unittest.TestCase):
    _tmp_dir_ctx: tempfile.TemporaryDirectory[str] | None = None
    _db_env_ctx = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        if len(GAME_CHANGERS_SET) < 4:
            raise unittest.SkipTest("Need at least 4 local game changers for guardrail tests.")

        cls._tmp_dir_ctx = tempfile.TemporaryDirectory()
        db_path = create_guardrails_fixture_db(Path(cls._tmp_dir_ctx.name))
        cls._db_env_ctx = set_guardrails_fixture_env(db_path)
        cls._db_env_ctx.__enter__()

        cls.gc_names = sorted(name for name in GAME_CHANGERS_SET if isinstance(name, str))

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

    def test_b3_three_game_changers_allowed(self) -> None:
        current_cards = [self.gc_names[0], self.gc_names[1]]
        violation = would_violate_gc_limit_v1(
            candidate_card=self.gc_names[2],
            current_cards=current_cards,
            bracket_id="B3",
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
        )

        self.assertEqual(VERSION, "bracket_gc_enforcement_v1")
        self.assertIs(violation, False)

    def test_b3_fourth_game_changer_rejected(self) -> None:
        current_cards = [self.gc_names[0], self.gc_names[1], self.gc_names[2]]
        violation = would_violate_gc_limit_v1(
            candidate_card=self.gc_names[3],
            current_cards=current_cards,
            bracket_id="B3",
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
        )
        self.assertIs(violation, True)

    def test_non_gc_card_ignored(self) -> None:
        current_cards = [self.gc_names[0], self.gc_names[1], self.gc_names[2]]
        violation = would_violate_gc_limit_v1(
            candidate_card="Arcane Signet",
            current_cards=current_cards,
            bracket_id="B3",
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
        )

        self.assertIs(violation, False)
        self.assertEqual(
            count_game_changers_v1(current_cards, db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID),
            3,
        )

    def test_unavailable_bracket_rules_returns_unknown_rules_code(self) -> None:
        with patch("api.engine.bracket_gc_enforcement_v1.resolve_gc_limits", side_effect=RuntimeError("missing")):
            verdict = would_violate_gc_limit_v1(
                candidate_card=self.gc_names[0],
                current_cards=[],
                bracket_id="B3",
                db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            )
        self.assertEqual(verdict, UNKNOWN_BRACKET_RULES)


if __name__ == "__main__":
    unittest.main()
