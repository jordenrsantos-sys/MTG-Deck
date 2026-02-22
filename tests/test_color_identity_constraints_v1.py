from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from api.engine.color_identity_constraints_v1 import (
    COLOR_IDENTITY_UNAVAILABLE,
    UNKNOWN_COLOR_IDENTITY,
    VERSION,
    get_commander_color_identity_union_v1,
    get_commander_color_identity_v1,
    is_card_color_legal_v1,
)
from tests.guardrails_fixture_harness import (
    GUARDRAILS_FIXTURE_SNAPSHOT_ID,
    create_guardrails_fixture_db,
    set_guardrails_fixture_env,
)


class ColorIdentityConstraintsV1Tests(unittest.TestCase):
    _tmp_dir_ctx: tempfile.TemporaryDirectory[str] | None = None
    _db_env_ctx = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._tmp_dir_ctx = tempfile.TemporaryDirectory()
        db_path = create_guardrails_fixture_db(Path(cls._tmp_dir_ctx.name))
        cls._db_env_ctx = set_guardrails_fixture_env(db_path)
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

    def test_legal_card_passes(self) -> None:
        commander_colors = get_commander_color_identity_v1(
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            commander_name="Niv-Mizzet, Parun",
        )

        self.assertEqual(VERSION, "color_identity_constraints_v1")
        self.assertEqual(commander_colors, {"U", "R"})

        legal = is_card_color_legal_v1(
            card_name="Arcane Signet",
            commander_color_set=commander_colors,
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
        )
        self.assertIs(legal, True)

    def test_illegal_card_blocked(self) -> None:
        commander_colors = get_commander_color_identity_v1(
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            commander_name="Niv-Mizzet, Parun",
        )

        illegal = is_card_color_legal_v1(
            card_name="Cultivate",
            commander_color_set=commander_colors,
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
        )
        self.assertIs(illegal, False)

    def test_missing_identity_returns_unknown(self) -> None:
        commander_colors = get_commander_color_identity_v1(
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            commander_name="Niv-Mizzet, Parun",
        )

        unknown = is_card_color_legal_v1(
            card_name="Mystery Card",
            commander_color_set=commander_colors,
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
        )
        self.assertEqual(unknown, UNKNOWN_COLOR_IDENTITY)

    def test_missing_commander_identity_returns_unavailable_code(self) -> None:
        unavailable = get_commander_color_identity_v1(
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            commander_name="Commander That Does Not Exist",
        )
        self.assertEqual(unavailable, COLOR_IDENTITY_UNAVAILABLE)

    def test_partner_union_color_identity(self) -> None:
        colors = get_commander_color_identity_union_v1(
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            commander_names=["Esior, Wardwing Familiar", "Ishai, Ojutai Dragonspeaker"],
        )
        self.assertEqual(colors, {"W", "U"})


if __name__ == "__main__":
    unittest.main()
