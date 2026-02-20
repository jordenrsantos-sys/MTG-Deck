from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import api.engine.bracket_gc_limits as bracket_gc_limits


class BracketGcLimitsV1Tests(unittest.TestCase):
    def test_known_brackets_resolve_expected_limits(self) -> None:
        self.assertEqual(
            bracket_gc_limits.resolve_gc_limits("B1"),
            (0, 0, "gc_limits_v1", False),
        )
        self.assertEqual(
            bracket_gc_limits.resolve_gc_limits("B2"),
            (0, 0, "gc_limits_v1", False),
        )
        self.assertEqual(
            bracket_gc_limits.resolve_gc_limits("B3"),
            (1, 3, "gc_limits_v1", False),
        )

    def test_b4_and_b5_are_unlimited(self) -> None:
        self.assertEqual(
            bracket_gc_limits.resolve_gc_limits("B4"),
            (None, None, "gc_limits_v1", False),
        )
        self.assertEqual(
            bracket_gc_limits.resolve_gc_limits("B5"),
            (None, None, "gc_limits_v1", False),
        )

    def test_unknown_bracket_is_unlimited_with_unknown_flag(self) -> None:
        self.assertEqual(
            bracket_gc_limits.resolve_gc_limits("not_a_bracket"),
            (None, None, "gc_limits_v1", True),
        )

    def test_repeated_resolve_calls_are_deterministic(self) -> None:
        first = bracket_gc_limits.resolve_gc_limits("B3")
        second = bracket_gc_limits.resolve_gc_limits("B3")
        self.assertEqual(first, second)

    def test_missing_gc_limits_file_raises_deterministic_runtime_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            missing = Path(tmp_dir) / "missing_gc_limits.json"
            with patch.object(bracket_gc_limits, "_GC_LIMITS_FILE", missing):
                with self.assertRaises(RuntimeError) as first_exc:
                    bracket_gc_limits.load_gc_limits_v1()
                with self.assertRaises(RuntimeError) as second_exc:
                    bracket_gc_limits.load_gc_limits_v1()

        first_message = str(first_exc.exception)
        second_message = str(second_exc.exception)

        self.assertEqual(first_message, second_message)
        self.assertIn("GC_LIMITS_V1_MISSING", first_message)
        self.assertIn(str(missing), first_message)


if __name__ == "__main__":
    unittest.main()
