"""Mega-task v3 Phase 8 — notifier tests.

Verifies:
  - is_enabled gates on the env var
  - compose_notification produces expected title/body
  - notify(disabled) returns status='disabled' with no side effects
  - notify(enabled) writes the file always
  - notify(enabled) silently falls back when desktop toast fails
"""
from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from api.engine.integrations import new_set_notifier_v1 as notifier


class IsEnabledTests(unittest.TestCase):
    def test_returns_false_when_unset(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(notifier.is_enabled())

    def test_returns_true_for_truthy_values(self) -> None:
        for val in ("1", "true", "yes", "on", "True", "YES"):
            with patch.dict(os.environ,
                            {notifier.ENABLED_ENV_VAR: val}, clear=True):
                self.assertTrue(notifier.is_enabled(),
                                msg=f"value {val!r} should be enabled")

    def test_returns_false_for_falsy_values(self) -> None:
        for val in ("0", "false", "no", "off", ""):
            with patch.dict(os.environ,
                            {notifier.ENABLED_ENV_VAR: val}, clear=True):
                self.assertFalse(notifier.is_enabled(),
                                 msg=f"value {val!r} should be disabled")


class ComposeNotificationTests(unittest.TestCase):
    def test_basic_shape(self) -> None:
        n = notifier.compose_notification(
            set_code="tst", set_name="Test Set", card_count=10,
            top_archetypes=["aristocrats", "blink", "storm"],
            report_path="NEW_SETS/2026-05-01_tst_test-set.md",
        )
        self.assertEqual(n.set_code, "tst")
        self.assertEqual(n.card_count, 10)
        self.assertIn("MTG set processed", n.title)
        self.assertIn("Test Set", n.title)
        self.assertIn("aristocrats", n.body)
        self.assertIn("10 new cards", n.body)

    def test_truncates_to_top_3_archetypes(self) -> None:
        n = notifier.compose_notification(
            set_code="tst", set_name="Test Set", card_count=10,
            top_archetypes=["a", "b", "c", "d", "e"],
            report_path="x",
        )
        self.assertEqual(len(n.top_archetypes), 3)

    def test_empty_archetypes_graceful(self) -> None:
        n = notifier.compose_notification(
            set_code="tst", set_name="Test Set", card_count=5,
            top_archetypes=[],
            report_path="x",
        )
        self.assertNotIn(",", n.body)   # no archetype list to comma-separate


class NotifyTests(unittest.TestCase):
    def setUp(self) -> None:
        # Each test gets a fresh temp notifications dir.
        self._td = tempfile.TemporaryDirectory()
        self._orig_dir = notifier.NOTIFICATIONS_DIR
        notifier.NOTIFICATIONS_DIR = Path(self._td.name)

    def tearDown(self) -> None:
        notifier.NOTIFICATIONS_DIR = self._orig_dir
        self._td.cleanup()

    def _mk(self):
        return notifier.compose_notification(
            "tst", "Test Set", 10, ["a", "b"], "x",
        )

    def test_disabled_when_env_var_not_set(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            r = notifier.notify(self._mk(), allow_desktop_toast=False)
            self.assertEqual(r.status, "disabled")
            self.assertEqual(r.actions, [])
            self.assertIsNone(r.file_path)

    def test_enabled_writes_file_and_skips_toast(self) -> None:
        with patch.dict(os.environ,
                        {notifier.ENABLED_ENV_VAR: "1"}, clear=True):
            r = notifier.notify(self._mk(), allow_desktop_toast=False)
            self.assertIn(r.status, ("file_only", "ok"))
            self.assertIsNotNone(r.file_path)
            # File written.
            self.assertTrue(Path(r.file_path).is_file())

    def test_toast_failure_falls_back_to_file_only(self) -> None:
        with patch.dict(os.environ,
                        {notifier.ENABLED_ENV_VAR: "1"}, clear=True), \
             patch.object(notifier, "_try_desktop_toast",
                          return_value="mock failure"):
            r = notifier.notify(self._mk(), allow_desktop_toast=True)
            self.assertEqual(r.status, "file_only")
            self.assertTrue(any("toast skipped" in w for w in r.warnings))


if __name__ == "__main__":
    unittest.main()
