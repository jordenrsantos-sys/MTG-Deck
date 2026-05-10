"""Light-touch sanity check for repo/scripts/verify_release.* — Phase 6 Stage 9.

Verifies that the release-verification scripts exist + are non-empty +
declare the expected baseline constants. Does NOT execute them (integration
testing the bash + bat scripts is out of v1.0 scope; the scripts are meant
to be run by a human or CI runner against a fully-set-up environment).

Per autonomous_repair_log soft-safety #4: ships both bash + bat scripts;
the test covers both to catch the "missing platform-specific script" gap.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]


class ReleaseVerificationScriptTests(unittest.TestCase):
    def test_bash_script_exists_and_is_nonempty(self) -> None:
        script = _REPO_ROOT / "scripts" / "verify_release.sh"
        self.assertTrue(script.is_file(), f"missing: {script}")
        self.assertGreater(script.stat().st_size, 1000, "verify_release.sh suspiciously small (<1KB)")

    def test_bat_script_exists_and_is_nonempty(self) -> None:
        script = _REPO_ROOT / "scripts" / "verify_release.bat"
        self.assertTrue(script.is_file(), f"missing: {script}")
        self.assertGreater(script.stat().st_size, 1000, "verify_release.bat suspiciously small (<1KB)")

    def test_bash_script_declares_baseline_constants(self) -> None:
        """The bash script must reference the v1.0 baseline constants documented in ROLLBACK_PLAN.md."""
        script = _REPO_ROOT / "scripts" / "verify_release.sh"
        text = script.read_text(encoding="utf-8")
        self.assertIn("EXPECTED_PYTEST_MIN=747", text, "pytest baseline constant missing/wrong")
        self.assertIn("EXPECTED_VITEST_MIN=244", text, "vitest baseline constant missing/wrong")
        self.assertIn("EXPECTED_MANIFEST_SHA=", text, "manifest SHA constant missing")
        self.assertIn("a63ae1feec8b9e3a", text, "manifest SHA prefix v1.0 baseline missing")
        self.assertIn("EXPECTED_CORPUS_SHA=", text, "corpus SHA constant missing")
        self.assertIn("6d5c6af51ff7b", text, "corpus SHA prefix v1.0 baseline missing")

    def test_bat_script_declares_baseline_constants(self) -> None:
        """Same baseline-constant coverage on the Windows-native version."""
        script = _REPO_ROOT / "scripts" / "verify_release.bat"
        text = script.read_text(encoding="utf-8")
        self.assertIn("EXPECTED_PYTEST_MIN=747", text)
        self.assertIn("EXPECTED_VITEST_MIN=244", text)
        self.assertIn("a63ae1feec8b9e3a", text)
        self.assertIn("6d5c6af51ff7b", text)

    def test_bash_script_runs_the_seven_documented_checks(self) -> None:
        """Smoke check that the script structure mentions all 7 numbered steps."""
        script = _REPO_ROOT / "scripts" / "verify_release.sh"
        text = script.read_text(encoding="utf-8")
        for step in ("[1/7]", "[2/7]", "[3/7]", "[4/7]", "[5/7]", "[6/7]", "[7/7]"):
            self.assertIn(step, text, f"missing step marker {step} in verify_release.sh")

    def test_bat_script_runs_the_seven_documented_checks(self) -> None:
        script = _REPO_ROOT / "scripts" / "verify_release.bat"
        text = script.read_text(encoding="utf-8")
        for step in ("[1/7]", "[2/7]", "[3/7]", "[4/7]", "[5/7]", "[6/7]", "[7/7]"):
            self.assertIn(step, text, f"missing step marker {step} in verify_release.bat")

    def test_bash_script_exits_nonzero_on_any_failure(self) -> None:
        """Sanity: the script's summary block must `exit 1` when FAIL_COUNT > 0."""
        script = _REPO_ROOT / "scripts" / "verify_release.sh"
        text = script.read_text(encoding="utf-8")
        self.assertIn("RELEASE GATE: FAIL", text)
        self.assertIn("exit 1", text)
        self.assertIn("RELEASE GATE: PASS", text)
        self.assertIn("exit 0", text)


if __name__ == "__main__":
    unittest.main()
