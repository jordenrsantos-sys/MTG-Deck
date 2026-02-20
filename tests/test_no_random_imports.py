from __future__ import annotations

import re
import unittest
from pathlib import Path


_RANDOM_IMPORT_RE = re.compile(r"^\s*(?:import\s+random\b|from\s+random\s+import\b)")
_EXCLUDED_FILES = {
    "run_history_v0.py",  # persistence metadata logging; not part of deterministic build runtime output
}


class NoRandomImportsTests(unittest.TestCase):
    def test_runtime_modules_avoid_nondeterministic_time_and_random_usage(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        scan_roots = [
            repo_root / "api" / "engine",
            repo_root / "engine",
        ]

        violations: list[str] = []

        for scan_root in scan_roots:
            if not scan_root.is_dir():
                continue

            for py_path in sorted(scan_root.rglob("*.py")):
                if py_path.name in _EXCLUDED_FILES:
                    continue

                rel_path = py_path.relative_to(repo_root).as_posix()
                text = py_path.read_text(encoding="utf-8")
                lines = text.splitlines()

                for line_number, line in enumerate(lines, start=1):
                    if _RANDOM_IMPORT_RE.search(line):
                        violations.append(f"{rel_path}:{line_number}: {line.strip()}")
                    if "datetime.now(" in line:
                        violations.append(f"{rel_path}:{line_number}: {line.strip()}")
                    if "time.time(" in line:
                        violations.append(f"{rel_path}:{line_number}: {line.strip()}")

        self.assertFalse(
            violations,
            "Detected non-deterministic runtime usage in engine modules:\n- " + "\n- ".join(sorted(violations)),
        )


if __name__ == "__main__":
    unittest.main()
