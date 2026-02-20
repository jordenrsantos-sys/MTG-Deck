from __future__ import annotations

import re
import unittest
from pathlib import Path


_FORBIDDEN_IMPORT_TOKENS = (
    "tools",
    "snapshot_build",
    "taxonomy_source",
)

_IMPORT_RE = re.compile(r"^\s*(?:from|import)\s+([^\s]+)")


class RuntimeImportIsolationTests(unittest.TestCase):
    def test_runtime_modules_do_not_import_update_tooling_paths(self) -> None:
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
                rel_path = py_path.relative_to(repo_root).as_posix()
                text = py_path.read_text(encoding="utf-8")

                for line_number, line in enumerate(text.splitlines(), start=1):
                    match = _IMPORT_RE.match(line)
                    if match is None:
                        continue

                    import_target = match.group(1)
                    normalized = import_target.replace("/", ".").replace("\\", ".")

                    if any(token in normalized.split(".") for token in _FORBIDDEN_IMPORT_TOKENS):
                        violations.append(f"{rel_path}:{line_number}: {line.strip()}")

        self.assertFalse(
            violations,
            "Runtime import isolation violation(s) detected:\n- " + "\n- ".join(sorted(violations)),
        )


if __name__ == "__main__":
    unittest.main()
