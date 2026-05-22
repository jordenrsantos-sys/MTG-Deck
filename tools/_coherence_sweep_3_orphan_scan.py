"""Coherence Sweep #3 Phase 8 — orphan code detection.

Walks the repo, parses every .py file's imports via AST, and flags files
in api/engine/layers/ + extractors/ + integrations/ + tools/ that nothing
imports.

NOT part of production — written only for the sweep. Safe to delete after.
"""
from __future__ import annotations

import ast
import os
import sys
from pathlib import Path


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    os.chdir(repo_root)

    files_examined: dict[str, str] = {}
    for root, _, files in os.walk("."):
        norm = root.replace("\\", "/")
        if ".venv" in norm or "__pycache__" in norm or "/.git" in norm:
            continue
        for f in files:
            if not f.endswith(".py"):
                continue
            p = os.path.join(root, f).replace("\\", "/").lstrip("./")
            files_examined[p] = p

    def module_path(p: str) -> str:
        rel = p.replace("\\", "/").lstrip("./")
        if rel.endswith(".py"):
            rel = rel[:-3]
        return rel.replace("/", ".")

    import_users: dict[str, list[str]] = {}
    for src_path in files_examined:
        try:
            with open(src_path, encoding="utf-8") as fh:
                src = fh.read()
            tree = ast.parse(src)
        except (SyntaxError, UnicodeDecodeError):
            continue
        for n in ast.walk(tree):
            if isinstance(n, ast.ImportFrom):
                if n.module:
                    import_users.setdefault(n.module, []).append(src_path)
            elif isinstance(n, ast.Import):
                for alias in n.names:
                    import_users.setdefault(alias.name, []).append(src_path)

    target_prefixes = (
        "api/engine/layers/",
        "api/engine/extractors/",
        "api/engine/integrations/",
        "tools/",
    )
    import_candidates: list[str] = []
    for src_path in files_examined:
        norm = src_path.replace("\\", "/").lstrip("./")
        if not any(norm.startswith(p) for p in target_prefixes):
            continue
        if norm.endswith("__init__.py"):
            continue
        module_full = module_path(norm)
        leaf = module_full.split(".")[-1]
        is_imported = False
        for k in import_users:
            if k == module_full:
                is_imported = True
                break
            k_leaf = k.split(".")[-1]
            if k_leaf == leaf:
                is_imported = True
                break
        if not is_imported:
            import_candidates.append(norm)

    print(f"Total .py files examined: {len(files_examined)}")
    print(f"Total imported module names: {len(import_users)}")
    print(f"Apparent orphan candidates in target dirs: {len(import_candidates)}")
    print()
    for c in sorted(import_candidates):
        print(f"  {c}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
