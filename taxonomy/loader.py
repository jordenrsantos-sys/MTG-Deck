from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from .pack_manifest import read_manifest, validate_manifest_hashes
from .schema import TaxonomyPack


REQUIRED_PACK_FILES = (
    "rulespec_rules.json",
    "rulespec_facets.json",
    "qa_rules.json",
)


def _load_json_file(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load(pack_folder: str | Path) -> TaxonomyPack:
    pack_path = Path(pack_folder).resolve()
    if not pack_path.exists() or not pack_path.is_dir():
        raise FileNotFoundError(f"Taxonomy pack folder not found: {pack_path}")

    manifest = read_manifest(pack_path)
    validate_manifest_hashes(pack_path, manifest)

    missing_required = [
        name
        for name in REQUIRED_PACK_FILES
        if not (pack_path / name).exists()
    ]
    if missing_required:
        raise ValueError(f"Taxonomy pack missing required files: {missing_required}")

    rulespec_rules_obj = _load_json_file(pack_path / "rulespec_rules.json")
    rulespec_facets_obj = _load_json_file(pack_path / "rulespec_facets.json")
    qa_rules_obj = _load_json_file(pack_path / "qa_rules.json")

    rulespec_rules = tuple(rulespec_rules_obj if isinstance(rulespec_rules_obj, list) else [])
    rulespec_facets = tuple(rulespec_facets_obj if isinstance(rulespec_facets_obj, list) else [])
    qa_rules = tuple(qa_rules_obj if isinstance(qa_rules_obj, list) else [])

    required_set = set(REQUIRED_PACK_FILES)
    other: Dict[str, Any] = {}
    for file_entry in manifest.files:
        file_name = file_entry.file_name
        if file_name == "pack_manifest.json":
            continue
        if file_name in required_set:
            continue
        file_path = pack_path / file_name
        try:
            other[file_name] = _load_json_file(file_path)
        except Exception:
            # Keep loader strict for required files but tolerant for optional auxiliary files.
            other[file_name] = None

    return TaxonomyPack(
        taxonomy_version=manifest.taxonomy_version,
        ruleset_version=manifest.taxonomy_version,
        pack_folder=pack_path,
        manifest=manifest,
        rulespec_rules=rulespec_rules,
        rulespec_facets=rulespec_facets,
        qa_rules=qa_rules,
        other_sheets=other,
    )
