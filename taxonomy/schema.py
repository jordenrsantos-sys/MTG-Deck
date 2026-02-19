from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Tuple


@dataclass(frozen=True)
class ManifestFileHash:
    file_name: str
    sha256: str
    size_bytes: int


@dataclass(frozen=True)
class PackManifest:
    taxonomy_version: str
    generated_at: str
    files: Tuple[ManifestFileHash, ...]


@dataclass(frozen=True)
class TaxonomyPack:
    taxonomy_version: str
    ruleset_version: str
    pack_folder: Path
    manifest: PackManifest
    rulespec_rules: Tuple[Dict[str, Any], ...]
    rulespec_facets: Tuple[Dict[str, Any], ...]
    qa_rules: Tuple[Dict[str, Any], ...]
    other_sheets: Mapping[str, Any]
