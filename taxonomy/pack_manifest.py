from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

from .schema import ManifestFileHash, PackManifest


def stable_json_dumps(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True, ensure_ascii=False)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def manifest_to_dict(manifest: PackManifest) -> Dict[str, Any]:
    return {
        "taxonomy_version": manifest.taxonomy_version,
        "generated_at": manifest.generated_at,
        "files": [
            {
                "file_name": item.file_name,
                "sha256": item.sha256,
                "size_bytes": item.size_bytes,
            }
            for item in manifest.files
        ],
    }


def manifest_from_dict(obj: Dict[str, Any]) -> PackManifest:
    taxonomy_version = obj.get("taxonomy_version")
    generated_at = obj.get("generated_at")
    files_raw = obj.get("files") if isinstance(obj.get("files"), list) else []

    if not isinstance(taxonomy_version, str) or taxonomy_version.strip() == "":
        raise ValueError("pack_manifest.json missing taxonomy_version")
    if not isinstance(generated_at, str) or generated_at.strip() == "":
        raise ValueError("pack_manifest.json missing generated_at")

    file_entries: List[ManifestFileHash] = []
    for raw in files_raw:
        if not isinstance(raw, dict):
            continue
        file_name = raw.get("file_name")
        sha256 = raw.get("sha256")
        size_bytes = raw.get("size_bytes")
        if not isinstance(file_name, str) or file_name.strip() == "":
            continue
        if not isinstance(sha256, str) or sha256.strip() == "":
            continue
        try:
            size_int = int(size_bytes)
        except Exception:
            continue
        file_entries.append(
            ManifestFileHash(
                file_name=file_name,
                sha256=sha256,
                size_bytes=size_int,
            )
        )

    file_entries_sorted = tuple(sorted(file_entries, key=lambda item: item.file_name))
    return PackManifest(
        taxonomy_version=taxonomy_version,
        generated_at=generated_at,
        files=file_entries_sorted,
    )


def build_manifest(
    taxonomy_version: str,
    pack_folder: Path,
    file_names: Iterable[str],
    generated_at: str | None = None,
) -> PackManifest:
    file_entries: List[ManifestFileHash] = []
    for file_name in sorted(set([name for name in file_names if isinstance(name, str) and name.strip() != ""])):
        file_path = (pack_folder / file_name).resolve()
        if not file_path.exists() or not file_path.is_file():
            raise FileNotFoundError(f"Expected file missing for manifest: {file_path}")
        file_entries.append(
            ManifestFileHash(
                file_name=file_name,
                sha256=sha256_file(file_path),
                size_bytes=file_path.stat().st_size,
            )
        )

    generated_at_value = generated_at.strip() if isinstance(generated_at, str) and generated_at.strip() != "" else utc_now_iso()

    return PackManifest(
        taxonomy_version=taxonomy_version,
        generated_at=generated_at_value,
        files=tuple(file_entries),
    )


def write_manifest(pack_folder: Path, manifest: PackManifest) -> Path:
    manifest_path = (pack_folder / "pack_manifest.json").resolve()
    manifest_path.write_text(stable_json_dumps(manifest_to_dict(manifest)), encoding="utf-8")
    return manifest_path


def read_manifest(pack_folder: Path) -> PackManifest:
    manifest_path = (pack_folder / "pack_manifest.json").resolve()
    if not manifest_path.exists():
        raise FileNotFoundError(f"pack_manifest.json not found in: {pack_folder}")
    obj = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError("pack_manifest.json must contain a JSON object")
    return manifest_from_dict(obj)


def validate_manifest_hashes(pack_folder: Path, manifest: PackManifest) -> None:
    for entry in manifest.files:
        file_path = (pack_folder / entry.file_name).resolve()
        if not file_path.exists() or not file_path.is_file():
            raise ValueError(f"Manifest file missing: {entry.file_name}")
        actual_size = file_path.stat().st_size
        if int(actual_size) != int(entry.size_bytes):
            raise ValueError(
                f"Manifest size mismatch for {entry.file_name}: expected={entry.size_bytes} actual={actual_size}"
            )
        actual_hash = sha256_file(file_path)
        if actual_hash != entry.sha256:
            raise ValueError(
                f"Manifest sha256 mismatch for {entry.file_name}: expected={entry.sha256} actual={actual_hash}"
            )
