from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List


CURATED_PACK_MANIFEST_V1_VERSION = "curated_pack_manifest_v1"
_REPO_ROOT = Path(__file__).resolve().parents[2]
CURATED_PACK_MANIFEST_V1_REL_PATH = Path("api") / "engine" / "data" / "packs" / "curated_pack_manifest_v1.json"
CURATED_PACK_MANIFEST_V1_PATH = (_REPO_ROOT / CURATED_PACK_MANIFEST_V1_REL_PATH).resolve()


def _runtime_error(code: str, detail: str) -> RuntimeError:
    return RuntimeError(f"{code}: {detail}")


def stable_json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, ensure_ascii=False)


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _normalize_rel_path(path_value: str) -> str:
    raw = path_value.strip().replace("\\", "/")
    if raw.startswith("/"):
        raise _runtime_error("CURATED_PACK_MANIFEST_V1_INVALID", f"path must be a normalized repo-relative path: {path_value}")
    if len(raw) >= 2 and raw[1] == ":" and raw[0].isalpha():
        raise _runtime_error("CURATED_PACK_MANIFEST_V1_INVALID", f"path must be a normalized repo-relative path: {path_value}")
    normalized = "/".join(token for token in raw.split("/") if token not in {"", "."})
    if normalized == "" or normalized.startswith("../") or "/../" in f"/{normalized}":
        raise _runtime_error("CURATED_PACK_MANIFEST_V1_INVALID", f"path must be a normalized repo-relative path: {path_value}")
    return normalized


def _entry_sort_key(entry: Dict[str, Any]) -> tuple[Any, ...]:
    return (
        int(entry.get("load_order") or 0),
        str(entry.get("pack_id") or ""),
        str(entry.get("pack_version") or ""),
        str(entry.get("path") or ""),
        str(entry.get("sha256") or ""),
        str(entry.get("created_by") or ""),
    )


def _normalize_entry(raw: Any, *, index: int) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise _runtime_error("CURATED_PACK_MANIFEST_V1_INVALID", f"packs[{index}] must be an object")

    pack_id = _nonempty_str(raw.get("pack_id"))
    if pack_id is None:
        raise _runtime_error("CURATED_PACK_MANIFEST_V1_INVALID", f"packs[{index}].pack_id must be a non-empty string")

    pack_version = _nonempty_str(raw.get("pack_version"))
    if pack_version is None:
        raise _runtime_error("CURATED_PACK_MANIFEST_V1_INVALID", f"packs[{index}].pack_version must be a non-empty string")

    path_raw = _nonempty_str(raw.get("path"))
    if path_raw is None:
        raise _runtime_error("CURATED_PACK_MANIFEST_V1_INVALID", f"packs[{index}].path must be a non-empty string")

    sha256 = _nonempty_str(raw.get("sha256"))
    if sha256 is None:
        raise _runtime_error("CURATED_PACK_MANIFEST_V1_INVALID", f"packs[{index}].sha256 must be a non-empty string")
    sha256 = sha256.lower()
    if len(sha256) != 64 or any(ch not in "0123456789abcdef" for ch in sha256):
        raise _runtime_error("CURATED_PACK_MANIFEST_V1_INVALID", f"packs[{index}].sha256 must be a 64-char hex string")

    load_order_raw = raw.get("load_order")
    if not isinstance(load_order_raw, int) or isinstance(load_order_raw, bool) or load_order_raw < 0:
        raise _runtime_error("CURATED_PACK_MANIFEST_V1_INVALID", f"packs[{index}].load_order must be a non-negative integer")

    normalized: Dict[str, Any] = {
        "pack_id": pack_id,
        "pack_version": pack_version,
        "path": _normalize_rel_path(path_raw),
        "sha256": sha256,
        "load_order": int(load_order_raw),
    }

    created_by = _nonempty_str(raw.get("created_by"))
    if created_by is not None:
        normalized["created_by"] = created_by

    return normalized


def _manifest_file_path(manifest_path: Path | None = None) -> Path:
    if isinstance(manifest_path, Path):
        return manifest_path.resolve()
    return CURATED_PACK_MANIFEST_V1_PATH


def load_curated_pack_manifest_v1(*, manifest_path: Path | None = None) -> Dict[str, Any]:
    path = _manifest_file_path(manifest_path)
    if not path.is_file():
        raise _runtime_error("CURATED_PACK_MANIFEST_V1_MISSING", str(path))

    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise _runtime_error("CURATED_PACK_MANIFEST_V1_INVALID_JSON", str(path)) from exc

    if not isinstance(parsed, dict):
        raise _runtime_error("CURATED_PACK_MANIFEST_V1_INVALID", "root must be an object")

    version = _nonempty_str(parsed.get("version"))
    if version != CURATED_PACK_MANIFEST_V1_VERSION:
        raise _runtime_error(
            "CURATED_PACK_MANIFEST_V1_INVALID",
            f"version must equal '{CURATED_PACK_MANIFEST_V1_VERSION}'",
        )

    packs_raw = parsed.get("packs")
    if not isinstance(packs_raw, list):
        raise _runtime_error("CURATED_PACK_MANIFEST_V1_INVALID", "packs must be a list")

    normalized = [_normalize_entry(item, index=index) for index, item in enumerate(packs_raw)]
    normalized_sorted = sorted(normalized, key=_entry_sort_key)

    seen: set[tuple[str, str]] = set()
    for entry in normalized_sorted:
        key = (str(entry["pack_id"]), str(entry["pack_version"]))
        if key in seen:
            raise _runtime_error("CURATED_PACK_MANIFEST_V1_DUPLICATE_ENTRY", f"duplicate entry for {key[0]}@{key[1]}")
        seen.add(key)

    return {
        "version": CURATED_PACK_MANIFEST_V1_VERSION,
        "packs": normalized_sorted,
    }


def write_curated_pack_manifest_v1(payload: Dict[str, Any], *, manifest_path: Path | None = None) -> Path:
    path = _manifest_file_path(manifest_path)
    normalized = load_curated_pack_manifest_v1(manifest_path=path) if path.is_file() else {
        "version": CURATED_PACK_MANIFEST_V1_VERSION,
        "packs": [],
    }

    base = payload if isinstance(payload, dict) else {}
    if "version" in base:
        normalized["version"] = base.get("version")
    if "packs" in base:
        normalized["packs"] = base.get("packs")

    validated = {
        "version": CURATED_PACK_MANIFEST_V1_VERSION,
        "packs": [
            _normalize_entry(item, index=index)
            for index, item in enumerate(normalized.get("packs") if isinstance(normalized.get("packs"), list) else [])
        ],
    }
    validated["packs"] = sorted(validated["packs"], key=_entry_sort_key)

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(stable_json_dumps(validated), encoding="utf-8")
    return path


def curated_pack_manifest_sha256(*, manifest_path: Path | None = None) -> str:
    path = _manifest_file_path(manifest_path)
    if not path.is_file():
        raise _runtime_error("CURATED_PACK_MANIFEST_V1_MISSING", str(path))
    return sha256_file(path)


def validate_manifest_hashes(*, manifest_path: Path | None = None) -> None:
    manifest = load_curated_pack_manifest_v1(manifest_path=manifest_path)

    for entry in manifest.get("packs") if isinstance(manifest.get("packs"), list) else []:
        rel_path = str(entry.get("path") or "")
        abs_path = (_REPO_ROOT / rel_path).resolve()
        if not abs_path.is_file():
            raise _runtime_error("CURATED_PACK_MANIFEST_V1_FILE_MISSING", rel_path)

        actual_sha = sha256_file(abs_path)
        expected_sha = str(entry.get("sha256") or "")
        if actual_sha != expected_sha:
            raise _runtime_error(
                "CURATED_PACK_MANIFEST_V1_SHA256_MISMATCH",
                f"{rel_path} expected={expected_sha} actual={actual_sha}",
            )


def resolve_pack_entry(
    *,
    pack_id: str,
    pack_version: str | None = None,
    manifest_path: Path | None = None,
) -> Dict[str, Any]:
    pack_id_token = _nonempty_str(pack_id)
    if pack_id_token is None:
        raise _runtime_error("CURATED_PACK_MANIFEST_V1_INVALID_QUERY", "pack_id must be a non-empty string")

    version_token = _nonempty_str(pack_version)
    manifest = load_curated_pack_manifest_v1(manifest_path=manifest_path)
    packs = manifest.get("packs") if isinstance(manifest.get("packs"), list) else []

    candidates = [entry for entry in packs if str(entry.get("pack_id") or "") == pack_id_token]
    if version_token is not None:
        candidates = [entry for entry in candidates if str(entry.get("pack_version") or "") == version_token]

    if len(candidates) == 0:
        requested = f"{pack_id_token}@{version_token}" if version_token is not None else pack_id_token
        raise _runtime_error("CURATED_PACK_MANIFEST_V1_PACK_NOT_FOUND", requested)

    selected = sorted(candidates, key=_entry_sort_key)[-1]
    return dict(selected)


def resolve_pack_file_path(
    *,
    pack_id: str,
    pack_version: str | None = None,
    manifest_path: Path | None = None,
) -> Path:
    entry = resolve_pack_entry(pack_id=pack_id, pack_version=pack_version, manifest_path=manifest_path)
    rel_path = str(entry.get("path") or "")
    abs_path = (_REPO_ROOT / rel_path).resolve()
    if not abs_path.is_file():
        raise _runtime_error("CURATED_PACK_MANIFEST_V1_FILE_MISSING", rel_path)
    return abs_path


def collect_taxonomy_pack_refs(*, taxonomy_version: str, manifest_path: Path | None = None) -> List[Dict[str, Any]]:
    taxonomy_token = _nonempty_str(taxonomy_version)
    if taxonomy_token is None:
        return []

    manifest = load_curated_pack_manifest_v1(manifest_path=manifest_path)
    packs = manifest.get("packs") if isinstance(manifest.get("packs"), list) else []

    refs = [
        dict(entry)
        for entry in packs
        if str(entry.get("pack_version") or "") == taxonomy_token
        and str(entry.get("pack_id") or "") in {"taxonomy_primitives", "taxonomy_primitive_mappings"}
    ]
    refs.sort(key=_entry_sort_key)
    return refs
