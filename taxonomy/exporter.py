from __future__ import annotations

import argparse
import json
import re
import zipfile
from datetime import datetime, timezone
from pathlib import PurePosixPath, Path
from typing import Any, Dict, List, Tuple
from xml.etree import ElementTree

from .pack_manifest import build_manifest, stable_json_dumps, write_manifest
from .taxonomy_pack_v1 import build_taxonomy_pack_v1


MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"

EXPORTER_VERSION = "taxonomy_exporter_v2"
PRIMARY_KEY_CANDIDATES = (
    "rule_id",
    "primitive_id",
    "facet_key",
    "qa_rule_id",
    "module_name",
    "id",
    "name",
    "field",
)


def _normalize_token(value: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", value.lower())).strip("_")


def _sheet_file_name(sheet_name: str) -> str:
    normalized = _normalize_token(sheet_name)
    tokens = set([token for token in normalized.split("_") if token])

    if normalized == "primitives" or normalized.startswith("primitives_"):
        return "primitives.json"

    if normalized == "rulespec_rules" or {"rulespec", "rules"}.issubset(tokens):
        return "rulespec_rules.json"
    if normalized == "rulespec_facets" or {"rulespec", "facets"}.issubset(tokens):
        return "rulespec_facets.json"
    if normalized == "qa_rules" or ({"qa", "rules"}.issubset(tokens) or {"quality", "rules"}.issubset(tokens)):
        return "qa_rules.json"

    if normalized == "":
        normalized = "sheet"
    return f"{normalized}.json"


def _extract_version_tuple(text: str) -> Tuple[int, int] | None:
    match = re.search(r"v\s*(\d+)(?:[._](\d+))?", str(text or "").lower())
    if match is None:
        return None

    major = int(match.group(1))
    minor = int(match.group(2) or 0)
    return (major, minor)


def _canonical_taxonomy_version(raw_value: str | None, workbook_path: Path) -> str:
    if isinstance(raw_value, str) and raw_value.strip() != "":
        version_tuple = _extract_version_tuple(raw_value)
        if version_tuple is not None:
            major, minor = version_tuple
            return f"taxonomy_v{major}_{minor}"

        token = _normalize_token(raw_value)
        if token.startswith("taxonomy_"):
            return token
        if token != "":
            return f"taxonomy_{token}"

    stem = workbook_path.stem
    version_tuple = _extract_version_tuple(stem)
    if version_tuple is not None:
        major, minor = version_tuple
        return f"taxonomy_v{major}_{minor}"

    fallback = _normalize_token(stem)
    return fallback if fallback.startswith("taxonomy_") else (f"taxonomy_{fallback}" if fallback else "taxonomy_pack")


def _sheet_version_tuple(sheet_name: str, prefix: str) -> Tuple[int, int] | None:
    normalized = _normalize_token(sheet_name)
    match = re.search(rf"^{re.escape(prefix)}_v(\d+)(?:_(\d+))?", normalized)
    if match is None:
        return None
    return (int(match.group(1)), int(match.group(2) or 0))


def _deterministic_iso_from_file_mtime(path: Path) -> str:
    dt = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).replace(microsecond=0)
    return dt.isoformat()


def _read_shared_strings(zf: zipfile.ZipFile) -> List[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = ElementTree.fromstring(zf.read("xl/sharedStrings.xml"))
    ns = {"a": MAIN_NS}
    values: List[str] = []
    for node in root.findall(".//a:si", ns):
        text_parts: List[str] = []
        for text_node in node.findall(".//a:t", ns):
            text_parts.append(text_node.text or "")
        values.append("".join(text_parts))
    return values


def _normalize_relationship_target(target: str) -> str:
    target_clean = str(target or "").replace("\\", "/")
    if target_clean.startswith("/"):
        target_clean = target_clean[1:]
    if target_clean.startswith("xl/"):
        candidate = PurePosixPath(target_clean)
    else:
        candidate = PurePosixPath("xl") / PurePosixPath(target_clean)

    parts: List[str] = []
    for part in candidate.parts:
        if part == ".":
            continue
        if part == "..":
            if parts:
                parts.pop()
            continue
        parts.append(part)
    return "/".join(parts)


def _workbook_sheet_targets(zf: zipfile.ZipFile) -> List[Tuple[str, str]]:
    wb_root = ElementTree.fromstring(zf.read("xl/workbook.xml"))
    rels_root = ElementTree.fromstring(zf.read("xl/_rels/workbook.xml.rels"))

    rel_map: Dict[str, str] = {}
    for rel in rels_root.findall(f"{{{PKG_REL_NS}}}Relationship"):
        rel_id = rel.attrib.get("Id")
        target = rel.attrib.get("Target")
        if isinstance(rel_id, str) and isinstance(target, str):
            rel_map[rel_id] = _normalize_relationship_target(target)

    ns = {"a": MAIN_NS}
    targets: List[Tuple[str, str]] = []
    for sheet_node in wb_root.findall(".//a:sheets/a:sheet", ns):
        sheet_name = sheet_node.attrib.get("name")
        rel_id = sheet_node.attrib.get(f"{{{REL_NS}}}id")
        if not isinstance(sheet_name, str) or not isinstance(rel_id, str):
            continue
        target = rel_map.get(rel_id)
        if not isinstance(target, str):
            continue
        targets.append((sheet_name, target))

    return targets


def _col_ref_to_index(cell_ref: str) -> int:
    letters = "".join([ch for ch in str(cell_ref) if ch.isalpha()]).upper()
    if letters == "":
        return 0
    out = 0
    for ch in letters:
        out = (out * 26) + (ord(ch) - ord("A") + 1)
    return max(0, out - 1)


def _decode_cell_value(cell_node: ElementTree.Element, shared_strings: List[str]) -> Any:
    ns = {"a": MAIN_NS}
    cell_type = cell_node.attrib.get("t")

    if cell_type == "inlineStr":
        text_parts = [node.text or "" for node in cell_node.findall(".//a:is/a:t", ns)]
        return "".join(text_parts)

    value_node = cell_node.find("a:v", ns)
    value_text = value_node.text if value_node is not None else None

    if cell_type == "s":
        if value_text is None:
            return None
        try:
            idx = int(value_text)
        except Exception:
            return None
        if idx < 0 or idx >= len(shared_strings):
            return None
        return shared_strings[idx]

    if cell_type == "b":
        return "true" if str(value_text or "0") == "1" else "false"

    if value_text is None:
        return None
    return value_text


def _read_worksheet_rows(zf: zipfile.ZipFile, worksheet_target: str, shared_strings: List[str]) -> List[List[Any]]:
    ns = {"a": MAIN_NS}
    root = ElementTree.fromstring(zf.read(worksheet_target))

    rows: List[List[Any]] = []
    for row_node in root.findall(".//a:sheetData/a:row", ns):
        cell_map: Dict[int, Any] = {}
        max_col = -1
        fallback_col = 0

        for cell_node in row_node.findall("a:c", ns):
            ref = cell_node.attrib.get("r")
            col_idx = _col_ref_to_index(ref) if isinstance(ref, str) else fallback_col
            fallback_col = max(fallback_col, col_idx + 1)

            cell_value = _decode_cell_value(cell_node, shared_strings)
            if cell_value is None:
                continue
            cell_map[col_idx] = cell_value
            max_col = max(max_col, col_idx)

        if max_col < 0:
            rows.append([])
            continue

        dense_row: List[Any] = [None] * (max_col + 1)
        for idx, value in cell_map.items():
            dense_row[idx] = value

        while dense_row and dense_row[-1] is None:
            dense_row.pop()
        rows.append(dense_row)

    return rows


def _is_empty_row(row: List[Any]) -> bool:
    for value in row:
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        return False
    return True


def _normalize_headers(header_row: List[Any]) -> List[str]:
    headers: List[str] = []
    seen: Dict[str, int] = {}
    for idx, raw in enumerate(header_row):
        base = _normalize_token(str(raw)) if raw is not None else ""
        if base == "":
            base = f"col_{idx + 1}"
        seen[base] = seen.get(base, 0) + 1
        if seen[base] > 1:
            headers.append(f"{base}_{seen[base]}")
        else:
            headers.append(base)
    return headers


def _rows_to_records(rows: List[List[Any]]) -> List[Dict[str, Any]]:
    header_idx = None
    for idx, row in enumerate(rows):
        if not _is_empty_row(row):
            header_idx = idx
            break

    if header_idx is None:
        return []

    header_row = rows[header_idx]
    headers = _normalize_headers(header_row)

    out: List[Dict[str, Any]] = []
    for row in rows[header_idx + 1 :]:
        if _is_empty_row(row):
            continue
        record: Dict[str, Any] = {}
        for idx, header in enumerate(headers):
            value = row[idx] if idx < len(row) else None
            if value is None:
                continue
            if isinstance(value, str):
                value_clean = value.strip()
                if value_clean == "":
                    continue
                record[header] = value_clean
            else:
                record[header] = value
        if record:
            out.append(record)

    return out


def _records_primary_key(records: List[Dict[str, Any]]) -> str | None:
    for candidate in PRIMARY_KEY_CANDIDATES:
        for record in records:
            value = record.get(candidate)
            if value is None:
                continue
            if isinstance(value, str) and value.strip() == "":
                continue
            return candidate
    return None


def _merge_and_sort_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not records:
        return []

    primary_key = _records_primary_key(records)
    if primary_key is None:
        return sorted(records, key=lambda row: stable_json_dumps(row))

    keyed: Dict[str, Dict[str, Any]] = {}
    without_key: List[Dict[str, Any]] = []
    for record in records:
        raw = record.get(primary_key)
        key = str(raw).strip() if raw is not None else ""
        if key == "":
            without_key.append(record)
            continue
        keyed[key] = record

    merged = list(keyed.values()) + without_key
    merged.sort(
        key=lambda row: (
            str(row.get(primary_key) or ""),
            stable_json_dumps(row),
        )
    )
    return merged


def _split_source_sheets(raw: Any) -> List[str]:
    if not isinstance(raw, str):
        return []
    parts = re.split(r"[,;|\n]+", raw)
    return [part.strip() for part in parts if isinstance(part, str) and part.strip() != ""]


def _resolve_sheet_name(source_sheet_name: str, available_sheet_names: List[str]) -> str | None:
    if not isinstance(source_sheet_name, str) or source_sheet_name.strip() == "":
        return None

    exact = source_sheet_name.strip()
    for candidate in available_sheet_names:
        if candidate == exact:
            return candidate

    normalized_source = _normalize_token(source_sheet_name)
    normalized_map: Dict[str, str] = {}
    for candidate in available_sheet_names:
        normalized_candidate = _normalize_token(candidate)
        if normalized_candidate not in normalized_map:
            normalized_map[normalized_candidate] = candidate

    variants = {
        normalized_source,
        normalized_source.replace("_and_", "_"),
        normalized_source.replace("commander", "cmdr"),
        normalized_source.replace("cmdr", "commander"),
        normalized_source.replace("_aliases_", "_alias_"),
        normalized_source.replace("_dependencies_", "_dep_"),
        normalized_source.replace("_dependency_", "_dep_"),
    }
    for variant in variants:
        if variant in normalized_map:
            return normalized_map[variant]

    source_tokens = set([token for token in normalized_source.split("_") if token != ""])
    source_version = _extract_version_tuple(source_sheet_name)
    best_name = None
    best_score = -10**9
    for candidate in available_sheet_names:
        normalized_candidate = _normalize_token(candidate)
        candidate_tokens = set([token for token in normalized_candidate.split("_") if token != ""])
        overlap = len(source_tokens & candidate_tokens)
        if overlap <= 0:
            continue

        score = overlap * 10
        score -= abs(len(candidate_tokens) - len(source_tokens))
        if normalized_candidate.startswith(normalized_source) or normalized_source.startswith(normalized_candidate):
            score += 3

        candidate_version = _extract_version_tuple(candidate)
        if source_version is not None and candidate_version is not None and source_version == candidate_version:
            score += 5

        if score > best_score:
            best_score = score
            best_name = candidate

    if isinstance(best_name, str) and best_score >= 15:
        return best_name

    return None


def _extract_meta_info(meta_records: List[Dict[str, Any]], workbook_path: Path) -> Tuple[str, str]:
    taxonomy_meta_value = None
    exporter_meta_value = None

    for record in meta_records:
        if not isinstance(record, dict):
            continue

        direct_taxonomy = record.get("taxonomy_version")
        if isinstance(direct_taxonomy, str) and direct_taxonomy.strip() != "":
            taxonomy_meta_value = direct_taxonomy.strip()

        direct_exporter = record.get("exporter_version")
        if isinstance(direct_exporter, str) and direct_exporter.strip() != "":
            exporter_meta_value = direct_exporter.strip()

        field = record.get("field")
        value = record.get("value")
        if isinstance(field, str) and isinstance(value, str):
            if field.strip().lower() == "taxonomy_version" and value.strip() != "":
                taxonomy_meta_value = value.strip()
            if field.strip().lower() == "exporter_version" and value.strip() != "":
                exporter_meta_value = value.strip()

    taxonomy_version = _canonical_taxonomy_version(taxonomy_meta_value, workbook_path)
    exporter_version = exporter_meta_value if isinstance(exporter_meta_value, str) and exporter_meta_value != "" else "unknown"
    return taxonomy_version, exporter_version


def _candidate_export_module_sheets(available_sheet_names: List[str], target_taxonomy_version: str) -> List[str]:
    target_tuple = _extract_version_tuple(target_taxonomy_version)
    versioned: List[Tuple[Tuple[int, int], str]] = []

    for sheet_name in available_sheet_names:
        sheet_tuple = _sheet_version_tuple(sheet_name=sheet_name, prefix="export_modules")
        if sheet_tuple is None:
            continue
        if target_tuple is not None and sheet_tuple > target_tuple:
            continue
        versioned.append((sheet_tuple, sheet_name))

    versioned_sorted = sorted(versioned, key=lambda item: (item[0][0], item[0][1], _normalize_token(item[1])))
    if versioned_sorted:
        return [item[1] for item in versioned_sorted]

    fallback = [name for name in available_sheet_names if _normalize_token(name) == "export_modules"]
    return sorted(fallback, key=lambda item: _normalize_token(item))


def _latest_sheet_for_file(available_sheet_names: List[str], file_name: str) -> str | None:
    candidates: List[Tuple[Tuple[int, int], str]] = []
    for sheet_name in available_sheet_names:
        if _sheet_file_name(sheet_name) != file_name:
            continue
        version_tuple = _extract_version_tuple(sheet_name) or (-1, -1)
        candidates.append((version_tuple, sheet_name))

    if not candidates:
        return None

    candidates.sort(key=lambda item: (item[0][0], item[0][1], _normalize_token(item[1])))
    return candidates[-1][1]


def export_workbook_to_pack(
    workbook_path: str | Path,
    out_dir: str | Path,
    taxonomy_version: str | None = None,
) -> Path:
    workbook = Path(workbook_path).resolve()
    if not workbook.exists() or not workbook.is_file():
        raise FileNotFoundError(f"Workbook not found: {workbook}")

    output_base = Path(out_dir).resolve()
    output_base.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(workbook, "r") as zf:
        shared_strings = _read_shared_strings(zf)
        sheet_targets = _workbook_sheet_targets(zf)

        rows_by_sheet: Dict[str, List[List[Any]]] = {}
        records_by_sheet: Dict[str, List[Dict[str, Any]]] = {}
        for sheet_name, target in sheet_targets:
            rows = _read_worksheet_rows(zf=zf, worksheet_target=target, shared_strings=shared_strings)
            rows_by_sheet[sheet_name] = rows
            records_by_sheet[sheet_name] = _rows_to_records(rows)

    available_sheet_names = [name for name, _ in sheet_targets]
    meta_sheet_name = _resolve_sheet_name("Meta", available_sheet_names)
    meta_records = records_by_sheet.get(meta_sheet_name, []) if isinstance(meta_sheet_name, str) else []
    meta_taxonomy_version, meta_exporter_version = _extract_meta_info(meta_records=meta_records, workbook_path=workbook)

    if isinstance(taxonomy_version, str) and taxonomy_version.strip() != "":
        version = taxonomy_version.strip()
    else:
        version = meta_taxonomy_version

    pack_dir = (output_base / version).resolve()
    pack_dir.mkdir(parents=True, exist_ok=True)

    selected_source_sheets: List[str] = []
    export_module_sheet_names = _candidate_export_module_sheets(
        available_sheet_names=available_sheet_names,
        target_taxonomy_version=version,
    )

    for module_sheet_name in export_module_sheet_names:
        module_records = records_by_sheet.get(module_sheet_name, [])
        for record in module_records:
            source_sheets = _split_source_sheets(record.get("source_sheets"))
            for source_sheet in source_sheets:
                resolved = _resolve_sheet_name(source_sheet, available_sheet_names)
                if not isinstance(resolved, str):
                    continue
                normalized_resolved = _normalize_token(resolved)
                if normalized_resolved == "meta" or normalized_resolved.startswith("export_modules"):
                    continue
                if resolved in selected_source_sheets:
                    continue
                selected_source_sheets.append(resolved)

    if not selected_source_sheets:
        for sheet_name in available_sheet_names:
            normalized = _normalize_token(sheet_name)
            if normalized == "meta" or normalized.startswith("export_modules"):
                continue
            selected_source_sheets.append(sheet_name)

    required_files = ("primitives.json", "rulespec_rules.json", "rulespec_facets.json", "qa_rules.json")
    for required_file in required_files:
        has_source = any(_sheet_file_name(sheet_name) == required_file for sheet_name in selected_source_sheets)
        if has_source:
            continue
        fallback_sheet = _latest_sheet_for_file(available_sheet_names=available_sheet_names, file_name=required_file)
        if isinstance(fallback_sheet, str) and fallback_sheet not in selected_source_sheets:
            selected_source_sheets.append(fallback_sheet)

    file_payloads: Dict[str, List[Dict[str, Any]]] = {}
    for sheet_name in selected_source_sheets:
        file_name = _sheet_file_name(sheet_name)
        file_payloads.setdefault(file_name, [])
        file_payloads[file_name].extend(records_by_sheet.get(sheet_name, []))

    for required_name in required_files:
        file_payloads.setdefault(required_name, [])

    normalized_file_payloads: Dict[str, Any] = {}
    for file_name, records in file_payloads.items():
        normalized_file_payloads[file_name] = _merge_and_sort_records(records)

    taxonomy_pack_v1_payload = build_taxonomy_pack_v1(
        {
            "taxonomy_source_id": workbook.name,
            "tag_taxonomy_version": version,
            "generator_version": EXPORTER_VERSION,
            "rulespec_rules": normalized_file_payloads.get("rulespec_rules.json", []),
            "rulespec_facets": normalized_file_payloads.get("rulespec_facets.json", []),
            "primitives": normalized_file_payloads.get("primitives.json", []),
        }
    )
    normalized_file_payloads["taxonomy_pack_v1.json"] = taxonomy_pack_v1_payload

    deterministic_created_at = _deterministic_iso_from_file_mtime(workbook)
    normalized_file_payloads["meta.json"] = {
        "taxonomy_version": version,
        "exporter_version": EXPORTER_VERSION,
        "source_workbook": workbook.name,
        "meta_exporter_version": meta_exporter_version,
        "created_at": deterministic_created_at,
    }

    for existing_file in pack_dir.iterdir():
        if existing_file.is_file() and existing_file.suffix.lower() == ".json":
            existing_file.unlink()

    written_files: List[str] = []
    for file_name in sorted(normalized_file_payloads.keys()):
        file_path = (pack_dir / file_name).resolve()
        file_path.write_text(stable_json_dumps(normalized_file_payloads[file_name]), encoding="utf-8")
        written_files.append(file_name)

    manifest = build_manifest(
        taxonomy_version=version,
        pack_folder=pack_dir,
        file_names=written_files,
        generated_at=deterministic_created_at,
    )
    write_manifest(pack_folder=pack_dir, manifest=manifest)

    return pack_dir


def main() -> int:
    ap = argparse.ArgumentParser(description="Export taxonomy workbook into a deterministic taxonomy pack")
    ap.add_argument("--workbook", required=True, help="Workbook path (xlsx)")
    ap.add_argument("--out", required=True, help="Output packs folder")
    ap.add_argument("--taxonomy-version", default=None, help="Optional explicit taxonomy version")
    args = ap.parse_args()

    pack_dir = export_workbook_to_pack(
        workbook_path=args.workbook,
        out_dir=args.out,
        taxonomy_version=args.taxonomy_version,
    )

    out = {
        "taxonomy_version": pack_dir.name,
        "pack_folder": str(pack_dir),
        "pack_manifest": str((pack_dir / "pack_manifest.json").resolve()),
    }
    print(json.dumps(out, separators=(",", ":"), sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
