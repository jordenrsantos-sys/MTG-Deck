from __future__ import annotations

import argparse
import json
import re
import zipfile
from pathlib import PurePosixPath, Path
from typing import Any, Dict, List, Tuple
from xml.etree import ElementTree

from .pack_manifest import build_manifest, stable_json_dumps, write_manifest


MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"


def _normalize_token(value: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", value.lower())).strip("_")


def _sheet_file_name(sheet_name: str) -> str:
    normalized = _normalize_token(sheet_name)
    tokens = set([token for token in normalized.split("_") if token])

    if normalized == "rulespec_rules" or {"rulespec", "rules"}.issubset(tokens):
        return "rulespec_rules.json"
    if normalized == "rulespec_facets" or {"rulespec", "facets"}.issubset(tokens):
        return "rulespec_facets.json"
    if normalized == "qa_rules" or ({"qa", "rules"}.issubset(tokens) or {"quality", "rules"}.issubset(tokens)):
        return "qa_rules.json"

    if normalized == "":
        normalized = "sheet"
    return f"{normalized}.json"


def _extract_taxonomy_version(workbook_path: Path) -> str:
    stem = workbook_path.stem
    match = re.search(r"(v\d+(?:_\d+)*)", stem.lower())
    if match:
        return f"taxonomy_{match.group(1)}"
    return _normalize_token(stem) or "taxonomy_pack"


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

    version = taxonomy_version.strip() if isinstance(taxonomy_version, str) and taxonomy_version.strip() != "" else _extract_taxonomy_version(workbook)
    pack_dir = (output_base / version).resolve()
    pack_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(workbook, "r") as zf:
        shared_strings = _read_shared_strings(zf)
        sheet_targets = _workbook_sheet_targets(zf)

        file_payloads: Dict[str, List[Dict[str, Any]]] = {}
        for sheet_name, target in sheet_targets:
            rows = _read_worksheet_rows(zf=zf, worksheet_target=target, shared_strings=shared_strings)
            records = _rows_to_records(rows)
            file_name = _sheet_file_name(sheet_name)

            if file_name in file_payloads:
                suffix = 2
                candidate = file_name.replace(".json", "")
                while f"{candidate}_{suffix}.json" in file_payloads:
                    suffix += 1
                file_name = f"{candidate}_{suffix}.json"

            file_payloads[file_name] = records

    for required_name in ("rulespec_rules.json", "rulespec_facets.json", "qa_rules.json"):
        file_payloads.setdefault(required_name, [])

    written_files: List[str] = []
    for file_name in sorted(file_payloads.keys()):
        file_path = (pack_dir / file_name).resolve()
        file_path.write_text(stable_json_dumps(file_payloads[file_name]), encoding="utf-8")
        written_files.append(file_name)

    manifest = build_manifest(
        taxonomy_version=version,
        pack_folder=pack_dir,
        file_names=written_files,
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
