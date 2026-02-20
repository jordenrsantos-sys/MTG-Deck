from __future__ import annotations

import argparse
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Tuple

from engine.db import connect, snapshot_exists
from engine.determinism import sha256_hex, stable_json_dumps
from taxonomy.loader import load
from taxonomy.pack_manifest import sha256_file
from taxonomy.schema import TaxonomyPack
from taxonomy.taxonomy_pack_v1 import TAXONOMY_PACK_V1_VERSION

from .index_build import build_indices as build_runtime_indices
from .patch_apply import (
    PatchAppliedRow,
    apply_patch_overrides,
    ensure_patches_applied_table,
    record_patch_rows,
)
from .unknowns_queue import (
    UnknownQueueRow,
    ensure_unknowns_queue_table,
    insert_unknowns,
)


@dataclass(frozen=True)
class CompiledRule:
    rule_id: str
    primitive_id: str | None
    pattern: str
    field: str
    match_mode: str
    facet_key: str | None
    facet_value: str | None
    exclusive_group: str | None
    unknown_on_match: bool
    priority: int


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_card_tags_table(con: Any) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS card_tags (
          oracle_id TEXT NOT NULL,
          snapshot_id TEXT NOT NULL,
          taxonomy_version TEXT NOT NULL,
          ruleset_version TEXT NOT NULL,
          primitive_ids_json TEXT NOT NULL,
          equiv_class_ids_json TEXT NOT NULL,
          facets_json TEXT NOT NULL,
          evidence_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          PRIMARY KEY (oracle_id, snapshot_id, taxonomy_version)
        );

        CREATE INDEX IF NOT EXISTS idx_card_tags_snapshot_taxonomy
          ON card_tags (snapshot_id, taxonomy_version);

        CREATE INDEX IF NOT EXISTS idx_card_tags_taxonomy_ruleset
          ON card_tags (taxonomy_version, ruleset_version);
        """
    )


def _safe_str(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned if cleaned != "" else None


def _pick_first_str(record: Dict[str, Any], keys: Iterable[str]) -> str | None:
    for key in keys:
        value = _safe_str(record.get(key))
        if value is not None:
            return value
    return None


def _pick_bool(record: Dict[str, Any], keys: Iterable[str], default: bool) -> bool:
    for key in keys:
        value = record.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return int(value) != 0
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "y", "on"}:
                return True
            if normalized in {"0", "false", "no", "n", "off"}:
                return False
    return default


def _pick_int(record: Dict[str, Any], keys: Iterable[str], default: int) -> int:
    for key in keys:
        value = record.get(key)
        try:
            return int(value)
        except Exception:
            continue
    return default


def _normalize_field(raw_field: str | None) -> str:
    value = (raw_field or "oracle_text").strip().lower()
    if value in {"type_line", "type", "typeline"}:
        return "type_line"
    if value in {"both", "all", "oracle_text+type_line", "oracle_text,type_line"}:
        return "both"
    return "oracle_text"


def _normalize_match_mode(raw_mode: str | None) -> str:
    value = (raw_mode or "substring").strip().lower()
    if "regex" in value:
        return "regex"
    return "substring"


def _compile_rules(rulespec_rules: Iterable[Any]) -> List[CompiledRule]:
    compiled: List[CompiledRule] = []

    for idx, raw in enumerate(rulespec_rules):
        if not isinstance(raw, dict):
            continue

        record = {str(k): v for k, v in raw.items() if isinstance(k, str)}
        enabled = _pick_bool(record, ["enabled", "is_enabled", "active"], default=True)
        if not enabled:
            continue

        rule_id = _pick_first_str(record, ["rule_id", "id", "rule", "rid"])
        if rule_id is None:
            rule_id = f"ROW_{idx + 1:06d}"

        primitive_id = _pick_first_str(
            record,
            ["primitive_id", "primitive", "tag", "primitive_tag"],
        )
        pattern = _pick_first_str(record, ["pattern", "match", "contains", "regex", "text"])
        if pattern is None:
            continue

        field = _normalize_field(_pick_first_str(record, ["field", "match_field", "target_field"]))
        match_mode = _normalize_match_mode(_pick_first_str(record, ["rule_type", "match_type", "pattern_type"]))

        facet_key = _pick_first_str(record, ["facet_key", "facet_id", "facet", "facet_name"])
        facet_value = _pick_first_str(record, ["facet_value", "value", "facet_val"]) or primitive_id
        exclusive_group = _pick_first_str(record, ["exclusive_group", "equiv_group", "group", "mutex_group"])
        unknown_on_match = _pick_bool(record, ["unknown_on_match", "route_unknown", "ambiguous"], default=False)
        priority = _pick_int(record, ["priority", "rank", "order"], default=100)

        compiled.append(
            CompiledRule(
                rule_id=rule_id,
                primitive_id=primitive_id,
                pattern=pattern,
                field=field,
                match_mode=match_mode,
                facet_key=facet_key,
                facet_value=facet_value,
                exclusive_group=exclusive_group,
                unknown_on_match=unknown_on_match,
                priority=priority,
            )
        )

    compiled.sort(
        key=lambda r: (
            int(r.priority),
            str(r.rule_id),
            str(r.primitive_id or ""),
            str(r.pattern),
        )
    )
    return compiled


def _normalize_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split()).strip()


def _baseline_facets_v1(type_line: str, oracle_text: str) -> Dict[str, str]:
    type_line_lc = str(type_line or "").lower()
    oracle_text_lc = str(oracle_text or "").lower()

    is_legendary = "legendary" in type_line_lc
    is_creature = "creature" in type_line_lc
    is_legendary_creature = is_legendary and is_creature
    has_commander_clause = "can be your commander" in oracle_text_lc
    commander_eligible = is_legendary_creature or has_commander_clause

    return {
        "is_legendary": "true" if is_legendary else "false",
        "is_creature": "true" if is_creature else "false",
        "is_legendary_creature": "true" if is_legendary_creature else "false",
        "commander_eligible": "true" if commander_eligible else "false",
    }


def _match_rule_on_text(rule: CompiledRule, text: str) -> Tuple[bool, int, int, str]:
    if text == "":
        return (False, -1, -1, "")

    if rule.match_mode == "regex":
        try:
            pattern = re.compile(rule.pattern, re.IGNORECASE)
        except re.error:
            return (False, -1, -1, "")
        match = pattern.search(text)
        if match is None:
            return (False, -1, -1, "")
        start, end = match.span()
        snippet = text[start:end]
        return (True, start, end, snippet[:120])

    haystack = text.lower()
    needle = rule.pattern.lower()
    idx = haystack.find(needle)
    if idx < 0:
        return (False, -1, -1, "")
    end = idx + len(needle)
    snippet = text[idx:end]
    return (True, idx, end, snippet[:120])


def _derive_equiv_class_ids(primitive_ids: List[str], facets: Dict[str, List[str]]) -> List[str]:
    components: List[str] = [f"prim:{pid}" for pid in sorted(set(primitive_ids))]
    for facet_key in sorted(facets.keys()):
        values = sorted(set([v for v in facets.get(facet_key, []) if isinstance(v, str) and v != ""]))
        for value in values:
            components.append(f"facet:{facet_key}={value}")

    if not components:
        return []

    hash_input = "|".join(components)
    equiv_class_id = f"EQ_{sha256_hex(hash_input)[:16]}"
    return [equiv_class_id]


def _fetch_snapshot_cards(snapshot_id: str) -> List[Dict[str, Any]]:
    with connect() as con:
        rows = con.execute(
            """
            SELECT
              oracle_id,
              name,
              type_line,
              oracle_text
            FROM cards
            WHERE snapshot_id = ?
            ORDER BY
              CASE WHEN oracle_id IS NULL THEN 1 ELSE 0 END ASC,
              oracle_id ASC,
              name ASC
            """,
            (snapshot_id,),
        ).fetchall()

    return [dict(row) for row in rows]


def _build_card_rows(
    cards: List[Dict[str, Any]],
    compiled_rules: List[CompiledRule],
    snapshot_id: str,
    taxonomy_version: str,
    ruleset_version: str,
) -> Tuple[List[Dict[str, Any]], List[UnknownQueueRow]]:
    tag_rows: List[Dict[str, Any]] = []
    unknown_rows: List[UnknownQueueRow] = []

    for card in cards:
        oracle_id = _safe_str(card.get("oracle_id"))
        card_name = _safe_str(card.get("name"))
        if oracle_id is None:
            continue

        oracle_text = _normalize_text(card.get("oracle_text"))
        type_line = _normalize_text(card.get("type_line"))

        primitive_ids_set: set[str] = set()
        facets_temp: Dict[str, set[str]] = {}
        evidence: List[Dict[str, Any]] = []
        exclusivity_hits: Dict[str, set[str]] = {}
        card_unknowns: List[UnknownQueueRow] = []

        for rule in compiled_rules:
            targets: List[Tuple[str, str]]
            if rule.field == "type_line":
                targets = [("type_line", type_line)]
            elif rule.field == "both":
                targets = [("oracle_text", oracle_text), ("type_line", type_line)]
            else:
                targets = [("oracle_text", oracle_text)]

            matched = False
            for field_name, target_text in targets:
                ok, start, end, snippet = _match_rule_on_text(rule, target_text)
                if not ok:
                    continue
                matched = True

                evidence.append(
                    {
                        "rule_id": rule.rule_id,
                        "field": field_name,
                        "span": [start, end],
                        "snippet": snippet,
                    }
                )

                if isinstance(rule.primitive_id, str) and rule.primitive_id != "":
                    primitive_ids_set.add(rule.primitive_id)
                    if isinstance(rule.exclusive_group, str) and rule.exclusive_group != "":
                        exclusivity_hits.setdefault(rule.exclusive_group, set()).add(rule.primitive_id)
                else:
                    card_unknowns.append(
                        UnknownQueueRow(
                            oracle_id=oracle_id,
                            snapshot_id=snapshot_id,
                            taxonomy_version=taxonomy_version,
                            rule_id=rule.rule_id,
                            reason="MATCH_WITHOUT_PRIMITIVE",
                            snippet=snippet,
                            created_at=utc_now_iso(),
                        )
                    )

                if isinstance(rule.facet_key, str) and rule.facet_key != "":
                    facet_value = rule.facet_value if isinstance(rule.facet_value, str) and rule.facet_value != "" else "true"
                    facets_temp.setdefault(rule.facet_key, set()).add(facet_value)

                if rule.unknown_on_match:
                    card_unknowns.append(
                        UnknownQueueRow(
                            oracle_id=oracle_id,
                            snapshot_id=snapshot_id,
                            taxonomy_version=taxonomy_version,
                            rule_id=rule.rule_id,
                            reason="RULE_MARKED_UNKNOWN_ON_MATCH",
                            snippet=snippet,
                            created_at=utc_now_iso(),
                        )
                    )

                # Deterministic behavior: consume first match for each rule.
                break

            if not matched:
                continue

        for group_id, group_primitives in sorted(exclusivity_hits.items(), key=lambda item: item[0]):
            if len(group_primitives) > 1:
                card_unknowns.append(
                    UnknownQueueRow(
                        oracle_id=oracle_id,
                        snapshot_id=snapshot_id,
                        taxonomy_version=taxonomy_version,
                        rule_id=f"EXCLUSIVE_GROUP:{group_id}",
                        reason="AMBIGUOUS_GROUP_MATCH",
                        snippet="|".join(sorted(group_primitives)),
                        created_at=utc_now_iso(),
                    )
                )

        evidence_sorted = sorted(
            evidence,
            key=lambda item: (
                str(item.get("rule_id") or ""),
                str(item.get("field") or ""),
                int((item.get("span") or [0, 0])[0]),
                int((item.get("span") or [0, 0])[1]),
                str(item.get("snippet") or ""),
            ),
        )

        facets_final = {
            key: sorted(list(values))
            for key, values in sorted(facets_temp.items(), key=lambda item: item[0])
        }
        for facet_key, facet_value in sorted(_baseline_facets_v1(type_line=type_line, oracle_text=oracle_text).items()):
            facets_final[facet_key] = [facet_value]
        primitive_ids = sorted(list(primitive_ids_set))
        equiv_class_ids = _derive_equiv_class_ids(primitive_ids=primitive_ids, facets=facets_final)

        if card_unknowns:
            unknown_rows.extend(card_unknowns)

        tag_rows.append(
            {
                "oracle_id": oracle_id,
                "snapshot_id": snapshot_id,
                "taxonomy_version": taxonomy_version,
                "ruleset_version": ruleset_version,
                "primitive_ids": primitive_ids,
                "equiv_class_ids": equiv_class_ids,
                "facets": facets_final,
                "evidence": evidence_sorted,
                "created_at": utc_now_iso(),
                "card_name": card_name,
            }
        )

    tag_rows_sorted = sorted(
        tag_rows,
        key=lambda row: (
            str(row.get("oracle_id") or ""),
            str(row.get("card_name") or ""),
        ),
    )
    unknown_rows_sorted = sorted(
        unknown_rows,
        key=lambda row: (
            str(row.snapshot_id),
            str(row.oracle_id or ""),
            str(row.rule_id or ""),
            str(row.reason),
            str(row.snippet or ""),
        ),
    )

    return tag_rows_sorted, unknown_rows_sorted


def _persist_rows(
    snapshot_id: str,
    taxonomy_pack: TaxonomyPack,
    tag_rows: List[Dict[str, Any]],
    unknown_rows: List[UnknownQueueRow],
    patch_rows_applied: List[PatchAppliedRow],
) -> Dict[str, Any]:
    def _read_snapshot_manifest_json(con: Any, target_snapshot_id: str) -> Dict[str, Any]:
        try:
            row = con.execute(
                "SELECT manifest_json FROM snapshots WHERE snapshot_id = ? LIMIT 1",
                (target_snapshot_id,),
            ).fetchone()
        except sqlite3.Error:
            return {}

        if row is None:
            return {}

        manifest_raw: Any = None
        if isinstance(row, sqlite3.Row):
            manifest_raw = row["manifest_json"] if "manifest_json" in row.keys() else None
        elif isinstance(row, (tuple, list)) and len(row) > 0:
            manifest_raw = row[0]

        if isinstance(manifest_raw, dict):
            return manifest_raw
        if not isinstance(manifest_raw, str):
            return {}

        manifest_text = manifest_raw.strip()
        if manifest_text == "":
            return {}

        try:
            parsed = json.loads(manifest_text)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _resolve_taxonomy_pack_refs(pack: TaxonomyPack) -> tuple[str | None, str | None]:
        pack_version = None
        pack_sha256 = None

        taxonomy_pack_payload = (
            pack.other_sheets.get("taxonomy_pack_v1.json")
            if isinstance(pack.other_sheets, dict)
            else None
        )
        if isinstance(taxonomy_pack_payload, dict):
            raw_version = taxonomy_pack_payload.get("version")
            if isinstance(raw_version, str) and raw_version.strip() != "":
                pack_version = raw_version.strip()

        pack_file_path = pack.pack_folder / "taxonomy_pack_v1.json"
        if pack_file_path.exists() and pack_file_path.is_file():
            pack_sha256 = sha256_file(pack_file_path)
            if pack_version is None:
                pack_version = TAXONOMY_PACK_V1_VERSION

        return pack_version, pack_sha256

    def _write_snapshot_manifest_refs(con: Any, target_snapshot_id: str, pack: TaxonomyPack) -> Dict[str, Any]:
        pack_version, pack_sha256 = _resolve_taxonomy_pack_refs(pack)

        manifest_obj = _read_snapshot_manifest_json(con, target_snapshot_id)
        next_manifest = dict(manifest_obj)
        next_manifest["tags_compiled"] = True

        if isinstance(pack_version, str) and pack_version != "":
            next_manifest["taxonomy_pack_version"] = pack_version
        if isinstance(pack_sha256, str) and pack_sha256 != "":
            next_manifest["taxonomy_pack_sha256"] = pack_sha256

        try:
            con.execute(
                "UPDATE snapshots SET manifest_json = ? WHERE snapshot_id = ?",
                (stable_json_dumps(next_manifest), target_snapshot_id),
            )
        except sqlite3.Error:
            return {}

        out: Dict[str, Any] = {}
        if isinstance(pack_version, str) and pack_version != "":
            out["taxonomy_pack_version"] = pack_version
        if isinstance(pack_sha256, str) and pack_sha256 != "":
            out["taxonomy_pack_sha256"] = pack_sha256
        return out

    taxonomy_version = taxonomy_pack.taxonomy_version
    manifest_summary: Dict[str, Any] = {}

    with connect() as con:
        ensure_card_tags_table(con)
        ensure_unknowns_queue_table(con)
        ensure_patches_applied_table(con)

        con.execute(
            "DELETE FROM card_tags WHERE snapshot_id = ? AND taxonomy_version = ?",
            (snapshot_id, taxonomy_version),
        )
        con.execute(
            "DELETE FROM unknowns_queue WHERE snapshot_id = ? AND taxonomy_version = ?",
            (snapshot_id, taxonomy_version),
        )
        con.execute(
            "DELETE FROM patches_applied WHERE snapshot_id = ? AND taxonomy_version = ?",
            (snapshot_id, taxonomy_version),
        )

        tag_insert_rows = [
            (
                row["oracle_id"],
                row["snapshot_id"],
                row["taxonomy_version"],
                row["ruleset_version"],
                stable_json_dumps(row["primitive_ids"]),
                stable_json_dumps(row["equiv_class_ids"]),
                stable_json_dumps(row["facets"]),
                stable_json_dumps(row["evidence"]),
                row["created_at"],
            )
            for row in tag_rows
        ]

        if tag_insert_rows:
            con.executemany(
                """
                INSERT INTO card_tags (
                  oracle_id,
                  snapshot_id,
                  taxonomy_version,
                  ruleset_version,
                  primitive_ids_json,
                  equiv_class_ids_json,
                  facets_json,
                  evidence_json,
                  created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                tag_insert_rows,
            )

        unknown_inserted = insert_unknowns(con=con, rows=unknown_rows)

        patch_rows_with_meta: List[PatchAppliedRow] = []
        for row in patch_rows_applied:
            patch_rows_with_meta.append(
                PatchAppliedRow(
                    oracle_id=row.oracle_id,
                    snapshot_id=snapshot_id,
                    taxonomy_version=taxonomy_version,
                    patch_pack_version=row.patch_pack_version,
                    patch_json=row.patch_json,
                    created_at=row.created_at,
                )
            )
        patches_inserted = record_patch_rows(con=con, rows=patch_rows_with_meta)

        manifest_summary = _write_snapshot_manifest_refs(
            con=con,
            target_snapshot_id=snapshot_id,
            pack=taxonomy_pack,
        )

        con.commit()

    summary = {
        "card_tags_written": len(tag_insert_rows),
        "unknowns_written": int(unknown_inserted),
        "patches_written": int(patches_inserted),
    }
    if manifest_summary:
        summary.update(manifest_summary)
    return summary


def compile_snapshot_tags(
    snapshot_id: str,
    taxonomy_pack_folder: str,
    patch_rows: List[Dict[str, Any]] | None = None,
    build_indices: bool = False,
) -> Dict[str, Any]:
    if not snapshot_exists(snapshot_id):
        raise ValueError(f"snapshot_id not found: {snapshot_id}")

    taxonomy_pack = load(taxonomy_pack_folder)
    compiled_rules = _compile_rules(taxonomy_pack.rulespec_rules)

    cards = _fetch_snapshot_cards(snapshot_id=snapshot_id)
    tag_rows, unknown_rows = _build_card_rows(
        cards=cards,
        compiled_rules=compiled_rules,
        snapshot_id=snapshot_id,
        taxonomy_version=taxonomy_pack.taxonomy_version,
        ruleset_version=taxonomy_pack.ruleset_version,
    )

    tag_rows_after_patch, patch_rows_applied = apply_patch_overrides(tag_rows, patch_rows)

    persist_summary = _persist_rows(
        snapshot_id=snapshot_id,
        taxonomy_pack=taxonomy_pack,
        tag_rows=tag_rows_after_patch,
        unknown_rows=unknown_rows,
        patch_rows_applied=patch_rows_applied,
    )

    run_hash_rows = [
        (
            row.get("oracle_id"),
            stable_json_dumps(row.get("primitive_ids", [])),
            stable_json_dumps(row.get("equiv_class_ids", [])),
        )
        for row in tag_rows_after_patch
        if isinstance(row.get("oracle_id"), str)
    ]
    run_hash_rows_sorted = sorted(run_hash_rows)
    run_hash = sha256_hex(stable_json_dumps(run_hash_rows_sorted))

    summary = {
        "snapshot_id": snapshot_id,
        "taxonomy_version": taxonomy_pack.taxonomy_version,
        "ruleset_version": taxonomy_pack.ruleset_version,
        "cards_seen": len(cards),
        "rules_compiled": len(compiled_rules),
        **persist_summary,
        "run_hash": run_hash,
    }

    if build_indices:
        index_summary = build_runtime_indices(
            snapshot_id=snapshot_id,
            taxonomy_version=taxonomy_pack.taxonomy_version,
        )
        summary.update(
            {
                "indices_built": bool(index_summary.get("indices_built")),
                "card_tags_scanned": int(index_summary.get("card_tags_scanned") or 0),
                "primitive_to_cards_rows": int(index_summary.get("primitive_to_cards_rows") or 0),
                "equiv_to_cards_rows": int(index_summary.get("equiv_to_cards_rows") or 0),
            }
        )

    return summary


def _resolve_cli_taxonomy_version(
    taxonomy_pack_folder: str | None,
    taxonomy_version: str | None,
) -> tuple[str, str | None]:
    pack_manifest_hash = None
    pack_taxonomy_version = None

    if isinstance(taxonomy_pack_folder, str) and taxonomy_pack_folder.strip() != "":
        pack = load(taxonomy_pack_folder)
        pack_taxonomy_version = pack.taxonomy_version
        manifest_path = pack.pack_folder / "pack_manifest.json"
        if manifest_path.exists() and manifest_path.is_file():
            pack_manifest_hash = sha256_file(manifest_path)

    taxonomy_version_clean = None
    if isinstance(taxonomy_version, str) and taxonomy_version.strip() != "":
        taxonomy_version_clean = taxonomy_version.strip()

    if taxonomy_version_clean is None:
        taxonomy_version_clean = pack_taxonomy_version

    if not isinstance(taxonomy_version_clean, str) or taxonomy_version_clean == "":
        raise ValueError("taxonomy_version is required when --taxonomy_pack is omitted")

    if (
        isinstance(pack_taxonomy_version, str)
        and pack_taxonomy_version != ""
        and pack_taxonomy_version != taxonomy_version_clean
    ):
        raise ValueError(
            "taxonomy_version does not match taxonomy pack manifest: "
            f"provided={taxonomy_version_clean} pack={pack_taxonomy_version}"
        )

    return taxonomy_version_clean, pack_manifest_hash


def get_tag_status(
    snapshot_id: str,
    taxonomy_version: str,
    taxonomy_pack_folder: str | None = None,
) -> Dict[str, Any]:
    _, manifest_hash = _resolve_cli_taxonomy_version(
        taxonomy_pack_folder=taxonomy_pack_folder,
        taxonomy_version=taxonomy_version,
    )

    card_tags_count = 0
    unknowns_count = 0
    with connect() as con:
        try:
            row = con.execute(
                "SELECT COUNT(*) FROM card_tags WHERE snapshot_id = ? AND taxonomy_version = ?",
                (snapshot_id, taxonomy_version),
            ).fetchone()
            card_tags_count = int(row[0] if row else 0)
        except sqlite3.OperationalError:
            card_tags_count = 0

        try:
            row = con.execute(
                "SELECT COUNT(*) FROM unknowns_queue WHERE snapshot_id = ? AND taxonomy_version = ?",
                (snapshot_id, taxonomy_version),
            ).fetchone()
            unknowns_count = int(row[0] if row else 0)
        except sqlite3.OperationalError:
            unknowns_count = 0

    return {
        "snapshot_id": snapshot_id,
        "taxonomy_version": taxonomy_version,
        "card_tags_count": card_tags_count,
        "unknowns_count": unknowns_count,
        "tags_exist": card_tags_count > 0,
        "pack_manifest_sha256": manifest_hash,
    }


def get_unknowns_report(
    snapshot_id: str,
    taxonomy_version: str,
    top_n: int = 50,
    examples_per_rule: int = 3,
) -> Dict[str, Any]:
    total_unknowns = 0
    top_rules: List[Dict[str, Any]] = []

    with connect() as con:
        try:
            total_row = con.execute(
                "SELECT COUNT(*) FROM unknowns_queue WHERE snapshot_id = ? AND taxonomy_version = ?",
                (snapshot_id, taxonomy_version),
            ).fetchone()
            total_unknowns = int(total_row[0] if total_row else 0)
        except sqlite3.OperationalError:
            total_unknowns = 0

        try:
            rows = con.execute(
                """
                SELECT rule_id, COUNT(*) AS cnt
                FROM unknowns_queue
                WHERE snapshot_id = ? AND taxonomy_version = ?
                GROUP BY rule_id
                ORDER BY cnt DESC, COALESCE(rule_id, '') ASC
                LIMIT ?
                """,
                (snapshot_id, taxonomy_version, int(top_n)),
            ).fetchall()
        except sqlite3.OperationalError:
            rows = []

        for row in rows:
            row_dict = dict(row)
            rule_id = row_dict.get("rule_id") if isinstance(row_dict.get("rule_id"), str) else None
            count = int(row_dict.get("cnt") or 0)

            examples_rows = con.execute(
                """
                SELECT oracle_id, snippet
                FROM unknowns_queue
                WHERE snapshot_id = ?
                  AND taxonomy_version = ?
                  AND COALESCE(rule_id, '') = ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (snapshot_id, taxonomy_version, rule_id or "", int(examples_per_rule)),
            ).fetchall()
            examples = []
            for ex_row in examples_rows:
                ex = dict(ex_row)
                examples.append(
                    {
                        "oracle_id": ex.get("oracle_id") if isinstance(ex.get("oracle_id"), str) else None,
                        "snippet": ex.get("snippet") if isinstance(ex.get("snippet"), str) else None,
                    }
                )

            top_rules.append(
                {
                    "rule_id": rule_id,
                    "count": count,
                    "examples": examples,
                }
            )

    return {
        "snapshot_id": snapshot_id,
        "taxonomy_version": taxonomy_version,
        "total_unknowns": total_unknowns,
        "top_rules": top_rules,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Compile deterministic taxonomy tags for one snapshot")
    ap.add_argument("--snapshot_id", required=True, help="Snapshot id from cards.snapshot_id")
    ap.add_argument("--taxonomy_pack", default=None, help="Path to taxonomy pack folder")
    ap.add_argument("--taxonomy_version", default=None, help="Taxonomy version override for status/report")
    ap.add_argument(
        "--patch_json",
        default=None,
        help="Optional JSON file containing patch rows (list of objects)",
    )
    ap.add_argument(
        "--build_indices",
        action="store_true",
        help="Build lookup and inverted indices after successful compile",
    )
    ap.add_argument(
        "--unknowns_report",
        action="store_true",
        help="Read-only unknowns triage report for snapshot/taxonomy_version",
    )
    ap.add_argument(
        "--status",
        action="store_true",
        help="Read-only status check for card_tags and unknowns_queue counts",
    )
    args = ap.parse_args()

    if args.status:
        taxonomy_version, _ = _resolve_cli_taxonomy_version(
            taxonomy_pack_folder=args.taxonomy_pack,
            taxonomy_version=args.taxonomy_version,
        )
        status_obj = get_tag_status(
            snapshot_id=args.snapshot_id,
            taxonomy_version=taxonomy_version,
            taxonomy_pack_folder=args.taxonomy_pack,
        )
        print(json.dumps(status_obj, separators=(",", ":"), sort_keys=True, ensure_ascii=False))
        return 0

    if args.unknowns_report:
        taxonomy_version, _ = _resolve_cli_taxonomy_version(
            taxonomy_pack_folder=args.taxonomy_pack,
            taxonomy_version=args.taxonomy_version,
        )
        report_obj = get_unknowns_report(
            snapshot_id=args.snapshot_id,
            taxonomy_version=taxonomy_version,
        )
        print(json.dumps(report_obj, separators=(",", ":"), sort_keys=True, ensure_ascii=False))
        return 0

    if not isinstance(args.taxonomy_pack, str) or args.taxonomy_pack.strip() == "":
        raise ValueError("--taxonomy_pack is required for compile mode")

    patch_rows = None
    if isinstance(args.patch_json, str) and args.patch_json.strip() != "":
        patch_obj = json.loads(open(args.patch_json, "r", encoding="utf-8").read())
        patch_rows = patch_obj if isinstance(patch_obj, list) else None

    summary = compile_snapshot_tags(
        snapshot_id=args.snapshot_id,
        taxonomy_pack_folder=args.taxonomy_pack,
        patch_rows=patch_rows,
        build_indices=bool(args.build_indices),
    )
    print(json.dumps(summary, separators=(",", ":"), sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
