import argparse
import json
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.engine.utils import sha256_hex, stable_json_dumps
from engine.db import DB_PATH, list_snapshots


PRIMITIVE_DEFS_REL = Path("data/primitives/primitive_defs_v0.json")
PRIMITIVE_RULES_REL = Path("data/primitives/primitive_rules_v0.json")

TAG_BATCH_SIZE = 2000


def _ensure_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS primitive_defs_v0 (
          primitive_id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          description TEXT NOT NULL,
          category TEXT NOT NULL,
          is_engine_primitive INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS primitive_rules_v0 (
          rule_id TEXT PRIMARY KEY,
          primitive_id TEXT NOT NULL,
          rule_type TEXT NOT NULL,
          pattern TEXT NOT NULL,
          weight REAL NOT NULL DEFAULT 1.0,
          priority INTEGER NOT NULL DEFAULT 100,
          notes TEXT,
          enabled INTEGER NOT NULL DEFAULT 1,
          ruleset_version TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS card_primitive_tags_v0 (
          oracle_id TEXT NOT NULL,
          card_name TEXT NOT NULL,
          primitive_id TEXT NOT NULL,
          ruleset_version TEXT NOT NULL,
          confidence REAL NOT NULL,
          evidence_json TEXT NOT NULL,
          PRIMARY KEY (oracle_id, primitive_id, ruleset_version)
        );

        CREATE INDEX IF NOT EXISTS idx_cpt_v0_primitive
        ON card_primitive_tags_v0(primitive_id, ruleset_version);

        CREATE INDEX IF NOT EXISTS idx_cpt_v0_oracle
        ON card_primitive_tags_v0(oracle_id, ruleset_version);

        CREATE TABLE IF NOT EXISTS primitive_tag_runs_v0 (
          run_id TEXT PRIMARY KEY,
          db_snapshot_id TEXT NOT NULL,
          ruleset_version TEXT NOT NULL,
          cards_processed INTEGER NOT NULL,
          tags_emitted INTEGER NOT NULL,
          unknowns_emitted INTEGER NOT NULL,
          run_hash_v1 TEXT NOT NULL,
          created_at TEXT
        );

        CREATE TABLE IF NOT EXISTS primitive_tag_unknowns_v0 (
          oracle_id TEXT NOT NULL,
          card_name TEXT NOT NULL,
          reason TEXT NOT NULL,
          details_json TEXT NOT NULL,
          ruleset_version TEXT NOT NULL,
          PRIMARY KEY (oracle_id, reason, ruleset_version)
        );
        """
    )


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_snapshot_id(requested_snapshot_id: str | None) -> str:
    if isinstance(requested_snapshot_id, str) and requested_snapshot_id.strip() != "":
        return requested_snapshot_id.strip()

    snapshots = list_snapshots(limit=1)
    if snapshots and isinstance(snapshots[0].get("snapshot_id"), str):
        return snapshots[0]["snapshot_id"]

    raise ValueError("No local snapshot available")


def _normalize_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split()).strip()


def _snippet_from_match(value: str, match_start: int, match_end: int) -> str:
    normalized = _normalize_text(value)
    if normalized == "":
        return ""

    start = max(0, int(match_start))
    end = max(start, int(match_end))
    raw = normalized[start:end]
    if raw == "":
        raw = normalized[start:start + 80]
    return raw[:80]


def _parse_keywords(value: Any) -> List[str]:
    if isinstance(value, list):
        return [k for k in value if isinstance(k, str)]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return []
        if isinstance(parsed, list):
            return [k for k in parsed if isinstance(k, str)]
    return []


def _normalize_rules(
    rules_raw: List[Dict[str, Any]],
    ruleset_version: str,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rules_raw:
        if not isinstance(row, dict):
            continue

        rule_id = row.get("rule_id")
        primitive_id = row.get("primitive_id")
        rule_type = row.get("rule_type")
        pattern = row.get("pattern")
        if not isinstance(rule_id, str) or rule_id == "":
            continue
        if not isinstance(primitive_id, str) or primitive_id == "":
            continue
        if not isinstance(rule_type, str) or rule_type == "":
            continue
        if not isinstance(pattern, str):
            continue

        enabled_value = row.get("enabled", 1)
        enabled = 1 if int(enabled_value) == 1 else 0

        out.append(
            {
                "rule_id": rule_id,
                "primitive_id": primitive_id,
                "rule_type": rule_type,
                "pattern": pattern,
                "weight": float(row.get("weight", 1.0)),
                "priority": int(row.get("priority", 100)),
                "notes": row.get("notes") if isinstance(row.get("notes"), str) else None,
                "enabled": enabled,
                "ruleset_version": ruleset_version,
            }
        )

    out.sort(key=lambda rule: (int(rule.get("priority", 100)), str(rule.get("rule_id") or "")))
    return out


def _compile_regex_rules(rules: List[Dict[str, Any]]) -> Dict[str, re.Pattern[str]]:
    compiled: Dict[str, re.Pattern[str]] = {}
    for rule in rules:
        rule_type = rule.get("rule_type")
        rule_id = rule.get("rule_id")
        pattern = rule.get("pattern")
        if not isinstance(rule_id, str) or not isinstance(pattern, str):
            continue
        if rule_type not in {"oracle_regex", "type_line_regex"}:
            continue
        compiled[rule_id] = re.compile(pattern, flags=re.IGNORECASE)
    return compiled


def _matches_keyword_rule(pattern_raw: str, keywords: List[str]) -> Tuple[bool, str]:
    keywords_lower = {k.lower() for k in keywords if isinstance(k, str)}
    if not keywords_lower:
        return False, ""

    try:
        parsed = json.loads(pattern_raw)
    except (TypeError, ValueError):
        parsed = pattern_raw

    keyword_candidates: List[str] = []
    if isinstance(parsed, dict):
        any_values = parsed.get("any")
        if isinstance(any_values, list):
            keyword_candidates = [v for v in any_values if isinstance(v, str)]
    elif isinstance(parsed, list):
        keyword_candidates = [v for v in parsed if isinstance(v, str)]
    elif isinstance(parsed, str):
        keyword_candidates = [parsed]

    for candidate in keyword_candidates:
        candidate_clean = candidate.strip().lower()
        if candidate_clean != "" and candidate_clean in keywords_lower:
            return True, candidate[:80]

    return False, ""


def _match_rule(
    rule: Dict[str, Any],
    regex_cache: Dict[str, re.Pattern[str]],
    oracle_text: str,
    type_line: str,
    keywords: List[str],
) -> Tuple[bool, str, str]:
    rule_id = rule.get("rule_id")
    rule_type = rule.get("rule_type")
    if not isinstance(rule_id, str) or not isinstance(rule_type, str):
        return False, "", ""

    if rule_type == "oracle_regex":
        pattern = regex_cache.get(rule_id)
        if pattern is None:
            return False, "", ""
        match = pattern.search(oracle_text)
        if match is None:
            return False, "", ""
        snippet = _snippet_from_match(oracle_text, match.start(), match.end())
        return True, "oracle_text", snippet

    if rule_type == "type_line_regex":
        pattern = regex_cache.get(rule_id)
        if pattern is None:
            return False, "", ""
        match = pattern.search(type_line)
        if match is None:
            return False, "", ""
        snippet = _snippet_from_match(type_line, match.start(), match.end())
        return True, "type_line", snippet

    if rule_type == "keyword":
        matched, snippet = _matches_keyword_rule(pattern_raw=str(rule.get("pattern") or ""), keywords=keywords)
        if not matched:
            return False, "", ""
        # Keep evidence field constrained to oracle_text/type_line in v0.
        return True, "oracle_text", snippet

    return False, "", ""


def _select_matched_field(matched_fields: List[str]) -> str:
    normalized = {field for field in matched_fields if isinstance(field, str)}
    if "oracle_text" in normalized:
        return "oracle_text"
    if "type_line" in normalized:
        return "type_line"
    return "oracle_text"


def _compute_confidence(rule_weights: List[float]) -> float:
    distinct_rules = len(rule_weights)
    if distinct_rules <= 0:
        return 0.0
    confidence_start = min(0.95, 0.60 + (0.10 * float(distinct_rules - 1)))
    avg_weight = sum(rule_weights) / float(distinct_rules)
    confidence = min(0.95, confidence_start * avg_weight)
    return float(f"{confidence:.6f}")


def _insert_batches(
    con: sqlite3.Connection,
    tags_batch: List[Tuple[Any, ...]],
    unknowns_batch: List[Tuple[Any, ...]],
) -> None:
    if tags_batch:
        con.executemany(
            """
            INSERT OR REPLACE INTO card_primitive_tags_v0 (
              oracle_id,
              card_name,
              primitive_id,
              ruleset_version,
              confidence,
              evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            tags_batch,
        )
        tags_batch.clear()

    if unknowns_batch:
        con.executemany(
            """
            INSERT OR REPLACE INTO primitive_tag_unknowns_v0 (
              oracle_id,
              card_name,
              reason,
              details_json,
              ruleset_version
            ) VALUES (?, ?, ?, ?, ?)
            """,
            unknowns_batch,
        )
        unknowns_batch.clear()


def main() -> int:
    ap = argparse.ArgumentParser(description="Build deterministic global primitive tag index v0")
    ap.add_argument("--snapshot-id", default=None, help="Optional snapshot_id. Defaults to latest local snapshot.")
    args = ap.parse_args()

    db_snapshot_id = _resolve_snapshot_id(args.snapshot_id)

    defs_path = (REPO_ROOT / PRIMITIVE_DEFS_REL).resolve()
    rules_path = (REPO_ROOT / PRIMITIVE_RULES_REL).resolve()

    defs_obj = _load_json(defs_path)
    rules_obj = _load_json(rules_path)

    ruleset_version_defs = defs_obj.get("ruleset_version")
    ruleset_version_rules = rules_obj.get("ruleset_version")
    if not isinstance(ruleset_version_defs, str) or ruleset_version_defs == "":
        raise ValueError("primitive_defs_v0.json missing ruleset_version")
    if ruleset_version_rules != ruleset_version_defs:
        raise ValueError("ruleset_version mismatch between primitive_defs_v0.json and primitive_rules_v0.json")

    ruleset_version = ruleset_version_defs

    primitive_defs = defs_obj.get("primitives") if isinstance(defs_obj.get("primitives"), list) else []
    primitive_defs_rows: List[Tuple[Any, ...]] = []
    for primitive in primitive_defs:
        if not isinstance(primitive, dict):
            continue
        primitive_id = primitive.get("primitive_id")
        name = primitive.get("name")
        description = primitive.get("description")
        category = primitive.get("category")
        if not all(isinstance(v, str) and v != "" for v in [primitive_id, name, description, category]):
            continue
        is_engine_primitive = 1 if int(primitive.get("is_engine_primitive", 0)) == 1 else 0
        primitive_defs_rows.append((primitive_id, name, description, category, is_engine_primitive))

    rules_raw = rules_obj.get("rules") if isinstance(rules_obj.get("rules"), list) else []
    rules = _normalize_rules(rules_raw=rules_raw, ruleset_version=ruleset_version)

    primitive_rules_rows = [
        (
            row["rule_id"],
            row["primitive_id"],
            row["rule_type"],
            row["pattern"],
            float(row["weight"]),
            int(row["priority"]),
            row["notes"],
            int(row["enabled"]),
            row["ruleset_version"],
        )
        for row in rules
    ]

    active_rules = [row for row in rules if int(row.get("enabled", 0)) == 1]
    regex_cache = _compile_regex_rules(active_rules)

    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    try:
        _ensure_schema(con)
        con.execute("BEGIN")

        con.executemany(
            """
            INSERT OR REPLACE INTO primitive_defs_v0 (
              primitive_id,
              name,
              description,
              category,
              is_engine_primitive
            ) VALUES (?, ?, ?, ?, ?)
            """,
            primitive_defs_rows,
        )

        con.executemany(
            """
            INSERT OR REPLACE INTO primitive_rules_v0 (
              rule_id,
              primitive_id,
              rule_type,
              pattern,
              weight,
              priority,
              notes,
              enabled,
              ruleset_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            primitive_rules_rows,
        )

        con.execute(
            "DELETE FROM card_primitive_tags_v0 WHERE ruleset_version = ?",
            (ruleset_version,),
        )
        con.execute(
            "DELETE FROM primitive_tag_unknowns_v0 WHERE ruleset_version = ?",
            (ruleset_version,),
        )

        cards_cursor = con.execute(
            """
            SELECT
              oracle_id,
              name,
              oracle_text,
              type_line,
              keywords
            FROM cards
            WHERE snapshot_id = ?
            ORDER BY
              CASE WHEN oracle_id IS NULL THEN 1 ELSE 0 END ASC,
              oracle_id ASC,
              name ASC
            """,
            (db_snapshot_id,),
        )

        tags_batch: List[Tuple[Any, ...]] = []
        unknowns_batch: List[Tuple[Any, ...]] = []
        run_hash_rows: List[Tuple[str, str, str]] = []

        cards_processed = 0
        tags_emitted = 0
        unknowns_emitted = 0

        for row in cards_cursor:
            oracle_id = row["oracle_id"]
            card_name = row["name"]
            if not isinstance(oracle_id, str) or oracle_id == "":
                continue
            if not isinstance(card_name, str) or card_name == "":
                continue

            cards_processed += 1

            oracle_text = _normalize_text(row["oracle_text"])
            type_line = _normalize_text(row["type_line"])
            keywords = _parse_keywords(row["keywords"])

            matches_by_primitive: Dict[str, Dict[str, Any]] = {}

            for rule in active_rules:
                matched, matched_field, snippet = _match_rule(
                    rule=rule,
                    regex_cache=regex_cache,
                    oracle_text=oracle_text,
                    type_line=type_line,
                    keywords=keywords,
                )
                if not matched:
                    continue

                primitive_id = rule.get("primitive_id")
                rule_id = rule.get("rule_id")
                weight = float(rule.get("weight", 1.0))
                if not isinstance(primitive_id, str) or not isinstance(rule_id, str):
                    continue

                bucket = matches_by_primitive.setdefault(
                    primitive_id,
                    {
                        "rule_ids": [],
                        "weights": [],
                        "snippets": [],
                        "matched_fields": [],
                    },
                )
                bucket["rule_ids"].append(rule_id)
                bucket["weights"].append(weight)
                bucket["snippets"].append({"rule_id": rule_id, "snippet": snippet[:80]})
                bucket["matched_fields"].append(matched_field)

            primitive_ids_sorted = sorted(matches_by_primitive.keys())
            for primitive_id in primitive_ids_sorted:
                match_obj = matches_by_primitive[primitive_id]
                rule_ids = [rid for rid in match_obj.get("rule_ids", []) if isinstance(rid, str)]
                weights = [float(w) for w in match_obj.get("weights", [])]
                if not rule_ids or not weights:
                    continue

                confidence = _compute_confidence(rule_weights=weights)
                low_weight_present = any(float(w) < 0.5 for w in weights)

                snippets = [
                    {
                        "rule_id": snippet_obj.get("rule_id"),
                        "snippet": str(snippet_obj.get("snippet") or "")[:80],
                    }
                    for snippet_obj in match_obj.get("snippets", [])
                    if isinstance(snippet_obj, dict)
                ]

                matched_fields = [
                    field
                    for field in match_obj.get("matched_fields", [])
                    if isinstance(field, str)
                ]
                matched_field_value = _select_matched_field(matched_fields)

                evidence_obj = {
                    "matched_rule_ids": rule_ids,
                    "matched_field": matched_field_value,
                    "snippets": snippets,
                }
                evidence_json = stable_json_dumps(evidence_obj)

                if low_weight_present or confidence < 0.60:
                    reason = "LOW_RULE_WEIGHT" if low_weight_present else "LOW_CONFIDENCE"
                    reason_with_primitive = f"{reason}:{primitive_id}"
                    details_json = stable_json_dumps(
                        {
                            "primitive_id": primitive_id,
                            "confidence": confidence,
                            "avg_rule_weight": float(f"{(sum(weights) / float(len(weights))):.6f}"),
                            "matched_rule_ids": rule_ids,
                            "matched_field": matched_field_value,
                        }
                    )
                    unknowns_batch.append(
                        (
                            oracle_id,
                            card_name,
                            reason_with_primitive,
                            details_json,
                            ruleset_version,
                        )
                    )
                    unknowns_emitted += 1
                else:
                    confidence_rounded_6 = float(f"{confidence:.6f}")
                    confidence_for_hash = f"{confidence_rounded_6:.6f}"
                    tags_batch.append(
                        (
                            oracle_id,
                            card_name,
                            primitive_id,
                            ruleset_version,
                            confidence_rounded_6,
                            evidence_json,
                        )
                    )
                    run_hash_rows.append((oracle_id, primitive_id, confidence_for_hash))
                    tags_emitted += 1

                if len(tags_batch) >= TAG_BATCH_SIZE or len(unknowns_batch) >= TAG_BATCH_SIZE:
                    _insert_batches(con=con, tags_batch=tags_batch, unknowns_batch=unknowns_batch)

        _insert_batches(con=con, tags_batch=tags_batch, unknowns_batch=unknowns_batch)

        run_hash_rows_sorted = sorted(run_hash_rows)
        run_hash_v1 = sha256_hex(stable_json_dumps(run_hash_rows_sorted))
        run_id = sha256_hex(f"{db_snapshot_id}:{ruleset_version}:{run_hash_v1}")

        created_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        con.execute(
            """
            INSERT OR REPLACE INTO primitive_tag_runs_v0 (
              run_id,
              db_snapshot_id,
              ruleset_version,
              cards_processed,
              tags_emitted,
              unknowns_emitted,
              run_hash_v1,
              created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                db_snapshot_id,
                ruleset_version,
                cards_processed,
                tags_emitted,
                unknowns_emitted,
                run_hash_v1,
                created_at,
            ),
        )

        con.commit()
    finally:
        con.close()

    summary = {
        "db_snapshot_id": db_snapshot_id,
        "ruleset_version": ruleset_version,
        "cards_processed": cards_processed,
        "tags_emitted": tags_emitted,
        "unknowns_emitted": unknowns_emitted,
        "run_hash_v1": run_hash_v1,
        "run_id": run_id,
    }
    print(stable_json_dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
