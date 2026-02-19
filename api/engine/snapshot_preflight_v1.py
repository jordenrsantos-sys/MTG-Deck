from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Tuple

from engine.db import DB_PATH as DEFAULT_DB_PATH
from engine.db import connect as cards_db_connect
from engine.db_tags import ensure_tag_tables
from api.engine.constants import MIN_PRIMITIVE_COVERAGE, MIN_PRIMITIVE_TO_CARDS


class SnapshotPreflightError(RuntimeError):
    code = "TAGS_NOT_COMPILED"

    def __init__(self, report: Dict[str, Any]):
        self.report = dict(report)
        reason = self.report.get("reason")
        if not isinstance(reason, str) or reason.strip() == "":
            reason = "Snapshot preflight failed."
        super().__init__(f"{self.code}: {reason}")

    def to_unknown(self) -> Dict[str, Any]:
        payload = dict(self.report)
        payload["code"] = self.code
        return payload


def _normalize_str(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned if cleaned != "" else None


def _connect_for_preflight(db: Any) -> Tuple[sqlite3.Connection, bool]:
    if isinstance(db, sqlite3.Connection):
        return db, False

    if isinstance(db, (str, Path)):
        con = sqlite3.connect(str(db))
        con.row_factory = sqlite3.Row
        return con, True

    con = cards_db_connect()
    return con, True


def _is_nonempty_json_object(raw: Any) -> bool:
    if not isinstance(raw, str):
        return False
    stripped = raw.strip()
    if stripped in {"", "{}", "null"}:
        return False
    try:
        parsed = json.loads(stripped)
    except Exception:
        return False
    return isinstance(parsed, dict) and len(parsed) > 0


def _build_remediation_commands(snapshot_id: str, taxonomy_pack_hint: str | None) -> list[str]:
    taxonomy_pack_value = taxonomy_pack_hint if isinstance(taxonomy_pack_hint, str) and taxonomy_pack_hint else "<taxonomy_pack>"
    db_path_value = str(DEFAULT_DB_PATH)
    return [
        (
            "python -m snapshot_build.tag_snapshot "
            f"--db {db_path_value} "
            f"--snapshot_id {snapshot_id} "
            f"--taxonomy_pack {taxonomy_pack_value}"
        ),
        (
            "python -m snapshot_build.index_build "
            f"--db {db_path_value} "
            f"--snapshot_id {snapshot_id} "
            f"--taxonomy_pack {taxonomy_pack_value}"
        ),
    ]


def _missing_runtime_versions_report(
    snapshot_id: str,
    taxonomy_version: str | None,
    ruleset_version: str | None,
) -> Dict[str, Any]:
    snapshot_hint = snapshot_id if isinstance(snapshot_id, str) and snapshot_id.strip() != "" else "<snapshot_id>"
    return {
        "status": "TAGS_NOT_COMPILED",
        "code": SnapshotPreflightError.code,
        "snapshot_id": snapshot_id if isinstance(snapshot_id, str) else "",
        "taxonomy_version": taxonomy_version,
        "ruleset_version": ruleset_version,
        "message": "Runtime snapshot/taxonomy/ruleset versions are required for deterministic preflight.",
        "reason": "MISSING_RUNTIME_VERSIONS",
        "counts": {
            "card_tags_rows_snapshot_taxonomy_ruleset": 0,
            "card_tags_facets_nonempty_rows": 0,
            "primitive_to_cards_rows_snapshot_taxonomy": 0,
            "cards_with_any_primitive_rows_snapshot_taxonomy_ruleset": 0,
            "commander_rows_snapshot_taxonomy_ruleset": 0,
            "commander_facets_nonempty_rows": 0,
        },
        "rates": {
            "facets_nonempty_rate_overall": 0.0,
            "facets_nonempty_rate_commander": None,
            "cards_with_any_primitive_rate": 0.0,
        },
        "thresholds": {
            "min_primitive_to_cards": MIN_PRIMITIVE_TO_CARDS,
            "min_primitive_coverage": MIN_PRIMITIVE_COVERAGE,
        },
        "version_consistency": {
            "taxonomy_mismatch_rows_same_snapshot_ruleset": 0,
            "ruleset_mismatch_rows_same_snapshot_taxonomy": 0,
            "distinct_taxonomy_versions_same_snapshot_ruleset": 0,
            "distinct_ruleset_versions_same_snapshot_taxonomy": 0,
            "ruleset_enforced": False,
            "consistent": False,
        },
        "remediation_commands": _build_remediation_commands(
            snapshot_id=snapshot_hint,
            taxonomy_pack_hint=taxonomy_version,
        ),
    }


def run_snapshot_preflight(
    db,
    db_snapshot_id,
    taxonomy_version,
    ruleset_version,
    commander_oracle_id: str | None = None,
) -> Dict[str, Any]:
    """
    Deterministic runtime preflight for compiled tags/indexes.

    Semantics:
    - `db_snapshot_id`, `taxonomy_version`, and `ruleset_version` are all required.
      If any are missing, preflight fails immediately with TAGS_NOT_COMPILED and
      reason="MISSING_RUNTIME_VERSIONS".
    - Card-tag metrics are evaluated on rows filtered by:
      (snapshot_id, taxonomy_version, ruleset_version).
    - Version-consistency diagnostics are scoped to the same snapshot and compared
      against the expected taxonomy/ruleset values.
    - `primitive_to_cards` does not carry a ruleset_version column; that metric is
      filtered by (snapshot_id, taxonomy_version) only.
    """
    snapshot_id_clean = _normalize_str(db_snapshot_id) or ""
    taxonomy_version_clean = _normalize_str(taxonomy_version)
    requested_ruleset_version = _normalize_str(ruleset_version)
    commander_oracle_id_clean = _normalize_str(commander_oracle_id)

    con, should_close = _connect_for_preflight(db)
    try:
        ensure_tag_tables(con)

        if (
            snapshot_id_clean == ""
            or not isinstance(taxonomy_version_clean, str)
            or taxonomy_version_clean == ""
            or not isinstance(requested_ruleset_version, str)
            or requested_ruleset_version == ""
        ):
            raise SnapshotPreflightError(
                _missing_runtime_versions_report(
                    snapshot_id=snapshot_id_clean,
                    taxonomy_version=taxonomy_version_clean,
                    ruleset_version=requested_ruleset_version,
                )
            )

        table_exists_row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='primitive_to_cards' LIMIT 1"
        ).fetchone()
        primitive_to_cards_table_exists = table_exists_row is not None

        card_tags_rows_selected = int(
            (
                con.execute(
                    """
                    SELECT COUNT(1)
                    FROM card_tags
                    WHERE snapshot_id = ?
                      AND taxonomy_version = ?
                      AND ruleset_version = ?
                    """,
                    (snapshot_id_clean, taxonomy_version_clean, requested_ruleset_version),
                ).fetchone()
                or [0]
            )[0]
        )

        facets_nonempty_rows = int(
            (
                con.execute(
                    """
                    SELECT COUNT(1)
                    FROM card_tags
                    WHERE snapshot_id = ?
                      AND taxonomy_version = ?
                      AND ruleset_version = ?
                      AND facets_json IS NOT NULL
                      AND TRIM(facets_json) NOT IN ('', '{}', 'null')
                    """,
                    (snapshot_id_clean, taxonomy_version_clean, requested_ruleset_version),
                ).fetchone()
                or [0]
            )[0]
        )

        commander_rows = 0
        commander_facets_nonempty_rows = 0
        if isinstance(commander_oracle_id_clean, str):
            commander_row = con.execute(
                """
                SELECT facets_json
                FROM card_tags
                WHERE snapshot_id = ?
                  AND taxonomy_version = ?
                  AND ruleset_version = ?
                  AND oracle_id = ?
                LIMIT 1
                """,
                (
                    snapshot_id_clean,
                    taxonomy_version_clean,
                    requested_ruleset_version,
                    commander_oracle_id_clean,
                ),
            ).fetchone()
            if commander_row is not None:
                commander_rows = 1
                commander_facets_raw = commander_row[0]
                commander_facets_nonempty_rows = 1 if _is_nonempty_json_object(commander_facets_raw) else 0

        primitive_to_cards_rows = 0
        if primitive_to_cards_table_exists:
            primitive_to_cards_rows = int(
                (
                    con.execute(
                        "SELECT COUNT(1) FROM primitive_to_cards WHERE snapshot_id = ? AND taxonomy_version = ?",
                        (snapshot_id_clean, taxonomy_version_clean),
                    ).fetchone()
                    or [0]
                )[0]
            )

        cards_with_any_primitive_rows = int(
            (
                con.execute(
                    """
                    SELECT COUNT(1)
                    FROM card_tags
                    WHERE snapshot_id = ?
                      AND taxonomy_version = ?
                      AND ruleset_version = ?
                      AND primitive_ids_json IS NOT NULL
                      AND LENGTH(TRIM(primitive_ids_json)) > 2
                    """,
                    (snapshot_id_clean, taxonomy_version_clean, requested_ruleset_version),
                ).fetchone()
                or [0]
            )[0]
        )

        distinct_taxonomy_versions = int(
            (
                con.execute(
                    """
                    SELECT COUNT(DISTINCT taxonomy_version)
                    FROM card_tags
                    WHERE snapshot_id = ?
                      AND ruleset_version = ?
                    """,
                    (snapshot_id_clean, requested_ruleset_version),
                ).fetchone()
                or [0]
            )[0]
        )

        taxonomy_mismatch_rows = int(
            (
                con.execute(
                    """
                    SELECT COUNT(1)
                    FROM card_tags
                    WHERE snapshot_id = ?
                      AND ruleset_version = ?
                      AND taxonomy_version <> ?
                    """,
                    (snapshot_id_clean, requested_ruleset_version, taxonomy_version_clean),
                ).fetchone()
                or [0]
            )[0]
        )

        ruleset_rows = con.execute(
            """
            SELECT ruleset_version, COUNT(1)
            FROM card_tags
            WHERE snapshot_id = ?
              AND taxonomy_version = ?
            GROUP BY ruleset_version
            ORDER BY COUNT(1) DESC, ruleset_version ASC
            """,
            (snapshot_id_clean, taxonomy_version_clean),
        ).fetchall()
        ruleset_counts: list[tuple[str, int]] = []
        for row in ruleset_rows:
            ruleset_value = row[0]
            row_count = row[1]
            if isinstance(ruleset_value, str):
                ruleset_counts.append((ruleset_value, int(row_count)))

        distinct_ruleset_versions = len(ruleset_counts)
        ruleset_mismatch_rows = 0
        ruleset_mismatch_rows = sum(
            row_count
            for ruleset_value, row_count in ruleset_counts
            if ruleset_value != requested_ruleset_version
        )

        facets_nonempty_rate_overall = (
            float(facets_nonempty_rows) / float(card_tags_rows_selected)
            if card_tags_rows_selected > 0
            else 0.0
        )
        facets_nonempty_rate_commander = (
            float(commander_facets_nonempty_rows) / float(commander_rows)
            if commander_rows > 0
            else None
        )
        cards_with_any_primitive_rate = (
            float(cards_with_any_primitive_rows) / float(card_tags_rows_selected)
            if card_tags_rows_selected > 0
            else 0.0
        )

        counts = {
            "card_tags_rows_snapshot_taxonomy_ruleset": card_tags_rows_selected,
            "card_tags_facets_nonempty_rows": facets_nonempty_rows,
            "primitive_to_cards_rows_snapshot_taxonomy": primitive_to_cards_rows,
            "cards_with_any_primitive_rows_snapshot_taxonomy_ruleset": cards_with_any_primitive_rows,
            "commander_rows_snapshot_taxonomy_ruleset": commander_rows,
            "commander_facets_nonempty_rows": commander_facets_nonempty_rows,
        }
        rates = {
            "facets_nonempty_rate_overall": round(facets_nonempty_rate_overall, 6),
            "facets_nonempty_rate_commander": (
                round(float(facets_nonempty_rate_commander), 6)
                if isinstance(facets_nonempty_rate_commander, float)
                else None
            ),
            "cards_with_any_primitive_rate": round(cards_with_any_primitive_rate, 6),
        }
        thresholds = {
            "min_primitive_to_cards": MIN_PRIMITIVE_TO_CARDS,
            "min_primitive_coverage": MIN_PRIMITIVE_COVERAGE,
        }

        version_consistency = {
            "taxonomy_mismatch_rows_same_snapshot_ruleset": taxonomy_mismatch_rows,
            "ruleset_mismatch_rows_same_snapshot_taxonomy": ruleset_mismatch_rows,
            "distinct_taxonomy_versions_same_snapshot_ruleset": distinct_taxonomy_versions,
            "distinct_ruleset_versions_same_snapshot_taxonomy": distinct_ruleset_versions,
            "ruleset_enforced": True,
            "consistent": (taxonomy_mismatch_rows == 0 and ruleset_mismatch_rows == 0),
        }

        remediation_commands = _build_remediation_commands(
            snapshot_id=snapshot_id_clean,
            taxonomy_pack_hint=taxonomy_version_clean,
        )

        failures: list[str] = []
        if card_tags_rows_selected <= 0:
            failures.append("card_tags rows missing for snapshot/taxonomy_version")
        if facets_nonempty_rows <= 0:
            failures.append("facets_json appears empty for all card_tags rows")
        if isinstance(commander_oracle_id_clean, str):
            if commander_rows <= 0:
                failures.append("commander oracle_id missing from card_tags for snapshot/taxonomy_version")
            elif commander_facets_nonempty_rows <= 0:
                failures.append("commander card_tags facets_json is empty")
        if not primitive_to_cards_table_exists:
            failures.append("primitive_to_cards table is missing")
        elif primitive_to_cards_rows <= 0:
            failures.append("primitive_to_cards rows missing for snapshot/taxonomy_version")
        elif primitive_to_cards_rows < MIN_PRIMITIVE_TO_CARDS:
            failures.append(
                "primitive_to_cards rows below stale-compilation threshold "
                f"({primitive_to_cards_rows} < {MIN_PRIMITIVE_TO_CARDS})"
            )
        if card_tags_rows_selected > 0 and cards_with_any_primitive_rate < MIN_PRIMITIVE_COVERAGE:
            failures.append(
                "cards_with_any_primitive_rate below stale-compilation threshold "
                f"({cards_with_any_primitive_rate:.6f} < {MIN_PRIMITIVE_COVERAGE:.6f})"
            )
        if taxonomy_mismatch_rows > 0:
            failures.append("snapshot contains mixed taxonomy_version rows")
        if ruleset_mismatch_rows > 0:
            failures.append("snapshot/taxonomy contains mixed ruleset_version rows")

        report: Dict[str, Any] = {
            "status": "OK",
            "code": SnapshotPreflightError.code,
            "snapshot_id": snapshot_id_clean,
            "taxonomy_version": taxonomy_version_clean,
            "ruleset_version": requested_ruleset_version,
            "commander_oracle_id": commander_oracle_id_clean,
            "counts": counts,
            "rates": rates,
            "thresholds": thresholds,
            "version_consistency": version_consistency,
            "remediation_commands": remediation_commands,
        }

        if failures:
            report["status"] = "TAGS_NOT_COMPILED"
            report["message"] = (
                "Compiled tags/index preflight failed for snapshot/taxonomy_version. "
                "Run snapshot_build.tag_snapshot and snapshot_build.index_build."
            )
            report["reason"] = "; ".join(failures)
            report["failures"] = failures
            raise SnapshotPreflightError(report)

        return report
    finally:
        if should_close:
            con.close()
