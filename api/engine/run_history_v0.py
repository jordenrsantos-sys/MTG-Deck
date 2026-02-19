import json
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List

from api.engine.utils import sha256_hex, stable_json_dumps


_HASH_KEY_PATTERN = re.compile(r".*_hash_v\d+$")


def _connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    return con


def _ensure_schema(db_path: str) -> None:
    with _connect(db_path) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS run_history_v0 (
              run_id TEXT PRIMARY KEY,
              created_at TEXT NOT NULL,
              engine_version TEXT,
              db_snapshot_id TEXT,
              profile_id TEXT,
              bracket_id TEXT,
              endpoint TEXT NOT NULL,
              input_hash_v1 TEXT NOT NULL,
              output_build_hash_v1 TEXT NOT NULL,
              output_proof_attempts_hash_v2 TEXT,
              layer_hashes_json TEXT NOT NULL,
              request_json TEXT NOT NULL,
              response_json TEXT NOT NULL,
              notes TEXT
            )
            """
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_run_history_created_at ON run_history_v0(created_at)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_run_history_output_build_hash ON run_history_v0(output_build_hash_v1)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_run_history_input_hash ON run_history_v0(input_hash_v1)"
        )


def compute_input_hash_v1(payload: dict) -> str:
    normalized_payload = payload if isinstance(payload, dict) else {}
    return sha256_hex(stable_json_dumps(normalized_payload))


def compute_run_id_v0(input_hash_v1: str, build_hash_v1: str) -> str:
    return sha256_hex(f"{input_hash_v1}:{build_hash_v1}")


def _parse_json_blob(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return None
    return None


def _extract_build_payload(response: dict) -> Dict[str, Any]:
    if not isinstance(response, dict):
        return {}

    deck_payload = response.get("deck_complete_v0")
    if isinstance(deck_payload, dict):
        build_report = deck_payload.get("build_report")
        if isinstance(build_report, dict):
            return build_report

    if isinstance(response.get("build_hash_v1"), str):
        return response

    return {}


def _extract_layer_hashes(build_payload: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}

    for key, value in build_payload.items():
        if isinstance(key, str) and isinstance(value, str):
            if _HASH_KEY_PATTERN.match(key):
                out[key] = value

    result = build_payload.get("result") if isinstance(build_payload.get("result"), dict) else {}
    for key, value in result.items():
        if isinstance(key, str) and isinstance(value, str):
            if _HASH_KEY_PATTERN.match(key) and key not in out:
                out[key] = value

    return {key: out[key] for key in sorted(out.keys())}


def save_run_v0(db_path: str, endpoint: str, request: dict, response: dict, meta: dict) -> dict:
    _ensure_schema(db_path)

    request_payload = request if isinstance(request, dict) else {}
    response_payload = response if isinstance(response, dict) else {}
    meta_payload = meta if isinstance(meta, dict) else {}

    input_hash_v1 = compute_input_hash_v1(request_payload)

    build_payload = _extract_build_payload(response_payload)
    output_build_hash_v1 = build_payload.get("build_hash_v1")
    if not isinstance(output_build_hash_v1, str) or output_build_hash_v1 == "":
        raise ValueError("output build_hash_v1 is required to save deterministic run history")

    run_id = compute_run_id_v0(input_hash_v1=input_hash_v1, build_hash_v1=output_build_hash_v1)

    result_payload = build_payload.get("result") if isinstance(build_payload.get("result"), dict) else {}
    output_proof_attempts_hash_v2 = result_payload.get("proof_attempts_hash_v2")
    if not isinstance(output_proof_attempts_hash_v2, str):
        output_proof_attempts_hash_v2 = None

    engine_version = build_payload.get("engine_version")
    if not isinstance(engine_version, str):
        engine_version = meta_payload.get("engine_version") if isinstance(meta_payload.get("engine_version"), str) else None

    db_snapshot_id = build_payload.get("db_snapshot_id")
    if not isinstance(db_snapshot_id, str):
        db_snapshot_id = request_payload.get("db_snapshot_id") if isinstance(request_payload.get("db_snapshot_id"), str) else None

    profile_id = build_payload.get("profile_id")
    if not isinstance(profile_id, str):
        profile_id = request_payload.get("profile_id") if isinstance(request_payload.get("profile_id"), str) else None

    bracket_id = build_payload.get("bracket_id")
    if not isinstance(bracket_id, str):
        bracket_id = request_payload.get("bracket_id") if isinstance(request_payload.get("bracket_id"), str) else None

    layer_hashes = _extract_layer_hashes(build_payload)

    created_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    request_json = stable_json_dumps(request_payload)
    response_json = stable_json_dumps(response_payload)
    layer_hashes_json = stable_json_dumps(layer_hashes)
    notes = meta_payload.get("notes") if isinstance(meta_payload.get("notes"), str) else None

    with _connect(db_path) as con:
        con.execute(
            """
            INSERT OR IGNORE INTO run_history_v0 (
              run_id,
              created_at,
              engine_version,
              db_snapshot_id,
              profile_id,
              bracket_id,
              endpoint,
              input_hash_v1,
              output_build_hash_v1,
              output_proof_attempts_hash_v2,
              layer_hashes_json,
              request_json,
              response_json,
              notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
              run_id,
              created_at,
              engine_version,
              db_snapshot_id,
              profile_id,
              bracket_id,
              endpoint,
              input_hash_v1,
              output_build_hash_v1,
              output_proof_attempts_hash_v2,
              layer_hashes_json,
              request_json,
              response_json,
              notes,
            ),
        )

    return {
        "run_id": run_id,
        "input_hash_v1": input_hash_v1,
        "output_build_hash_v1": output_build_hash_v1,
    }


def list_runs_v0(db_path: str, limit: int = 50, endpoint: str | None = None) -> list[dict]:
    _ensure_schema(db_path)
    limit_safe = max(1, int(limit))

    with _connect(db_path) as con:
        if isinstance(endpoint, str) and endpoint.strip() != "":
            rows = con.execute(
                """
                SELECT
                  run_id,
                  created_at,
                  endpoint,
                  engine_version,
                  db_snapshot_id,
                  profile_id,
                  bracket_id,
                  input_hash_v1,
                  output_build_hash_v1
                FROM run_history_v0
                WHERE endpoint = ?
                ORDER BY created_at DESC, run_id DESC
                LIMIT ?
                """,
                (endpoint.strip(), limit_safe),
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT
                  run_id,
                  created_at,
                  endpoint,
                  engine_version,
                  db_snapshot_id,
                  profile_id,
                  bracket_id,
                  input_hash_v1,
                  output_build_hash_v1
                FROM run_history_v0
                ORDER BY created_at DESC, run_id DESC
                LIMIT ?
                """,
                (limit_safe,),
            ).fetchall()

    return [dict(row) for row in rows]


def get_run_v0(db_path: str, run_id: str) -> dict | None:
    _ensure_schema(db_path)
    with _connect(db_path) as con:
        row = con.execute(
            """
            SELECT
              run_id,
              created_at,
              engine_version,
              db_snapshot_id,
              profile_id,
              bracket_id,
              endpoint,
              input_hash_v1,
              output_build_hash_v1,
              output_proof_attempts_hash_v2,
              layer_hashes_json,
              request_json,
              response_json,
              notes
            FROM run_history_v0
            WHERE run_id = ?
            LIMIT 1
            """,
            (run_id,),
        ).fetchone()

    if row is None:
        return None

    row_dict = dict(row)
    return {
        "run_id": row_dict.get("run_id"),
        "created_at": row_dict.get("created_at"),
        "engine_version": row_dict.get("engine_version"),
        "db_snapshot_id": row_dict.get("db_snapshot_id"),
        "profile_id": row_dict.get("profile_id"),
        "bracket_id": row_dict.get("bracket_id"),
        "endpoint": row_dict.get("endpoint"),
        "input_hash_v1": row_dict.get("input_hash_v1"),
        "output_build_hash_v1": row_dict.get("output_build_hash_v1"),
        "output_proof_attempts_hash_v2": row_dict.get("output_proof_attempts_hash_v2"),
        "layer_hashes": _parse_json_blob(row_dict.get("layer_hashes_json")) or {},
        "request": _parse_json_blob(row_dict.get("request_json")) or {},
        "response": _parse_json_blob(row_dict.get("response_json")) or {},
        "notes": row_dict.get("notes"),
    }


def _extract_signal_bundle(run_obj: Dict[str, Any]) -> Dict[str, Any]:
    response = run_obj.get("response") if isinstance(run_obj.get("response"), dict) else {}
    build_payload = _extract_build_payload(response)
    result = build_payload.get("result") if isinstance(build_payload.get("result"), dict) else {}

    structural_snapshot_v1 = (
        result.get("structural_snapshot_v1")
        if isinstance(result.get("structural_snapshot_v1"), dict)
        else {}
    )
    dead_slot_ids_v1 = (
        structural_snapshot_v1.get("dead_slot_ids_v1")
        if isinstance(structural_snapshot_v1.get("dead_slot_ids_v1"), list)
        else []
    )

    return {
        "commander_dependency_signal_v1": structural_snapshot_v1.get("commander_dependency_signal_v1"),
        "primitive_concentration_index_v1": structural_snapshot_v1.get("primitive_concentration_index_v1"),
        "dead_slots_count_v1": len(dead_slot_ids_v1),
        "graph_nodes_total": result.get("graph_nodes_total"),
        "graph_edges_total": result.get("graph_edges_total"),
        "combo_candidates_v0_total": result.get("combo_candidates_v0_total"),
        "combo_proof_scaffolds_v0_total": result.get("combo_proof_scaffolds_v0_total"),
        "combo_proof_attempts_v0_total": result.get("combo_proof_attempts_v0_total"),
    }


def _dict_diff(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    keys_a = set(a.keys())
    keys_b = set(b.keys())

    added = sorted(keys_b - keys_a)
    removed = sorted(keys_a - keys_b)
    changed = sorted([k for k in (keys_a & keys_b) if a.get(k) != b.get(k)])

    return {
        "added": {k: b.get(k) for k in added},
        "removed": {k: a.get(k) for k in removed},
        "changed": {
            k: {
                "a": a.get(k),
                "b": b.get(k),
            }
            for k in changed
        },
    }


def diff_runs_v0(db_path: str, run_id_a: str, run_id_b: str) -> dict:
    run_a = get_run_v0(db_path=db_path, run_id=run_id_a)
    run_b = get_run_v0(db_path=db_path, run_id=run_id_b)

    if run_a is None or run_b is None:
        return {
            "status": "ERROR",
            "run_id_a": run_id_a,
            "run_id_b": run_id_b,
            "message": "Run not found",
        }

    top_level_hashes_a = {
        "input_hash_v1": run_a.get("input_hash_v1"),
        "output_build_hash_v1": run_a.get("output_build_hash_v1"),
        "output_proof_attempts_hash_v2": run_a.get("output_proof_attempts_hash_v2"),
    }
    top_level_hashes_b = {
        "input_hash_v1": run_b.get("input_hash_v1"),
        "output_build_hash_v1": run_b.get("output_build_hash_v1"),
        "output_proof_attempts_hash_v2": run_b.get("output_proof_attempts_hash_v2"),
    }

    layer_hashes_a = run_a.get("layer_hashes") if isinstance(run_a.get("layer_hashes"), dict) else {}
    layer_hashes_b = run_b.get("layer_hashes") if isinstance(run_b.get("layer_hashes"), dict) else {}

    signals_a = _extract_signal_bundle(run_a)
    signals_b = _extract_signal_bundle(run_b)

    return {
        "status": "OK",
        "run_id_a": run_id_a,
        "run_id_b": run_id_b,
        "top_level_hashes": _dict_diff(top_level_hashes_a, top_level_hashes_b),
        "layer_hashes": _dict_diff(layer_hashes_a, layer_hashes_b),
        "signals": _dict_diff(signals_a, signals_b),
    }
