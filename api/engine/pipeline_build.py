import os
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

from engine.db import (
    connect as cards_db_connect,
    snapshot_exists,
    find_card_by_name,
    suggest_card_names,
    is_legal_commander_card,
    CommanderEligibilityUnknownError,
)
from engine.db_tags import TagSnapshotMissingError, bulk_get_card_tags, ensure_tag_tables
from engine.game_changers import detect_game_changers, bracket_floor_from_count

from api.engine.constants import *
from api.engine.layers.canonical_v1 import run_canonical_v1
from api.engine.layers.combo_candidate_v0 import run_combo_candidate_v0
from api.engine.layers.combo_skeleton_v0 import run_combo_skeleton_v0
from api.engine.layers.disruption_v1 import run_disruption_v1
from api.engine.layers.graph_v3_typed import run_graph_v3_typed
from api.engine.layers.motif_v1 import run_motif_v1
from api.engine.layers.pathways_v1 import run_pathways_v1
from api.engine.layers.primitive_index_v1 import run_primitive_index_v1
from api.engine.layers.proof_attempt_v1 import run_proof_attempt_v1
from api.engine.layers.proof_scaffold_v1 import run_proof_scaffold_v1
from api.engine.graph_expand_v1 import build_bipartite_graph_v1, expand_candidate_edges_v1
from api.engine.snapshot_preflight_v1 import SnapshotPreflightError, run_snapshot_preflight
from api.engine.layers.structural_v1 import run_structural_v1
from api.engine.structural_snapshot_v1 import build_structural_snapshot_v1
from api.engine.unknowns import add_unknown, sort_unknowns
from api.engine.utils import stable_json_dumps, sha256_hex, strip_hash_fields
from api.engine.validate_invariants_v1 import validate_invariants_v1
from api.engine.version_resolve_v1 import resolve_runtime_ruleset_version, resolve_runtime_taxonomy_version


def is_singleton_exempt(card_name: str, resolved: dict | None) -> bool:
    if card_name in SINGLETON_EXEMPT_NAMES:
        return True
    type_line = (resolved or {}).get("type_line") or ""
    return "basic land" in type_line.lower()


def get_format_legality(card: dict, fmt: str) -> tuple[bool, str]:
    legalities = card.get("legalities") or {}
    status = legalities.get(fmt)
    if status == "legal":
        return True, "legal"
    if status is None:
        return False, "missing"
    return False, status


def sorted_unique(seq):
    return sorted(set(x for x in seq if x is not None))


def lookup_cards_by_oracle_id(snapshot_id: str, oracle_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    _ = snapshot_id
    _ = oracle_ids
    assert_runtime_no_oracle_text(
        "pipeline_build.lookup_cards_by_oracle_id attempted forbidden runtime oracle_text access"
    )
    return {}


def normalize_primitives_source(value: Any) -> List[str]:
    if value is None:
        return []
    parsed = value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            parsed = []
    if not isinstance(parsed, list):
        return []
    return sorted_unique([p for p in parsed if isinstance(p, str)])


def make_slot_id(prefix: str, idx: int) -> str:
    return f"{prefix}{idx}"


def slot_sort_key(slot_id: str) -> tuple:
    if not isinstance(slot_id, str) or len(slot_id) < 2:
        return (2, 10**9, str(slot_id))
    prefix = slot_id[0]
    suffix = slot_id[1:]
    prefix_rank = 0 if prefix == "C" else 1 if prefix == "S" else 2
    if suffix.isdigit():
        return (prefix_rank, int(suffix), slot_id)
    return (prefix_rank, 10**9, slot_id)


def order_by_node_order(slot_ids: list[str], node_order: list[str]) -> list[str]:
    idx = {sid: i for i, sid in enumerate(node_order)}
    return sorted(slot_ids, key=lambda sid: (idx.get(sid, 10**9), sid))


def rules_db_connect() -> sqlite3.Connection | None:
    if not RULES_DB_AVAILABLE:
        return None
    try:
        return sqlite3.connect(f"file:{RULES_DB_ABS_PATH.as_posix()}?mode=ro", uri=True)
    except Exception:
        try:
            return sqlite3.connect(str(RULES_DB_ABS_PATH))
        except Exception:
            return None


def rules_lookup_by_fts(conn, ruleset_id: str, query: str, limit: int) -> list[dict]:
    return rules_lookup_by_fts_with_trace(conn, ruleset_id, query, limit).get("matches", [])


def rules_lookup_by_fts_with_trace(
    conn,
    ruleset_id: str,
    query: str,
    limit: int,
    fetch_limit_raw: int | None = None,
    prefer_sections: list[str] | None = None,
) -> dict:
    fetch_limit = max(int(fetch_limit_raw or max(limit * 5, 25)), int(limit))
    prefer_sections_clean = sorted_unique([s for s in (prefer_sections or []) if isinstance(s, str)])
    prefer_sections_set = set(prefer_sections_clean)

    if conn is None:
        return {
            "matches": [],
            "trace": {
                "fts_fetch_limit_raw": fetch_limit,
                "prefer_sections_applied": prefer_sections_clean,
                "fts_returned_rule_ids_raw": [],
                "selected_rule_ids_sorted": [],
                "preferred_rule_ids_sorted": [],
                "nonpreferred_rule_ids_sorted": [],
                "selected_rule_ids_final": [],
            },
        }

    try:
        rows = conn.execute(
            """
            SELECT rule_id, section_id
            FROM rules_rule_fts
            WHERE rules_rule_fts MATCH ?
              AND ruleset_id = ?
            LIMIT ?
            """,
            (query, ruleset_id, fetch_limit),
        ).fetchall()
    except Exception:
        return {
            "matches": [],
            "trace": {
                "fts_fetch_limit_raw": fetch_limit,
                "fts_returned_rule_ids_raw": [],
                "selected_rule_ids_sorted": [],
                "selected_rule_ids_final": [],
            },
        }

    raw_rule_ids: list[str] = []
    items = []
    for rule_id, section_id in rows:
        if isinstance(rule_id, str):
            raw_rule_ids.append(rule_id)
            items.append(
                {
                    "rule_id": rule_id,
                    "section_id": section_id if isinstance(section_id, str) else None,
                }
            )

    items.sort(key=lambda x: ((x.get("section_id") or ""), x.get("rule_id") or ""))
    selected_rule_ids_sorted = [item.get("rule_id") for item in items if isinstance(item.get("rule_id"), str)]

    preferred_items = []
    nonpreferred_items = []
    if prefer_sections_set:
        for item in items:
            section_id = item.get("section_id")
            if isinstance(section_id, str) and section_id in prefer_sections_set:
                preferred_items.append(item)
            else:
                nonpreferred_items.append(item)
    else:
        nonpreferred_items = list(items)

    preferred_items.sort(key=lambda x: ((x.get("section_id") or ""), x.get("rule_id") or ""))
    nonpreferred_items.sort(key=lambda x: ((x.get("section_id") or ""), x.get("rule_id") or ""))

    ordered_items = preferred_items + nonpreferred_items
    selected_final = ordered_items[:limit]
    preferred_rule_ids_sorted = [item.get("rule_id") for item in preferred_items if isinstance(item.get("rule_id"), str)]
    nonpreferred_rule_ids_sorted = [
        item.get("rule_id") for item in nonpreferred_items if isinstance(item.get("rule_id"), str)
    ]
    selected_rule_ids_final = [item.get("rule_id") for item in selected_final if isinstance(item.get("rule_id"), str)]

    return {
        "matches": selected_final,
        "trace": {
            "fts_fetch_limit_raw": fetch_limit,
            "prefer_sections_applied": prefer_sections_clean,
            "fts_returned_rule_ids_raw": raw_rule_ids,
            "selected_rule_ids_sorted": selected_rule_ids_sorted,
            "preferred_rule_ids_sorted": preferred_rule_ids_sorted,
            "nonpreferred_rule_ids_sorted": nonpreferred_rule_ids_sorted,
            "selected_rule_ids_final": selected_rule_ids_final,
        },
    }


def rules_fetch_text(conn, ruleset_id: str, rule_id: str) -> dict | None:
    if conn is None:
        return None
    try:
        row = conn.execute(
            """
            SELECT rule_text, section_id
            FROM rules_rule
            WHERE ruleset_id = ? AND rule_id = ?
            """,
            (ruleset_id, rule_id),
        ).fetchone()
    except Exception:
        return None
    if not row:
        return None
    return {
        "rule_text": row[0],
        "section_id": row[1],
    }


def make_rule_citation(conn, ruleset_id: str, rule_id: str, ruleset_sha_cache: dict[str, Any]) -> dict:
    citation = {
        "ruleset_id": ruleset_id,
        "rule_id": rule_id,
        "section_id": None,
        "source_sha256": None,
        "quote": None,
    }
    if conn is None:
        return citation

    if ruleset_id not in ruleset_sha_cache:
        try:
            row = conn.execute(
                "SELECT source_sha256 FROM ruleset_source WHERE ruleset_id = ?",
                (ruleset_id,),
            ).fetchone()
            ruleset_sha_cache[ruleset_id] = row[0] if row and isinstance(row[0], str) else None
        except Exception:
            ruleset_sha_cache[ruleset_id] = None

    rule_row = rules_fetch_text(conn, ruleset_id, rule_id)
    if rule_row is None:
        citation["source_sha256"] = ruleset_sha_cache.get(ruleset_id)
        return citation

    normalized_rule_text = " ".join(str(rule_row.get("rule_text") or "").split())
    citation["section_id"] = rule_row.get("section_id") if isinstance(rule_row.get("section_id"), str) else None
    citation["source_sha256"] = ruleset_sha_cache.get(ruleset_id)
    citation["quote"] = normalized_rule_text[:160] if normalized_rule_text else None
    return citation


def apply_primitive_overrides(
    prims: list[str],
    oracle_id: str,
    primitive_overrides_by_oracle: Dict[str, List[Dict[str, Any]]],
) -> tuple[list[str], list[dict]]:
    base_set = set([p for p in prims if isinstance(p, str)])
    applied_patches_summary = []

    for patch in primitive_overrides_by_oracle.get(oracle_id, []):
        remove_items = sorted_unique([p for p in (patch.get("remove") or []) if isinstance(p, str)])
        add_items = sorted_unique([p for p in (patch.get("add") or []) if isinstance(p, str)])
        for item in remove_items:
            if item in base_set:
                base_set.remove(item)
        for item in add_items:
            base_set.add(item)
        applied_patches_summary.append(
            {
                "patch_id": patch.get("patch_id"),
                "oracle_id": oracle_id,
                "add_applied": add_items,
                "remove_applied": remove_items,
            }
        )

    return sorted_unique(base_set), applied_patches_summary


def is_ci_compatible(commander: dict, card: dict) -> bool:
    return set(card.get("color_identity") or []).issubset(set(commander.get("color_identity") or []))


def run_build_pipeline(req, conn=None, repo_root_path: Path | None = None) -> dict:
    _ = conn
    _ = repo_root_path
    from api.main import BuildResponse

    def _ui_result_envelope(extra: Dict[str, Any] | None = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "ui_contract_version": UI_CONTRACT_VERSION,
            "available_panels_v1": {},
            "ui_index_v1": {},
        }
        if isinstance(extra, dict):
            for key, value in extra.items():
                payload[key] = value
        return payload

    def _execute():
        # 1) Snapshot gating
        if not snapshot_exists(req.db_snapshot_id):
            return BuildResponse(
                engine_version=ENGINE_VERSION,
                ruleset_version=RULESET_VERSION,
                bracket_definition_version=BRACKET_DEFINITION_VERSION,
                game_changers_version=GAME_CHANGERS_VERSION,
                db_snapshot_id=req.db_snapshot_id,
                profile_id=req.profile_id,
                bracket_id=req.bracket_id,
                status="UNKNOWN_SNAPSHOT",
                unknowns=[
                    {
                        "code": "UNKNOWN_SNAPSHOT",
                        "snapshot_id": req.db_snapshot_id,
                        "message": "Snapshot ID not found in local DB.",
                    }
                ],
                result=_ui_result_envelope(),
            )

        # 2) Resolve commander (Commander format requires it)
        commander_resolved = None
        commander_oracle_id = None
        runtime_taxonomy_version = resolve_runtime_taxonomy_version(
            snapshot_id=req.db_snapshot_id,
            requested=getattr(req, "taxonomy_version", None),
        )
        runtime_ruleset_version = resolve_runtime_ruleset_version(
            snapshot_id=req.db_snapshot_id,
            taxonomy_version=runtime_taxonomy_version,
            requested=getattr(req, "ruleset_version", None),
        )

        if (
            not isinstance(runtime_taxonomy_version, str)
            or runtime_taxonomy_version == ""
            or not isinstance(runtime_ruleset_version, str)
            or runtime_ruleset_version == ""
        ):
            return BuildResponse(
                engine_version=ENGINE_VERSION,
                ruleset_version=RULESET_VERSION,
                bracket_definition_version=BRACKET_DEFINITION_VERSION,
                game_changers_version=GAME_CHANGERS_VERSION,
                db_snapshot_id=req.db_snapshot_id,
                profile_id=req.profile_id,
                bracket_id=req.bracket_id,
                status="TAGS_NOT_COMPILED",
                unknowns=[
                    {
                        "code": "TAGS_NOT_COMPILED",
                        "snapshot_id": req.db_snapshot_id,
                        "taxonomy_version": runtime_taxonomy_version,
                        "ruleset_version": runtime_ruleset_version,
                        "reason": "MISSING_RUNTIME_VERSIONS",
                        "message": "Runtime snapshot/taxonomy/ruleset versions are required for deterministic preflight.",
                        "counts": {},
                        "rates": {},
                    }
                ],
                result=_ui_result_envelope(
                    {
                        "snapshot_id": req.db_snapshot_id,
                        "taxonomy_version": runtime_taxonomy_version,
                        "ruleset_version": runtime_ruleset_version,
                    }
                ),
            )

        if req.format == "commander":
            if not req.commander:
                return BuildResponse(
                    engine_version=ENGINE_VERSION,
                    ruleset_version=RULESET_VERSION,
                    bracket_definition_version=BRACKET_DEFINITION_VERSION,
                    game_changers_version=GAME_CHANGERS_VERSION,
                    db_snapshot_id=req.db_snapshot_id,
                    profile_id=req.profile_id,
                    bracket_id=req.bracket_id,
                    status="MISSING_COMMANDER",
                    unknowns=[
                        {
                            "code": "MISSING_COMMANDER",
                            "message": "format=commander requires a commander name.",
                        }
                    ],
                    result=_ui_result_envelope(),
                )

            commander_resolved = find_card_by_name(req.db_snapshot_id, req.commander)

            # 2a) Unknown commander (not found in DB)
            if commander_resolved is None:
                return BuildResponse(
                    engine_version=ENGINE_VERSION,
                    ruleset_version=RULESET_VERSION,
                    bracket_definition_version=BRACKET_DEFINITION_VERSION,
                    game_changers_version=GAME_CHANGERS_VERSION,
                    db_snapshot_id=req.db_snapshot_id,
                    profile_id=req.profile_id,
                    bracket_id=req.bracket_id,
                    status="UNKNOWN_COMMANDER",
                    unknowns=[
                        {
                            "code": "UNKNOWN_COMMANDER",
                            "input": req.commander,
                            "message": "Commander not found in local snapshot by exact name match.",
                            "suggestions": suggest_card_names(
                                req.db_snapshot_id, req.commander, limit=5
                            ),
                        }
                    ],
                    result=_ui_result_envelope(),
                )

            commander_oracle_id = commander_resolved.get("oracle_id")

            try:
                with cards_db_connect() as preflight_con:
                    _ = run_snapshot_preflight(
                        db=preflight_con,
                        db_snapshot_id=req.db_snapshot_id,
                        taxonomy_version=runtime_taxonomy_version,
                        ruleset_version=runtime_ruleset_version,
                        commander_oracle_id=commander_oracle_id,
                    )
            except SnapshotPreflightError as exc:
                preflight_unknown = exc.to_unknown()
                return BuildResponse(
                    engine_version=ENGINE_VERSION,
                    ruleset_version=RULESET_VERSION,
                    bracket_definition_version=BRACKET_DEFINITION_VERSION,
                    game_changers_version=GAME_CHANGERS_VERSION,
                    db_snapshot_id=req.db_snapshot_id,
                    profile_id=req.profile_id,
                    bracket_id=req.bracket_id,
                    status="TAGS_NOT_COMPILED",
                    unknowns=[preflight_unknown],
                    result=_ui_result_envelope(
                        {
                            "snapshot_id": req.db_snapshot_id,
                            "taxonomy_version": preflight_unknown.get("taxonomy_version"),
                            "ruleset_version": preflight_unknown.get("ruleset_version"),
                            "snapshot_preflight_v1": preflight_unknown,
                        }
                    ),
                )

            # 2b) Commander legality check (only AFTER we found the card)
            try:
                legal, reason = is_legal_commander_card(commander_resolved)
            except CommanderEligibilityUnknownError as exc:
                return BuildResponse(
                    engine_version=ENGINE_VERSION,
                    ruleset_version=RULESET_VERSION,
                    bracket_definition_version=BRACKET_DEFINITION_VERSION,
                    game_changers_version=GAME_CHANGERS_VERSION,
                    db_snapshot_id=req.db_snapshot_id,
                    profile_id=req.profile_id,
                    bracket_id=req.bracket_id,
                    status="COMMANDER_ELIGIBILITY_UNKNOWN",
                    unknowns=[exc.to_unknown()],
                    result=_ui_result_envelope(
                        {
                            "snapshot_id": req.db_snapshot_id,
                            "taxonomy_version": commander_resolved.get("taxonomy_version"),
                            "oracle_id": commander_resolved.get("oracle_id"),
                        }
                    ),
                )
            if not legal:
                return BuildResponse(
                    engine_version=ENGINE_VERSION,
                    ruleset_version=RULESET_VERSION,
                    bracket_definition_version=BRACKET_DEFINITION_VERSION,
                    game_changers_version=GAME_CHANGERS_VERSION,
                    db_snapshot_id=req.db_snapshot_id,
                    profile_id=req.profile_id,
                    bracket_id=req.bracket_id,
                    status="ILLEGAL_COMMANDER",
                    unknowns=[
                        {
                            "code": "ILLEGAL_COMMANDER",
                            "input": req.commander,
                            "message": (
                                "Card is not a legal Commander based on compiled commander-eligibility facets."
                            ),
                            "reason": reason,
                            "suggestions": suggest_card_names(
                                req.db_snapshot_id, req.commander, limit=5
                            ),
                        }
                    ],
                    result=_ui_result_envelope(),
                )

            ok, status = get_format_legality(commander_resolved, req.format)
            if not ok:
                return BuildResponse(
                    engine_version=ENGINE_VERSION,
                    ruleset_version=RULESET_VERSION,
                    bracket_definition_version=BRACKET_DEFINITION_VERSION,
                    game_changers_version=GAME_CHANGERS_VERSION,
                    db_snapshot_id=req.db_snapshot_id,
                    profile_id=req.profile_id,
                    bracket_id=req.bracket_id,
                    status="ILLEGAL_COMMANDER",
                    unknowns=[
                        {
                            "code": "ILLEGAL_COMMANDER",
                            "input": commander_resolved.get("name"),
                            "message": "Card is not legal in requested format.",
                            "reason": f"{commander_resolved.get('name')} is \"{status}\" in {req.format}.",
                            "suggestions": [],
                        }
                    ],
                    result=_ui_result_envelope(),
                )

        # 3) Resolve seed cards
        unknowns: List[Dict[str, Any]] = []
        resolved_cards: List[Dict[str, Any]] = []
        resolved_oracle_id_queues: Dict[str, List[Optional[str]]] = {}
        resolved_card_ci_queues: Dict[str, List[Any]] = {}
        resolved_format_status_queues: Dict[str, List[str]] = {}
        resolved_card_primitives_queues: Dict[str, List[Any]] = {}

        if GAME_CHANGERS_VERSION == "gc_missing":
            add_unknown(
                unknowns,
                code="MISSING_GAME_CHANGERS_LIST",
                input_value=GAME_CHANGERS_VERSION,
                message="Game Changers list not loaded.",
                reason="Local Game Changers file missing; bracket classification may be incomplete.",
                suggestions=[],
            )

        for name in req.cards:
            card = find_card_by_name(req.db_snapshot_id, name)
            if card is None:
                add_unknown(
                    unknowns,
                    code="UNKNOWN_CARD",
                    input_value=name,
                    message="Card not found in local snapshot by exact name match.",
                    reason="Card not found in local snapshot by exact name match.",
                    suggestions=suggest_card_names(req.db_snapshot_id, name, limit=5),
                )
            else:
                resolved_oracle_id_queues.setdefault(name, []).append(card.get("oracle_id"))
                resolved_card_ci_queues.setdefault(name, []).append(card.get("color_identity"))
                resolved_card_primitives_queues.setdefault(name, []).append(
                    card.get("primitives") if card.get("primitives") is not None else card.get("primitives_json")
                )

                ok, status = get_format_legality(card, req.format)
                resolved_format_status_queues.setdefault(name, []).append(status)
                if not ok:
                    add_unknown(
                        unknowns,
                        code="ILLEGAL_CARD",
                        input_value=card.get("name") or name,
                        message="Card is not legal in requested format.",
                        reason=f"{card.get('name')} is \"{status}\" in {req.format}.",
                    )
                    continue

                if not is_ci_compatible(commander_resolved, card):
                    add_unknown(
                        unknowns,
                        code="COLOR_IDENTITY_VIOLATION",
                        input_value=name,
                        message="Card color identity is not compatible with commander",
                        reason=f"{card.get('name')} CI {card.get('color_identity')} is not within commander CI {commander_resolved.get('color_identity')}",
                    )
                    continue

                if commander_oracle_id is not None and card.get("oracle_id") == commander_oracle_id:
                    add_unknown(
                        unknowns,
                        code="DUPLICATE_CARD",
                        input_value=name,
                        message="Card violates singleton rule",
                        reason="Deck cannot contain the commander as a non-commander card",
                    )
                    continue

                resolved_cards.append(card)

        status = "OK" if not unknowns else "OK_WITH_UNKNOWNS"

        allowed_patch_types = {
            "RULE_TOPIC_SELECTION_OVERRIDE",
            "TYPED_EDGE_RULE_TOGGLE",
            "PRIMITIVE_OVERRIDE",
            "STRUCTURAL_THRESHOLD_OVERRIDE",
        }
        patch_error_v0 = None
        patch_ids_unique = True
        patch_hash_consistent = True
        patch_effect_summary: List[Dict[str, Any]] = []
        patch_effect_entry_by_id: Dict[str, Dict[str, Any]] = {}
        patch_ids_sorted: List[str] = []
        patch_hash_v1 = None
        patches_total = 0
        patches_applied_total = 0

        engine_primitive_override_patches: List[Dict[str, Any]] = []
        engine_structural_threshold_overrides: Dict[str, int] = {}
        engine_typed_edge_rule_toggle_by_index: Dict[int, bool] = {}
        engine_typed_edge_toggle_patches: List[Dict[str, Any]] = []
        engine_topic_prefer_sections_override_by_topic: Dict[str, List[str]] = {}
        engine_topic_take_final_override_by_topic: Dict[str, int] = {}

        raw_patches = req.engine_patches_v0
        normalized_patches: List[Dict[str, Any]] = []
        if not isinstance(raw_patches, list):
            patch_error_v0 = {
                "error": "INVALID_PATCH",
                "patch_id": None,
                "reason": "engine_patches_v0 must be a list",
            }
        else:
            seen_patch_ids: set[str] = set()
            for idx, patch in enumerate(raw_patches):
                if not isinstance(patch, dict):
                    patch_error_v0 = {
                        "error": "INVALID_PATCH",
                        "patch_id": None,
                        "reason": f"patch at index {idx} must be an object",
                    }
                    break
                patch_id = patch.get("patch_id")
                patch_type = patch.get("patch_type")
                enabled = patch.get("enabled")
                payload = patch.get("payload")

                if not isinstance(patch_id, str) or patch_id == "":
                    patch_error_v0 = {
                        "error": "INVALID_PATCH",
                        "patch_id": patch_id if isinstance(patch_id, str) else None,
                        "reason": "patch_id must be a non-empty string",
                    }
                    break
                if patch_id in seen_patch_ids:
                    patch_ids_unique = False
                    patch_error_v0 = {
                        "error": "INVALID_PATCH",
                        "patch_id": patch_id,
                        "reason": "duplicate patch_id",
                    }
                    break
                seen_patch_ids.add(patch_id)

                if not isinstance(patch_type, str) or patch_type not in allowed_patch_types:
                    patch_error_v0 = {
                        "error": "INVALID_PATCH",
                        "patch_id": patch_id,
                        "reason": "unknown patch_type",
                    }
                    break
                if not isinstance(enabled, bool):
                    patch_error_v0 = {
                        "error": "INVALID_PATCH",
                        "patch_id": patch_id,
                        "reason": "enabled must be bool",
                    }
                    break
                if not isinstance(payload, dict):
                    patch_error_v0 = {
                        "error": "INVALID_PATCH",
                        "patch_id": patch_id,
                        "reason": "payload must be an object",
                    }
                    break

                normalized_patches.append(
                    {
                        "patch_id": patch_id,
                        "patch_type": patch_type,
                        "enabled": enabled,
                        "payload": payload,
                    }
                )

        if patch_error_v0 is None:
            normalized_patches = sorted(normalized_patches, key=lambda p: p.get("patch_id") or "")
            patch_ids_sorted = [p["patch_id"] for p in normalized_patches]
            patches_total = len(normalized_patches)
            if patches_total > 0:
                patch_hash_v1 = sha256_hex(stable_json_dumps(normalized_patches))

            for patch in normalized_patches:
                patch_id = patch["patch_id"]
                patch_type = patch["patch_type"]
                payload = patch["payload"]
                if not patch["enabled"]:
                    patch_effect_entry = {
                        "patch_id": patch_id,
                        "patch_type": patch_type,
                        "effect": "SKIPPED_DISABLED",
                    }
                    patch_effect_summary.append(patch_effect_entry)
                    patch_effect_entry_by_id[patch_id] = patch_effect_entry
                    continue

                if patch_type == "PRIMITIVE_OVERRIDE":
                    oracle_id = payload.get("oracle_id")
                    add_items = payload.get("add", [])
                    remove_items = payload.get("remove", [])
                    if (
                        not isinstance(oracle_id, str)
                        or not isinstance(add_items, list)
                        or not isinstance(remove_items, list)
                        or not all(isinstance(x, str) for x in add_items)
                        or not all(isinstance(x, str) for x in remove_items)
                    ):
                        patch_error_v0 = {
                            "error": "INVALID_PATCH",
                            "patch_id": patch_id,
                            "reason": "PRIMITIVE_OVERRIDE payload invalid",
                        }
                        break
                    engine_primitive_override_patches.append(
                        {
                            "patch_id": patch_id,
                            "oracle_id": oracle_id,
                            "add": sorted_unique(add_items),
                            "remove": sorted_unique(remove_items),
                        }
                    )
                elif patch_type == "STRUCTURAL_THRESHOLD_OVERRIDE":
                    primitive = payload.get("primitive")
                    min_required = payload.get("min_required")
                    if (
                        not isinstance(primitive, str)
                        or primitive not in GENERIC_MINIMUMS
                        or not isinstance(min_required, int)
                        or min_required < 0
                    ):
                        patch_error_v0 = {
                            "error": "INVALID_PATCH",
                            "patch_id": patch_id,
                            "reason": "STRUCTURAL_THRESHOLD_OVERRIDE payload invalid",
                        }
                        break
                    engine_structural_threshold_overrides[primitive] = min_required
                elif patch_type == "TYPED_EDGE_RULE_TOGGLE":
                    matched_rule_version = payload.get("matched_rule_version")
                    rule_index = payload.get("rule_index")
                    rule_enabled = payload.get("enabled")
                    if (
                        not isinstance(matched_rule_version, str)
                        or matched_rule_version != GRAPH_TYPED_RULES_VERSION
                        or not isinstance(rule_index, int)
                        or rule_index < 0
                        or rule_index >= len(TYPED_EDGE_RULES_V0)
                        or not isinstance(rule_enabled, bool)
                    ):
                        patch_error_v0 = {
                            "error": "INVALID_PATCH",
                            "patch_id": patch_id,
                            "reason": "TYPED_EDGE_RULE_TOGGLE payload invalid",
                        }
                        break
                    engine_typed_edge_rule_toggle_by_index[rule_index] = rule_enabled
                    engine_typed_edge_toggle_patches.append(
                        {
                            "patch_id": patch_id,
                            "matched_rule_version": matched_rule_version,
                            "rule_index": rule_index,
                            "enabled_target": rule_enabled,
                        }
                    )
                elif patch_type == "RULE_TOPIC_SELECTION_OVERRIDE":
                    topic_id = payload.get("topic_id")
                    prefer_sections_override = payload.get("prefer_sections_override")
                    take_final_override = payload.get("take_final_override")
                    if not isinstance(topic_id, str):
                        patch_error_v0 = {
                            "error": "INVALID_PATCH",
                            "patch_id": patch_id,
                            "reason": "RULE_TOPIC_SELECTION_OVERRIDE missing topic_id",
                        }
                        break
                    if prefer_sections_override is not None:
                        if not isinstance(prefer_sections_override, list) or not all(
                            isinstance(x, str) for x in prefer_sections_override
                        ):
                            patch_error_v0 = {
                                "error": "INVALID_PATCH",
                                "patch_id": patch_id,
                                "reason": "prefer_sections_override must be list[str]",
                            }
                            break
                        engine_topic_prefer_sections_override_by_topic[topic_id] = sorted_unique(prefer_sections_override)
                    if take_final_override is not None:
                        if not isinstance(take_final_override, int) or take_final_override <= 0:
                            patch_error_v0 = {
                                "error": "INVALID_PATCH",
                                "patch_id": patch_id,
                                "reason": "take_final_override must be positive int",
                            }
                            break
                        engine_topic_take_final_override_by_topic[topic_id] = take_final_override

                patch_effect_entry = {
                    "patch_id": patch_id,
                    "patch_type": patch_type,
                    "effect": "APPLIED",
                }
                patch_effect_summary.append(patch_effect_entry)
                patch_effect_entry_by_id[patch_id] = patch_effect_entry
                patches_applied_total += 1

        if patch_error_v0 is not None:
            engine_primitive_override_patches = []
            engine_structural_threshold_overrides = {}
            engine_typed_edge_rule_toggle_by_index = {}
            engine_typed_edge_toggle_patches = []
            engine_topic_prefer_sections_override_by_topic = {}
            engine_topic_take_final_override_by_topic = {}
            patches_applied_total = 0
            patch_effect_summary = []
            patch_effect_entry_by_id = {}
            patch_hash_v1 = None
            patch_hash_consistent = False
            patch_ids_sorted = sorted([p.get("patch_id") for p in normalized_patches if isinstance(p.get("patch_id"), str)])
            patches_total = len(normalized_patches)
        else:
            recomputed_patch_hash = (
                sha256_hex(stable_json_dumps(normalized_patches)) if len(normalized_patches) > 0 else None
            )
            patch_hash_consistent = patch_hash_v1 == recomputed_patch_hash

        patch_loop_v0 = {
            "patches_total": patches_total,
            "patches_applied_total": patches_applied_total,
            "patch_ids_sorted": patch_ids_sorted,
            "patch_effect_summary": patch_effect_summary,
            "patch_hash_v1": patch_hash_v1,
        }

        effective_generic_minimums = dict(GENERIC_MINIMUMS)
        for primitive in sorted(engine_structural_threshold_overrides.keys()):
            effective_generic_minimums[primitive] = engine_structural_threshold_overrides[primitive]

        deck_size_total = 1 + len(resolved_cards)
        if deck_size_total < 100:
            deck_status = "UNDER_100"
            cards_needed = 100 - deck_size_total
            cards_to_cut = None
        elif deck_size_total > 100:
            deck_status = "OVER_100"
            cards_to_cut = deck_size_total - 100
            cards_needed = None
        else:
            deck_status = "EXACT_100"
            cards_needed = None
            cards_to_cut = None

        cut_order = None
        if deck_status == "OVER_100":
            illegal_cards: List[str] = []
            ci_violations: List[str] = []
            duplicate_violations: List[str] = []

            for u in unknowns:
                code = u.get("code")
                name = u.get("input")
                if not name:
                    continue
                if code == "ILLEGAL_CARD":
                    illegal_cards.append(name)
                elif code == "COLOR_IDENTITY_VIOLATION":
                    ci_violations.append(name)
                elif code == "DUPLICATE_CARD":
                    duplicate_violations.append(name)

            valid_card_names_sorted = sorted(
                [c.get("name") for c in resolved_cards if c.get("name")],
                key=lambda s: str(s).lower(),
            )

            cut_order = illegal_cards + ci_violations + duplicate_violations + valid_card_names_sorted

        INVALID_CODES = {"ILLEGAL_CARD", "COLOR_IDENTITY_VIOLATION", "UNKNOWN_CARD"}
        invalid_inputs = {u.get("input") for u in unknowns if u.get("code") in INVALID_CODES}
        playable_cards_resolved = [c for c in resolved_cards if c.get("name") not in invalid_inputs]

        seen_counts: dict[str, int] = {}
        new_playable: list[dict] = []

        for card in playable_cards_resolved:
            name = card.get("name")
            if not name:
                continue

            if name in SINGLETON_EXEMPT_NAMES:
                new_playable.append(card)
                continue

            count = seen_counts.get(name, 0)

            if count == 0:
                new_playable.append(card)
            else:
                add_unknown(
                    unknowns,
                    code="DUPLICATE_CARD",
                    input_value=name,
                    message="Card violates singleton rule",
                    reason=f"Duplicate copy of {name}",
                    suggestions=[],
                )

            seen_counts[name] = count + 1

        playable_cards_resolved = new_playable

        tag_oracle_ids = sorted_unique(
            [
                *(
                    [commander_resolved.get("oracle_id")]
                    if commander_resolved is not None
                    else []
                ),
                *[
                    oracle_id
                    for oracle_queue in resolved_oracle_id_queues.values()
                    for oracle_id in oracle_queue
                ],
            ]
        )

        compiled_tags_by_oracle: Dict[str, Dict[str, Any]] = {}
        if tag_oracle_ids:
            try:
                with cards_db_connect() as con:
                    ensure_tag_tables(con)
                    compiled_tags_by_oracle = bulk_get_card_tags(
                        conn=con,
                        oracle_ids=tag_oracle_ids,
                        snapshot_id=req.db_snapshot_id,
                        taxonomy_version=runtime_taxonomy_version,
                    )
            except TagSnapshotMissingError as exc:
                return BuildResponse(
                    engine_version=ENGINE_VERSION,
                    ruleset_version=RULESET_VERSION,
                    bracket_definition_version=BRACKET_DEFINITION_VERSION,
                    game_changers_version=GAME_CHANGERS_VERSION,
                    db_snapshot_id=req.db_snapshot_id,
                    profile_id=req.profile_id,
                    bracket_id=req.bracket_id,
                    status="TAGS_NOT_COMPILED",
                    unknowns=[
                        {
                            "code": "TAGS_NOT_COMPILED",
                            "snapshot_id": req.db_snapshot_id,
                            "taxonomy_version": runtime_taxonomy_version,
                            "message": (
                                "Tags/index not compiled for snapshot/taxonomy_version. "
                                "Run snapshot_build.tag_snapshot then snapshot_build.index_build."
                            ),
                            "reason": str(exc),
                            "missing_oracle_ids": list(exc.missing_oracle_ids),
                        }
                    ],
                    result=_ui_result_envelope(
                        {
                            "snapshot_id": req.db_snapshot_id,
                            "taxonomy_version": runtime_taxonomy_version,
                        }
                    ),
                )

        runtime_tag_ruleset_versions = sorted_unique(
            [
                tags.get("ruleset_version")
                for tags in compiled_tags_by_oracle.values()
                if isinstance(tags, dict)
            ]
        )
        if len(runtime_tag_ruleset_versions) > 1:
            return BuildResponse(
                engine_version=ENGINE_VERSION,
                ruleset_version=RULESET_VERSION,
                bracket_definition_version=BRACKET_DEFINITION_VERSION,
                game_changers_version=GAME_CHANGERS_VERSION,
                db_snapshot_id=req.db_snapshot_id,
                profile_id=req.profile_id,
                bracket_id=req.bracket_id,
                status="TAGS_INCONSISTENT",
                unknowns=[
                    {
                        "code": "TAGS_INCONSISTENT",
                        "snapshot_id": req.db_snapshot_id,
                        "taxonomy_version": runtime_taxonomy_version,
                        "message": "Multiple ruleset_version values found in card_tags for this snapshot/taxonomy_version.",
                        "ruleset_versions": runtime_tag_ruleset_versions,
                    }
                ],
                result=_ui_result_envelope(
                    {
                        "snapshot_id": req.db_snapshot_id,
                        "taxonomy_version": runtime_taxonomy_version,
                    }
                ),
            )
        if (
            len(runtime_tag_ruleset_versions) == 1
            and isinstance(runtime_ruleset_version, str)
            and runtime_tag_ruleset_versions[0] != runtime_ruleset_version
        ):
            return BuildResponse(
                engine_version=ENGINE_VERSION,
                ruleset_version=RULESET_VERSION,
                bracket_definition_version=BRACKET_DEFINITION_VERSION,
                game_changers_version=GAME_CHANGERS_VERSION,
                db_snapshot_id=req.db_snapshot_id,
                profile_id=req.profile_id,
                bracket_id=req.bracket_id,
                status="TAGS_INCONSISTENT",
                unknowns=[
                    {
                        "code": "TAGS_INCONSISTENT",
                        "snapshot_id": req.db_snapshot_id,
                        "taxonomy_version": runtime_taxonomy_version,
                        "message": "Resolved runtime ruleset_version does not match loaded card_tags ruleset_version.",
                        "ruleset_version_expected": runtime_ruleset_version,
                        "ruleset_version_loaded": runtime_tag_ruleset_versions[0],
                    }
                ],
                result=_ui_result_envelope(
                    {
                        "snapshot_id": req.db_snapshot_id,
                        "taxonomy_version": runtime_taxonomy_version,
                        "ruleset_version": runtime_ruleset_version,
                    }
                ),
            )
        runtime_tag_ruleset_version = runtime_ruleset_version

        def lookup_snapshot_evidence_by_oracle_id(oracle_ids: List[str]) -> Dict[str, Dict[str, Any]]:
            out: Dict[str, Dict[str, Any]] = {}
            oracle_ids_clean = sorted_unique([oid for oid in oracle_ids if isinstance(oid, str) and oid != ""])
            for oracle_id in oracle_ids_clean:
                payload = compiled_tags_by_oracle.get(oracle_id)
                if not isinstance(payload, dict):
                    out[oracle_id] = {}
                    continue
                evidence_payload = payload.get("evidence")
                if isinstance(evidence_payload, dict):
                    out[oracle_id] = evidence_payload
                elif isinstance(evidence_payload, list):
                    out[oracle_id] = {
                        "matches": [item for item in evidence_payload if isinstance(item, dict)]
                    }
                else:
                    out[oracle_id] = {}
            return out

        def _compiled_primitives_for_oracle(oracle_id: Any) -> List[str]:
            if not isinstance(oracle_id, str):
                return []
            payload = compiled_tags_by_oracle.get(oracle_id)
            if not isinstance(payload, dict):
                return []
            value = payload.get("primitive_ids")
            return [p for p in value if isinstance(p, str)] if isinstance(value, list) else []

        if commander_resolved is not None:
            commander_primitives = _compiled_primitives_for_oracle(commander_resolved.get("oracle_id"))
            commander_resolved["primitives"] = commander_primitives
            commander_resolved["primitives_json"] = stable_json_dumps(commander_primitives)

        for card in resolved_cards:
            card_primitives = _compiled_primitives_for_oracle(card.get("oracle_id"))
            card["primitives"] = card_primitives
            card["primitives_json"] = stable_json_dumps(card_primitives)

        resolved_card_primitives_queues = {
            name: [
                _compiled_primitives_for_oracle(oracle_id)
                for oracle_id in oracle_queue
            ]
            for name, oracle_queue in resolved_oracle_id_queues.items()
        }

        loaded_overrides_version = OVERRIDES_OBJ.get("overrides_version") if isinstance(OVERRIDES_OBJ, dict) else None
        overrides_scope_db_snapshot_id = (
            OVERRIDES_OBJ.get("db_snapshot_id_scope") if isinstance(OVERRIDES_OBJ, dict) else None
        )
        primitive_overrides_by_oracle: Dict[str, List[Dict[str, Any]]] = {}
        if (
            OVERRIDES_AVAILABLE
            and loaded_overrides_version == OVERRIDES_VERSION
            and overrides_scope_db_snapshot_id == req.db_snapshot_id
            and isinstance((OVERRIDES_OBJ or {}).get("primitive_overrides"), list)
        ):
            primitive_overrides_clean = []
            for patch in (OVERRIDES_OBJ or {}).get("primitive_overrides", []):
                if not isinstance(patch, dict):
                    continue
                patch_id = patch.get("patch_id")
                oracle_id = patch.get("oracle_id")
                if not isinstance(patch_id, str) or not isinstance(oracle_id, str):
                    continue
                primitive_overrides_clean.append(
                    {
                        "patch_id": patch_id,
                        "oracle_id": oracle_id,
                        "add": sorted_unique([p for p in (patch.get("add") or []) if isinstance(p, str)]),
                        "remove": sorted_unique([p for p in (patch.get("remove") or []) if isinstance(p, str)]),
                    }
                )
            primitive_overrides_clean.sort(key=lambda p: (p.get("oracle_id") or "", p.get("patch_id") or ""))
            for patch in primitive_overrides_clean:
                primitive_overrides_by_oracle.setdefault(patch["oracle_id"], []).append(patch)
        for patch in engine_primitive_override_patches:
            primitive_overrides_by_oracle.setdefault(patch["oracle_id"], []).append(patch)
        for oracle_id in list(primitive_overrides_by_oracle.keys()):
            primitive_overrides_by_oracle[oracle_id] = sorted(
                primitive_overrides_by_oracle[oracle_id],
                key=lambda p: (p.get("patch_id") or ""),
            )

        primitive_override_outcome_by_oracle: Dict[str, Dict[str, Any]] = {}
        applied_patches_by_key: Dict[str, Dict[str, Any]] = {}

        def get_overridden_primitives_for_oracle(oracle_id: str, source_value: Any) -> List[str]:
            if oracle_id in primitive_override_outcome_by_oracle:
                return list(primitive_override_outcome_by_oracle[oracle_id]["final_primitives"])

            original_primitives = normalize_primitives_source(source_value)
            final_primitives, applied_summaries = apply_primitive_overrides(
                original_primitives,
                oracle_id,
                primitive_overrides_by_oracle,
            )
            primitive_override_outcome_by_oracle[oracle_id] = {
                "oracle_id": oracle_id,
                "original_primitives": original_primitives,
                "final_primitives": final_primitives,
                "applied": applied_summaries,
            }
            for summary in applied_summaries:
                patch_id = summary.get("patch_id")
                if isinstance(patch_id, str):
                    applied_patches_by_key[f"{patch_id}|{oracle_id}"] = summary
            return list(final_primitives)

        playable_names = [c.get("name") for c in playable_cards_resolved if c.get("name")]
        commander_name = commander_resolved.get("name") if commander_resolved is not None else None
        game_changers_found, game_changers_count = detect_game_changers(
            playable_names=playable_names,
            commander_name=commander_name,
            gc_set=GAME_CHANGERS_SET,
        )

        bracket_floor_from_gc = bracket_floor_from_count(game_changers_count)

        bracket_conflict = False
        if bracket_floor_from_gc is not None:
            if req.bracket_id == "B3" and bracket_floor_from_gc == "B4":
                bracket_conflict = True

        primitive_counts: Dict[str, int] = {}

        def add_primitives_count(primitives_value: Any, oracle_id: Any):
            primitives_list = normalize_primitives_source(primitives_value)
            if isinstance(oracle_id, str) and oracle_id in primitive_overrides_by_oracle:
                primitives_list = get_overridden_primitives_for_oracle(oracle_id, primitives_value)
            for primitive in primitives_list:
                primitive_counts[primitive] = primitive_counts.get(primitive, 0) + 1

        if commander_resolved is not None:
            commander_oracle_for_override = commander_resolved.get("oracle_id")
            commander_primitives_value = (
                commander_resolved.get("primitives")
                if commander_resolved.get("primitives") is not None
                else commander_resolved.get("primitives_json")
            )
            add_primitives_count(commander_primitives_value, commander_oracle_for_override)
        for card in playable_cards_resolved:
            card_oracle_for_override = card.get("oracle_id")
            card_primitives_value = card.get("primitives") if card.get("primitives") is not None else card.get("primitives_json")
            add_primitives_count(card_primitives_value, card_oracle_for_override)

        primitives_present = sorted_unique(primitive_counts.keys())

        total_valid_cards = len(playable_cards_resolved)
        primitive_density: Dict[str, float] = {}
        for primitive in sorted_unique(primitive_counts.keys()):
            count = primitive_counts[primitive]
            density = count / max(total_valid_cards, 1)
            primitive_density[primitive] = round(density, 3)

        needs: List[Dict[str, Any]] = []
        for primitive in sorted(effective_generic_minimums.keys()):
            have = primitive_counts.get(primitive, 0)
            minv = effective_generic_minimums[primitive]
            if have < minv:
                needs.append(
                    {
                        "primitive": primitive,
                        "have": have,
                        "min": minv,
                        "need": minv - have,
                    }
                )

        canonical_state = {
            "req": req,
            "unknowns": unknowns,
            "playable_cards_resolved": playable_cards_resolved,
            "resolved_oracle_id_queues": resolved_oracle_id_queues,
            "resolved_card_ci_queues": resolved_card_ci_queues,
            "resolved_format_status_queues": resolved_format_status_queues,
            "resolved_card_primitives_queues": resolved_card_primitives_queues,
            "commander_resolved": commander_resolved,
            "get_format_legality": get_format_legality,
            "make_slot_id": make_slot_id,
            "primitive_overrides_by_oracle": primitive_overrides_by_oracle,
            "get_overridden_primitives_for_oracle": get_overridden_primitives_for_oracle,
            "normalize_primitives_source": normalize_primitives_source,
            "sorted_unique": sorted_unique,
        }
        canonical_state = run_canonical_v1(canonical_state)

        unknown_cards = canonical_state["unknown_cards"]
        deck_cards_nonplayable = canonical_state["deck_cards_nonplayable"]
        deck_cards_playable = canonical_state["deck_cards_playable"]
        deck_cards_canonical_input_order = canonical_state["deck_cards_canonical_input_order"]
        playable_index_counter = canonical_state["playable_index_counter"]
        nonplayable_index_counter = canonical_state["nonplayable_index_counter"]
        deck_cards_slot_ids_playable = canonical_state["deck_cards_slot_ids_playable"]
        deck_cards_slot_ids_nonplayable = canonical_state["deck_cards_slot_ids_nonplayable"]
        deck_cards_unknowns_by_slot = canonical_state["deck_cards_unknowns_by_slot"]
        commander_canonical_slot = canonical_state["commander_canonical_slot"]
        canonical_slots_all = canonical_state["canonical_slots_all"]

        primitive_index_state = {
            "commander_resolved": commander_resolved,
            "primitive_overrides_by_oracle": primitive_overrides_by_oracle,
            "get_overridden_primitives_for_oracle": get_overridden_primitives_for_oracle,
            "deck_cards_canonical_input_order": deck_cards_canonical_input_order,
            "slot_primitives_source_by_slot_id": canonical_state["slot_primitives_source_by_slot_id"],
            "canonical_slots_all": canonical_slots_all,
            "normalize_primitives_source": normalize_primitives_source,
        }
        primitive_index_state = run_primitive_index_v1(primitive_index_state)
        primitive_index_by_slot = primitive_index_state["primitive_index_by_slot"]
        slot_ids_by_primitive = primitive_index_state["slot_ids_by_primitive"]
        primitive_index_totals = primitive_index_state["primitive_index_totals"]

        required_primitives_v1 = sorted(
            [primitive for primitive in effective_generic_minimums.keys() if isinstance(primitive, str)]
        )
        basic_name_set = {
            name.strip().lower()
            for name in SINGLETON_EXEMPT_NAMES
            if isinstance(name, str) and name.strip() != ""
        }
        basic_land_slot_ids: List[str] = []
        for entry in deck_cards_canonical_input_order:
            if entry.get("status") != "PLAYABLE":
                continue
            slot_id = entry.get("slot_id")
            resolved_name = entry.get("resolved_name")
            if not isinstance(slot_id, str) or not isinstance(resolved_name, str):
                continue
            if resolved_name.strip().lower() in basic_name_set:
                basic_land_slot_ids.append(slot_id)
        basic_land_slot_ids = sorted(set(basic_land_slot_ids))

        structural_state = {
            "deck_cards_canonical_input_order": deck_cards_canonical_input_order,
            "commander_canonical_slot": commander_canonical_slot,
            "primitive_index_by_slot": primitive_index_by_slot,
            "effective_generic_minimums": effective_generic_minimums,
        }
        structural_state = run_structural_v1(structural_state)

        primitive_counts_by_scope = structural_state["primitive_counts_by_scope"]
        primitive_counts_by_scope_totals = structural_state["primitive_counts_by_scope_totals"]
        structural_snapshot_v1 = build_structural_snapshot_v1(
            snapshot_id=str(req.db_snapshot_id),
            taxonomy_version=str(runtime_taxonomy_version),
            ruleset_version=str(runtime_tag_ruleset_version),
            profile_id=str(req.profile_id),
            bracket_id=req.bracket_id if isinstance(req.bracket_id, str) else None,
            commander_slot_id=str(commander_canonical_slot.get("slot_id") or "C0"),
            deck_slot_ids=list(deck_cards_slot_ids_playable),
            primitive_index_by_slot=primitive_index_by_slot,
            required_primitives=required_primitives_v1,
            basic_land_slot_ids=basic_land_slot_ids,
        )

        graph_expand_bounds_v1 = {
            "MAX_PRIMS_PER_SLOT": int(GRAPH_EXPAND_V1_MAX_PRIMS_PER_SLOT),
            "MAX_SLOTS_PER_PRIM": int(GRAPH_EXPAND_V1_MAX_SLOTS_PER_PRIM),
            "MAX_CARD_CARD_EDGES_TOTAL": int(GRAPH_EXPAND_V1_MAX_CARD_CARD_EDGES_TOTAL),
        }
        graph_expand_bipartite_v1 = build_bipartite_graph_v1(
            deck_slot_ids=list(deck_cards_slot_ids_playable),
            primitive_index_by_slot=primitive_index_by_slot,
        )
        graph_expand_candidates_v1 = expand_candidate_edges_v1(
            graph=graph_expand_bipartite_v1,
            bounds=graph_expand_bounds_v1,
        )
        graph_expand_candidate_edges_v1 = (
            graph_expand_candidates_v1.get("candidate_edges")
            if isinstance(graph_expand_candidates_v1.get("candidate_edges"), list)
            else []
        )
        graph_expand_candidate_stats_v1 = (
            graph_expand_candidates_v1.get("stats")
            if isinstance(graph_expand_candidates_v1.get("stats"), dict)
            else {}
        )

        graph_v1 = {
            "bipartite": graph_expand_bipartite_v1,
            "candidate_edges": graph_expand_candidate_edges_v1,
            "bounds": graph_expand_bounds_v1,
            "stats": graph_expand_candidate_stats_v1,
        }

        graph_state = {
            "req": req,
            "canonical_slots_all": canonical_slots_all,
            "primitive_index_by_slot": primitive_index_by_slot,
            "sorted_unique": sorted_unique,
            "engine_typed_edge_rule_toggle_by_index": engine_typed_edge_rule_toggle_by_index,
            "typed_edge_rules_v0": TYPED_EDGE_RULES_V0,
            "graph_typed_rules_version": GRAPH_TYPED_RULES_VERSION,
            "graph_layer_version": GRAPH_LAYER_VERSION,
            "graph_ruleset_version": GRAPH_RULESET_VERSION,
            "stable_json_dumps": stable_json_dumps,
            "sha256_hex": sha256_hex,
        }
        graph_state = run_graph_v3_typed(graph_state)

        graph_nodes = graph_state["graph_nodes"]
        typed_rule_match_counts_before = graph_state["typed_rule_match_counts_before"]
        typed_rule_match_counts_after = graph_state["typed_rule_match_counts_after"]
        graph_edges = graph_state["graph_edges"]
        graph_edge_index = graph_state["graph_edge_index"]
        graph_adjacency = graph_state["graph_adjacency"]
        graph_node_degrees = graph_state["graph_node_degrees"]
        graph_components = graph_state["graph_components"]
        graph_component_by_node = graph_state["graph_component_by_node"]
        graph_totals = graph_state["graph_totals"]
        connected_components_total = int(graph_totals.get("connected_components_total", 0))
        isolated_nodes_total = int(graph_totals.get("isolated_nodes_total", 0))
        max_degree = int(graph_totals.get("max_degree", 0))
        avg_degree = graph_totals.get("avg_degree", 0)
        largest_component_size = int(graph_totals.get("largest_component_size", 0))
        largest_component_id = graph_totals.get("largest_component_id")
        graph_typed_edges_total = graph_state["graph_typed_edges_total"]
        graph_typed_match_counts_by_type = graph_state["graph_typed_match_counts_by_type"]
        graph_typed_edges_by_type = graph_state["graph_typed_edges_by_type"]
        graph_rules_meta = graph_state["graph_rules_meta"]
        graph_fingerprint_payload_v1 = graph_state["graph_fingerprint_payload_v1"]
        graph_hash_v1 = graph_state["graph_hash_v1"]
        graph_fingerprint_payload_v2 = graph_state["graph_fingerprint_payload_v2"]
        graph_hash_v2 = graph_state["graph_hash_v2"]
        node_order = graph_state["node_order"]

        motif_state = {
            "graph_edges": graph_edges,
            "graph_nodes": graph_nodes,
            "graph_component_by_node": graph_component_by_node,
            "graph_components": graph_components,
            "graph_adjacency": graph_adjacency,
            "connected_components_total": connected_components_total,
            "isolated_nodes_total": isolated_nodes_total,
            "largest_component_id": largest_component_id,
            "largest_component_size": largest_component_size,
            "avg_degree": avg_degree,
            "max_degree": max_degree,
            "node_order": node_order,
            "order_by_node_order": order_by_node_order,
            "graph_hash_v2": graph_hash_v2,
            "motif_layer_version": MOTIF_LAYER_VERSION,
            "motif_ruleset_version": MOTIF_RULESET_VERSION,
            "stable_json_dumps": stable_json_dumps,
            "sha256_hex": sha256_hex,
        }
        motif_state = run_motif_v1(motif_state)
        motifs = motif_state["motifs"]
        motif_totals = motif_state["motif_totals"]
        motif_fingerprint_payload_v1 = motif_state["motif_fingerprint_payload_v1"]
        motif_hash_v1 = motif_state["motif_hash_v1"]

        adj_simple: Dict[str, List[str]] = {
            sid: [n.get("neighbor") for n in graph_adjacency.get(sid, []) if isinstance(n.get("neighbor"), str)]
            for sid in node_order
        }

        disruption_state = {
            "graph_totals": graph_totals,
            "graph_nodes": graph_nodes,
            "graph_node_degrees": graph_node_degrees,
            "node_order": node_order,
            "adj_simple": adj_simple,
            "graph_component_by_node": graph_component_by_node,
            "graph_edges": graph_edges,
            "sorted_unique": sorted_unique,
            "make_slot_id": make_slot_id,
            "graph_components": graph_components,
            "disruption_layer_version": DISRUPTION_LAYER_VERSION,
            "disruption_ruleset_version": DISRUPTION_RULESET_VERSION,
            "graph_hash_v2": graph_hash_v2,
            "stable_json_dumps": stable_json_dumps,
            "sha256_hex": sha256_hex,
        }
        disruption_state = run_disruption_v1(disruption_state)

        disruption_articulation_nodes = disruption_state["disruption_articulation_nodes"]
        disruption_node_impact = disruption_state["disruption_node_impact"]
        disruption_bridge_edges = disruption_state["disruption_bridge_edges"]
        disruption_commander_risk = disruption_state["disruption_commander_risk"]
        disruption_totals = disruption_state["disruption_totals"]
        disruption_fingerprint_payload_v1 = disruption_state["disruption_fingerprint_payload_v1"]
        disruption_hash_v1 = disruption_state["disruption_hash_v1"]

        pathways_state = {
            "node_order": node_order,
            "adj_simple": adj_simple,
            "graph_nodes": graph_nodes,
            "disruption_commander_risk": disruption_commander_risk,
            "graph_component_by_node": graph_component_by_node,
            "graph_components": graph_components,
            "pathways_layer_version": PATHWAYS_LAYER_VERSION,
            "pathways_ruleset_version": PATHWAYS_RULESET_VERSION,
            "graph_hash_v2": graph_hash_v2,
            "stable_json_dumps": stable_json_dumps,
            "sha256_hex": sha256_hex,
        }
        pathways_state = run_pathways_v1(pathways_state)

        commander_in_graph = pathways_state["commander_in_graph"]
        commander_playable = pathways_state["commander_playable"]
        distance_by_node = pathways_state["distance_by_node"]
        pathways_commander_distances = pathways_state["pathways_commander_distances"]
        pathways_commander_reachable_slots = pathways_state["pathways_commander_reachable_slots"]
        pathways_commander_unreachable_slots = pathways_state["pathways_commander_unreachable_slots"]
        pathways_hubs = pathways_state["pathways_hubs"]
        pathways_commander_bridge_candidates = pathways_state["pathways_commander_bridge_candidates"]
        pathways_totals = pathways_state["pathways_totals"]
        pathways_fingerprint_payload_v1 = pathways_state["pathways_fingerprint_payload_v1"]
        pathways_hash_v1 = pathways_state["pathways_hash_v1"]

        def edge_key_norm(a: str, b: str) -> str:
            return f"{a}|{b}" if a < b else f"{b}|{a}"

        combo_skeleton_state = {
            "graph_components": graph_components,
            "adj_simple": adj_simple,
            "edge_key_norm": edge_key_norm,
            "combo_skeleton_bfs_node_cap": COMBO_SKELETON_BFS_NODE_CAP,
            "max_triangles": MAX_TRIANGLES,
            "max_4cycles": MAX_4CYCLES,
            "combo_skeleton_layer_version": COMBO_SKELETON_LAYER_VERSION,
            "combo_skeleton_ruleset_version": COMBO_SKELETON_RULESET_VERSION,
            "graph_hash_v2": graph_hash_v2,
            "stable_json_dumps": stable_json_dumps,
            "sha256_hex": sha256_hex,
        }
        combo_skeleton_state = run_combo_skeleton_v0(combo_skeleton_state)

        combo_skeleton_components = combo_skeleton_state["combo_skeleton_components"]
        combo_skeleton_totals = combo_skeleton_state["combo_skeleton_totals"]
        combo_skeleton_fingerprint_payload_v1 = combo_skeleton_state["combo_skeleton_fingerprint_payload_v1"]
        combo_skeleton_hash_v1 = combo_skeleton_state["combo_skeleton_hash_v1"]

        slot_by_id = {
            entry.get("slot_id"): entry
            for entry in canonical_slots_all
            if isinstance(entry.get("slot_id"), str)
        }
        edge_by_key_for_candidates: Dict[str, Dict[str, Any]] = {}
        for edge in graph_edges:
            edge_key = f"{edge.get('a')}|{edge.get('b')}"
            edge_by_key_for_candidates[edge_key] = edge

        combo_candidate_state = {
            "combo_skeleton_components": combo_skeleton_components,
            "edge_key_norm": edge_key_norm,
            "edge_by_key_for_candidates": edge_by_key_for_candidates,
            "sorted_unique": sorted_unique,
            "slot_by_id": slot_by_id,
            "primitive_index_by_slot": primitive_index_by_slot,
            "stable_json_dumps": stable_json_dumps,
            "sha256_hex": sha256_hex,
            "combo_candidate_layer_version": COMBO_CANDIDATE_LAYER_VERSION,
            "combo_candidate_ruleset_version": COMBO_CANDIDATE_RULESET_VERSION,
            "combo_skeleton_hash_v1": combo_skeleton_hash_v1,
            "graph_hash_v2": graph_hash_v2,
        }
        combo_candidate_state = run_combo_candidate_v0(combo_candidate_state)

        combo_candidates_v0 = combo_candidate_state["combo_candidates_v0"]
        combo_candidates_by_component = combo_candidate_state["combo_candidates_by_component"]
        combo_candidates_by_cycle_len = combo_candidate_state["combo_candidates_by_cycle_len"]
        combo_candidate_fingerprint_payload_v1 = combo_candidate_state["combo_candidate_fingerprint_payload_v1"]
        combo_candidates_hash_v1 = combo_candidate_state["combo_candidates_hash_v1"]

        node_by_slot_id = {
            n.get("slot_id"): n
            for n in graph_nodes
            if isinstance(n.get("slot_id"), str)
        }
        edge_by_key = {
            f"{e.get('a')}|{e.get('b')}": e
            for e in graph_edges
        }
        component_by_slot = {
            sid: cid
            for sid, cid in graph_component_by_node.items()
            if isinstance(sid, str) and isinstance(cid, str)
        }
        articulation_set = {
            a.get("slot_id")
            for a in disruption_articulation_nodes
            if isinstance(a.get("slot_id"), str)
        }
        bridge_edge_set = {
            b.get("edge_key")
            for b in disruption_bridge_edges
            if isinstance(b.get("edge_key"), str)
        }
        path_distance_by_slot = {
            row.get("slot_id"): row.get("distance")
            for row in pathways_commander_distances
            if isinstance(row.get("slot_id"), str)
        }
        hub_rank_by_slot = {
            row.get("slot_id"): idx
            for idx, row in enumerate(pathways_hubs)
            if isinstance(row.get("slot_id"), str)
        }
        disruption_node_impact_by_slot = {
            row.get("slot_id"): row
            for row in disruption_node_impact
            if isinstance(row.get("slot_id"), str)
        }
        node_order_index = {sid: idx for idx, sid in enumerate(node_order)}

        proof_scaffold_state = {
            "combo_candidates_v0": combo_candidates_v0,
            "slot_by_id": slot_by_id,
            "path_distance_by_slot": path_distance_by_slot,
            "commander_playable": commander_playable,
            "disruption_commander_risk": disruption_commander_risk,
            "sorted_unique": sorted_unique,
            "order_by_node_order": order_by_node_order,
            "articulation_set": articulation_set,
            "node_order": node_order,
            "bridge_edge_set": bridge_edge_set,
            "hub_rank_by_slot": hub_rank_by_slot,
            "node_by_slot_id": node_by_slot_id,
            "disruption_node_impact_by_slot": disruption_node_impact_by_slot,
            "proof_rule_topics_v1": PROOF_RULE_TOPICS_V1,
            "rules_db_available": RULES_DB_AVAILABLE,
            "rules_db_connect": rules_db_connect,
            "ruleset_id_default": RULESET_ID_DEFAULT,
            "builtin_topic_selection_defaults": BUILTIN_TOPIC_SELECTION_DEFAULTS,
            "builtin_topic_selection_topics": BUILTIN_TOPIC_SELECTION_TOPICS,
            "rules_topic_config_obj": RULES_TOPIC_CONFIG_OBJ,
            "rules_topic_config_available": RULES_TOPIC_CONFIG_AVAILABLE,
            "rules_topic_config_version": RULES_TOPIC_CONFIG_VERSION,
            "engine_topic_prefer_sections_override_by_topic": engine_topic_prefer_sections_override_by_topic,
            "engine_topic_take_final_override_by_topic": engine_topic_take_final_override_by_topic,
            "rules_lookup_by_fts_with_trace": rules_lookup_by_fts_with_trace,
            "make_rule_citation": make_rule_citation,
            "rules_db_abs_path": RULES_DB_ABS_PATH,
            "default_topic_selection_policy_id": DEFAULT_TOPIC_SELECTION_POLICY_ID,
            "proof_scaffold_rules_policy_version": PROOF_SCAFFOLD_RULES_POLICY_VERSION,
            "proof_scaffold_layer_version": PROOF_SCAFFOLD_LAYER_VERSION,
            "proof_scaffold_ruleset_version": PROOF_SCAFFOLD_RULESET_VERSION,
            "make_slot_id": make_slot_id,
            "component_by_slot": component_by_slot,
            "graph_hash_v2": graph_hash_v2,
            "stable_json_dumps": stable_json_dumps,
            "sha256_hex": sha256_hex,
            "combo_candidates_hash_v1": combo_candidates_hash_v1,
            "disruption_hash_v1": disruption_hash_v1,
            "pathways_hash_v1": pathways_hash_v1,
        }
        proof_scaffold_state = run_proof_scaffold_v1(proof_scaffold_state)

        combo_proof_scaffolds_v0 = proof_scaffold_state["combo_proof_scaffolds_v0"]
        rules_db_available_for_build = proof_scaffold_state["rules_db_available_for_build"]
        ruleset_id_for_build = proof_scaffold_state["ruleset_id_for_build"]
        topic_selection_rules_version = proof_scaffold_state["topic_selection_rules_version"]
        rules_topic_selection_trace = proof_scaffold_state["rules_topic_selection_trace"]
        proof_scaffolds_rules_context_consistent = proof_scaffold_state["proof_scaffolds_rules_context_consistent"]
        proof_scaffold_fingerprint_payload_v1 = proof_scaffold_state["proof_scaffold_fingerprint_payload_v1"]
        proof_scaffold_fingerprint_payload_v2 = proof_scaffold_state["proof_scaffold_fingerprint_payload_v2"]
        proof_scaffold_fingerprint_payload_v3 = proof_scaffold_state["proof_scaffold_fingerprint_payload_v3"]
        proof_scaffolds_hash_v1 = proof_scaffold_state["proof_scaffolds_hash_v1"]
        proof_scaffolds_hash_v2 = proof_scaffold_state["proof_scaffolds_hash_v2"]
        proof_scaffolds_hash_v3 = proof_scaffold_state["proof_scaffolds_hash_v3"]

        for patch in engine_primitive_override_patches:
            patch_id = patch.get("patch_id")
            if not isinstance(patch_id, str):
                continue
            patch_effect_entry = patch_effect_entry_by_id.get(patch_id)
            if not isinstance(patch_effect_entry, dict):
                continue
            oracle_id = patch.get("oracle_id")
            outcome = primitive_override_outcome_by_oracle.get(oracle_id) if isinstance(oracle_id, str) else None
            original_primitives = list((outcome or {}).get("original_primitives") or [])
            final_primitives = list((outcome or {}).get("final_primitives") or [])
            original_set = set([p for p in original_primitives if isinstance(p, str)])
            final_set = set([p for p in final_primitives if isinstance(p, str)])
            add_candidates = sorted_unique([p for p in (patch.get("add") or []) if isinstance(p, str)])
            remove_candidates = sorted_unique([p for p in (patch.get("remove") or []) if isinstance(p, str)])
            add_applied = sorted([p for p in add_candidates if p in final_set and p not in original_set])
            remove_applied = sorted([p for p in remove_candidates if p in original_set and p not in final_set])
            patch_effect_entry["details"] = {
                "before_primitives_total": len(original_primitives),
                "after_primitives_total": len(final_primitives),
                "add_applied": add_applied,
                "remove_applied": remove_applied,
            }

        typed_rule_current_counts = {
            i: typed_rule_match_counts_before.get(i, 0)
            for i in range(len(TYPED_EDGE_RULES_V0))
        }
        for patch in sorted(engine_typed_edge_toggle_patches, key=lambda p: str(p.get("patch_id") or "")):
            patch_id = patch.get("patch_id")
            rule_index = patch.get("rule_index")
            enabled_target = patch.get("enabled_target")
            matched_rule_version = patch.get("matched_rule_version")
            if not isinstance(patch_id, str) or not isinstance(rule_index, int) or not isinstance(enabled_target, bool):
                continue
            patch_effect_entry = patch_effect_entry_by_id.get(patch_id)
            if not isinstance(patch_effect_entry, dict):
                continue
            before_typed_matches_total = int(typed_rule_current_counts.get(rule_index, 0))
            baseline_count = int(typed_rule_match_counts_before.get(rule_index, 0))
            after_typed_matches_total = baseline_count if enabled_target else 0
            typed_rule_current_counts[rule_index] = after_typed_matches_total
            patch_effect_entry["details"] = {
                "before_typed_matches_total": before_typed_matches_total,
                "after_typed_matches_total": after_typed_matches_total,
                "delta_typed_matches": after_typed_matches_total - before_typed_matches_total,
                "matched_rule_version": matched_rule_version,
                "rule_index": rule_index,
                "enabled_target": enabled_target,
            }

        applied_patches_sorted = sorted(
            [v for _, v in sorted(applied_patches_by_key.items(), key=lambda kv: kv[0])],
            key=lambda x: ((x.get("patch_id") or ""), (x.get("oracle_id") or "")),
        )
        primitive_override_deltas = sorted(
            [
                {
                    "oracle_id": item.get("oracle_id"),
                    "original_primitives": item.get("original_primitives") or [],
                    "final_primitives": item.get("final_primitives") or [],
                }
                for item in primitive_override_outcome_by_oracle.values()
            ],
            key=lambda x: (x.get("oracle_id") or ""),
        )

        proof_attempt_state = {
            "combo_proof_scaffolds_v0": combo_proof_scaffolds_v0,
            "slot_by_id": slot_by_id,
            "commander_canonical_slot": commander_canonical_slot,
            "sorted_unique": sorted_unique,
            "slot_sort_key": slot_sort_key,
            "lookup_snapshot_evidence_by_oracle_id": lookup_snapshot_evidence_by_oracle_id,
            "assert_runtime_no_oracle_text": assert_runtime_no_oracle_text,
            "add_unknown": add_unknown,
            "unknowns": unknowns,
            "strip_hash_fields": strip_hash_fields,
            "stable_json_dumps": stable_json_dumps,
            "sha256_hex": sha256_hex,
            "proof_attempt_layer_version_v1": PROOF_ATTEMPT_LAYER_VERSION_V3,
            "proof_scaffolds_hash_v2": proof_scaffolds_hash_v2,
        }
        proof_attempt_layer_name = "proof_attempt_v1"
        proof_attempt_layer_skipped_for_oracle_text = ENGINE_ALLOW_RUNTIME_ORACLE_TEXT is not True
        if proof_attempt_layer_skipped_for_oracle_text:
            add_unknown(
                unknowns,
                code="LAYER_SKIPPED_ORACLE_TEXT_REQUIRED",
                input_value=proof_attempt_layer_name,
                message=(
                    f"Layer {proof_attempt_layer_name} skipped because tag-only runtime forbids runtime oracle parsing."
                ),
                reason="ENGINE_ALLOW_RUNTIME_ORACLE_TEXT=False",
                suggestions=[],
            )
            combo_proof_attempts_v0 = []
            proof_attempt_hash_stable = True
            proof_attempts_total_matches_scaffolds = len(combo_proof_scaffolds_v0) == 0

            proof_attempt_skip_payload_v1 = {
                "proof_scaffolds_hash_v2": proof_scaffolds_hash_v2,
                "layer": proof_attempt_layer_name,
                "skip_code": "LAYER_SKIPPED_ORACLE_TEXT_REQUIRED",
            }
            proof_attempts_hash_v1 = sha256_hex(stable_json_dumps(proof_attempt_skip_payload_v1))
            proof_attempts_hash_v2 = sha256_hex(
                stable_json_dumps(
                    {
                        "proof_attempts_hash_v1": proof_attempts_hash_v1,
                        "skip_code": "LAYER_SKIPPED_ORACLE_TEXT_REQUIRED",
                    }
                )
            )
            proof_attempts_hash_v3 = sha256_hex(
                stable_json_dumps(
                    {
                        "proof_attempts_hash_v1": proof_attempts_hash_v1,
                        "proof_attempts_hash_v2": proof_attempts_hash_v2,
                        "skip_code": "LAYER_SKIPPED_ORACLE_TEXT_REQUIRED",
                    }
                )
            )
        else:
            proof_attempt_state = run_proof_attempt_v1(proof_attempt_state)

            combo_proof_attempts_v0 = proof_attempt_state["combo_proof_attempts_v0"]
            proof_attempt_hash_stable = proof_attempt_state["proof_attempt_hash_stable"]
            proof_attempts_total_matches_scaffolds = proof_attempt_state["proof_attempts_total_matches_scaffolds"]
            proof_attempts_hash_v1 = proof_attempt_state["proof_attempts_hash_v1"]
            proof_attempts_hash_v2 = proof_attempt_state["proof_attempts_hash_v2"]
            proof_attempts_hash_v3 = proof_attempt_state["proof_attempts_hash_v3"]

        nonplayable_by_code: Dict[str, int] = {}
        duplicate_exclusions = []
        first_copy_slot_by_name_temp: Dict[str, str] = {}
        playable_slots_total = 0
        nonplayable_slots_total = 0
        unknown_slots_total = 0
        for entry in deck_cards_canonical_input_order:
            status_value = entry.get("status")
            if status_value == "PLAYABLE":
                playable_slots_total += 1
            elif status_value == "UNKNOWN":
                unknown_slots_total += 1
            else:
                nonplayable_slots_total += 1
            for code in (entry.get("codes") or []):
                if isinstance(code, str):
                    nonplayable_by_code[code] = nonplayable_by_code.get(code, 0) + 1

            duplicate_detail = entry.get("duplicate_detail") or {}
            first_copy_slot_id = duplicate_detail.get("first_copy_slot_id")
            if isinstance(first_copy_slot_id, str):
                name_for_dup = entry.get("resolved_name") or entry.get("input")
                duplicate_exclusions.append(
                    {
                        "slot_id": entry.get("slot_id"),
                        "name": name_for_dup,
                        "first_copy_slot_id": first_copy_slot_id,
                    }
                )
                if isinstance(name_for_dup, str) and name_for_dup not in first_copy_slot_by_name_temp:
                    first_copy_slot_by_name_temp[name_for_dup] = first_copy_slot_id

        first_copy_slot_by_name = {
            k: first_copy_slot_by_name_temp[k]
            for k in sorted(first_copy_slot_by_name_temp.keys())
        }
        duplicate_exclusions.sort(
            key=lambda x: (
                slot_sort_key(x.get("slot_id")) if isinstance(x.get("slot_id"), str) else (2, 10**9, ""),
                str(x.get("name") or ""),
            )
        )
        nonplayable_by_code_sorted = {
            k: nonplayable_by_code[k]
            for k in sorted(nonplayable_by_code.keys())
        }

        first_scaffold_rules_context = ((combo_proof_scaffolds_v0 or [{}])[0].get("rules_context") or {})
        first_scaffold_citations_total = len(((combo_proof_scaffolds_v0 or [{}])[0].get("rules_citations_flat") or []))

        trace_v1 = {
            "inputs": {
                "commander": req.commander,
                "cards_input_total": len(req.cards),
                "db_snapshot_id": req.db_snapshot_id,
                "taxonomy_version": runtime_taxonomy_version,
                "taxonomy_ruleset_version": runtime_tag_ruleset_version,
                "profile_id": req.profile_id,
                "bracket_id": req.bracket_id,
                "format": req.format,
            },
            "playability_summary": {
                "playable_slots_total": playable_slots_total,
                "nonplayable_slots_total": nonplayable_slots_total,
                "unknown_slots_total": unknown_slots_total,
                "nonplayable_by_code": nonplayable_by_code_sorted,
            },
            "duplicate_enforcement": {
                "exempt_names_count": len(SINGLETON_EXEMPT_NAMES),
                "first_copy_slot_by_name": first_copy_slot_by_name,
                "duplicate_exclusions": duplicate_exclusions,
            },
            "primitive_overrides": {
                "overrides_available": OVERRIDES_AVAILABLE,
                "overrides_version": loaded_overrides_version if isinstance(loaded_overrides_version, str) else "none",
                "overrides_path": str(OVERRIDES_ABS_PATH),
                "overrides_scope_db_snapshot_id": overrides_scope_db_snapshot_id if isinstance(overrides_scope_db_snapshot_id, str) else None,
                "applied_patches_total": len(applied_patches_sorted),
                "applied_patches": applied_patches_sorted,
                "oracle_primitive_deltas": primitive_override_deltas,
            },
            "rules_citations": {
                "rules_db_available": first_scaffold_rules_context.get("rules_db_available", False),
                "rules_db_path": str(RULES_DB_ABS_PATH),
                "ruleset_id_default": RULESET_ID_DEFAULT,
                "total_citations_attached_to_first_scaffold": first_scaffold_citations_total,
            },
            "rules_topic_selection": rules_topic_selection_trace,
        }

        request_payload_for_hash = {
            "db_snapshot_id": req.db_snapshot_id,
            "taxonomy_version": runtime_taxonomy_version,
            "profile_id": req.profile_id,
            "bracket_id": req.bracket_id,
            "format": req.format,
            "commander": req.commander,
            "cards": req.cards,
            "engine_patches_v0": normalized_patches,
        }
        request_hash_v1 = sha256_hex(stable_json_dumps(request_payload_for_hash))

        input_to_slot_ids: Dict[str, List[str]] = {}
        commander_input_for_map = commander_canonical_slot.get("input")
        if isinstance(commander_input_for_map, str):
            input_to_slot_ids.setdefault(commander_input_for_map, []).append("C0")
        for entry in deck_cards_canonical_input_order:
            input_value = entry.get("input")
            slot_id = entry.get("slot_id")
            if isinstance(input_value, str) and isinstance(slot_id, str):
                input_to_slot_ids.setdefault(input_value, []).append(slot_id)

        slot_id_to_codes: Dict[str, List[str]] = {
            "C0": sorted(commander_canonical_slot.get("codes") or [])
        }
        for entry in deck_cards_canonical_input_order:
            slot_id = entry.get("slot_id")
            if isinstance(slot_id, str):
                slot_id_to_codes[slot_id] = sorted(entry.get("codes") or [])

        unknowns_canonical: List[Dict[str, Any]] = []
        for u in unknowns:
            code = u.get("code")
            input_value = u.get("input")
            message = u.get("message")
            reason = u.get("reason")

            candidate_slot_ids = []
            if isinstance(input_value, str):
                candidate_slot_ids = list(input_to_slot_ids.get(input_value, []))

            matched_slot_ids = [sid for sid in candidate_slot_ids if code in (slot_id_to_codes.get(sid) or [])]
            target_slot_ids = matched_slot_ids if matched_slot_ids else candidate_slot_ids

            if target_slot_ids:
                for sid in target_slot_ids:
                    unknowns_canonical.append(
                        {
                            "slot_id": sid,
                            "code": code,
                            "input": input_value,
                            "message": message,
                            "reason": reason,
                        }
                    )
            else:
                unknowns_canonical.append(
                    {
                        "slot_id": None,
                        "code": code,
                        "input": input_value,
                        "message": message,
                        "reason": reason,
                    }
                )

        unknowns_canonical.sort(
            key=lambda x: (
                slot_sort_key(x.get("slot_id")) if isinstance(x.get("slot_id"), str) else (2, 10**9, ""),
                x.get("code") or "",
                x.get("input") or "",
            )
        )

        # Internal pipeline context (reporting/debug organization only)
        ctx = {
            "req": req,
            "commander_resolved": commander_resolved,
            "unknowns": unknowns,
            "canonical_slots_all": canonical_slots_all,
            "graph_nodes": graph_nodes,
            "graph_edges": graph_edges,
            "node_order": [n.get("slot_id") for n in graph_nodes if isinstance(n.get("slot_id"), str)],
            "combo_candidates_v0": combo_candidates_v0,
            "combo_proof_scaffolds_v0": combo_proof_scaffolds_v0,
        }

        canonical_slot_ids = [s.get("slot_id") for s in canonical_slots_all if isinstance(s.get("slot_id"), str)]
        canonical_slot_ids_set = set(canonical_slot_ids)
        deck_playable_from_canonical = {
            e.get("slot_id")
            for e in deck_cards_canonical_input_order
            if e.get("status") == "PLAYABLE" and isinstance(e.get("slot_id"), str)
        }
        graph_edges_consistent_with_adjacency = True
        if len(graph_nodes) > 0:
            for edge in graph_edges:
                a = edge.get("a")
                b = edge.get("b")
                if not isinstance(a, str) or not isinstance(b, str):
                    graph_edges_consistent_with_adjacency = False
                    break
                neighbors_a = {n.get("neighbor") for n in graph_adjacency.get(a, [])}
                neighbors_b = {n.get("neighbor") for n in graph_adjacency.get(b, [])}
                if b not in neighbors_a or a not in neighbors_b:
                    graph_edges_consistent_with_adjacency = False
                    break

        combo_candidates_edges_exist = True
        for candidate in combo_candidates_v0:
            cycle_edge_keys = [k for k in (candidate.get("cycle_edge_keys") or []) if isinstance(k, str)]
            cycle_edges_missing = [k for k in (candidate.get("cycle_edges_missing") or []) if isinstance(k, str)]
            if cycle_edges_missing:
                combo_candidates_edges_exist = False
                break
            if any(k not in edge_by_key_for_candidates for k in cycle_edge_keys):
                combo_candidates_edges_exist = False
                break

        invariants_v1 = {
            "all_slot_ids_unique": len(canonical_slot_ids) == len(canonical_slot_ids_set),
            "playable_slot_ids_consistent": set(deck_cards_slot_ids_playable) == deck_playable_from_canonical,
            "unknowns_canonical_consistent": all(
                isinstance(u.get("slot_id"), str) and u.get("slot_id") in canonical_slot_ids_set
                for u in unknowns_canonical
            ),
            "graph_edges_consistent_with_adjacency": graph_edges_consistent_with_adjacency,
            "combo_candidates_edges_exist": combo_candidates_edges_exist,
            "canonical_slots_total": len(canonical_slots_all),
            "playable_slots_total": len(deck_cards_slot_ids_playable)
            + (1 if commander_canonical_slot.get("status") == "PLAYABLE" else 0),
            "graph_nodes_total": len(graph_nodes),
            "graph_edges_total": len(graph_edges),
            "combo_candidates_total": len(combo_candidates_v0),
            "proof_scaffolds_total": len(combo_proof_scaffolds_v0),
            "proof_scaffolds_rules_context_consistent": proof_scaffolds_rules_context_consistent,
            "proof_attempts_total_matches_scaffolds": proof_attempts_total_matches_scaffolds,
            "proof_attempt_hash_stable": proof_attempt_hash_stable,
            "patch_ids_unique": patch_ids_unique,
            "patch_hash_consistent": patch_hash_consistent,
            "patch_hash_in_build_hash": True,
        }

        # --- Invariants Debug (reporting only) ---
        passed_keys = []
        failed_keys = []
        for k, v in invariants_v1.items():
            if v is True:
                passed_keys.append(k)
            elif v is False:
                failed_keys.append(k)

        passed_keys.sort()
        failed_keys.sort()

        invariants_debug_v1 = {
            "passed": passed_keys,
            "failed": failed_keys,
            "failed_total": len(failed_keys),
            "passed_total": len(passed_keys),
        }
        invariants_all_pass_bool_only = len(failed_keys) == 0

        pipeline_versions = {
            "engine_version": ENGINE_VERSION,
            "ruleset_version": RULESET_VERSION,
            "bracket_definition_version": BRACKET_DEFINITION_VERSION,
            "game_changers_version": GAME_CHANGERS_VERSION,
            "taxonomy_version": runtime_taxonomy_version,
            "taxonomy_ruleset_version": runtime_tag_ruleset_version,
            "canonical_layer_version": CANONICAL_LAYER_VERSION,
            "primitive_index_version": PRIMITIVE_INDEX_VERSION,
            "structural_reporting_version": STRUCTURAL_REPORTING_VERSION,
            "build_pipeline_stage": BUILD_PIPELINE_STAGE,
            "graph_layer_version": GRAPH_LAYER_VERSION,
            "graph_ruleset_version": GRAPH_RULESET_VERSION,
            "graph_fingerprint_version": GRAPH_FINGERPRINT_VERSION,
            "graph_typed_rules_version": GRAPH_TYPED_RULES_VERSION,
            "motif_layer_version": MOTIF_LAYER_VERSION,
            "motif_ruleset_version": MOTIF_RULESET_VERSION,
            "motif_fingerprint_version": MOTIF_FINGERPRINT_VERSION,
            "disruption_layer_version": DISRUPTION_LAYER_VERSION,
            "disruption_ruleset_version": DISRUPTION_RULESET_VERSION,
            "disruption_fingerprint_version": DISRUPTION_FINGERPRINT_VERSION,
            "pathways_layer_version": PATHWAYS_LAYER_VERSION,
            "pathways_ruleset_version": PATHWAYS_RULESET_VERSION,
            "pathways_fingerprint_version": PATHWAYS_FINGERPRINT_VERSION,
            "combo_skeleton_layer_version": COMBO_SKELETON_LAYER_VERSION,
            "combo_skeleton_ruleset_version": COMBO_SKELETON_RULESET_VERSION,
            "combo_skeleton_fingerprint_version": COMBO_SKELETON_FINGERPRINT_VERSION,
            "combo_candidate_layer_version": COMBO_CANDIDATE_LAYER_VERSION,
            "combo_candidate_ruleset_version": COMBO_CANDIDATE_RULESET_VERSION,
            "combo_candidate_fingerprint_version": COMBO_CANDIDATE_FINGERPRINT_VERSION,
            "proof_scaffold_layer_version": PROOF_SCAFFOLD_LAYER_VERSION,
            "proof_scaffold_ruleset_version": PROOF_SCAFFOLD_RULESET_VERSION,
            "proof_scaffold_fingerprint_version": PROOF_SCAFFOLD_FINGERPRINT_VERSION,
            "build_fingerprint_version": "build_fingerprint_v1",
        }

        canonical_slots_compact = [
            {
                "slot_id": entry.get("slot_id"),
                "input": entry.get("input"),
                "resolved_name": entry.get("resolved_name"),
                "resolved_oracle_id": entry.get("resolved_oracle_id"),
                "status": entry.get("status"),
                "codes": sorted(entry.get("codes") or []),
            }
            for entry in canonical_slots_all
        ]
        unknowns_canonical_compact = [
            {
                "slot_id": entry.get("slot_id"),
                "code": entry.get("code"),
                "input": entry.get("input"),
            }
            for entry in sorted(
                unknowns_canonical,
                key=lambda x: (
                    slot_sort_key(x.get("slot_id")) if isinstance(x.get("slot_id"), str) else (2, 10**9, ""),
                    x.get("code") or "",
                    x.get("input") or "",
                ),
            )
        ]

        fingerprint_payload_v1 = {
            "engine_version": ENGINE_VERSION,
            "ruleset_version": RULESET_VERSION,
            "bracket_definition_version": BRACKET_DEFINITION_VERSION,
            "game_changers_version": GAME_CHANGERS_VERSION,
            "taxonomy_version": runtime_taxonomy_version,
            "taxonomy_ruleset_version": runtime_tag_ruleset_version,
            "canonical_layer_version": CANONICAL_LAYER_VERSION,
            "primitive_index_version": PRIMITIVE_INDEX_VERSION,
            "structural_reporting_version": STRUCTURAL_REPORTING_VERSION,
            "build_pipeline_stage": BUILD_PIPELINE_STAGE,
            "db_snapshot_id": req.db_snapshot_id,
            "profile_id": req.profile_id,
            "bracket_id": req.bracket_id,
            "format": req.format,
            "commander_input": req.commander,
            "commander_resolved_name": (commander_resolved or {}).get("name"),
            "commander_oracle_id": (commander_resolved or {}).get("oracle_id"),
            "canonical_slots_compact": canonical_slots_compact,
            "unknowns_canonical_compact": unknowns_canonical_compact,
            "game_changers_count": game_changers_count,
            "game_changers_found": sorted(game_changers_found),
            "bracket_floor_from_game_changers": bracket_floor_from_gc,
            "bracket_conflict": bracket_conflict,
            "patch_hash_v1": patch_hash_v1 if patch_hash_v1 is not None else "null",
            "primitive_index_totals": primitive_index_totals,
            "structural_snapshot_v1": structural_snapshot_v1,
        }
        fingerprint_json_v1 = stable_json_dumps(fingerprint_payload_v1)
        build_hash_v1 = sha256_hex(fingerprint_json_v1)

        for scaffold in combo_proof_scaffolds_v0:
            patch_anchor = scaffold.get("patch_anchor_v1")
            if isinstance(patch_anchor, dict):
                patch_anchor["build_hash_v1"] = build_hash_v1

        repro_v1 = {
            "db_snapshot_id": req.db_snapshot_id,
            "taxonomy_version": runtime_taxonomy_version,
            "taxonomy_ruleset_version": runtime_tag_ruleset_version,
            "ruleset_id": RULESET_ID_DEFAULT,
            "game_changers_version": GAME_CHANGERS_VERSION,
            "overrides_version": OVERRIDES_VERSION if OVERRIDES_AVAILABLE else "none",
            "request_hash_v1": request_hash_v1,
            "build_hash_v1": build_hash_v1,
            "proof_scaffolds_hash_v2": proof_scaffolds_hash_v2,
            "proof_scaffolds_hash_v3": proof_scaffolds_hash_v3,
            "graph_hash_v2": graph_hash_v2,
        }

        sort_unknowns(unknowns)

        return BuildResponse(
            engine_version=ENGINE_VERSION,
            ruleset_version=RULESET_VERSION,
            bracket_definition_version=BRACKET_DEFINITION_VERSION,
            game_changers_version=GAME_CHANGERS_VERSION,
            db_snapshot_id=req.db_snapshot_id,
            profile_id=req.profile_id,
            bracket_id=req.bracket_id,
            status=status,
            deck_size_total=deck_size_total,
            deck_status=deck_status,
            cards_needed=cards_needed,
            cards_to_cut=cards_to_cut,
            cut_order=cut_order,
            build_hash_v1=build_hash_v1,
            graph_hash_v1=graph_hash_v1,
            graph_hash_v2=graph_hash_v2,
            motif_hash_v1=motif_hash_v1,
            disruption_hash_v1=disruption_hash_v1,
            pathways_hash_v1=pathways_hash_v1,
            combo_skeleton_hash_v1=combo_skeleton_hash_v1,
            combo_candidates_hash_v1=combo_candidates_hash_v1,
            proof_scaffolds_hash_v1=proof_scaffolds_hash_v1,
            proof_scaffolds_hash_v2=proof_scaffolds_hash_v2,
            proof_scaffolds_hash_v3=proof_scaffolds_hash_v3,
            unknowns=unknowns,
            result={
                "format": req.format,
                "snapshot_id": req.db_snapshot_id,
                "taxonomy_version": runtime_taxonomy_version,
                "taxonomy_ruleset_version": runtime_tag_ruleset_version,
                "ruleset_version": RULESET_VERSION,
                "commander": req.commander,
                "commander_resolved": commander_resolved,
                "cards_input": req.cards,
                "cards_resolved": resolved_cards,
                "game_changers_path": str(GAME_CHANGERS_ABS_PATH),
                "game_changers_found": game_changers_found,
                "game_changers_count": game_changers_count,
                "canonical_layer_version": CANONICAL_LAYER_VERSION,
                "primitive_index_version": PRIMITIVE_INDEX_VERSION,
                "structural_reporting_version": STRUCTURAL_REPORTING_VERSION,
                "build_pipeline_stage": PROOF_ATTEMPT_BUILD_PIPELINE_STAGE_V3,
                "graph_layer_version": GRAPH_LAYER_VERSION,
                "graph_ruleset_version": GRAPH_RULESET_VERSION,
                "graph_fingerprint_version": GRAPH_FINGERPRINT_VERSION,
                "graph_hash_v1": graph_hash_v1,
                "graph_hash_v2": graph_hash_v2,
                "graph_fingerprint_payload_v1": graph_fingerprint_payload_v1,
                "graph_fingerprint_payload_v2": graph_fingerprint_payload_v2,
                "motif_layer_version": MOTIF_LAYER_VERSION,
                "motif_ruleset_version": MOTIF_RULESET_VERSION,
                "motifs": motifs,
                "motif_totals": motif_totals,
                "motif_fingerprint_version": MOTIF_FINGERPRINT_VERSION,
                "motif_hash_v1": motif_hash_v1,
                "motif_fingerprint_payload_v1": motif_fingerprint_payload_v1,
                "disruption_layer_version": DISRUPTION_LAYER_VERSION,
                "disruption_ruleset_version": DISRUPTION_RULESET_VERSION,
                "disruption_articulation_nodes": disruption_articulation_nodes,
                "disruption_articulation_nodes_total": len(disruption_articulation_nodes),
                "disruption_bridge_edges": disruption_bridge_edges,
                "disruption_bridge_edges_total": len(disruption_bridge_edges),
                "disruption_node_impact": disruption_node_impact,
                "disruption_node_impact_total": len(disruption_node_impact),
                "disruption_commander_risk": disruption_commander_risk,
                "disruption_totals": disruption_totals,
                "disruption_fingerprint_version": DISRUPTION_FINGERPRINT_VERSION,
                "disruption_hash_v1": disruption_hash_v1,
                "disruption_fingerprint_payload_v1": disruption_fingerprint_payload_v1,
                "pathways_layer_version": PATHWAYS_LAYER_VERSION,
                "pathways_ruleset_version": PATHWAYS_RULESET_VERSION,
                "pathways_commander_distances": pathways_commander_distances,
                "pathways_commander_reachable_slots": pathways_commander_reachable_slots,
                "pathways_commander_unreachable_slots": pathways_commander_unreachable_slots,
                "pathways_commander_reachable_total": len(pathways_commander_reachable_slots),
                "pathways_commander_unreachable_total": len(pathways_commander_unreachable_slots),
                "pathways_hubs": pathways_hubs,
                "pathways_hubs_total": len(pathways_hubs),
                "pathways_commander_bridge_candidates": pathways_commander_bridge_candidates,
                "pathways_totals": pathways_totals,
                "pathways_fingerprint_version": PATHWAYS_FINGERPRINT_VERSION,
                "pathways_hash_v1": pathways_hash_v1,
                "pathways_fingerprint_payload_v1": pathways_fingerprint_payload_v1,
                "combo_skeleton_layer_version": COMBO_SKELETON_LAYER_VERSION,
                "combo_skeleton_ruleset_version": COMBO_SKELETON_RULESET_VERSION,
                "combo_skeleton_components": combo_skeleton_components,
                "combo_skeleton_totals": combo_skeleton_totals,
                "combo_skeleton_fingerprint_version": COMBO_SKELETON_FINGERPRINT_VERSION,
                "combo_skeleton_hash_v1": combo_skeleton_hash_v1,
                "combo_skeleton_fingerprint_payload_v1": combo_skeleton_fingerprint_payload_v1,
                "combo_candidate_layer_version": COMBO_CANDIDATE_LAYER_VERSION,
                "combo_candidate_ruleset_version": COMBO_CANDIDATE_RULESET_VERSION,
                "combo_candidates_v0": combo_candidates_v0,
                "combo_candidates_v0_total": len(combo_candidates_v0),
                "combo_candidates_by_component": combo_candidates_by_component,
                "combo_candidates_by_cycle_len": combo_candidates_by_cycle_len,
                "combo_candidate_fingerprint_version": COMBO_CANDIDATE_FINGERPRINT_VERSION,
                "combo_candidates_hash_v1": combo_candidates_hash_v1,
                "combo_candidate_fingerprint_payload_v1": combo_candidate_fingerprint_payload_v1,
                "proof_scaffold_layer_version": PROOF_SCAFFOLD_LAYER_VERSION,
                "proof_scaffold_ruleset_version": PROOF_SCAFFOLD_RULESET_VERSION,
                "proof_scaffold_rules_policy_version": PROOF_SCAFFOLD_RULES_POLICY_VERSION,
                "combo_proof_scaffolds_v0": combo_proof_scaffolds_v0,
                "combo_proof_scaffolds_v0_total": len(combo_proof_scaffolds_v0),
                "proof_attempt_layer_version": PROOF_ATTEMPT_LAYER_VERSION_V3,
                "combo_proof_attempts_v0": combo_proof_attempts_v0,
                "combo_proof_attempts_v0_total": len(combo_proof_attempts_v0),
                "proof_attempt_layer_skipped_for_oracle_text": proof_attempt_layer_skipped_for_oracle_text,
                "proof_attempts_hash_v1": proof_attempts_hash_v1,
                "proof_attempts_hash_v2": proof_attempts_hash_v2,
                "proof_attempts_hash_v3": proof_attempts_hash_v3,
                "proof_scaffold_fingerprint_version": PROOF_SCAFFOLD_FINGERPRINT_VERSION,
                "proof_scaffolds_hash_v1": proof_scaffolds_hash_v1,
                "proof_scaffolds_hash_v2": proof_scaffolds_hash_v2,
                "proof_scaffolds_hash_v3": proof_scaffolds_hash_v3,
                "proof_scaffold_fingerprint_payload_v1": proof_scaffold_fingerprint_payload_v1,
                "proof_scaffold_fingerprint_payload_v2": proof_scaffold_fingerprint_payload_v2,
                "proof_scaffold_fingerprint_payload_v3": proof_scaffold_fingerprint_payload_v3,
                "proof_scaffold_hash_version": "proof_scaffold_hash_v3",
                "rules_db_path": str(RULES_DB_ABS_PATH),
                "rules_db_available": rules_db_available_for_build,
                "ruleset_id_default": RULESET_ID_DEFAULT,
                "overrides_path": str(OVERRIDES_ABS_PATH),
                "overrides_available": OVERRIDES_AVAILABLE,
                "overrides_version": OVERRIDES_VERSION if OVERRIDES_AVAILABLE else "none",
                "patch_loop_v0": patch_loop_v0,
                "patch_error_v0": patch_error_v0,
                "repro_v1": repro_v1,
                "trace_v1": trace_v1,
                "build_fingerprint_version": "build_fingerprint_v1",
                "build_hash_v1": build_hash_v1,
                "build_fingerprint_payload_v1": fingerprint_payload_v1,
                "invariants_v1": invariants_v1,
                "invariants_debug_v1": invariants_debug_v1,
                "invariants_all_pass_bool_only": invariants_all_pass_bool_only,
                "pipeline_versions": pipeline_versions,
                "drift_guard": {
                    "hash_algo": "sha256",
                    "canonicalization": "stable_json_dumps(sort_keys=True,separators)",
                },
                "bracket_floor_from_game_changers": bracket_floor_from_gc,
                "bracket_conflict": bracket_conflict,
                "deck_cards_playable": deck_cards_playable,
                "deck_cards_playable_total": len(deck_cards_playable),
                "deck_cards_nonplayable": deck_cards_nonplayable,
                "deck_cards_unknown": unknown_cards,
                "deck_cards_canonical_input_order": deck_cards_canonical_input_order,
                "deck_cards_canonical_input_order_total": len(deck_cards_canonical_input_order),
                "deck_cards_canonical_playable_slots_total": playable_index_counter,
                "deck_cards_canonical_nonplayable_slots_total": nonplayable_index_counter,
                "deck_cards_slot_ids_playable": deck_cards_slot_ids_playable,
                "deck_cards_slot_ids_nonplayable": deck_cards_slot_ids_nonplayable,
                "deck_cards_unknowns_by_slot": deck_cards_unknowns_by_slot,
                "commander_canonical_slot": commander_canonical_slot,
                "canonical_slots_all": canonical_slots_all,
                "canonical_slots_all_total": len(canonical_slots_all),
                "primitive_index_by_slot": primitive_index_by_slot,
                "slot_ids_by_primitive": slot_ids_by_primitive,
                "primitive_index_totals": primitive_index_totals,
                "graph_nodes": graph_nodes,
                "graph_nodes_total": len(graph_nodes),
                "graph_edges": graph_edges,
                "graph_edges_total": len(graph_edges),
                "graph_edge_index": graph_edge_index,
                "graph_typed_edges_total": graph_typed_edges_total,
                "graph_typed_match_counts_by_type": graph_typed_match_counts_by_type,
                "graph_typed_edges_by_type": graph_typed_edges_by_type,
                "graph_rules_meta": graph_rules_meta,
                "graph_adjacency": graph_adjacency,
                "graph_node_degrees": graph_node_degrees,
                "graph_components": graph_components,
                "graph_component_by_node": graph_component_by_node,
                "graph_totals": graph_totals,
                "primitive_counts_by_scope": primitive_counts_by_scope,
                "primitive_counts_by_scope_totals": primitive_counts_by_scope_totals,
                "structural_snapshot_v1": structural_snapshot_v1,
                "graph_v1": graph_v1,
                "unknowns_canonical": unknowns_canonical,
                "unknowns_canonical_total": len(unknowns_canonical),
                "primitive_counts": primitive_counts,
                "primitives_present": primitives_present,
                "display_title": f"{(req.commander or '')} — {req.format.upper()}",
                "display_subtitle": f"Snapshot {req.db_snapshot_id} • Bracket {req.bracket_id} • {status}",
                "ui_contract_version": UI_CONTRACT_VERSION,
                "available_panels_v1": {
                    "has_canonical_slots": True if deck_cards_canonical_input_order else False,
                    "has_unknowns_canonical": True if unknowns_canonical else False,
                    "has_deck_cards_summary": True if deck_cards_playable is not None else False,
                    "has_rules_db": bool(RULES_DB_AVAILABLE),
                    "has_rules_topic_selection_trace": bool((trace_v1 or {}).get("rules_topic_selection")),
                    "has_proof_scaffolds": bool(len(combo_proof_scaffolds_v0)),
                    "has_proof_attempts": bool(len(combo_proof_attempts_v0)),
                    "has_primitive_index": bool(primitive_index_by_slot),
                    "has_structural_reporting": bool(structural_snapshot_v1),
                    "has_graph": bool(graph_edges or graph_components or graph_typed_edges_total is not None),
                    "has_motifs": bool(motifs),
                    "has_disruption": bool(DISRUPTION_LAYER_VERSION or disruption_totals),
                    "has_pathways": bool(PATHWAYS_LAYER_VERSION or pathways_totals),
                    "has_combo_skeleton": bool(COMBO_SKELETON_LAYER_VERSION or combo_skeleton_components),
                    "has_combo_candidates": bool(len(combo_candidates_v0) or combo_candidates_v0),
                    "has_patch_loop": bool(trace_v1 or repro_v1),
                },
                "ui_index_v1": {
                    "primary_ids": {
                        "commander_slot_id": (commander_canonical_slot or {}).get("slot_id"),
                        "first_deck_slot_id": (
                            (deck_cards_canonical_input_order or [{}])[0].get("slot_id")
                            if deck_cards_canonical_input_order
                            else None
                        ),
                        "first_component_id": (
                            (graph_components or [{}])[0].get("component_id") if graph_components else None
                        ),
                        "first_candidate_id": (
                            combo_candidates_v0[0].get("candidate_id")
                            if combo_candidates_v0 and isinstance(combo_candidates_v0[0], dict)
                            else None
                        ),
                    },
                    "totals": {
                        "canonical_slots_all_total": len(canonical_slots_all),
                        "playable_slots_total": len(deck_cards_slot_ids_playable),
                        "nonplayable_slots_total": len(deck_cards_slot_ids_nonplayable),
                        "unknowns_total": len(unknowns),
                        "unknowns_canonical_total": len(unknowns_canonical),
                        "unique_primitives_total": (primitive_index_totals or {}).get("unique_primitives_total"),
                        "slots_with_primitives": (primitive_index_totals or {}).get("slots_with_primitives"),
                        "graph_nodes_total": len(graph_nodes),
                        "graph_edges_total": len(graph_edges),
                        "graph_components_total": (graph_totals or {}).get("connected_components_total"),
                        "combo_candidates_total": len(combo_candidates_v0) or 0,
                        "proof_scaffolds_total": len(combo_proof_scaffolds_v0) or 0,
                    },
                    "versions": {
                        "canonical_layer_version": CANONICAL_LAYER_VERSION,
                        "primitive_index_version": PRIMITIVE_INDEX_VERSION,
                        "structural_reporting_version": STRUCTURAL_REPORTING_VERSION,
                        "graph_layer_version": GRAPH_LAYER_VERSION,
                        "graph_ruleset_version": GRAPH_RULESET_VERSION,
                        "motif_layer_version": MOTIF_LAYER_VERSION,
                        "disruption_layer_version": DISRUPTION_LAYER_VERSION,
                        "pathways_layer_version": PATHWAYS_LAYER_VERSION,
                        "combo_skeleton_layer_version": COMBO_SKELETON_LAYER_VERSION,
                        "combo_candidate_layer_version": COMBO_CANDIDATE_LAYER_VERSION,
                        "proof_scaffold_layer_version": PROOF_SCAFFOLD_LAYER_VERSION,
                    },
                    "hashes": {
                        "build_hash_v1": build_hash_v1,
                        "graph_hash_v1": graph_hash_v1,
                        "graph_hash_v2": graph_hash_v2,
                        "motif_hash_v1": motif_hash_v1,
                        "disruption_hash_v1": disruption_hash_v1,
                        "pathways_hash_v1": pathways_hash_v1,
                        "combo_skeleton_hash_v1": combo_skeleton_hash_v1,
                        "combo_candidates_hash_v1": combo_candidates_hash_v1,
                        "proof_scaffolds_hash_v2": proof_scaffolds_hash_v2,
                        "proof_scaffolds_hash_v3": proof_scaffolds_hash_v3,
                    },
                },
                "deck_profile": {
                    "total_valid_cards": total_valid_cards,
                    "primitive_density": primitive_density,
                },
                "needs": needs,
                "note": "DB lookup v1: snapshot gating + exact-name resolution.",
            },
        )
    response = _execute()
    if hasattr(response, "model_dump"):
        response_payload = response.model_dump()
    else:
        response_payload = dict(response)

    if os.getenv("VALIDATE_INVARIANTS") == "1":
        validate_invariants_v1(response_payload)

    return response_payload
