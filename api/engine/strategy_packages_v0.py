from types import SimpleNamespace
from typing import Any, Dict, List

from api.engine.constants import TagsNotCompiledError
from api.engine.pipeline_build import run_build_pipeline
from api.engine.utils import normalize_primitives_source, sorted_unique
from api.engine.version_resolve_v1 import resolve_runtime_taxonomy_version
from engine.db import connect as cards_db_connect


def _round_metric(value: float) -> float:
    return float(f"{value:.6f}")


def _ensure_runtime_primitive_index(con, snapshot_id: str, taxonomy_version: str | None) -> str:
    taxonomy = taxonomy_version if isinstance(taxonomy_version, str) and taxonomy_version != "" else None
    if taxonomy is None:
        raise TagsNotCompiledError(
            snapshot_id=snapshot_id,
            taxonomy_version=taxonomy,
            reason=(
                "No taxonomy_version resolved from card_tags for package candidate retrieval. "
                "Run snapshot_build.tag_snapshot and snapshot_build.index_build."
            ),
        )

    table_exists_row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='primitive_to_cards' LIMIT 1"
    ).fetchone()
    if table_exists_row is None:
        raise TagsNotCompiledError(
            snapshot_id=snapshot_id,
            taxonomy_version=taxonomy,
            reason=(
                "primitive_to_cards table is missing. "
                "Run snapshot_build.tag_snapshot and snapshot_build.index_build."
            ),
        )

    row = con.execute(
        "SELECT COUNT(1) FROM primitive_to_cards WHERE snapshot_id = ? AND taxonomy_version = ?",
        (snapshot_id, taxonomy),
    ).fetchone()
    row_count = int(row[0]) if row else 0
    if row_count <= 0:
        raise TagsNotCompiledError(
            snapshot_id=snapshot_id,
            taxonomy_version=taxonomy,
            reason=(
                "primitive_to_cards has no rows for snapshot/taxonomy_version. "
                "Run snapshot_build.tag_snapshot and snapshot_build.index_build."
            ),
        )

    return taxonomy


def _query_cards_for_primitive(snapshot_id: str, primitive: str, limit: int = 64) -> List[Dict[str, Any]]:
    primitive_clean = primitive.strip() if isinstance(primitive, str) else ""
    if primitive_clean == "":
        return []

    rows: List[Dict[str, Any]] = []
    with cards_db_connect() as con:
        taxonomy_version = resolve_runtime_taxonomy_version(
            snapshot_id=snapshot_id,
            requested=None,
            db=con,
        )
        taxonomy_version = _ensure_runtime_primitive_index(
            con=con,
            snapshot_id=snapshot_id,
            taxonomy_version=taxonomy_version,
        )
        db_rows = con.execute(
            """
            SELECT DISTINCT
              c.name,
              t.primitive_ids_json AS primitives_json
            FROM primitive_to_cards p
            JOIN cards c
              ON c.snapshot_id = p.snapshot_id
             AND c.oracle_id = p.oracle_id
            LEFT JOIN card_tags t
              ON t.snapshot_id = p.snapshot_id
             AND t.taxonomy_version = p.taxonomy_version
             AND t.oracle_id = p.oracle_id
            WHERE p.snapshot_id = ?
              AND p.taxonomy_version = ?
              AND p.primitive_id = ?
            ORDER BY c.name ASC
            LIMIT ?
            """,
            (snapshot_id, taxonomy_version, primitive_clean, int(limit)),
        ).fetchall()

    for row in db_rows:
        row_dict = dict(row)
        name = row_dict.get("name")
        if not isinstance(name, str) or name == "":
            continue
        primitives = normalize_primitives_source(row_dict.get("primitives_json"))
        rows.append({"name": name, "primitives": primitives})

    unique: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        name = row["name"]
        if name not in unique:
            unique[name] = row

    return [unique[name] for name in sorted(unique.keys())]


def _signal_for_card(
    card_primitives: List[str],
    missing_primitives: List[str],
    core_primitive_set: set[str],
    primitive_frequency: Dict[str, int],
) -> Dict[str, Any]:
    primitives = [p for p in card_primitives if isinstance(p, str)]
    primitive_set = set(primitives)

    covered_missing = [p for p in missing_primitives if p in primitive_set]
    role_compression = len(covered_missing)
    overlap = len([p for p in primitive_set if p in core_primitive_set])
    redundancy = sum(1.0 / (1.0 + float(int(primitive_frequency.get(p, 0)))) for p in covered_missing)

    return {
        "covered_missing": covered_missing,
        "role_compression_score": _round_metric(float(role_compression)),
        "overlap_score": _round_metric(float(overlap)),
        "redundancy_score": _round_metric(float(redundancy)),
    }


def _validate_package(
    snapshot_id: str,
    commander: str | None,
    anchor_cards_for_validation: List[str],
    package_cards: List[str],
    profile_id: str,
    bracket_id: str,
) -> Dict[str, Any]:
    build_cards = list(anchor_cards_for_validation) + list(package_cards)
    req = SimpleNamespace(
        db_snapshot_id=snapshot_id,
        profile_id=profile_id,
        bracket_id=bracket_id,
        format="commander",
        commander=commander,
        cards=build_cards,
        engine_patches_v0=[],
    )

    try:
        output = run_build_pipeline(req=req, conn=None, repo_root_path=None)
        result = output.get("result") or {}
        dead_slot_ids = result.get("dead_slot_ids") or []
        status_value = output.get("status")
        validation_status = "ERROR" if status_value == "ERROR" else "OK"
        return {
            "status": validation_status,
            "build_hash_v1": output.get("build_hash_v1"),
            "key_signals": {
                "commander_dependency_signal": result.get("commander_dependency_signal"),
                "primitive_concentration_index": result.get("primitive_concentration_index"),
                "dead_slot_ids_count": len(dead_slot_ids),
            },
        }
    except Exception:
        return {
            "status": "ERROR",
            "build_hash_v1": None,
            "key_signals": {
                "commander_dependency_signal": None,
                "primitive_concentration_index": None,
                "dead_slot_ids_count": 0,
            },
        }


def build_completion_packages_v0_1(
    snapshot_id: str,
    hypothesis: Dict[str, Any],
    primitive_frequency: Dict[str, int],
    slot_ids_by_primitive: Dict[str, List[str]],
    primitive_index_by_slot: Dict[str, List[str]],
    slot_name_by_id: Dict[str, str],
    max_packages_per_hypothesis: int,
    max_cards_per_package: int,
    validate_packages: bool,
    commander: str | None,
    anchor_cards_for_validation: List[str],
    profile_id: str,
    bracket_id: str,
) -> List[Dict[str, Any]]:
    missing_primitives = sorted_unique(
        [p for p in (hypothesis.get("required_primitives_missing") or []) if isinstance(p, str)]
    )
    if not missing_primitives:
        return []

    max_packages = max(0, int(max_packages_per_hypothesis))
    max_cards = max(0, int(max_cards_per_package))
    if max_packages == 0 or max_cards == 0:
        return []

    core_primitives = sorted_unique([p for p in (hypothesis.get("core_primitives") or []) if isinstance(p, str)])
    core_primitive_set = set(core_primitives)

    candidate_pool_by_primitive: Dict[str, List[Dict[str, Any]]] = {}
    candidate_by_name: Dict[str, Dict[str, Any]] = {}
    missing_without_pool: List[str] = []

    for primitive in missing_primitives:
        primitive_candidates: List[Dict[str, Any]] = []

        slot_candidates = [
            sid
            for sid in (slot_ids_by_primitive.get(primitive) or [])
            if isinstance(sid, str) and isinstance(slot_name_by_id.get(sid), str)
        ]
        slot_candidates_sorted = sorted_unique(slot_candidates)

        if slot_candidates_sorted:
            for slot_id in slot_candidates_sorted:
                card_name = slot_name_by_id.get(slot_id)
                if not isinstance(card_name, str) or card_name == "":
                    continue
                primitives = [p for p in (primitive_index_by_slot.get(slot_id) or []) if isinstance(p, str)]
                primitive_candidates.append({"name": card_name, "primitives": sorted_unique(primitives)})
        else:
            primitive_candidates.extend(_query_cards_for_primitive(snapshot_id=snapshot_id, primitive=primitive, limit=64))

        deduped_by_name: Dict[str, Dict[str, Any]] = {}
        for candidate in primitive_candidates:
            name = candidate.get("name")
            if isinstance(name, str) and name not in deduped_by_name:
                deduped_by_name[name] = {
                    "name": name,
                    "primitives": sorted_unique([p for p in (candidate.get("primitives") or []) if isinstance(p, str)]),
                }

        sorted_candidates = [deduped_by_name[name] for name in sorted(deduped_by_name.keys())]
        candidate_pool_by_primitive[primitive] = sorted_candidates

        if not sorted_candidates:
            missing_without_pool.append(primitive)

        for candidate in sorted_candidates:
            name = candidate["name"]
            if name not in candidate_by_name:
                candidate_by_name[name] = candidate

    packages: List[Dict[str, Any]] = []
    seen_package_cards: set[tuple[str, ...]] = set()

    for package_index in range(max_packages):
        selected_cards: List[str] = []
        selected_card_set: set[str] = set()
        covered_primitives_ordered: List[str] = []
        covered_primitive_set: set[str] = set()
        skip_once = package_index

        while len(selected_cards) < max_cards:
            uncovered = [p for p in missing_primitives if p not in covered_primitive_set]
            if not uncovered:
                break

            ranked_candidates: List[Dict[str, Any]] = []
            for card_name in sorted(candidate_by_name.keys()):
                if card_name in selected_card_set:
                    continue
                candidate = candidate_by_name[card_name]
                signal = _signal_for_card(
                    card_primitives=candidate.get("primitives") or [],
                    missing_primitives=uncovered,
                    core_primitive_set=core_primitive_set,
                    primitive_frequency=primitive_frequency,
                )
                role_score = float(signal.get("role_compression_score") or 0.0)
                if role_score <= 0.0:
                    continue
                ranked_candidates.append(
                    {
                        "card_name": card_name,
                        "signal": signal,
                    }
                )

            ranked_candidates.sort(
                key=lambda item: (
                    -float((item.get("signal") or {}).get("role_compression_score") or 0.0),
                    -float((item.get("signal") or {}).get("overlap_score") or 0.0),
                    str(item.get("card_name") or ""),
                )
            )

            if not ranked_candidates:
                break

            pick_index = 0
            if skip_once > 0 and len(ranked_candidates) > 1:
                pick_index = min(skip_once, len(ranked_candidates) - 1)
                skip_once = 0

            picked = ranked_candidates[pick_index]
            card_name = picked["card_name"]
            selected_cards.append(card_name)
            selected_card_set.add(card_name)

            covered_now = [
                p
                for p in ((picked.get("signal") or {}).get("covered_missing") or [])
                if isinstance(p, str)
            ]
            for primitive in covered_now:
                if primitive not in covered_primitive_set:
                    covered_primitive_set.add(primitive)
                    covered_primitives_ordered.append(primitive)

        selected_tuple = tuple(selected_cards)
        if selected_tuple in seen_package_cards:
            continue
        seen_package_cards.add(selected_tuple)

        if not selected_cards and package_index > 0:
            continue

        if selected_cards:
            total_role = 0.0
            total_overlap = 0.0
            total_redundancy = 0.0
            for card_name in selected_cards:
                signal = _signal_for_card(
                    card_primitives=(candidate_by_name.get(card_name) or {}).get("primitives") or [],
                    missing_primitives=missing_primitives,
                    core_primitive_set=core_primitive_set,
                    primitive_frequency=primitive_frequency,
                )
                total_role += float(signal.get("role_compression_score") or 0.0)
                total_overlap += float(signal.get("overlap_score") or 0.0)
                total_redundancy += float(signal.get("redundancy_score") or 0.0)

            card_total = float(len(selected_cards))
            package_metrics = {
                "role_compression_score": _round_metric(total_role / card_total),
                "overlap_score": _round_metric(total_overlap / card_total),
                "redundancy_score": _round_metric(total_redundancy / card_total),
            }
        else:
            package_metrics = {
                "role_compression_score": 0.0,
                "overlap_score": 0.0,
                "redundancy_score": 0.0,
            }

        uncovered_after = [p for p in missing_primitives if p not in covered_primitive_set]
        if selected_cards and not uncovered_after:
            explanation = "Deterministic greedy completion package covering all missing primitives."
        elif selected_cards:
            explanation = "Deterministic greedy completion package with partial primitive coverage."
        elif missing_without_pool:
            explanation = (
                "No candidate pool available in snapshot for primitive "
                f"{missing_without_pool[0]} (requires DB primitive index expansion)."
            )
        else:
            explanation = "No deterministic package candidates available for required primitives."

        package_obj: Dict[str, Any] = {
            "package_id": "",
            "covers_primitives": sorted_unique(covered_primitives_ordered),
            "cards": selected_cards,
            "package_metrics": package_metrics,
            "explanation": explanation,
        }

        if validate_packages:
            package_obj["validation"] = _validate_package(
                snapshot_id=snapshot_id,
                commander=commander,
                anchor_cards_for_validation=anchor_cards_for_validation,
                package_cards=selected_cards,
                profile_id=profile_id,
                bracket_id=bracket_id,
            )

        packages.append(package_obj)

    for idx, package in enumerate(packages):
        package["package_id"] = f"P{idx}"

    if not packages:
        fallback_explanation = "No deterministic package candidates available for required primitives."
        if missing_without_pool:
            fallback_explanation = (
                "No candidate pool available in snapshot for primitive "
                f"{missing_without_pool[0]} (requires DB primitive index expansion)."
            )
        fallback: Dict[str, Any] = {
            "package_id": "P0",
            "covers_primitives": [],
            "cards": [],
            "package_metrics": {
                "role_compression_score": 0.0,
                "overlap_score": 0.0,
                "redundancy_score": 0.0,
            },
            "explanation": fallback_explanation,
        }
        if validate_packages:
            fallback["validation"] = _validate_package(
                snapshot_id=snapshot_id,
                commander=commander,
                anchor_cards_for_validation=anchor_cards_for_validation,
                package_cards=[],
                profile_id=profile_id,
                bracket_id=bracket_id,
            )
        return [fallback]

    return packages
