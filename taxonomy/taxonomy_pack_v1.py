from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Iterable, List


TAXONOMY_PACK_V1_VERSION = "taxonomy_pack_v1"


def _stable_json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, ensure_ascii=False)


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _clean_string(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()


def _pick_first_string(source: Dict[str, Any], keys: Iterable[str]) -> str:
    for key in keys:
        candidate = _clean_string(source.get(key))
        if candidate != "":
            return candidate
    return ""


def _canonicalize_json(value: Any) -> Any:
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for key in sorted(value.keys(), key=lambda item: str(item)):
            key_str = key if isinstance(key, str) else str(key)
            out[key_str] = _canonicalize_json(value.get(key))
        return out

    if isinstance(value, (list, tuple)):
        return [_canonicalize_json(item) for item in value]

    if isinstance(value, (set, frozenset)):
        normalized = [_canonicalize_json(item) for item in value]
        normalized.sort(key=_stable_json_dumps)
        return normalized

    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    return str(value)


def _normalize_list(raw: Any) -> List[Any]:
    if not isinstance(raw, list):
        return []

    normalized = [_canonicalize_json(item) for item in raw]
    normalized.sort(key=_stable_json_dumps)
    return normalized


def _extract_payload_lists(input_taxonomy_data: Dict[str, Any]) -> tuple[List[Any], List[Any], List[Any]]:
    rules = _normalize_list(
        input_taxonomy_data.get("rulespec_rules")
        if "rulespec_rules" in input_taxonomy_data
        else input_taxonomy_data.get("rulespec_rules.json")
    )
    facets = _normalize_list(
        input_taxonomy_data.get("rulespec_facets")
        if "rulespec_facets" in input_taxonomy_data
        else input_taxonomy_data.get("rulespec_facets.json")
    )
    primitives = _normalize_list(
        input_taxonomy_data.get("primitives")
        if "primitives" in input_taxonomy_data
        else input_taxonomy_data.get("primitives.json")
    )
    return rules, facets, primitives


def _extract_tag_ids(rules: List[Any], primitives: List[Any]) -> List[str]:
    tag_ids: set[str] = set()

    for rule in rules:
        if not isinstance(rule, dict):
            continue
        primitive_id = _pick_first_string(rule, ["primitive_id", "primitive", "tag", "primitive_tag", "tag_id"])
        if primitive_id != "":
            tag_ids.add(primitive_id)

    for primitive in primitives:
        if isinstance(primitive, dict):
            primitive_id = _pick_first_string(primitive, ["primitive_id", "id", "tag_id", "name"])
        elif isinstance(primitive, str):
            primitive_id = primitive.strip()
        else:
            primitive_id = ""

        if primitive_id != "":
            tag_ids.add(primitive_id)

    return sorted(tag_ids)


def _extract_edges(rules: List[Any]) -> List[Dict[str, str]]:
    edge_pairs: set[tuple[str, str]] = set()

    for rule in rules:
        if not isinstance(rule, dict):
            continue

        rule_id = _pick_first_string(rule, ["rule_id", "id", "rule", "rid"])
        primitive_id = _pick_first_string(rule, ["primitive_id", "primitive", "tag", "primitive_tag", "tag_id"])
        if rule_id == "" or primitive_id == "":
            continue

        edge_pairs.add((rule_id, primitive_id))

    return [
        {
            "rule_id": rule_id,
            "primitive_id": primitive_id,
        }
        for rule_id, primitive_id in sorted(edge_pairs)
    ]


def build_taxonomy_pack_v1(input_taxonomy_data: Any) -> Dict[str, Any]:
    source = input_taxonomy_data if isinstance(input_taxonomy_data, dict) else {}
    rulespec_rules, rulespec_facets, primitives = _extract_payload_lists(source)

    created_from_input = source.get("created_from") if isinstance(source.get("created_from"), dict) else {}
    tag_taxonomy_version = _pick_first_string(
        {
            "a": source.get("tag_taxonomy_version"),
            "b": source.get("taxonomy_version"),
            "c": created_from_input.get("tag_taxonomy_version"),
        },
        ["a", "b", "c"],
    )
    generator_version = _pick_first_string(
        {
            "a": source.get("generator_version"),
            "b": source.get("exporter_version"),
            "c": created_from_input.get("generator_version"),
        },
        ["a", "b", "c"],
    )

    taxonomy_source_id = _pick_first_string(
        {
            "a": source.get("taxonomy_source_id"),
            "b": source.get("source_workbook"),
            "c": source.get("snapshot_id"),
            "d": tag_taxonomy_version,
            "e": TAXONOMY_PACK_V1_VERSION,
        },
        ["a", "b", "c", "d", "e"],
    )

    tag_ids = _extract_tag_ids(rules=rulespec_rules, primitives=primitives)
    edges = _extract_edges(rulespec_rules)

    payload = {
        "tag_ids": tag_ids,
        "primitives": primitives,
        "facets": rulespec_facets,
        "rulespec_rules": rulespec_rules,
        "edges": edges,
    }

    pack = {
        "version": TAXONOMY_PACK_V1_VERSION,
        "taxonomy_source_id": taxonomy_source_id,
        "created_from": {
            "tag_taxonomy_version": tag_taxonomy_version,
            "generator_version": generator_version,
        },
        "hashes": {
            "pack_sha256": "",
        },
        "counts": {
            "tags": len(tag_ids),
            "primitives": len(primitives),
            "facets": len(rulespec_facets),
            "edges": len(edges),
        },
        "payload": payload,
    }

    pack_hash = _sha256_hex(_stable_json_dumps(pack))
    pack["hashes"]["pack_sha256"] = pack_hash
    return pack
