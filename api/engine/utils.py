import hashlib
import json
from typing import Any, Dict, List


def stable_json_dumps(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True, ensure_ascii=False)


def sha256_hex(value: str | bytes) -> str:
    if isinstance(value, bytes):
        data = value
    else:
        data = str(value).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def strip_hash_fields(value: Any) -> Any:
    if isinstance(value, dict):
        cleaned: Dict[str, Any] = {}
        for key, child in value.items():
            if isinstance(key, str):
                key_lower = key.lower()
                if key == "attempt_hash_v3":
                    continue
                if key.endswith("_hash_v1") or key.endswith("_hash_v2") or key.endswith("_hash_v3"):
                    continue
                if key.startswith("build_hash_"):
                    continue
                if "hash" in key_lower or "sha256" in key_lower:
                    continue
            cleaned[key] = strip_hash_fields(child)
        return cleaned
    if isinstance(value, list):
        return [strip_hash_fields(item) for item in value]
    return value


def sorted_unique(seq: Any) -> List[Any]:
    return sorted(set(x for x in seq if x is not None))


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
