import hashlib
import json
from typing import Any, Dict


def stable_json_dumps(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True, ensure_ascii=False)


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


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
