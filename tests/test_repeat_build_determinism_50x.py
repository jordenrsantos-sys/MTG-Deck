from __future__ import annotations

import hashlib
import re
from pathlib import Path
from types import SimpleNamespace

from api.engine.pipeline_build import run_build_pipeline
from api.engine.utils import stable_json_dumps

TEST_SNAPSHOT_ID = "TEST_SNAPSHOT_0001"
ISO_TIMESTAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")


def _build_request() -> SimpleNamespace:
    return SimpleNamespace(
        db_snapshot_id=TEST_SNAPSHOT_ID,
        profile_id="focused",
        bracket_id="B2",
        format="commander",
        commander="Missing Commander",
        cards=["Missing Card A", "Missing Card B"],
        engine_patches_v0=[],
    )


def test_repeat_build_determinism_50x(mtg_test_db_path: Path) -> None:
    _ = mtg_test_db_path

    baseline_bytes: bytes | None = None
    baseline_payload: dict | None = None
    baseline_build_hash = None
    baseline_json_hash = ""
    has_build_hash = False

    for _ in range(50):
        payload = run_build_pipeline(req=_build_request(), conn=None, repo_root_path=None)
        normalized_json = stable_json_dumps(payload)
        normalized_bytes = normalized_json.encode("utf-8")

        assert ISO_TIMESTAMP_RE.search(normalized_json) is None

        if baseline_bytes is None:
            baseline_bytes = normalized_bytes
            baseline_payload = payload
            has_build_hash = "build_hash_v1" in payload
            if has_build_hash:
                baseline_build_hash = payload.get("build_hash_v1")
            else:
                baseline_json_hash = hashlib.sha256(normalized_bytes).hexdigest()
            continue

        assert normalized_bytes == baseline_bytes

        if has_build_hash:
            assert payload.get("build_hash_v1") == baseline_build_hash
        else:
            current_json_hash = hashlib.sha256(normalized_bytes).hexdigest()
            assert current_json_hash == baseline_json_hash

    assert baseline_payload is not None
