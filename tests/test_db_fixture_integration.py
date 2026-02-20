from __future__ import annotations

from pathlib import Path

from engine.db import list_snapshots, resolve_db_path, snapshot_exists


def test_engine_resolves_mtg_engine_db_path_and_reads_fixture_snapshot(mtg_test_db_path: Path) -> None:
    resolved_path = resolve_db_path()
    assert resolved_path == mtg_test_db_path.resolve()

    assert snapshot_exists("TEST_SNAPSHOT_0001") is True

    snapshots = list_snapshots(limit=1)
    assert isinstance(snapshots, list)
    assert len(snapshots) == 1
    assert snapshots[0]["snapshot_id"] == "TEST_SNAPSHOT_0001"
