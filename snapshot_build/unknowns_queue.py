from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Tuple
import sqlite3


@dataclass(frozen=True)
class UnknownQueueRow:
    oracle_id: str | None
    snapshot_id: str
    taxonomy_version: str
    rule_id: str | None
    reason: str
    snippet: str | None
    created_at: str


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_unknowns_queue_table(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS unknowns_queue (
          id INTEGER PRIMARY KEY,
          oracle_id TEXT,
          snapshot_id TEXT,
          taxonomy_version TEXT,
          rule_id TEXT,
          reason TEXT,
          snippet TEXT,
          created_at TEXT
        )
        """
    )


def insert_unknowns(con: sqlite3.Connection, rows: Iterable[UnknownQueueRow]) -> int:
    prepared: List[Tuple[str | None, str, str, str | None, str, str | None, str]] = []
    for row in rows:
        prepared.append(
            (
                row.oracle_id,
                row.snapshot_id,
                row.taxonomy_version,
                row.rule_id,
                row.reason,
                row.snippet,
                row.created_at,
            )
        )

    if not prepared:
        return 0

    con.executemany(
        """
        INSERT INTO unknowns_queue (
          oracle_id,
          snapshot_id,
          taxonomy_version,
          rule_id,
          reason,
          snippet,
          created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        prepared,
    )
    return len(prepared)
