from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List


@dataclass
class BuildContext:
    req: Any
    unknowns: List[Dict[str, Any]] = field(default_factory=list)
    db_snapshot_id: str = ""
    fmt: str = "commander"
    bracket_id: str = ""
    commander_input: str = ""
    cards_input: List[str] = field(default_factory=list)
    repo_root: Path | None = None
    game_changers_set: set[str] = field(default_factory=set)
    game_changers_version: str = ""
    game_changers_abs_path: Path | None = None
    rules_db_path: str | None = None
    cards_db_conn: Any = None
    rules_db_conn: Any = None
