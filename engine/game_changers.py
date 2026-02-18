import json
from pathlib import Path


GAME_CHANGERS_FILENAME = "data/game_changers/gc_v0_userlist_2025-11-20.json"
GAME_CHANGERS_VERSION_DEFAULT = "gc_v0_userlist_2025-11-20"
GAME_CHANGERS_VERSION_MISSING = "gc_missing"


def load_game_changers(repo_root: Path) -> tuple[str, Path, set[str]]:
    abs_path = (repo_root / GAME_CHANGERS_FILENAME).resolve()
    try:
        loaded = json.loads(abs_path.read_text(encoding="utf-8"))
        if isinstance(loaded, list) and all(isinstance(name, str) for name in loaded):
            return GAME_CHANGERS_VERSION_DEFAULT, abs_path, set(loaded)
    except Exception:
        pass
    return GAME_CHANGERS_VERSION_MISSING, abs_path, set()


def detect_game_changers(
    playable_names: list[str],
    commander_name: str | None,
    gc_set: set[str],
) -> tuple[list[str], int]:
    gc_names = list(playable_names)
    if commander_name:
        gc_names.append(commander_name)

    found_sorted = sorted([name for name in gc_names if name in gc_set])
    return found_sorted, len(found_sorted)


def bracket_floor_from_count(count: int) -> str | None:
    if 1 <= count <= 3:
        return "B3"
    if count >= 4:
        return "B4"
    return None
