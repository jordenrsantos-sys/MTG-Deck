from typing import Any, Dict


def run_duplicate_enforcement(state: Dict[str, Any]) -> Dict[str, Any]:
    return state


def run_layer(state: Dict[str, Any]) -> Dict[str, Any]:
    return run_duplicate_enforcement(state)
