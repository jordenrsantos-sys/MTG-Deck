from typing import Any, Dict


def run_invariants_v1(state: Dict[str, Any]) -> Dict[str, Any]:
    return state


def run_layer(state: Dict[str, Any]) -> Dict[str, Any]:
    return run_invariants_v1(state)
