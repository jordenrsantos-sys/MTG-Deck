from typing import Any, Dict


def run_patch_loop_v0(state: Dict[str, Any]) -> Dict[str, Any]:
    return state


def run_layer(state: Dict[str, Any]) -> Dict[str, Any]:
    return run_patch_loop_v0(state)
