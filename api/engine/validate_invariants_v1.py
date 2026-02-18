from typing import Any


_REQUIRED_TOP_LEVEL_KEYS = (
    "status",
    "build_hash_v1",
    "result",
)

_REQUIRED_RESULT_KEYS = (
    "deck_cards_canonical_input_order",
    "primitive_index_by_slot",
    "structural_coverage",
    "graph_nodes",
    "motifs",
    "disruption_totals",
    "pathways_totals",
    "combo_candidates_v0",
    "combo_proof_scaffolds_v0",
    "combo_proof_attempts_v0",
    "proof_attempts_hash_v2",
)

_REQUIRED_HASH_KEYS = (
    "build_hash_v1",
    "graph_hash_v1",
    "graph_hash_v2",
    "motif_hash_v1",
    "disruption_hash_v1",
    "pathways_hash_v1",
    "combo_skeleton_hash_v1",
    "combo_candidates_hash_v1",
    "proof_scaffolds_hash_v1",
    "proof_scaffolds_hash_v2",
    "proof_scaffolds_hash_v3",
)


def _assert_no_sets(obj: Any, path: str) -> None:
    if isinstance(obj, (set, frozenset)):
        raise AssertionError(f"set-like container at {path}")

    if isinstance(obj, dict):
        for key, value in obj.items():
            child_path = f"{path}.{key!r}"
            _assert_no_sets(value, child_path)
        return

    if isinstance(obj, (list, tuple)):
        for idx, value in enumerate(obj):
            child_path = f"{path}[{idx}]"
            _assert_no_sets(value, child_path)


def validate_invariants_v1(state: dict) -> bool:
    """
    Assert-only invariants. Must not mutate state. Disabled by default.
    """
    assert isinstance(state, dict), "state must be a dict"

    for key in _REQUIRED_TOP_LEVEL_KEYS:
        assert key in state, f"missing top-level key: {key}"

    for key in _REQUIRED_HASH_KEYS:
        assert key in state, f"missing hash key: {key}"

    result = state.get("result")
    assert isinstance(result, dict), "result must be a dict"

    for key in _REQUIRED_RESULT_KEYS:
        assert key in result, f"missing result key: {key}"

    _assert_no_sets(state, "state")
    return True
