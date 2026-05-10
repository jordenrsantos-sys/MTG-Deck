"""Layer registry — Engine-4A topological partial-build resolver.

Catalog of engine-layer ids + their dependency edges. Provides a topological
resolver `resolve_partial(requested_layer_ids)` that returns the FULL set of
layers that must be computed (transitive dependency closure) plus an order
of execution.

This module is META: it does NOT execute layers. It does NOT modify or wrap
any existing layer module. The execution itself is delegated to the existing
`run_build_pipeline` (or in Phase 6, a future short-circuiting executor that
runs only the resolved subset).

Engine-4A scope per HARD safety: the registry is purely additive infrastructure
— it does NOT touch `repo/api/engine/layers/` or `repo/api/engine/scoring/` or
`pipeline_build.py`. The dependency edges below are sourced from the existing
top-level result-key contract (`test_schema_freeze_guard`'s 193-key snapshot)
+ documented layer ordering in pipeline_build's call sequence.

Determinism: the registry is a pure data structure + sort. No wall-clock,
no dict-iteration randomness. `resolve_partial` returns the same order for
the same input set every call.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, FrozenSet, Iterable, List, Set, Tuple

LAYER_REGISTRY_V1_VERSION = "layer_registry_v1"


@dataclass(frozen=True)
class LayerSpec:
    """Catalog entry for one engine layer.

    `layer_id` matches the BuildResponse top-level result key the layer
    produces. `dependencies` is the SET of other layer_ids that must run
    before this layer. `partial_safe` flags whether the layer can be invoked
    in isolation (currently True for analysis layers; False for layers with
    pipeline-state coupling that Phase 6 may unwind).
    """
    layer_id: str
    dependencies: FrozenSet[str]
    partial_safe: bool = True
    description: str = ""


# Subset of pipeline_build's call sequence catalogued for partial-build use.
# Order in this dict is purely organizational; execution order is computed
# topologically by `resolve_partial`.
#
# NOTE on coverage: Engine-4A ships a representative subset (the layers the
# Phase 4 UI actually reads via UI_OVERHAUL_DESIGN's Engine-UI API contract
# table). Adding more layers is purely additive — no existing layer is
# modified. Phase 6 may broaden the catalog as additional UI surfaces consume
# additional layer outputs.
LAYER_REGISTRY: Dict[str, LayerSpec] = {
    "snapshot_preflight_v1": LayerSpec(
        layer_id="snapshot_preflight_v1",
        dependencies=frozenset(),
        description="Snapshot reachability + manifest sanity check; no upstream deps.",
    ),
    "primitive_index_v1": LayerSpec(
        layer_id="primitive_index_v1",
        dependencies=frozenset({"snapshot_preflight_v1"}),
        description="Resolves card primitives per slot from canonical deck input.",
    ),
    "engine_requirement_detection_v1": LayerSpec(
        layer_id="engine_requirement_detection_v1",
        dependencies=frozenset({"primitive_index_v1"}),
        description="Detects required-effect coverage from primitive index.",
    ),
    "seed_synergy_detection_v1": LayerSpec(
        layer_id="seed_synergy_detection_v1",
        dependencies=frozenset({"primitive_index_v1"}),
        description="Phase 1 — theme matching from seed primitives.",
    ),
    "commander_recommendation_v1": LayerSpec(
        layer_id="commander_recommendation_v1",
        dependencies=frozenset({"primitive_index_v1"}),
        description="Phase 2 (Path C v1) — top_n commander candidates.",
    ),
    "seed_to_deck_v1": LayerSpec(
        layer_id="seed_to_deck_v1",
        dependencies=frozenset(
            {"seed_synergy_detection_v1", "commander_recommendation_v1"}
        ),
        description="Phase 3.1 — seed-to-deck orchestrator (input validation + commander resolution + envelope).",
    ),
    "engine_coherence_v1": LayerSpec(
        layer_id="engine_coherence_v1",
        dependencies=frozenset({"primitive_index_v1"}),
        description="Coherence metrics over primitive index.",
    ),
    "bracket_compliance_summary_v1": LayerSpec(
        layer_id="bracket_compliance_summary_v1",
        dependencies=frozenset({"primitive_index_v1", "engine_requirement_detection_v1"}),
        description="Per-bracket policy compliance summary.",
    ),
    "sufficiency_summary_v1": LayerSpec(
        layer_id="sufficiency_summary_v1",
        # Sufficiency summary aggregates 14 upstream layer payloads (per
        # `run_sufficiency_summary_v1` signature inspected during Phase 3
        # discovery). Cataloguing the full set is out of Engine-4A scope;
        # mark as non-partial-safe so callers requesting it get the full
        # pipeline (autonomous_repair_log soft-safety #4: layer dependency
        # cycles / coupling → mark non-partial-safe; Phase 6 unwinds).
        dependencies=frozenset({
            "engine_requirement_detection_v1",
            "engine_coherence_v1",
            "bracket_compliance_summary_v1",
        }),
        partial_safe=False,
        description="Aggregates 14 upstream payloads; Engine-4A flags non-partial-safe; Phase 6 may unwind.",
    ),
}


def known_layer_ids() -> List[str]:
    """All layer_ids in the registry, sorted alphabetically."""
    return sorted(LAYER_REGISTRY.keys())


def is_known_layer(layer_id: str) -> bool:
    return isinstance(layer_id, str) and layer_id in LAYER_REGISTRY


def resolve_partial(
    requested_layer_ids: Iterable[str],
) -> Dict[str, object]:
    """Compute the transitive dependency closure for `requested_layer_ids`.

    Returns a dict:
      {
        "version": "layer_registry_v1",
        "requested": [...],          # the input layer_ids (after dedup + sort)
        "resolved": [...],           # transitive closure incl. requested (topological order)
        "unknown": [...],            # any requested ids not in the registry
        "non_partial_safe": [...],   # resolved ids flagged partial_safe=False
        "partial_eligible": bool,    # True iff non_partial_safe is empty
      }

    The topological order is deterministic: layers with fewer dependencies
    come first; ties broken alphabetically by layer_id.
    """
    requested = sorted(set(
        s for s in requested_layer_ids if isinstance(s, str) and s
    ))
    unknown = [lid for lid in requested if not is_known_layer(lid)]
    known_requested = [lid for lid in requested if is_known_layer(lid)]

    # Transitive dependency closure via BFS/DFS.
    closure: Set[str] = set()
    stack: List[str] = list(known_requested)
    while stack:
        current = stack.pop()
        if current in closure:
            continue
        if not is_known_layer(current):
            continue
        closure.add(current)
        spec = LAYER_REGISTRY[current]
        for dep in spec.dependencies:
            if dep not in closure:
                stack.append(dep)

    # Topological sort: Kahn's algorithm with stable alphabetical tiebreak.
    in_degree: Dict[str, int] = {
        lid: sum(1 for dep in LAYER_REGISTRY[lid].dependencies if dep in closure)
        for lid in closure
    }
    # Adjacency: for each layer, list of dependents inside the closure.
    dependents: Dict[str, List[str]] = {lid: [] for lid in closure}
    for lid in closure:
        for dep in LAYER_REGISTRY[lid].dependencies:
            if dep in dependents:
                dependents[dep].append(lid)

    ready = sorted([lid for lid, d in in_degree.items() if d == 0])
    ordered: List[str] = []
    while ready:
        current = ready.pop(0)
        ordered.append(current)
        for next_lid in sorted(dependents.get(current, [])):
            in_degree[next_lid] -= 1
            if in_degree[next_lid] == 0:
                ready.append(next_lid)
        ready.sort()

    if len(ordered) != len(closure):
        # Cycle detected — registry has a bad edge. Should not happen with
        # Engine-4A's catalogued subset; surfacing here documents the failure
        # mode for Phase 6 if catalog grows.
        cycle_remaining = sorted(closure - set(ordered))
        ordered = ordered + cycle_remaining

    non_partial_safe = sorted(
        lid for lid in closure if not LAYER_REGISTRY[lid].partial_safe
    )

    return {
        "version": LAYER_REGISTRY_V1_VERSION,
        "requested": list(requested),
        "resolved": ordered,
        "unknown": unknown,
        "non_partial_safe": non_partial_safe,
        "partial_eligible": len(non_partial_safe) == 0 and len(unknown) == 0,
    }


def filter_response_to_resolved(
    response: Dict[str, object],
    resolved: Iterable[str],
) -> Dict[str, object]:
    """Project a full BuildResponse `result` dict down to only the resolved
    layer_ids + a small set of always-included envelope fields.

    The /build/partial endpoint uses this to return only the requested layer
    payloads, keeping client-side responsiveness-driven calls small.
    """
    resolved_set = set(resolved)
    always_keep = {
        "ui_contract_version",
        "available_panels_v1",
        "ruleset_version",
        "taxonomy_version",
        "snapshot_id",
        "pipeline_versions",
    }
    if not isinstance(response, dict):
        return {}
    return {
        k: v
        for k, v in response.items()
        if k in resolved_set or k in always_keep
    }
