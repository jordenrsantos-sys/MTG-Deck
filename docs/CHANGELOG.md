# CHANGELOG

## [2026-02-22] v2 optional layer docs + graph bounds spec namespace alignment

- Added UI contract documentation for optional v2 payloads in `UI_CONTRACT_v1.md`:
  - `result.commander_dependency_v2`
  - `result.engine_coherence_v2`
  - `result.stress_transform_engine_v2`
  - `result.stress_transform_engine_v2.policy_version`
  - ordered operators rendering term (`operators_applied_ordered`) sourced from `result.stress_transform_engine_v2.operators_applied`
  - `result.pipeline_versions.*` version pins for v2 layers
- Added graph bounds version and clamp indicator paths to UI contract docs:
  - `result.pipeline_versions.graph_bounds_policy_version`
  - `result.graph_v1.bounds.*`
  - `result.graph_v1.stats.caps_hit.*`
- Added canonical graph bounds spec file at:
  - `api/engine/data/graph/graph_bounds_spec_v1.json`
- Preserved backward compatibility for legacy path:
  - `api/engine/data/sufficiency/graph_bounds_policy_v1.json`
  - Loader now resolves new graph path first, then falls back to legacy sufficiency path.

Compatibility notes:

- No v1 payload fields were removed or renamed.
- Existing `load_graph_bounds_policy_v1()` call sites remain valid.
- Graph bounds policy version output (`graph_bounds_policy_version`) remains stable and sourced from loaded payload `version`.
