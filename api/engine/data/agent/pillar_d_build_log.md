# Pillar D — AI Deck-Building Agent: Build Log

This is the running log for the Pillar D agent build (POST `/agent/build_deck_v1`).
Append new entries at the bottom; oldest at top. Each entry records what landed,
what tests passed/failed, and any architectural decisions.

The build is structured in six phases (A–F) per the kickoff brief. The agent's
primary constraint is the **creativity envelope**: user picks dominate, no forced
staples that don't match user intent.

---

## Phase A — Endpoint scaffolding + contract stub

**Status:** ✅ Complete (2026-05-20)

### What landed
- `api/engine/layers/agent_build_deck_v1.py` — `compute_agent_build_deck_v1()`
  returning commander + 99 Wastes (colorless basic, always color-identity-legal
  regardless of commander). Contract-shape only — no candidate pool, no selection
  algorithm yet.
- Module-level constants for downstream phases:
  - `VALID_BRACKETS = ("B1","B2","B3","B4","B5")`
  - `BRACKET_COMBO_POLICY` — Fix 1 from the kickoff patch encoded as a table.
    B1/B2 reject all 2-card combos; B3 allows late combos only; B4 caps at 3
    distinct pairs; B5 unrestricted. User must_include_cards always override.
  - `ENDPOINT_CALL_BUDGET = 30` (Fix 5)
  - `MAX_SWAP_ITERATIONS = 12` (Fix 4)
- `api/main.py` — `AgentBuildDeckV1Request` / `AgentBuildDeckV1Response` Pydantic
  models with `extra="forbid"`, and the `POST /agent/build_deck_v1` route handler.
- `tests/test_agent_build_deck_v1.py` — 5 smoke tests pinning the contract:
  - 100-card response (commander + 99 Wastes), `source` field, non-empty reasons
  - Summary reports bracket and `user_picks_total`
  - Invalid bracket (`B9`) returns `status="FAILED"` + `INVALID_BRACKET` warning
  - Missing commander returns `status="FAILED"` + `MISSING_COMMANDER` warning
  - Every success carries `PHASE_A_STUB` warning until Phases B-D replace it

### Test results
- 5/5 new tests pass.
- Full suite: 860 passed, 17 skipped, **4 failures pre-existing** (verified by
  stashing changes and re-running):
  - `test_bracket_gc_limits_v1::test_b4_and_b5_are_unlimited`
  - `test_complete_bracket_violations_v1::TestHttpEndpointWiring` (×5 subtests,
    fixture DB missing combo cards → `UNKNOWN_PRESENT`)
  - `test_no_random_imports` (`mpa_card_hydration.py`, `mpa_game_state.py`)
  - `test_pipeline_profile_bracket_enforcement_v1`

### Architectural decisions
1. **Stub uses Wastes, not commander-matching basics.** Wastes are colorless
   basics and are legal in any deck. Phase A doesn't need DB connectivity to
   resolve commander color identity; that arrives in Phase B's candidate-pool
   build. Keeps Phase A pure-stub.
2. **Combo policy lives in the layer, not main.py.** `BRACKET_COMBO_POLICY` is
   a module-level constant so Phase C can import it directly and so any future
   bracket-policy change has one source of truth.
3. **`status` is a string ("OK"/"FAILED"), not HTTPException.** Matches the
   existing Pillar A endpoint convention (see `corpus/batch_ingest_v1`,
   `deck/strength_check_v1`) — keep the wire shape uniform across the API.
4. **`extra="forbid"` on Pydantic models.** Matches every other Pillar A
   endpoint; surfaces typos early.

### Next
Phase B — candidate pool with user-intent anchoring. The pool must rank user
must-includes at score=∞, theme-matched cards next, archetype staples last
(with a frequency penalty for "common-corpus" cards that don't synergize with
user picks). Implementation will compose `compute_archetype_brief_v1`,
`compute_theme_top_cards_v1`, and `compute_corpus_similar_decks_v1` via direct
Python imports (Fix 3: in-process, not MCP roundtrip).
