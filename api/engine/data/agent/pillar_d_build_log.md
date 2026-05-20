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

---

## Phase B — Candidate pool with user-intent anchoring

**Status:** ✅ Complete (2026-05-20)

### What landed
- `_build_candidate_pool()` in `agent_build_deck_v1.py` composes:
  1. `compute_archetype_brief_v1` → commander color identity + corpus staple
     frequencies.
  2. `_validate_must_includes` → resolves each user `must_include_cards` against
     the snapshot DB, drops with `MUST_INCLUDE_NOT_FOUND` or `MUST_INCLUDE_COLOR_ILLEGAL`
     warnings (Fix 4: warn-and-skip).
  3. `compute_theme_top_cards_v1` per `theme_hint` → primitive-overlap scored cards.
  4. Archetype staple list (descriptive baseline, heavily frequency-penalized).
- Skipped `compute_corpus_similar_decks_v1` from the pool build: it triggers a
  full corpus vectorization (~13K decks) on cold start, which exceeds the
  endpoint-call budget for a single per-theme-hint pass. Deferred to Phase D
  validation if needed.
- Pure scoring helpers:
  - `_score_theme_candidate(signal_count, freq)` — theme-bonus minus a halved
    frequency penalty (theme-matched cards keep some merit even when common).
  - `_score_archetype_staple(freq)` — baseline minus full frequency penalty.
    Sol Ring profile (92% freq, no theme) scores ~-13: pool may contain it
    at the bottom, but Phase C's top-N selection won't reach that far when
    real theme matches are available.
- Deterministic tie-break via `hashlib.sha1(name|seed)` rather than `random`
  (the latter is banned by the `test_no_random_imports` guardrail). Fix 4's
  `seed` semantic = stable ordering for equal-score candidates across runs.
- `call_counter: Dict[str, int]` is a single-key mutable dict passed by reference
  so the eventual Phase D outer build can enforce `ENDPOINT_CALL_BUDGET = 30`
  across the whole pipeline without each layer keeping its own counter.

### Test results
- 16/16 new tests pass:
  - 5 `_normalize_color_identity` cases (list, JSON string, comma-string, empty, dedupe).
  - 4 scoring-helper unit tests (theme high/low freq, pure staple high/low freq).
  - 7 `_build_candidate_pool` integration tests with mocked upstream layers:
    - User must-includes locked at top (score=INF), preserved exact name.
    - Theme-matched cards outscore Sol Ring / Command Tower (creativity envelope).
    - Missing must-include surfaces `MUST_INCLUDE_NOT_FOUND` warning + drops.
    - Color-illegal must-include (U for Edgar's BRW) surfaces `MUST_INCLUDE_COLOR_ILLEGAL`.
    - `color_identity` flows from archetype_brief into pool output.
    - `endpoint_calls` counter increments per upstream call (2 = 1 brief + 1 theme).
    - Seed=42 produces identical ordering across runs (determinism).
- Combined: 21 pass (5 from Phase A + 16 from Phase B). 0 new regressions.

### Architectural decisions
1. **`_build_candidate_pool` is private to the layer, not yet wired into
   `compute_agent_build_deck_v1`.** The public entrypoint still returns the Phase A
   stub. Phase C will wire the pool through `_select_deck` and replace the stub
   body. This keeps each phase's commit atomic and reviewable.
2. **Lazy upstream imports.** `_build_candidate_pool` imports
   `compute_archetype_brief_v1` and `compute_theme_top_cards_v1` from
   `api.engine.layers.agent_endpoints_v1` at call time. This matches the existing
   pattern in `compute_agent_context_bundle_v1` and keeps module import cheap.
3. **`find_card_by_name` is the source of truth for color-identity checks.**
   It already handles DFC face-name fallback and JSON-string normalization
   quirks in the cards table — re-deriving CI from raw scryfall data here
   would duplicate that logic.
4. **`_upsert` merges sources, takes max score.** A card that surfaces from
   both `theme:Vampire Tribal` and `archetype_staple` ends up with a single
   pool entry, the higher score, and both rationale components — so Phase C's
   per-card `reason` can show why a card was picked from multiple angles.
5. **`compute_corpus_similar_decks_v1` deferred from pool build.** The
   `_ensure_vectors` call inside it can take 30+ seconds against a cold corpus
   cache. The pool doesn't strictly need similar-deck signal; theme + archetype
   are sufficient. If Phase F shows quality gaps for under-cornered commanders,
   we can add it back as an optional path inside the 30-call budget.

### Next
Phase C — `_select_deck` greedy slot-filling with per-bracket combo policy and
mana-base construction. Will consume the Phase B pool, derive target slot
distributions from `archetype_brief.bracket_distribution`, fill greedy by
score within slot categories, and reject combo completions per `BRACKET_COMBO_POLICY`
(B1/B2 = no combos; B3 = late-only; B4 = cap 3; B5 = unrestricted; user picks
always override).
