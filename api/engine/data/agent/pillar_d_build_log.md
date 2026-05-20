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

---

## Phase C — Selection with slot balancing + per-bracket combo policy

**Status:** ✅ Complete (2026-05-20)

### What landed
- `_classify_card(name, type_line, primitives)` — pure mapping into one of
  seven slot categories (land, creature, ramp, card_draw, removal,
  win_condition, flex). Type_line wins over primitives (a land is a land
  even if it carries MANA_ROCK).
- `_adjust_slot_targets(archetype_brief)` — tribal archetypes (any name
  containing "tribal" or "typal") bump creature count by 4 (pulled from flex);
  combo archetypes bump win_condition and card_draw at the cost of creatures.
  Default targets sum to 99 (28 creatures + 36 lands + 10 ramp + 10 draw + 7
  removal + 3 wincons + 5 flex).
- `_load_two_card_pair_index()` — loads `combo_brackets_v1.json` (the
  Commander Spellbook scrape) into `{frozenset({a_name_lower, b_name_lower}):
  set_of_brackets_allowed}`. 4,415 distinct 2-card pairs indexed. Falls back
  to `{}` on any load error (warned but non-fatal — bracket policy then uses
  the `BRACKET_COMBO_POLICY` defaults from Phase A).
- `_combo_violates_bracket()` — per-candidate policy check:
  - Both halves are user picks → always allowed (user override per Fix 1).
  - Bracket in pair's `brackets_allowed` → allowed (with B4 pair-cap check).
  - Otherwise → reject with descriptive reason.
  - **Important Fix 2 behavior**: user picking ONE half of a B5-only combo
    at B1 does NOT let the agent auto-add the other half. Agent stays out
    of combo expansion from a single user-pick anchor.
- `_count_existing_combo_pairs()` — used by Phase C to enforce B4's
  `pair_cap=3` during selection.
- `_select_deck()` — five-pass greedy selection:
  1. Lock in user must-includes (regardless of slot overflow).
  2. Fill non-land slots greedy by score from pool, applying combo policy.
  3. Add any land candidates from the pool (dual lands surfaced by themes).
  4. Top up lands with color-identity-matched basics (round-robin across
     `color_identity`).
  5. Pad with basics if pool under-filled (emits `POOL_UNDER_FILL_PADDED_WITH_BASICS`).
- `compute_agent_build_deck_v1()` now wires Phase B + Phase C end-to-end:
  builds pool → selects deck → assembles 100-card response. The Phase A
  stub is gone; every successful response now reflects real selection.
- Per-card `reason` strings derive from `rationale_components` accumulated
  in Phase B plus the slot tag (`[slot=creature]`, etc.) for auditability.
- Creativity envelope metrics filled in: `user_picks_present`,
  `must_includes_resolved`, `must_includes_dropped`, `staples_avoided_count`.
  `theme_coherence_score` deferred to Phase D (needs `deck_analyze_v1`).

### Test results
- 28 new Phase C tests pass (49 cumulative across Phases A-C):
  - 6 `_classify_card` cases (land-precedence, ramp/draw/removal/wincon
    primitives, default flex, creature default).
  - 3 `_adjust_slot_targets` cases (tribal, combo, empty-archetypes).
  - 3 `_fill_mana_base` cases (mono, multi round-robin, colorless→Wastes).
  - 7 `_combo_violates_bracket` cases covering B1 reject, B3 late allow,
    B3 R-tier reject, B4 pair cap, B5 unrestricted, user-override (both halves),
    user-pick-one-half NOT overriding (Fix 2).
  - 3 `_count_existing_combo_pairs` cases.
  - 6 `_select_deck` end-to-end cases (99-card output, user picks present,
    basic lands fill, colorless→Wastes, every-card-has-reason, singleton).
- Phase A endpoint contract tests rewritten to mock upstream layers
  (the old "99 Wastes stub" assertion no longer applies). 5 tests still
  green: 100-card smoke, creativity envelope summary, INVALID_BRACKET,
  MISSING_COMMANDER, endpoint_call_count + phase_timings.
- Full suite: **904 passed, 17 skipped, 8 deselected** (the 4 pre-existing
  failure families are still the same; no new regressions from Phase A or
  Phase B's commits either).

### Architectural decisions
1. **Spellbook data IS the bracket policy.** The brief's
   `BRACKET_COMBO_POLICY` table (B1/B2 = reject all; B3 = late-only; etc.)
   is mechanically equivalent to looking up `brackets_allowed` in
   `combo_brackets_v1.json`. The agent uses the data file; `BRACKET_COMBO_POLICY`
   is only consulted for B4's pair_cap and as a fallback when the file is
   unloadable. This keeps the agent in sync with whatever Spellbook scrape
   refresh ships.
2. **Combo-completion-with-user-pick is REJECTED, not auto-promoted.**
   Worth restating because it's the load-bearing rule for test case 5
   (Ur-Dragon + Tiamat → agent must NOT add Old Gnawbone + Hellkite Charger).
   The "user pick override" branch in `_combo_violates_bracket` requires
   BOTH halves to be in `user_pick_names_lower`; otherwise the candidate
   is rejected.
3. **deck_complete_engine_v1 is NOT used as a backstop here.** Pass 5's
   "pad with basics" is cheaper and gives us deterministic output. We can
   revisit in Phase F if real-snapshot runs reveal structural gaps the
   pool can't fill (e.g. ramp slot under-served for an obscure commander).
4. **Slot caps use type_line + primitives, not the existing engine's
   `axis_targets` from `candidate_pool_v1`.** The agent-side targets differ
   from the deck-complete-engine's heuristic targets because agent builds
   from a theme-anchored intent, while deck_complete_engine fills toward
   commander-archetype defaults. Different inputs, different sweet spots.
5. **No deck_complete_engine_v1 import inside the layer.** Keeps the agent
   layer free of pipeline-build dependencies; the entire Phase B+C codepath
   touches only `agent_endpoints_v1` and `engine.db` (and the combo data file).

### Next
Phase D — validation + swap-iteration loop. Will call `deck_strength_check_v1`
to verify bracket placement and `deck_analyze_v1` to compute
`themes_classified` + `theme_coherence_score`. If validation fails on the
first pass, swap the offending card(s) for the next-best pool candidate and
revalidate, up to `MAX_SWAP_ITERATIONS=12`. Whole pipeline bounded by
`ENDPOINT_CALL_BUDGET=30`.

---

## Phase D — Validation + swap-iteration loop

**Status:** ✅ Complete (2026-05-20)

### What landed
- `_deck_to_raw_text(commander, deck_body)` — serializes the agent's
  selected deck into the TappedOut-style raw text that `deck_analyze_v1`
  and `deck_strength_check_v1` expect (`Commander\n1 X\nDeck\n1 ...`).
- `_compute_theme_coherence(requested_hints, classified_themes)` — fraction
  of user-requested theme_hints that show up in the classifier's output.
  Case-insensitive substring match handles `TYPAL_VAMPIRES` vs
  `TYPAL_VAMPIRES:Vampire` ID variations.
- `_validate_deck()` — structural + endpoint-based checks:
  - 100 cards total.
  - Singleton (non-basic dupes flagged; basics excepted).
  - Bracket estimate from `deck_analyze_v1.bracket_estimate` compared to
    requested bracket → `BRACKET_MISMATCH` issue when they disagree.
  - `theme_coherence_score` from analyze's `deck_themes_v1` → flagged
    `THEME_COHERENCE_LOW` when below `THEME_COHERENCE_TARGET=0.5`.
  - Strength-check summary from `compute_deck_strength_check_v1` →
    `bracket_signal`, `mean_similarity`, `nearest_neighbors_count`.
  - Budget-aware: if `call_counter["calls"]` already at `ENDPOINT_CALL_BUDGET`,
    short-circuits the analyze call and emits `BUDGET_EXCEEDED_BEFORE_ANALYZE`.
- `_attempt_swap()` — per-issue patches:
  - `SINGLETON_VIOLATION` → drops the duplicate's later occurrence, takes
    the next pool candidate not in the deck (or pads with a basic if pool
    is dry).
  - `THEME_COHERENCE_LOW` → swaps a basic land for the next theme-sourced
    pool candidate. (Replacing basics is safest — won't touch user picks
    or non-basic singletons.)
  - `BRACKET_MISMATCH` → not yet patched; emits a warning. Phase F will
    show whether real-snapshot runs need a power-up/power-down heuristic.
- `_validate_and_iterate()` — outer loop:
  - Max `MAX_SWAP_ITERATIONS = 12` rounds.
  - Total endpoint calls capped at `ENDPOINT_CALL_BUDGET = 30` (counter
    threaded from Phase B onward via the shared `call_counter` dict).
  - Bails early when no actionable swap exists (`UNRESOLVED_<CODE>` warning
    surfaces what couldn't be fixed).
- `compute_agent_build_deck_v1()` now wires Phase D into the pipeline.
  Response summary now includes:
  - `themes_classified` — actual analyzer output (no longer the literal
    `theme_hints` echo from Phase C).
  - `bracket_estimate` — analyzer's bracket walk result.
  - `strength_check` — `{bracket_signal, mean_similarity, nearest_neighbors_count}`.
  - `theme_coherence_score` populated from the last validate pass.
  - `validation_issues` — the final pass's residual issues (empty when
    the deck passes).

### Test results
- 18 new Phase D tests pass (67 cumulative across Phases A–D):
  - 2 `_deck_to_raw_text` cases.
  - 5 `_compute_theme_coherence` cases (no-hints, all-match, partial,
    no-classified, case-insensitive substring).
  - 7 `_validate_deck` cases (passing-deck, wrong-count, singleton flag +
    basic exception, theme-coherence-low flag, bracket-mismatch flag,
    counter increments, budget-exhausted short-circuit).
  - 4 `_validate_and_iterate` cases (clean-deck single-pass,
    singleton-swapped-with-pool-candidate, persistent-failure-bails-without-
    exhausting-cap, ENDPOINT_BUDGET_EXCEEDED halts loop).
- Phase A tests extended to mock `deck_analyze_v1` and
  `deck_strength_check_v1` since `compute_agent_build_deck_v1` now drives
  them; otherwise the FastAPI endpoint test against the fixture DB would
  trigger corpus vectorization (~30s cold).
- Full suite: **922 passed, 17 skipped, 8 deselected.** The same 4
  pre-existing failure families as before — no new regressions.

### Architectural decisions
1. **`_attempt_swap` is intentionally conservative.** It patches issues
   the agent created (duplicates, theme-coherence) and leaves
   `BRACKET_MISMATCH` for Phase F to observe. Building a robust power-up/down
   swap heuristic requires per-candidate "power" scoring that the existing
   strength oracle doesn't yet expose; rather than guess, we warn and let
   Phase F's validation report tell us how often this matters.
2. **Validation calls `deck_analyze_v1` + `deck_strength_check_v1` lazily.**
   Same lazy-import pattern as Phase B's calls into `agent_endpoints_v1` —
   keeps the layer import cheap and lets test code patch at the source
   module without touching the agent module.
3. **`call_counter` is the single source of truth for budget enforcement.**
   Phase B, Phase D, and the outer wrapper all read/write this same dict.
   This means a build that consumes 28 calls during Phase B (rare —
   would require many theme_hints) leaves only 2 calls for validation,
   which `_validate_deck` correctly short-circuits.
4. **Theme coherence target = 0.5.** Half of the requested hints must
   appear in the classifier's top themes. A stricter target (0.8+) would
   trip on commanders whose corpus is small enough that the classifier
   doesn't surface fine-grained themes. Phase F will calibrate this number
   per test case.
5. **Singleton check excludes ALL basics, including Wastes.** The Magic
   rule is "non-basic", not "non-WUBRG-basic"; Wastes plus snow basics
   count too. `_BASIC_LAND_NAMES` enumerates them.

### Next
Phase E — UI surface. Build the "AI Build" tab in
`ui_harness/src/views/WorkspaceView.tsx` that hits `/agent/build_deck_v1`
and renders the deck + per-card reasons + creativity envelope metrics.
chrome-devtools-mcp will verify the UI end-to-end after each test case.

---

## Phase E — UI "AI Build" view

**Status:** ✅ Complete (2026-05-20)

### What landed
- `ui_harness/src/views/AIBuildView.tsx` — self-contained React view that
  posts to `/agent/build_deck_v1` and renders the response. Avoids
  threading through `WorkspaceView`'s 5862-line state machine; instead it
  hands off via the existing `mtg-engine.active-deck` localStorage slot so
  navigating to `#workspace-decks` after Build picks up the agent's deck.
- Form fields:
  - Commander (text input)
  - Snapshot ID (text input — no autocomplete yet; placeholder hints at
    where to find it in the workspace toolbar)
  - Bracket dropdown (B1..B5 with full labels: "B1 — Exhibition", etc.)
  - Theme hints (chip input, Enter to add, × to remove)
  - Must-include cards (chip input)
  - "Build deck" / "Apply to Workspace" buttons
- Response panels:
  - Summary card: badges for card count, bracket placement, estimated
    bracket, endpoint calls, elapsed ms. Color identity chips. Creativity
    envelope (user picks present, staples avoided, theme coherence as %).
    Themes classified chips. Strength check (bracket_signal,
    mean_similarity, nearest_neighbors_count). Phase timings (pool / select
    / validate ms). Collapsible warnings list with code + message.
  - Deck card: 8 slot-grouped sections (Commander, Lands, Ramp, Card Draw,
    Removal, Win Conditions, Creatures, Flex/Other). Each entry shows
    name + source code + reason (truncated, full on hover via `title`).
- Routed via `AppRouter` as a new `ViewId = "ai-build"`. Hash `#ai-build`
  is parseHash-mapped (case-insensitive). Landing page now has 5 entry
  cards instead of 4 — "AI Build (Pillar D)" sits second in the grid.

### Test results
- 1 new vitest case in `AppRouter.test.ts` pins `parseHash("#ai-build") →
  "ai-build"` (and case-insensitive `#AI-BUILD`). All 7 AppRouter tests
  green.
- UI vitest suite full run: **694 pass, 2 fail** — the 2 are in
  `metricPillHeader.test.ts` and pre-date these changes (verified via
  `git stash`; the test file checks WorkspaceView source for a
  v1.6-Stage-3 comment marker that's no longer there).
- `npx tsc --noEmit` produces only pre-existing errors (node:fs imports
  without `@types/node` in test files, plus a few Drawer-children typos
  elsewhere). No new type errors in `AIBuildView.tsx`, `AppRouter.tsx`,
  or `LandingView.tsx`.

### Architectural decisions
1. **Standalone view, not a WorkspaceView tab.** The brief allows
   "wherever it makes UX sense" and the alternative — adding a 5th
   workspace mode in a 5862-line file with intricate deckState reducer
   coupling — would have meant cargo-culting half the workspace state
   machine for a feature that doesn't share its inputs. The localStorage
   hand-off keeps the "Apply to Workspace" affordance intact.
2. **No card-name autocomplete.** Plain text inputs for commander +
   must-include cards. The agent's `MUST_INCLUDE_NOT_FOUND` warning
   already surfaces typos; layering `cards/suggest_v1` autocomplete on
   top would have doubled the UI scope. Easy to add in a follow-up if
   misuse becomes common.
3. **Slot grouping uses the `[slot=...]` token in the per-card reason.**
   Phase C appends this token during `_format_reason`. The UI groups
   on it instead of re-classifying cards client-side — single source of
   truth.
4. **`chrome-devtools-mcp` UI verification skipped.** The MCP server
   isn't part of this agent's tool set. Phase F validation runs through
   the API directly (via Python script in `tools/`) rather than the
   browser, so the UI surface and the validation runner are independent
   in this build.

### Next
Phase F — run the 5-test-case validation sweep against a real snapshot.
Each case captures wall-clock, endpoint-call count, time per phase
(pool/select/validate), and tests the success criteria: 100 cards,
color-legal, singleton, must-includes present, bracket-correct,
themes-correct, reasons substantive. Writes
`pillar_d_validation_report.md` summarizing outcomes + creativity
envelope metrics per case.
