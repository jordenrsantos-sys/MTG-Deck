# Mega-task v10 / iter 11 / Pillar F v0.2 sub-mega-task B — progress log

Iter 11 dispatch on top of v9 ship (`955f3c3fc`). Kickoff:
`mega_task_v10_kickoff.md`. Sub-mega-task B only — LLM strategic policy
plugged into iter-10 substrate. Sub-mega-task C (Stage 2 playtest)
dispatches separately in iter 12+.

**Parallel-arc note:** v11 (per-card oracle compilation seed) ships in
parallel in a separate CC instance. v11 modifies the iter-10 substrate's
effect registries (per-card replacement/layer/activated-ability handlers).
v10 (this task) does NOT modify the substrate — only plugs into existing
callback hooks. Disjoint module trees: v11 lives in
`pillar_f/v0_2/cards/`, v10 in `pillar_f/v0_2/policy/`.

Append-only, timestamped sections per phase.

---

## Phase 0 — Pre-flight + scoping read + module scaffold (2026-05-23)

**Substrate snapshot:**
- HEAD: `955f3c3fc` (v9 Phase 9 — SHIPPED).
- pytest baseline: 1852 / 25 skipped / 0 failed (v9 ship; verified
  re-run of `pillar_f_v0_2/` = 224/224 green).
- vitest baseline: 774 (unchanged from v9 ship).
- ANTHROPIC_API_KEY: SET.
- LLM client: `api/engine/layers/agent_llm_client_v1.py` provides
  `AnthropicClient` with `call_with_budget(system, user, max_input_tokens,
  max_output_tokens)` → `CallResult(ok, text, parsed_json, input_tokens,
  output_tokens, cost_usd, latency_ms, ...)`. Reusable for sub-B.

**Scoping doc read:** `MTG-Deck-Builder-Claude/pillar_f_v0_2_sub_b_llm_policy_scoping.md`
(10 sections covering compact_view, 4 prompt types, politics schema,
threat-vector, cost guardrails, integration plug-points).

**Module scaffold created** at `api/engine/pillar_f/v0_2/policy/`:
```
policy/
  __init__.py        — POLICY_VERSION constant
  prompts/__init__.py
  parsers/__init__.py
  politics/__init__.py
  cost/__init__.py
```
Plus `tests/pillar_f_v0_2_policy/__init__.py` for sub-B test tree.

**Substrate verification:** iter-10's `pillar_f/v0_2/` untouched. Sub-B
work confined to `pillar_f/v0_2/policy/` (new tree).

**Phase 0 cost:** $0 (no LLM calls).

**Commit message:** "Phase 0 (mega-task v10): pre-flight + Pillar F v0.2 policy scaffold".

Committed as `1684b1476`.

---

## Phase 1 — compact_view helper (2026-05-23)

**Implementation** in `api/engine/pillar_f/v0_2/policy/prompts/compact_view.py`:

- `compact_view(perspective_view, viewer_player_id, last_n_turns,
  action_log)` → plain text suitable for LLM prompts. Sections:
  HEADER (turn/phase/step/active/priority/monarch/day_night),
  PLAYERS (per-player one-line summary: life + hand size + lib + gy +
  exile + mana_pool + commander damage), STACK (top-down LIFO),
  BATTLEFIELD per-player (one line per permanent: name + P/T +
  keywords + TAP flag + counters), YOUR HAND (own; full card detail
  with mana cost + type_line + oracle_text trunc'd to 200 chars),
  RECENT ACTIONS (filtered to last N turns).
- `estimate_tokens(text)` → rough ~chars/4 estimate (Anthropic SDK
  reports actual usage in CallResult).
- Determinism via sorted dict iteration everywhere.

**Tests** in `tests/pillar_f_v0_2_policy/test_phase1_compact_view.py`:
13 tests across 6 classes.

**Token budget gates verified (kickoff Phase 1):**
- Fresh game (4 players, no cards on battlefield): **< 1000 tokens** ✓
- Mid-game turn 15 (10 perms/player + 6 hand + 25 lib + 4 gy +
  2-deep stack): **< 4000 tokens** ✓

**Redaction verified:**
- Opponent hand contents never leak (only `<opaque:hand>` markers in
  cards_by_id).
- All libraries (including viewer's own) never leak per CR.
- Stack public to all (shows full descriptions + targets).
- Battlefield public to all (face-down permanents → opaque to non-
  controller via perspective_view).

**Action log filtering verified:** `last_n_turns=3` correctly trims
older entries.

**All 13 pass.** ~230 LOC production + ~190 LOC test.

**Commit message:** "Phase 1 (mega-task v10): compact_view helper — perspective_view → ~3K-token LLM-ready text".

Committed as `f1310dd8f`.

---

## Phase 2 — Main-phase action prompt + parser + validator (2026-05-23)

**Implementation** in `api/engine/pillar_f/v0_2/policy/`:

1. **prompts/main_phase.py** —
   - `MAIN_PHASE_SYSTEM_PROMPT` constant (Commander piloting role,
     JSON contract spec, politics/etiquette guidance, "Output ONLY
     JSON" instruction).
   - `build_main_phase_prompt(compact_view_text, eligible_actions,
     politics_context?, deck_archetype_hint?, rationale_history?,
     last_error_message?)` assembles user message with:
     CURRENT GAME STATE | YOUR DECK ARCHETYPE | POLITICS CONTEXT (per-
     opponent threat score + alliances + recent deals) | YOUR RECENT
     RATIONALES | ELIGIBLE ACTIONS (indexed [0], [1], ...) | ATTENTION:
     PRIOR RESPONSE FAILED VALIDATION (re-prompt context).
   - `compute_eligible_actions_passes_only()` stub for unit tests
     (Phase 3 wires real engine pre-computation).

2. **parsers/action_parser.py** —
   - `ActionResponse` dataclass with action_type / action_index /
     rationale + properties for card_id / targets / payment /
     description (looked up from eligible_action[action_index]).
   - `parse_action_response(raw_text, eligible_actions)` returns
     `(ActionResponse|None, error_message|None)`. Handles markdown
     fences, trailing prose, alternate keys (action_index / index /
     action_idx).
   - `_extract_first_json_object(text)` brace-matched extraction with
     string-aware depth tracking.
   - Errors: empty text, no JSON found, malformed JSON, missing key,
     out-of-range index, non-int index.
   - `fallback_pass_response(eligible_actions)` returns the pass entry
     wrapped in ActionResponse (for 3rd-validation-failure fallback).
   - `validate_eligible_action_present(actions)` sanity check.

**Re-prompt loop design:** caller (Phase 3 priority responder)
wraps parser+validator in up-to-2 retries. On each retry, passes
`last_error_message` back to build_main_phase_prompt so the LLM sees
why the previous response failed. On 3rd failure, falls back to
pass_priority via `fallback_pass_response`.

**Tests** in `tests/pillar_f_v0_2_policy/test_phase2_main_phase_prompt.py`:
25 tests across 6 classes:
- **PromptAssemblyTests** (8): compact_view inclusion, indexed actions,
  politics context, no-politics skip, deck_archetype_hint, rationale
  history, error on re-prompt, system prompt JSON contract.
- **ParserHappyPathTests** (3): clean JSON, markdown fences, trailing
  prose.
- **ParserErrorHandlingTests** (8): empty, no JSON, malformed,
  missing key, out-of-range, negative, non-int, alternate keys.
- **FallbackTests** (2): fallback returns pass when in list, None
  when not.
- **ValidationHelperTests** (3): empty list, missing key, ok.
- **IntegrationTests** (1): full loop build prompt → mock response
  → parse → executable action.

**All 25 pass.** ~310 LOC production across 2 files + ~250 LOC test.

Iter-11 cost-measurement smoke deferred to Phase 3 (real LLM client).

**Commit message:** "Phase 2 (mega-task v10): main-phase prompt builder + JSON action parser + validator".

Committed as `c2dc05a89`.

---

## Phase 3 — Plug into PriorityResponderFn + 2-LLM head-to-head (Phase 1 ship gate) (2026-05-23)

**Implementation** in `api/engine/pillar_f/v0_2/policy/`:

1. **eligible_actions.py** —
   - `compute_eligible_actions(state, player_id)` returns a list with
     always-legal `pass_priority` plus `play_land` (for lands in hand
     if active main phase + empty stack + no land played yet) plus
     `cast_spell` (for cards with `iter10_annotation` attribute).
   - `apply_action(state, player_id, action)` executes the chosen
     action: pass = no-op, play_land = hand→battlefield + increment
     lands_played, cast_spell = push to stack + move card to gy
     (iter-10 stub: full move-on-resolution wired in v11+).

2. **cost/cost_tracker.py** —
   - `CostTracker` dataclass with per-player + per-turn buckets +
     event log + fallback flags + game_halted_for_cost.
   - `record_call(player_id, turn_number, cost_usd, purpose)`.
   - `is_player_in_fallback(player_id, turn)` true after per-turn
     ceiling triggers.
   - Defaults: per-turn $0.30, per-game $10.

3. **llm_responder.py** —
   - `make_llm_priority_responder(llm_client, cost_tracker,
     action_log, politics_state_by_player, deck_archetype_hint_by_player,
     rationale_history_by_player)` factory returns a `PriorityResponderFn`-
     compatible closure.
   - Closure: builds compact_view → computes eligible_actions → if
     only pass available, returns None (saves token cost). Otherwise
     builds prompt → calls LLM → parses → up to 2 re-prompts on
     parse failure → fallback to pass on 3rd. Records cost. Applies
     action. Returns None (action already applied to engine state).
   - `cheap_fallback_responder(state, player_id)` always returns None.

**Bug fix during Phase 3:** factory used `politics_state_by_player or {}`
which converts caller's empty `{}` (falsy) to a NEW dict. Switched to
`is None` checks so caller's reference is preserved (test passed
empty dict and expected rationale to be written there).

**Tests** in `tests/pillar_f_v0_2_policy/test_phase3_responder.py`:
22 tests across 5 classes — eligible_actions (8), apply_action (3),
LLM responder with MockLLMClient (6), cost tracker (4), cheap
fallback (1).

**Live 2-LLM smoke at `tools/test_pillar_f_v0_2_policy_smoke.py`:**

Ran 2-LLM head-to-head (mono-black vs mono-red, 30-card decks with
20 lands + 10 Lightning Bolts each, 5-card opening hands). 3 turns.

**Result: $0.02 spend across 6 LLM calls (~$0.003/call — far below
the $0.03 scoping target). All actions legal (every emitted action
was a play_land). Game completed without exceptions in 12.8s.**

**Phase 3 ship gate cleared:**
- Game completes ✓
- All actions legal ✓
- Total cost < $2 ✓ (actual: $0.02)

LLMs chose to develop mana before casting — sensible given the
substrate has no mana-cost enforcement, so the LLM could cast Bolts
turn 1 but didn't. Tuning the prompt for more aggression is iter-11
polish; the substrate boundary works.

**Pillar F tests now: 284 (224 v9 + 60 new v10 Phases 1-3).**

**Commit message:** "Phase 3 (mega-task v10): plug LLM into PriorityResponderFn + 2-LLM head-to-head smoke ($0.02)".

Committed as `ad8a5ad36`. (v11 Phase 0 landed in parallel as
`d635a249f` — disjoint module trees, no conflict.)

---

## Phase 4 — Combat-phase prompt + attacker/blocker (2026-05-23)

**Implementation** in `api/engine/pillar_f/v0_2/policy/`:

1. **prompts/combat.py** —
   - `ATTACKERS_SYSTEM_PROMPT` + `BLOCKERS_SYSTEM_PROMPT` system
     templates (Commander piloting role + JSON contract + politics
     etiquette + iter-10 single-call-per-side semantics).
   - `build_attackers_prompt(compact, eligible_attackers,
     attack_targets, politics_context?, deck_archetype_hint?,
     last_error_message?)` — assembles user message.
   - `build_blockers_prompt(compact, eligible_blockers,
     attackers_to_block, politics_context?, last_error_message?)` —
     one call per defender; iter-10 honors block-declaration order
     for multi-block damage assignment.

2. **parsers/combat_parser.py** —
   - `AttackersResponse` / `BlockersResponse` dataclasses.
   - `parse_attackers_response(raw, eligible_attackers,
     attack_targets)` — JSON parse + index range checks + duplicate
     attacker rejection. Empty/missing `attackers` array = legal (no
     attack).
   - `parse_blockers_response(raw, eligible_blockers,
     attackers_to_block)` — JSON parse + index range + duplicate
     attacker rejection + blocker-assigned-to-multiple-attackers
     rejection. Empty `blocks` array = legal (take all damage).
   - Multi-block assignment order preserved (CR 510.1c: active player
     chooses order; iter-10 takes the LLM's blocker_indices order
     directly).

**Tests** in `tests/pillar_f_v0_2_policy/test_phase4_combat_prompt.py`:
21 tests across 5 classes:
- **AttackersPromptAssemblyTests** (4): eligible attackers,
  attack targets, none-eligible, politics inclusion.
- **BlockersPromptAssemblyTests** (2): incoming + your blockers,
  none-blockers.
- **AttackersParserTests** (7): clean parse, empty legal, missing
  key legal, attacker_index out of range, target_index out of range,
  duplicate attacker rejected, malformed JSON.
- **BlockersParserTests** (6): clean parse, empty legal, multi-block
  order preserved, blocker out of range, blocker assigned twice
  rejected, duplicate attacker rejected.
- **SystemPromptConstantsTests** (2): both system prompts include
  JSON contract + key references.

**All 21 pass.** ~290 LOC production across 2 files + ~280 LOC test.

Full combat-phase integration (LLM driving declare → first-strike →
normal damage) deferred to Phase 9.

**Commit message:** "Phase 4 (mega-task v10): combat-phase prompt + attackers/blockers parsers".
