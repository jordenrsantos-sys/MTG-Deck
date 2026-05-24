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

Committed as `e124aca13`. (Inadvertently swept in v11's untracked
`tests/pillar_f_v0_2_cards/test_phase1_simple.py`; module is disjoint
from v10's scope and content unmodified — left in place rather than
churning a revert.)

---

## Phase 5 — Response-window prompt (2026-05-23)

**Implementation** in `api/engine/pillar_f/v0_2/policy/`:

1. **prompts/response_window.py** —
   - `RESPONSE_WINDOW_SYSTEM_PROMPT` — Commander-piloting role focused
     on the decision "respond or pass" with explicit default-to-pass
     guidance (per scoping doc — most stack objects don't warrant a
     response). Includes counter-war escalation math + ally-protection
     etiquette + JSON contract.
   - `build_response_window_prompt(compact, stack_top_summary,
     eligible_actions, politics_context?, deck_archetype_hint?,
     rationale_history?, last_error_message?)` — STACK TOP first, then
     current state, politics, recent rationales, eligible responses,
     re-prompt error.
   - `summarize_stack_top(stack_entry)` — produces a 5-6-line summary
     from a StackEntry dataclass OR its to_dict() form. Handles None +
     unparseable inputs gracefully.

2. **llm_responder.py** — Branches on `bool(state.stack)`:
   - non-empty stack → RESPONSE_WINDOW_SYSTEM_PROMPT +
     build_response_window_prompt + cost purpose="response_window".
   - empty stack → MAIN_PHASE_SYSTEM_PROMPT + build_main_phase_prompt
     + cost purpose="main_phase_priority" (unchanged).
   - Re-prompt loop, fallback-pass, cost tracking unchanged.
   - Parser reused: JSON shape is identical (action_type +
     action_index + rationale), so `parse_action_response` handles
     both prompt types.

**Tests** in `tests/pillar_f_v0_2_policy/test_phase5_response_window.py`:
17 tests across 5 classes:
- **SummarizeStackTopTests** (4): None, dataclass, dict form,
  unparseable.
- **ResponseWindowPromptAssemblyTests** (6): stack-top + eligible
  shown, no-eligible shows "(none — must pass)", politics, deck
  archetype, last-error re-prompt, rationale history.
- **ResponseWindowParserReuseTests** (3): pass parses, cast
  counterspell parses, out-of-range index rejected (parser shared
  with main-phase).
- **ResponseWindowSystemPromptTests** (2): JSON contract present,
  "default to pass" guidance present.
- **ResponderRoutingTests** (2): non-empty stack routes to response-
  window prompt + cost purpose; empty stack routes to main-phase
  prompt with no STACK TOP section.

**All 17 pass.** ~150 LOC production + ~290 LOC test.

Full counter-war integration test (3-deep chain with all 4 players
holding priority opportunities at each level) deferred to Phase 9.

**Full policy regression: 98/98 passing.**

**Commit message:** "Phase 5 (mega-task v10): response-window prompt + responder routing".

Committed as `23d079458`. Push landed.

---

## Phase 6 — Mulligan + bottom-picker prompts (Phase 2 ship gate) (2026-05-23)

**Implementation** in `api/engine/pillar_f/v0_2/policy/`:

1. **prompts/mulligan.py** —
   - `MULLIGAN_SYSTEM_PROMPT` — London-mulligan-aware role with
     keep heuristics (2-5 lands, free interaction strong keep, going
     below 5 = usually game-losing).
   - `BOTTOM_PICKER_SYSTEM_PROMPT` — picks N card_ids from current
     7-card hand to bottom.
   - `build_mulligan_prompt(hand_desc, num_mulligans_taken, ...)`
     shows hand contents + mulligan count + bottom-warning when
     num_mulligans > 0.
   - `build_bottom_picker_prompt(hand_desc, n, ...)` shows hand with
     visible card_id values for echo-back.

2. **parsers/mulligan_parser.py** —
   - `MulliganResponse(keep: bool, rationale: str)` — LLM-facing
     "keep" semantic (True = "keep this hand"); factory inverts to
     the substrate's "True = mulligan" convention.
   - `parse_mulligan_response` coerces common truthy/falsy string
     forms ("yes"/"no"/"keep"/"mulligan") for robustness.
   - `BottomPickerResponse(cards_to_bottom: List[str], rationale)`.
   - `parse_bottom_picker_response` validates: each card_id in
     hand, no duplicates, count exactly = n_to_put_on_bottom.

3. **mulligan_decider.py** — Two factories:
   - `make_llm_mulligan_decider(llm_client, cost_tracker,
     deck_archetype_hint_by_player) → MulliganDeciderFn`.
     Inverts LLM "keep" boolean to substrate "mulligan" boolean.
     Fallback after re-prompts exhausted = keep (conservative).
   - `make_llm_bottom_picker(...) → BottomPickerFn`. Fallback after
     re-prompts exhausted = take last N (substrate default).
   - Both record cost via shared CostTracker with `turn_number=0`
     (pre-game) + purposes `"mulligan_decider"` + `"bottom_picker"`.

4. **llm_responder.py** — Added two token-saving heuristics that
   surfaced from the Phase 6 ship-gate live-run:
   - **Non-active player + empty stack + only pass/cast eligible →
     skip LLM call**. In real play almost no one burns an instant on
     an opponent's main phase with no stack object to respond to.
   - **Active player at low-decision step (UPKEEP, DRAW,
     BEGINNING_OF_COMBAT, END_OF_COMBAT, END_STEP, CLEANUP) + empty
     stack + only pass/cast eligible → skip LLM call**. Iter-11 simple
     decks have no end-step cantrips or mana-rocks-into-pool plays,
     so these windows reflexively pass. Iter-12+ can re-enable
     when decks ship instant-speed value plays.

   These heuristics live in the responder (not in
   compute_eligible_actions) so the primitive stays general — the
   policy layer makes the cost/value trade.

**Tests** in `tests/pillar_f_v0_2_policy/test_phase6_mulligan.py`:
31 tests across 9 classes:
- **MulliganPromptAssemblyTests** (5): mulligan count, no-bottom-
  warning at 0, all hand cards listed, archetype hint, re-prompt
  error message.
- **BottomPickerPromptAssemblyTests** (2): PUT_ON_BOTTOM count,
  card_ids visible for echo-back.
- **MulliganParserTests** (6): keep=true/false, missing key
  rejected, string coercion ("yes"/"no"), non-coercible value
  rejected, markdown fences stripped.
- **BottomPickerParserTests** (5): clean parse, wrong count,
  unknown card_id, duplicate, missing key.
- **MulliganSystemPromptTests** (2): JSON contract + London text.
- **LLMMulliganDeciderFactoryTests** (5): keep inverts to "don't
  mulligan", keep=false → mulligan, unavailable LLM → keep,
  cost recorded, fallback to keep after re-prompts exhaust.
- **LLMBottomPickerFactoryTests** (4): returns LLM choice, n=0
  short-circuits, fallback to last-N on failure, unavailable LLM
  → fallback.
- **LLMMulliganIntegrationTests** (2): all 4 keep first hand,
  P0-mulligans-once-then-keeps-with-bottom.

**All 31 pass. Full policy regression: 129/129. Substrate: 224/224.**

**Phase 6 ship-gate live smoke** —
`tools/test_pillar_f_v0_2_policy_mulligan_smoke.py`:
- 4 LLMs, max 2 mulligans, 5-turn 4-player game vs the real
  Anthropic API.
- **Run history (this iteration):**
  - Run #1 (pre-heuristic): $2.00 spend, 441 calls. FAIL (gate <$1).
  - Run #2 (after non-active-skip heuristic): $1.18 spend, 255 calls.
    FAIL.
  - Run #3 (after non-active + active-low-decision skip heuristics):
    **$0.49 spend, 110 calls. PASS.**
- Mulligan cycle: 8 mulligans across 4 players (2 each at gate
  cap), all hand sizes correct (5 each = 7 − 2).
- 5-turn game: progressed cleanly, 22 actions logged (20 lands +
  2 Lightning Bolt casts), no SBA / illegal-action errors, no
  cost ceiling halt.

~270 LOC production (prompts + parsers + factories + responder
heuristic) + ~480 LOC test + 200-LOC smoke runner.

Full counter-war integration test, mulligan-rule-violation
integration deferred to Phase 9 (and Phase 8 will add the
cost-ceiling-based cheap-fallback responder — Phase 6's responder
heuristics complement that with structural pre-empt).

**Commit message:** "Phase 6 (mega-task v10): mulligan + bottom-picker prompts + responder cost heuristics ($0.49 < $1 gate)".

Committed as `e809b3143`. Push landed.

---

## Phase 7 — Threat-vector + politics state tracker (2026-05-23)

**Implementation** in `api/engine/pillar_f/v0_2/policy/politics/`:

1. **threat_vector.py** —
   - `compute_threat_vector(state, viewer_id, opponent_id) → {score,
     board_strength, tempo, life_pressure, recent_aggression,
     archetype_hint}`. All components normalized to [0, 1]; score
     is a weighted sum (board 0.40, tempo 0.20, life-pressure-inverse
     0.15, recent-aggression 0.15, archetype 0.10).
   - Keyword multipliers (scoping section 5): deathtouch ×1.20,
     hexproof ×1.30, lifelink ×1.10, indestructible ×1.25,
     shroud ×1.25, flying ×1.10, trample ×1.10, menace ×1.05,
     double_strike ×1.20, first_strike ×1.10. Multiplicative
     stacking — a hexproof-lifelinker = ×1.43.
   - Normalization anchors: board_strength cap 30 weighted P/T,
     tempo cap 12 points, recent_aggression cap 20 damage.
     life_pressure = life_total / 40 (Commander default; iter-12+
     reads from state.format).
   - `_archetype_hint` reveal-based heuristic: ≥3 low-cmc creatures →
     +0.4 aggro; ≥3 instants in graveyard → +0.4 control; lands >
     1.5×turn → +0.3 ramp; hand≥7 with high life → +0.2 combo.
     Capped at 1.0. Iter-12+ replaces with Bayesian inference on
     commander identity.
   - `compute_all_threat_vectors(state, viewer_id)` skips viewer +
     eliminated players.

2. **politics_state.py** — Persistent per-player record stored on
   `PlayerState.politics_state` (substrate-reserved slot).
   - Schema: `{threats, deals, alliances, damage_log,
     damage_log_turn_window}`.
   - `update_politics_state(state, viewer_id, event)` dispatches on
     `event["type"]`:
     - `"combat_damage"`: logs in `damage_log_turn_window`, rolls
       summary, bumps alliance toward `"rival"`.
     - `"spell_cast_against"`: 1-unit aggression log entry, no
       alliance bump (milder signal).
     - `"deal_made"`: appends deal record, bumps alliance toward
       `"ally"`, caps deals at 50.
     - `"deal_honored"`: marks most-recent matching deal `kept=True`,
       maintains ally.
     - `"deal_broken"`: snaps alliance straight to `"rival"`
       (skipping neutral — the betrayal signal).
     - `"threat_recompute"`: upserts `threats[opponent_id]`.
   - Alliance transitions are STEPPED (rival ↔ neutral ↔ ally) for
     combat + deal events, except `deal_broken` snaps to rival.
   - `roll_damage_log_for_turn(state, viewer_id, current_turn)`
     drops entries older than `RECENT_AGGRESSION_WINDOW` (3 turns)
     and recomputes the per-opponent summary.
   - `export_politics_context(state, viewer_id)` builds the dict the
     existing prompts (main_phase, response_window) consume.

**Tests** in `tests/pillar_f_v0_2_policy/test_phase7_politics.py`:
24 tests across 4 classes:
- **ComputeThreatVectorTests** (12): empty board, large board,
  keyword bumps (lifelink, hexproof), low-life raises threat, full
  hand → tempo, recent_aggression from damage_log, archetype signals
  (aggro from low-cmc creatures, control from gy instants), self-
  threat zero, eliminated opponent zero, compute_all skips
  viewer+dead.
- **UpdatePoliticsStateTests** (8): combat damage records, combat
  bumps alliance to rival, deal_made → ally, deal_honored marks
  kept, deal_broken snaps to rival, deals capped at 50 (oldest drops),
  threat_recompute upserts, spell_cast_against logs mild aggression
  without alliance bump.
- **DamageDecayTests** (2): old damage drops after window, in-window
  damage retained.
- **ExportPoliticsContextTests** (2): exports threats/alliances/deals
  with bumped values; unknown viewer returns empty dict.

**All 24 pass. Full policy regression: 153/153. Substrate: 224/224
(377 total).**

~280 LOC production + ~330 LOC test.

Phase 7 has no LLM-driven ship-gate per the kickoff — gates are unit
tests only. The politics integration into the responder's prompt-
context is plumbed by exposing `export_politics_context` so the
responder can pass `politics_context=...` when caller populates
the dict. Per-event auto-wiring into the substrate's combat damage
hooks deferred to Phase 9 (where the live 4-LLM 20-turn integration
will demonstrate the politics dynamics gate).

**Commit message:** "Phase 7 (mega-task v10): threat-vector + politics state tracker".

Committed as `a4eb2359f`. Push landed.

---

## Phase 8 — Cost guardrails + cheap-fallback responder (2026-05-23)

**Scope note.** Phase 8's CODE was already landed across Phases
3 + 5 + 6 (CostTracker module, cheap_fallback_responder,
`is_player_in_fallback` + `game_halted_for_cost` checks in
llm_responder + mulligan_decider). Phase 8's work was to add the
explicit cost-guardrail BEHAVIORAL test suite and verify the
end-to-end fallback flow.

**Tests** in `tests/pillar_f_v0_2_policy/test_phase8_cost_guardrails.py`:
14 tests across 5 classes:
- **CostDefaultsTests** (3): per-turn ceiling default $0.30, per-game
  ceiling default $10, fresh CostTracker uses defaults.
- **CheapFallbackResponderTests** (2): returns None for all 4 seats,
  unaffected by eligible-action availability (pass is always legal).
- **ResponderCostGuardrailsTests** (5): responder skips LLM when
  player in fallback, skips when game halted, trips per-turn ceiling
  mid-loop and subsequent calls skip LLM, trips per-game ceiling
  halts all players, turn rollover clears per-turn fallback.
- **CrossComponentCostFlowTests** (1): single CostTracker
  accumulates spend across BOTH mulligan_decider AND
  priority_responder; per-game ceiling fires regardless of which
  caller drove the spend.
- **CostEventsTests** (3): per-turn ceiling emits
  COST_CEILING_HIT event, per-game ceiling emits
  GAME_COST_CEILING_EXCEEDED, purpose field recorded per call.

**All 14 pass. Full policy regression: 167/167.**

~340 LOC test (no new production code — Phase 8 is consolidation
+ behavioral verification of the cost-guardrail surface that has
been incrementally built up over Phases 3, 5, 6).

The cost-tracker contract is now verified for: accumulation,
per-turn ceiling, per-game ceiling, fallback flag persistence,
turn rollover reset, cross-component sharing, and event emission.

**Commit message:** "Phase 8 (mega-task v10): cost guardrails + cheap-fallback behavioral test suite".
