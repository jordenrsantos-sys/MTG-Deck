# Mega-task v10 / Pillar F v0.2 sub-mega-task B — LLM strategic policy kickoff

**Dispatch date:** 2026-05-23 (queued for user dispatch)
**Parent commit:** HEAD of `main` post-iter-10 (mega-task v9 ship `955f3c3fc`)
**Budget ceiling:** $120 API spend / ~6 weeks CC time
**Scope:** **Sub-mega-task B only** — LLM strategic policy layer plugged into the iter-10 rules engine substrate. Sub-mega-task C (Stage 2 graduated playtest, ~4 weeks) is OUT of iter 11 scope.

**Parallel-arc coordination note.** Mega-task v11 (per-card oracle compilation seed) ships in parallel to this one. v11 modifies the iter-10 substrate's effect registries (per-card replacement/layer/activated-ability handlers). v10 (this task) does NOT modify the substrate — it only plugs into the existing callback hooks (`PriorityResponderFn`, `MulliganDeciderFn`, `BottomPickerFn`, `register_replacement_fn`, `register_layer_effect`). The two arcs are in disjoint module trees so merge conflicts should be rare. Pull before push; if a merge conflict surfaces in the substrate's callback type signatures, halt and surface — the substrate is iter-10's source of truth and changes require user sign-off.

This is empirical work — LLM prompt-template tuning, response parsing, politics state design, threat-vector calibration. The 6-week estimate is realistic (unlike iter 10's substrate work which was code-authoring-bound and shipped in ~47 minutes; sub-B's iteration loop is LLM-debugging-bound). Expect multi-day debugging in Phase 7 (threat-vector + politics) and Phase 9 (integration test).

---

## Phase 0 — Pre-flight + memory sync + scoping read

Read in full before writing any production code:

- `MTG Root/mtg-engine/MTG-Deck-Builder-Claude/pillar_f_v0_2_sub_b_llm_policy_scoping.md` — the 10-section sub-B scoping document. Source of truth for prompts, politics schema, threat-vector feature, integration points, sub-phase breakdown.
- `MTG Root/mtg-engine/MTG-Deck-Builder-Claude/pillar_f_v0_2_game_engine_scoping.md` — parent v0.2 scoping doc (section c: LLM strategic policy with politics + threat assessment).
- `MTG Root/mtg-engine/MTG-Deck-Builder-Claude/mega_task_v9_final_report.md` — v9 ship state, sub-B hand-off section.
- `spaces/.../memory/project_mega_task_v9_shipped_2026-05-23.md` — iter-10 outcomes, what shipped, what's hooked.
- `repo/api/engine/pillar_f/v0_2/` — iter-10 substrate. Read EVERY file: `state/`, `stack/`, `turn/`, `replacement/`, `layers/`, `combat/`. Sub-B plugs into the callback points exposed in `stack/priority.py`, `turn/mulligan.py`, and the effect registration helpers.
- `repo/api/engine/data/agent/` for the in-repo workspace conventions (kickoffs, reports, agent-side configs).
- Anthropic SDK docs for Sonnet 4.6 (model id `claude-sonnet-4-6`) — pricing, max tokens, structured-output handling. Sub-B uses Sonnet for all LLM calls.

Run `pytest -x` and `cd ui_harness && npm test`. Confirm baseline green (pytest 1852+, vitest 774+). If anything red, **HALT** and surface.

Module layout: create `repo/api/engine/pillar_f/v0_2/policy/` with subdirs `prompts/`, `parsers/`, `politics/`, `cost/`. Add `__init__.py` files. Commit as Phase 0.

---

## Phase 1 — `compact_view` helper (~2-3 days)

Iter-10's `state.perspective_view(viewer_player_id)` returns the full state dict with hidden zones redacted. That's 30-50KB on a mid-game state — too expensive to feed to the LLM directly.

Build `compact_view(perspective_view) → str` per scoping doc section 3:

- Compact battlefield summary per player: creatures with name + P/T + relevant keywords + tapped flag; lands with tapped/untapped + colors produced; permanents with relevant abilities (Aetherflux Reservoir, Sanctum Seeker, Mox Diamond — abilities matter for LLM decision).
- Own hand contents (full card detail).
- Stack (full contents — public info; show top-down resolution order).
- Life totals + mana pools + commander damage matrix (4×4 grid).
- Last 3 turns action log compressed (one line per action: `T5 P2 cast Cyclonic_Rift target=board`).

**Target:** ~3000 tokens for a typical mid-game state. Add a token-count helper that uses tiktoken or equivalent.

**Gates.** Unit tests: compact_view of a fresh game state is < 1000 tokens; compact_view of a turn-15 mid-game state is < 4000 tokens; compact_view drops opponent hand contents (counts only); compact_view preserves all stack contents; the output is deterministic given the same perspective_view input.

---

## Phase 2 — Main-phase action prompt + parser + validator (~1 week)

Per scoping doc section 2a. Implement `build_main_phase_prompt(compact, eligible_actions, politics_context, deck_archetype_hint, last_3_turns) → str` + `parse_action_response(json_str) → ActionResponse` + `validate_action(action, state) → tuple[bool, str]`.

**Prompt template.** Single-shot prompt that asks the LLM to pick one action from a pre-computed `eligible_actions` list. The engine pre-computes legal moves (`compute_eligible_actions(state, player_id)`) so the LLM is choosing, not generating, the action space. This keeps illegal-action rate near zero.

**Response contract** (per scoping doc):
```json
{
  "action_type": "cast_spell" | "activate_ability" | "play_land" | "pass_priority",
  "card_id": "..." | null,
  "ability_idx": 0 | null,
  "targets": [...],
  "payment": {...},
  "rationale": "short reason"
}
```

**Validator.** Re-check the chosen action against `compute_eligible_actions(state, player_id)`. If invalid: re-prompt up to 2x with the error message embedded; on 3rd failure, fall back to `pass_priority` and log. Per scoping doc section 10 risk mitigation.

**Token budget per call.** ~3000-5000 input + ~500 output. Target $0.03/call. Measure actual via Anthropic SDK usage object; log per-call cost.

**Gates.** Unit tests with mocked LLM client: prompt assembles correctly given a fixture game state + politics context; parser handles malformed JSON gracefully; validator catches illegal targets, illegal mana payment, illegal action_type for current phase. Integration test: build prompt → mock LLM returns legal action → validator passes → action applies. Cost-measurement smoke: 10 mocked calls, ensure tracking sums correctly.

---

## Phase 3 — Plug into `PriorityResponderFn` + 2-LLM head-to-head test (~3-5 days)

Per scoping doc section 7a. Implement:

```python
def llm_priority_responder(state, player_id):
    compact = compact_view(state.perspective_view(player_id))
    eligible = compute_eligible_actions(state, player_id)
    if not eligible or all_eligible_are_pass(eligible):
        return None
    prompt = build_main_phase_prompt(compact, eligible, ...)
    response = llm_client.call_with_budget(prompt, max_tokens=500)
    return parse_and_validate_action_response(response, state, player_id)
```

Wire into `stack.priority_round` via the `responder` argument that iter-10 already exposes.

**Head-to-head test.** Two LLM players, hardcoded simple 60-card decks (mono-red goblin aggro + mono-white soldiers — pick decks the rules engine already supports without needing the per-card oracle compilation seed v11 is building). Run a 10-turn game; assert: every action taken is legal, no exceptions raised, total cost < $2 for the game.

**This is the sub-B Phase 1 ship gate.** Pass means the substrate ↔ policy boundary works end-to-end at the most common code path. Fail means re-iterate Phases 1-2 before moving on.

---

## Phase 4 — Combat-phase prompt + attacker/blocker (~1 week)

Per scoping doc section 2b. Single LLM call per combat phase decides attackers; one call per defending player decides blockers.

**Attackers prompt.** Input includes `eligible_attackers` (card_ids of creatures that can attack), `attack_targets` (per-attacker possible targets — player_ids + planeswalker card_ids), `attack_costs` (Propaganda-style costs). Output JSON contract per scoping doc.

**Blockers prompt.** One per defending player. Input includes `eligible_blockers` (per-defender) + per-attacker damage-assignment context. Output: which attackers each blocker is assigned to + the damage assignment order if multi-block.

**Validator.** Reject illegal attackers (tapped, summoning-sick without haste, "can't attack" affected), reject blockers with wrong colors against protection, reject unpaid attack costs. Re-prompt up to 2x then fall back to pass-combat / no-blocks.

**Gates.** Unit tests: attackers prompt assembles given mid-game state with 5 eligible attackers; parser handles attacker JSON; validator catches a tapped creature in attacker list; double-block scenario assigns damage order correctly. Integration test: full combat phase from declare_attackers → first-strike-damage → normal-damage with LLM driving both sides.

---

## Phase 5 — Response-window prompt (~3-4 days)

Per scoping doc section 2c. Fires when active player's spell is on stack and non-active player has priority. Shorter prompt: `stack_top` (top StackEntry — spell + controller + targets + cost) + viewer's available responses (counterspells, redirects, removal of the source).

**Token budget per call.** ~1500 input + ~300 output. Target $0.01/call.

**Counter-war handling.** This prompt may fire many times in a single counter war (Counterspell → counter-counter → counter-counter-counter). Each call is independent. The cost guardrail in Phase 8 will cap the war if it gets out of hand.

**Output:** same shape as main-phase action prompt — pass_priority or cast/activate response.

**Gates.** Unit tests: response prompt assembles with stack top context; parser handles "pass" + "cast Counterspell targeting top of stack"; counter-war integration test (3-deep counter chain resolves correctly with all 4 players having priority opportunities at each level).

---

## Phase 6 — Mulligan + bottom-picker prompts (~3-5 days)

Per scoping doc section 2d. Two callbacks: `MulliganDeciderFn` (per scoping doc section 7b) decides keep-vs-mulligan; `BottomPickerFn` (section 7c) picks N cards to bottom after a London-mulligan keep.

**Mulligan prompt input:** opening 7-card hand (own; opaque to others), deck_archetype_hint, current mulligan count.

**Mulligan output:** `{"keep": bool, "rationale": str}`.

**Bottom-picker prompt input:** current 7-card hand + N (number of cards to bottom = num_mulligans_taken).

**Bottom-picker output:** `{"cards_to_bottom": [card_id, ...], "rationale": str}`.

**Token budget per call.** ~1000 input + ~200 output. ~$0.005/call. At most 7 mulligans × 2 calls each = 14 calls per player game-start.

**Gates.** Unit tests: mulligan prompt + bottom-picker prompt assemble correctly; parsers handle expected outputs; integration test: 4-LLM full mulligan cycle through London-mulligan rules ends with 4 starting hands of correct sizes.

**This is the sub-B Phase 2 ship gate.** Combined with Phase 3, the substrate now has LLM-driven decision-making for every prompt point. Run a full 4-LLM 5-turn game; assert all actions legal, game progresses through phases correctly, total cost < $1.

---

## Phase 7 — Threat-vector feature + politics state tracker (~1 week)

Per scoping doc sections 4 + 5.

**`compute_threat_vector(state, viewer_id, opponent_id) → dict`:**
- `board_strength`: weighted sum of opponent's creatures' P/T (deathtouch +20%, hexproof +30%, lifelink +10%, indestructible +25%).
- `tempo`: opponent.mana_pool.total() + len(opponent.hand) × 1.5 + (1.0 if opponent.cards_drawn_this_turn > 0 else 0).
- `life_pressure`: opponent.life_total / 40 (lower = more pressure).
- `recent_aggression`: sum of damage dealt by opponent to viewer in last 3 turns (read from politics_state.deals/log).
- `archetype_hint`: opponent's deck archetype (revealed via played cards — count by type and infer; iter-12+ adds Bayesian inference on commander identity).
- **Final threat score:** weighted sum normalized 0-1. Tunable weights; start with `board_strength × 0.4 + tempo × 0.2 + (1 - life_pressure) × 0.15 + recent_aggression × 0.15 + archetype_hint × 0.1`.

**`update_politics_state(state, viewer_id, event)`:** called after every significant event (combat, spell-cast against opponent, deal-made, deal-honored, deal-broken). Updates `threats[opponent_id]`, appends to `deals`, adjusts `alliances` enum.

**Schema (per scoping doc section 4):**
```python
{
  "threats": {opponent_id: {score, board_strength, tempo, life_pressure, recent_aggression}},
  "deals": [{opponent_player_id, deal_type, agreed_turn, kept}],  # cap at 50 entries
  "alliances": {opponent_id: "ally" | "neutral" | "rival"}
}
```

**Gates.** Unit tests: threat_vector returns expected values for fixed-state opponents (empty board = low; 6 attackers = high; lifelink boost); update_politics_state correctly records combat damage as recent_aggression; deals history caps at 50 entries (oldest drops); alliances enum transitions on triggering events.

---

## Phase 8 — Cost guardrails + cheap-fallback responder (~3-4 days)

Per scoping doc section 6 + section 10 risk mitigation.

**Per-turn cost ceiling:** $0.30. If a turn's calls exceed this, switch the player's responder to a cheap-fallback (always-pass) for remaining priority windows in that turn. Log a `COST_CEILING_HIT` event with turn + player.

**Per-game cost ceiling:** $10. Hard halt the game + emit `GAME_COST_CEILING_EXCEEDED` event. Game ends; whoever has the most life wins by default.

**Cheap-fallback responder.** When invoked, returns `pass_priority` for all subsequent priority windows. Maintains game-legal behavior (passing is always legal).

**Cost-tracker plumbing.** Wrap `llm_client.call_with_budget(prompt, max_tokens)` so every call accumulates into `state.cost_log[player_id]`. Expose via `state.get_cost(player_id)` and `state.get_total_cost()`.

**Gates.** Unit tests: cost-tracker accumulates correctly across calls; per-turn ceiling triggers fallback; per-game ceiling halts game; fallback responder returns legal actions only.

---

## Phase 9 — 4-LLM 20-turn integration test (~1 week)

**This is the sub-B Phase 3 ship gate.** Full 4-LLM 20-turn game using 4 hardcoded decks (1 per color, simple archetypes the rules engine + the limited iter-10 effects registry can handle without v11's per-card oracle work).

**Gates:**
1. Game completes (turn 20 reached or earlier win condition triggered)
2. All actions legal — no engine exceptions, no validator overrides reach >5% of calls
3. Total cost < $5
4. Politics state shows non-trivial threat dynamics (at least one alliance transition; at least one deal made and tracked)
5. At least one combat turn with multi-block + damage assignment played correctly
6. At least one counter war (response prompt fires at depth ≥ 2)
7. Cost guardrails verifiable — synthetic test forces ceiling hit, fallback engages correctly

**If gate 1 fails repeatedly,** halt — likely a substrate boundary bug or a parser bug. Surface for user review.

**If gate 3 fails (cost > $5),** halt and surface — the token budget may need tightening or the responder loop is over-prompting.

**If gates 2-7 partially pass,** apply the iter-3-9 "revise criteria + ship" pattern. Don't over-iterate the integration test. 5/7 gates is a reasonable ship floor.

---

## Phase 10 — Final regression + report + memory + sub-C scoping prep

**Regression.** Full pytest + vitest from clean. Must hit iter-10 baseline (pytest 1852+, vitest 774+) + the new sub-B tests. No iter 1-10 regressions.

**Deliverable: `mega_task_v10_final_report.md`** in `MTG-Deck-Builder-Claude/`. Executive + per-phase + integration test results + hand-off section for sub-mega-task C.

**Deliverable: sub-mega-task C scoping prep.** A 2-3 page scoping note `pillar_f_v0_2_sub_c_stage_2_playtest_scoping.md` covering: pod orchestrator design (4 LLM players ↔ 1 engine instance), win-condition tracking, Stage 2 validation harness, integration with existing `agent_graduated_playtest_v1.py`, cost guardrails ($240/deck per scoping doc section 6 + section 7), per-deck Stage 2 report format.

**Memory update.** Write `project_mega_task_v10_shipped_2026-05-XX.md`. Add one-line MEMORY.md index entry under 200 chars.

**Halt-on-criteria-revision pattern.** If Phase 9 lands at 4-5/7 gates, surface a revise-or-iterate question. Below 4/7 likely indicates substrate or design issue — halt regardless.

---

## Architectural rules to honor

Per scoping doc section 9 + the locked feedback memories:

- **Pool score does not drive LLM picking** — sub-B's action selection uses current game state, NOT pool ranks from build-time. The deck Pillar D built is fixed; how it's piloted is the LLM's call given the state.
- **Corpus is descriptive not prescriptive** — threat-vector feature extraction must not bias toward "corpus-typical" play patterns. Threat score reads the actual board, not a corpus average.
- **User intent locks deck shape** — N/A directly to play-time piloting, but the LLM does not get to mulligan into a different deck. The 99-card deck Pillar D built is what gets piloted.
- **Live-test catches what unit tests miss** — Phase 3 head-to-head + Phase 6 4-LLM mulligan + Phase 9 full game are the structured live-tests. Don't skip them in favor of pure unit coverage.
- **Cowork Write/Edit may silently truncate large code-file writes** — applies. Verify large prompt-template files with `ast.parse` after every Edit.

---

## Wins from iter 10 that iter 11 MUST NOT regress

- All 100 fixtures still pass after sub-B integration (re-run after Phase 9)
- pytest 1852+, vitest 774+
- Pillar A-E untouched (sub-B touches `pillar_f/v0_2/policy/`, not Pillar A-E)
- Substrate's `pillar_f/v0_2/` module tree (state/stack/turn/replacement/layers/combat) unchanged in v10's work — if a substrate change is required, halt and surface (v11 may also be modifying it; coordinate via the user)

---

## Halt-trigger reference

- Baseline pytest or vitest red before Phase 1
- Phase 3 head-to-head produces > 5% illegal-action rate after validator → re-prompt loop → fallback chain — likely a prompt-template ambiguity
- Phase 9 game total cost > $5 — token budget needs tightening
- Cumulative spend approaches 80% of $120 ceiling before Phase 7 — surface scope-trim option
- Substrate change required to make a prompt work — halt; the substrate is iter-10's source of truth and sub-B should not mutate it
- Merge conflict with v11 in the substrate effect registries — surface; coordinate via user

---

## Dispatch checklist

1. Confirm parent commit (`955f3c3fc` from v9 ship) is what `main` is on
2. Confirm baseline pytest 1852 + vitest 774 green
3. Confirm sub-B scoping doc at `mtg-engine/MTG-Deck-Builder-Claude/pillar_f_v0_2_sub_b_llm_policy_scoping.md`
4. Confirm Anthropic API key is loaded into env (production paths now use LLM calls — unlike v9 which was pure code authoring)
5. Confirm v11 (per-card oracle compilation seed) is either also dispatched or queued — both are iter-11 work running in parallel
6. Confirm $120 budget envelope is acceptable

Good luck. This is the empirical layer that turns the iter-10 substrate into a piloted deck.
