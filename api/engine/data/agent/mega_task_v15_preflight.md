# Mega-task v15 — Phase 0 preflight

**Generated:** 2026-05-24
**Parent commit:** v14 ship `0dd0f41ec`

Phase 0 deliverable per kickoff: baseline counts + Agent SDK
billing confirmation + cycle-runner readiness.

---

## Baseline

| Suite | Count | Status |
|-------|-------|--------|
| pytest | 2372 pass + 25 skip + 88 subtests | green (iter-14 baseline matched) |
| vitest | 774 pass + 2 pre-existing fail | unchanged from iter-13 |
| oracle_seed_coverage | 500/500 (100.0%) addressed; 0 fall-through; 0 exception | iter-14 100% target preserved |
| iter-10 100-fixture suite | 100/100 | correctness floor held |

Substrate diff vs iter-10 ship: only v14's intentional changes
present. No drift.

---

## Agent SDK billing verification

**CLI:** `which claude` -> `/c/Users/jorde/AppData/Roaming/npm/claude`.
`claude --version` -> `2.1.144`.

**Mini-smoke** (`tools/test_v13_migration_smoke.py`), 1 call:

- `is_available()` -> True
- `_resolve_auth_mode()` -> `'subscription'`
- Call returned `ok=True` in 3.5s
- Text returned: ` ```json {"echo": "ping"} ``` `
- `parsed_json` -> `{'echo': 'ping'}`
- `input_tokens` = 3, `output_tokens` = 14
- **`cost_basis` = `'subscription_credit'`**
- `cost_usd` = $0.018287 (SDK-reported via
  `ResultMessage.total_cost_usd`; bills against Max subscription
  credit, NOT API balance)
- `latency_ms` = 3455
- 6/6 acceptance checks PASS

**Subscription auth holds.** Phase 0's CRITICAL halt-trigger (mini-
smoke routes through API key fallback instead of subscription) is
NOT triggered.

---

## v15-time discovery: Agent SDK budget formula tuning

**Issue surfaced.** First v13 smoke attempt failed with `"Reached
maximum budget ($0.05)"` -- v13's `max_budget_usd = max(raw_budget *
5.0, 0.05)` formula was too tight for current Agent SDK overhead.

**Self-correction.** Bumped the formula in
`agent_llm_client_v1.py call_with_budget()`:

```python
# Before (v13):
max_budget = max(raw_budget * 5.0, 0.05)
# After (v15-time fix):
max_budget = max(raw_budget * 10.0, 0.50)
```

10x multiplier (vs 5x) + $0.50 floor (vs $0.05) covers SDK
overhead with comfortable margin. Still well under per-game ($5
default from sub-C) and per-cycle ($200 default from sub-C) ceilings
that govern actual spend.

35/35 wrapper unit tests still pass (mocked path unchanged).

This fix lands in Phase 0's commit as a billing-readiness fix.
Includes a brief docstring update so the rationale is captured.

---

## Cycle-runner readiness

**Current state** (`playtest/cycle/cycle_runner.py`):

- `run_stage_two_cycle(cycle_config, llm_client, progress_callback)`
  exists, SERIAL (no parallelism).
- Per-cycle cost ceiling (`cycle_cost_ceiling_usd=200.0` default,
  v15 will use $50) + halt + partial-report writing -- all in place
  from v12 + v6.
- CYCLE_COST_HALT telemetry: present (v12 Phase 6).
- Per-game JSON persistence: present.

**v15 Phase 1 task**: add `parallelism: int = 4` config; wrap
`run_single_game` with `asyncio.gather`; rate-limit fallback to
serial; per-game JSON writes atomically (already atomic via
`write_per_game_json` -- one-shot file write).

**Deck availability** (commander + supporting creatures in
`playtest/orchestrator/card_factory.py`):

| Commander | Available | Notes |
|-----------|-----------|-------|
| Krenko, Mob Boss | YES (4-cmc Legendary 3/3 Goblin Warrior) | Mono-red support: Goblin Guide / Piledriver / Skirk Prospector / Battle Cry Goblin |
| Edgar Markov | YES (6-cmc Legendary 4/4 Vampire Knight, first strike + haste) | Vampire support: Vampire Nighthawk |
| The Ur-Dragon | YES (9-cmc Legendary 10/10 Dragon Avatar, flying) | Dragon support: Dragonlord Atarka |

Card factory has 16 known creatures total. Phase 2-4 cycle deck
builders inline mainboard lists (~30 cards/deck per v12 Phase 7
pattern: ~18 lands + ~9-12 creatures + ~3-6 instants). 5-color
mana fixing for Ur-Dragon is best-effort with basic lands; iter-12+
nonbasic mana base is out of v15 scope.

---

## Wallclock estimate

Per v12 Phase 7 baseline: 3 games × 8 turns = 643s (~3.5 min/game).

Extrapolating to v15 spec (30 games × 25 turns max):

- Per game: 3.5 min × (25/8) = ~11 min serial worst-case
- Per cycle serial: 30 × 11 min = ~5.5 hours
- Per cycle parallel-4 (Phase 1 unlocks): 5.5 / 4 = **~1.4 hours**
- Three cycles parallel: **~4-5 hours wallclock**

Games typically end before max_turns when one player wins (most v12
games reached max_turns in the 8-turn smoke because no creatures
were on board; v15 decks include creatures, so games will end
earlier via life loss). Realistic per-game wallclock likely
half the worst-case estimate -- **~30-45 min per cycle parallel**.

Three cycles in **~1.5-2.5 hours** is the realistic target.

---

## Phase 0 commit summary

- Baseline counts confirmed.
- Mini-smoke 6/6 PASS with `cost_basis="subscription_credit"`.
- v15-time fix: budget formula bumped from (5x, $0.05) to
  (10x, $0.50) -- enables Agent SDK calls under current SDK
  overhead. Single-line wrapper change; all wrapper unit tests
  pass.
- 3 commanders + supporting creatures confirmed in card factory.
- Wallclock estimate: 3 cycles in 1.5-2.5 hours parallel.

**Commit message:** "Phase 0 (mega-task v15): preflight + Agent SDK subscription auth verified (6/6 smoke; cost_basis=subscription_credit) + budget-formula bump for current SDK overhead".
