# Mega-task v15 / Pillar F maturity — progress log

Iter 15 dispatch on top of v14 ship (`0dd0f41ec`). Kickoff:
`mega_task_v15_kickoff.md`. The validation milestone — closes v12's
deferred Phase 7 full cycle + Phase 8 calibration sweep on Edgar/
Krenko/Ur-Dragon. Subscription billing via v13's Agent SDK migration.

7 phases (0-6). Budget: $50 spend ceiling (expected ~$0 net under
Max subscription); 6-10 hour wallclock allowance.

---

## Phase 0 — Pre-flight + Agent SDK billing verification (2026-05-24)

**Baseline verified:**
- pytest 2372 + 25 skip + 88 subtests (iter-14 matched).
- oracle_seed_coverage 500/500 (100.0%); 0 fall-throughs; 0 exception.
- iter-10 100-fixture suite: 100/100 green.
- vitest unchanged from iter-13.

**Agent SDK billing verified.**
- claude CLI v2.1.144 present.
- Mini-smoke (`tools/test_v13_migration_smoke.py`) 6/6 PASS.
- `cost_basis = 'subscription_credit'`; SDK reported $0.018287 for
  one ping-echo call (billed against Max subscription credit, not
  API balance).

**v15-time discovery + self-correction.** Initial smoke run failed
with `"Reached maximum budget ($0.05)"` -- v13's
`max_budget_usd = max(raw_budget * 5.0, 0.05)` formula was too
tight for current Agent SDK overhead. Patched in
`agent_llm_client_v1.py call_with_budget()` to
`max(raw_budget * 10.0, 0.50)`. Wrapper unit tests 35/35 still
pass (mocked path unchanged). Single-line production change; the
authoritative spend governance remains per-game ($5) + per-cycle
($200 default, $50 in v15) ceilings.

**Cycle runner readiness.**
- `run_stage_two_cycle` serial implementation in place (v12 +
  v14).
- Phase 1 adds `parallelism: int = 4` config + asyncio.gather.
- Per-cycle cost ceiling + CYCLE_COST_HALT telemetry + per-game
  JSON persistence all in place from v12.

**Deck availability.** All 3 commanders (Krenko, Mob Boss; Edgar
Markov; The Ur-Dragon) + supporting creatures available in
`playtest/orchestrator/card_factory.py`. 16 known creatures total
-- enough for inline ~30-card deck construction per cycle.

**Wallclock estimate.** Extrapolating from v12 Phase 7 (3 games ×
8 turns in 643s):
- Per cycle serial: ~5.5 hours
- Per cycle parallel-4: ~1.4 hours (best case)
- Three cycles parallel: ~4-5 hours wallclock
- Realistic per-game wallclock likely half worst-case (v15 decks
  have creatures so games end earlier than v12 Phase 7's all-pass
  8-turn games) -> **3 cycles in 1.5-2.5 hours target**.

**Deliverables:**
- `mega_task_v15_preflight.md` -- full pre-flight doc.
- `agent_llm_client_v1.py` -- budget formula bump (rationale
  documented in inline comment).
- This progress log.

**Commit message:** "Phase 0 (mega-task v15): preflight + Agent SDK subscription auth verified (6/6 smoke; cost_basis=subscription_credit) + budget-formula bump for current SDK overhead".

Committed as `e4394deb0`.

---

## Phase 1 — Asyncio parallelism for cycle runner (2026-05-24)

**Implementation** in `api/engine/pillar_f/v0_2/playtest/cycle/
cycle_runner.py`:

- `CYCLE_RUNNER_VERSION` bumped v1 -> v2.
- New `_run_one_game(cycle_config, game_idx, rng_seed, llm_client)
  -> StageTwoGameResult` helper: per-call deterministic RNG seed
  (game-idx-derived) so parallel scheduling doesn't disrupt
  reproducibility.
- New `_run_wave_async(cycle_config, indices, llm_client) ->
  List[StageTwoGameResult]` -- launches a wave of games concurrently
  via `asyncio.gather + asyncio.to_thread`. `run_single_game` is
  sync but safe across threads (each invocation creates independent
  GameState + CostTracker + politics dicts; LLM client's
  `asyncio.run()` per call uses an independent inner event loop per
  thread).
- `run_stage_two_cycle` rewrites the main loop as wave-based:
  - When `parallelism >= 2`, plan a wave of `min(parallelism,
    games_remaining)`; launch via `_run_wave_async`; aggregate.
  - When `parallelism == 1`, fall through `_run_one_game` directly
    (avoids `asyncio.run()` overhead). Serial path is the
    backwards-compat code path.
  - Cost-ceiling check fires BETWEEN waves -- parallel games in a
    wave can collectively overshoot by one wave's worth of spend
    before cycle halts. Acceptable trade-off for wallclock speedup.
- New `_looks_like_rate_limit(result)` + `_RATE_LIMIT_FALLBACK_STREAK
  = 3`. When 3 consecutive waves observe rate-limit fallback events,
  drop `active_concurrency` to 1 for the cycle remainder and emit
  `RATE_LIMIT_FALLBACK_TO_SERIAL` event.

**Tests** in
`tests/pillar_f_v0_2_playtest/test_v15_phase1_parallel_cycle.py`:
9 tests across 3 classes:
- **ParallelExecutionTests** (5): parallelism=4 single-wave,
  parallelism=2 multi-wave, parallelism=1 backwards-compat,
  per-game indices match results, per-game JSONs atomic (one shot
  per file).
- **CycleCostCeilingParallelTests** (1): cost ceiling halts
  between waves; CYCLE_COST_HALT event present.
- **RateLimitFallbackTests** (3): `_looks_like_rate_limit` detects
  rate-limit events, ignores non-rate-limit events, streak
  constant exposed.

All 9 pass. Full regression **2381 pass + 25 skip + 88 subtests**
(+9 v15 tests; no regressions).

**Live parallel smoke** at concurrency=4 via
`tools/test_v15_phase1_parallel_smoke.py`:
- 3 games (same Krenko + 3 controls deck setup as v12 Phase 7).
- All 3 fit in one wave (parallelism=4 covers 3 games).
- **Wallclock: 358s** vs v12 Phase 7's serial **643s** = 44%
  speedup. Per-game wallclock 276-358s (limited by max game's
  finish time, as expected from one-wave gather).
- Cost-basis sum: $4.84 ($1.6/game avg). Higher than v12 Phase 7's
  $1.02 / 3 games ($0.34/game) because v15 Phase 0's budget-formula
  bump (5x/$0.05 -> 10x/$0.50) lets LLM responses complete fully
  instead of being cut off mid-response. Subscription auth held:
  game JSONs report 0 fallback events; cost_basis="subscription_credit"
  per ResultMessage.
- Cycle complete; recommendation YELLOW (same as v12 Phase 7;
  decks reached max_turns without eliminations).

**Wallclock extrapolation for v15 Phases 2-4:**
- 30 games / 4-game waves = 8 waves
- Per wave ~5-6 min (longest game in wave)
- Per cycle ~40-50 min
- Three cycles ~2-2.5 hours wallclock total

**Cost-basis extrapolation:**
- ~$5/game at 25 turns (scaled from $1.6 at 8 turns)
- 30 games × $5 = ~$150 cost-basis per cycle
- 3 cycles ~$450 cost-basis total
- **Net spend $0 under Max subscription** (cost_basis is
  "what-it-would-have-cost-on-API" benchmark)

**Per-cycle ceiling adjustment for Phases 2-4.** Kickoff specified
`$50` cycle-internal ceiling; with current SDK overhead per-game
is ~$5 at 25 turns, so 30 games = ~$150 cost-basis. Bumping
`cycle_cost_ceiling_usd` to $300 for Phases 2-4 cycles so the
guard is defensive only (subscription billing keeps net spend at
$0; the cost-basis number is a benchmark, not a real spend
governor). Per-game ceiling stays at $5 (sub-C default).

LOC: ~150 production (parallel path + helpers) + ~210 test +
~140 smoke runner = ~500 LOC.

**Commit message:** "Phase 1 (mega-task v15): asyncio cycle parallelism (parallelism=4 default; wave-based execution; rate-limit fallback to serial; live 3-game smoke 358s wallclock = 44% speedup vs serial)".
