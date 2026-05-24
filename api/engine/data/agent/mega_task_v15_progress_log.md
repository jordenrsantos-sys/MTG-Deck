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
