# Mega-task v15.5 / Billing-routing fix-up — progress log

Iter 15.5 dispatch on top of v15 partial ship (`14bafbcd0`).
Kickoff: `mega_task_v15_5_kickoff.md`. Closes the silent-API-drain
bug surfaced by v15 Phase 2's Krenko cycle (~$13.62+ user damage).

6 phases (0-5). Budget: $5 spend, 4-8 hours CC.

---

## Phase 0 — Incident audit + HALT GATE (2026-05-24)

**Audit deliverable:** `mega_task_v15_5_incident_audit.md` written.

**Findings:**
1. `_resolve_auth_mode()` returns `"subscription"` whenever CLI on
   PATH, even when `ANTHROPIC_API_KEY` is also set.
2. The Agent SDK spawns `claude` CLI as subprocess; the subprocess
   inherits env including `ANTHROPIC_API_KEY` and prefers API key
   for its own auth.
3. `cost_basis="subscription_credit"` label is set based on
   `total_cost_usd > 0` which is true in BOTH billing modes.
4. The label was a FALSE POSITIVE; v13's "subscription auth
   confirmed end-to-end" verification was confirming nothing of the
   kind.

**User damage analysis:**
- 24 game artifacts in v15 Phase 2 Krenko cycle.
- 4 games (game_000-003) with $3+ spend each at $0.0247/call.
- $0.0247/call matches Sonnet 4.6 API rate-card exactly (not
  subscription credit math).
- **$13.62 lower bound** total v15 Phase 2 API drain.
- **~$18.50 upper bound** including v13 Phase 5 + v15 Phase 0/1
  smokes that also reported false-positive labels.

**Test coverage gap:** all 35 wrapper unit tests assert
`cost_basis="subscription_credit"` when SDK mock returns
`total_cost_usd > 0`. No test exercises the AMBIGUOUS auth state
(both env var + CLI present). No test mocks the actual
subprocess-auth-resolution behavior.

**Baseline:** pytest 2381 + 25 skip + 88 subtests (matches v15
post-Phase-1 baseline). vitest unchanged.

**Current env state IS the buggy ambiguous state:**
- `ANTHROPIC_API_KEY in env: True`
- `claude CLI in PATH: <present>`

**HALT GATE per kickoff Phase 0.** Surfaced to user for
prerequisite confirmation before Phase 1.

**No code changes in Phase 0.** Audit + halt only.

**Commit message:** "Phase 0 (mega-task v15.5): incident audit -- v13 cost_basis labeling false-positive caused v15 Phase 2 to silently bill ~$13.62 to user's API instead of Max subscription".
