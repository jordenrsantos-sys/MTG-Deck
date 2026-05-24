# Mega-task v13 / Engine LLM runtime migration to Claude Agent SDK — progress log

Iter 13 dispatch on top of v12 ship (`f5105ab16`). Kickoff:
`mega_task_v13_kickoff.md`. Migration is plumbing-only:
`anthropic` SDK -> `claude-agent-sdk` for the engine's runtime LLM
call surface, enabling Claude Code subscription auth.

7 phases (0-6). Budget: $30 API spend, ~2-3 days CC.

---

## Phase 0 — Pre-flight + scope audit + dependency check (2026-05-24)

**Baseline verified:**
- `pytest tests/ -q` -> 2312 pass + 25 skip + 88 subtests in 241s.
  Matches iter-12 baseline.
- vitest baseline 774 + 2 pre-existing fails (unchanged) -- not re-run
  for Phase 0; verified at v12 Phase 9.

**Reading list completed:**
- `agent_llm_client_v1.py` (~480 LOC central wrapper).
- `tests/conftest.py` (autouse MTG_ENGINE_DISABLE_LLM=1 kill switch).
- `tests/test_agent_llm_client_v1.py` (mocks `anthropic.Anthropic`).
- Anthropic Agent SDK Python introspection via `pip install` + dir().

**Audit deliverable:** `mega_task_v13_call_site_audit.md`.

**Findings (LOW-RISK migration):**
- **Category A (routes through central wrapper):** 10 production
  modules -- Pillar D (agent_build_deck_v1 with 6 LLM phases),
  Pillar C extractor, per-set report writer, all sub-B (responder
  + mulligan_decider), all sub-C (combat_decider + game_runner +
  cycle_runner + dispatcher), backfill_primitives_v2 tool.
- **Category B (direct anthropic.Anthropic() bypass):** ZERO.
- **Category C (AsyncAnthropic async client):** ZERO.
- **Category D (test-only mocks):** 2 files
  (`test_agent_llm_client_v1.py`, `test_agent_build_deck_v1_phase_a2.py`)
  + conftest infrastructure (SDK-agnostic).

The "central wrapper" design from iter-2 pays off here: migrating
ONE file (`agent_llm_client_v1.py`) carries all 10 production
callers transparently if the `CallResult` contract is preserved.

**Agent SDK introspection findings:**

Installed `claude-agent-sdk==0.2.87`. The actual API is friendlier
than initial doc-based research suggested:
- `ClaudeAgentOptions.system_prompt: str | ...` -- system prompt IS
  supported as a direct string.
- `ClaudeAgentOptions.max_turns: int` -- set to 1 for single-shot
  completion (skip the agent loop).
- `ClaudeAgentOptions.max_budget_usd: float` -- replaces
  `max_tokens` for cost capping.
- `ClaudeAgentOptions.allowed_tools=[]` -- pure completion, no tool
  surface.
- `AssistantMessage.error: Optional[Literal['authentication_failed'
  | 'billing_error' | 'rate_limit' | 'invalid_request' |
  'server_error' | 'unknown']]` -- CLEANER error classification
  than the old SDK's exception-name-based approach.
- `ResultMessage.total_cost_usd: float | None` -- real cost from
  the billing surface (api_estimate vs subscription_credit
  distinction goes in CallResult.cost_basis field).
- Async-only; wrapper uses `asyncio.run()` per call (sync surface
  preserved for callers).
- Transport via `claude` CLI subprocess; `CLINotFoundError` if CLI
  not in PATH (handled in `is_available()` pre-check).

**Phase 1 plan is now concrete** (see audit doc "Anticipated Phase
1 difficulty" section for the implementation sketch).

**Requirements bump.** Added `claude-agent-sdk>=0.1.0` to
`requirements.txt`. Kept `anthropic>=0.50.0` alongside for the
migration window; Phase 6 removes it after Phase 5 verifies the new
path operationally.

**Pre-existing pip warnings on Windows** (~ydantic_core temp dir)
are noise -- pydantic-core 2.41.5 installed cleanly per
requirements.txt; baseline tests still pass post-install.

**Commit message:** "Phase 0 (mega-task v13): pre-flight + LLM call site audit + claude-agent-sdk dependency bump".
