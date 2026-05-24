# Mega-task v13 — LLM call site audit

**Generated:** 2026-05-24 (Phase 0)
**Parent:** v12 ship `f5105ab16`

Goal: enumerate every `anthropic`-SDK call site in the engine to scope
the Agent SDK migration. Categories per kickoff Phase 0.

---

## Category A — Routes through `AnthropicClient` central wrapper

All production-code LLM calls in the engine route through
`api/engine/layers/agent_llm_client_v1.py`'s `get_default_client()` +
`call_with_budget()` surface. Migrating the wrapper to Agent SDK
**automatically migrates every Category-A caller** without further
edits — this was the architectural intent in iter 2 ("centralizing
budget-tracking, retry, fallback, and pricing prevents drift across
them").

**Production callers** (verified by `grep -rn "call_with_budget" api/`):

| File | Lines (call_with_budget refs) | Role |
|------|-------------------------------|------|
| `api/engine/layers/agent_build_deck_v1.py` | 3376, 4343, 4869, 5477, 5786, 5877 | Pillar D iter-2-9 deckbuilder; 6 LLM phases (B2 / C2.1 / C2.2 / D2 + 2 iter-3+ additions) |
| `api/engine/layers/new_set_report_writer_v1.py` | 366 | Per-set automation LLM writer (mega-task v3) |
| `api/engine/extractors/primitive_extractor_llm_v1.py` | 112 | Pillar C primitive extractor |
| `tools/backfill_primitives_v2.py` | n/a (uses extractor) | Ontology v2 backfill tool |
| `api/engine/pillar_f/v0_2/policy/llm_responder.py` | 125 | Sub-B main priority responder (v10) |
| `api/engine/pillar_f/v0_2/policy/mulligan_decider.py` | (similar) | Sub-B mulligan + bottom-picker deciders |
| `api/engine/pillar_f/v0_2/playtest/combat_glue/combat_decider.py` | 282, 422 | Sub-C combat attacker + blocker deciders (v12) |
| `api/engine/pillar_f/v0_2/playtest/orchestrator/game_runner.py` | (via above) | Sub-C per-game runner |
| `api/engine/pillar_f/v0_2/playtest/cycle/cycle_runner.py` | (via above) | Sub-C cycle runner |
| `api/engine/layers/agent_graduated_playtest_stage_2_v1.py` | (via above) | Sub-C dispatcher (Stage 1 + Stage 2 + calibration) |

**Total: 10 production modules, all Category A.**

---

## Category B — Bypasses wrapper to instantiate `anthropic.Anthropic()` directly

**NONE.** `grep -rn "anthropic\.Anthropic" api/` returns only the
central wrapper (`agent_llm_client_v1.py` line 259, inside
`call_with_budget()`'s lazy client init).

---

## Category C — Uses `anthropic.AsyncAnthropic` async client

**NONE.** `grep -rn "AsyncAnthropic" api/` returns no matches.

---

## Category D — Test-only references (mocks, import stubs)

| File | Lines | Role |
|------|-------|------|
| `tests/test_agent_llm_client_v1.py` | various (Pillar D iter-2 unit tests) | Mocks `anthropic.Anthropic` to test the wrapper itself |
| `tests/test_agent_build_deck_v1_phase_a2.py` | 9, 109, 140 | One test patches `anthropic.Anthropic` to assert NO instantiation when kill switch is active |
| `tests/conftest.py` | implicit | Sets `MTG_ENGINE_DISABLE_LLM=1` autouse for every test; reset_default_client_for_tests helper |

**Total: 2 test files with direct `anthropic` references** + conftest
infrastructure that's SDK-agnostic.

---

## Migration risk assessment

**LOW.** This is the textbook payoff for the "central wrapper" design:

- **One production file** to migrate (`agent_llm_client_v1.py`).
- **All 10 Category-A callers** pick up the migration transparently
  via the `call_with_budget` contract (which we preserve).
- **Two test files** need mock-target updates in Phase 4.
- **Zero refactors** of caller code required, assuming Phase 1's
  Agent SDK wrapper produces identical `CallResult` shape.

The kickoff anticipated a possible Phase 0 halt-trigger:
> "Phase 0 audit reveals a Category-B bypass with significant
> divergence from the central wrapper that would require >1 day to
> refactor"

**That trigger is NOT active.** No Category-B or C call sites exist.

---

## Anticipated Phase 1 difficulty

Verified by installing `claude-agent-sdk==0.2.87` and introspecting
the actual API surface (see `python -c "import claude_agent_sdk;
help(claude_agent_sdk.ClaudeAgentOptions)"`):

- **Async-only** — `query(prompt, options)` returns
  `AsyncIterator[UserMessage | AssistantMessage | SystemMessage |
  ResultMessage | StreamEvent | RateLimitEvent]`. The wrapper's
  `call_with_budget()` stays sync; we use `asyncio.run()` once per
  call inside the wrapper. Cost = one event-loop spin per call;
  acceptable for the engine's call volume.
- **`ClaudeAgentOptions.system_prompt: str | SystemPromptPreset |
  SystemPromptFile | None`** — system prompt IS supported as a
  string param. Engine's system + user separation maps cleanly.
- **`ClaudeAgentOptions.max_turns: int | None`** — set to `1` for
  single-shot completion. Skips the agent loop entirely.
- **`ClaudeAgentOptions.max_budget_usd: float | None`** — replaces
  `max_tokens` for cost capping. Wrapper computes a budget from
  `max_output_tokens * pricing` so the existing caller contract
  doesn't change.
- **`ClaudeAgentOptions.allowed_tools=[]`** — no tools; pure
  completion semantics. Required since iter-11 prompts don't use
  Claude Code tool surface.
- **`ClaudeAgentOptions.permission_mode="bypassPermissions"`** —
  required for headless server-side execution.
- **Response shape**: walk the async iterator; accumulate
  `TextBlock.text` from `AssistantMessage.content`; read
  `AssistantMessage.error` (stable enum: `'authentication_failed' |
  'billing_error' | 'rate_limit' | 'invalid_request' |
  'server_error' | 'unknown' | None`); read `ResultMessage.usage`
  (dict with `input_tokens`/`output_tokens`) and `total_cost_usd`
  + `api_error_status` (HTTP status when error).
- **Exception types** — `ClaudeSDKError`, `CLIConnectionError`,
  `CLINotFoundError`, `CLIJSONDecodeError`, `ProcessError`. These
  are TRANSPORT-level exceptions (CLI subprocess issues).
  API-level errors come back via `AssistantMessage.error` +
  `ResultMessage.is_error` + `api_error_status`, NOT as raised
  exceptions. The wrapper's `_classify_error_code` maps from the
  error enum directly — cleaner than the old name-based mapping.
- **Auth** — the Agent SDK invokes the `claude` CLI as a
  subprocess; auth flows through Claude Code's local credentials
  (`~/.claude/`). The CLI auto-discovers `ANTHROPIC_API_KEY` as a
  fallback. The wrapper's new `_resolve_auth()` checks for `claude`
  CLI in PATH + reports which auth mode appears active for
  `unavailable_reason()` diagnostics.
- **`CLINotFoundError`** — raised if `claude` CLI isn't installed.
  The wrapper's `is_available()` should pre-check by attempting a
  light SDK init OR by checking PATH for `claude` so the engine can
  fall back to deterministic-only behavior cleanly when CLI is
  missing.

**Verdict.** Phase 1 is doable as a single-file rewrite of
`agent_llm_client_v1.py`, holding the `CallResult` contract stable.
The Agent SDK actually has BETTER error classification (semantic
enum on the message) than the old SDK (exception class names).
Sync-wrapper via `asyncio.run()` is the agreed approach.
`cost_basis: "api_estimate" | "subscription_credit"` field added to
CallResult (Option A per kickoff Phase 1).

---

## Phase 0 deliverable summary

- This audit doc.
- Add `claude-agent-sdk` to `requirements.txt` (keep `anthropic`
  alongside through Phase 5; remove in Phase 6 per kickoff).
- Commit as Phase 0.

No production code changes in Phase 0.
