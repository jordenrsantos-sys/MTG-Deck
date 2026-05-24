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

Committed as `42f4dd7f3`.

---

## Phase 1 — Central wrapper migration (2026-05-24)

**Migration** in `api/engine/layers/agent_llm_client_v1.py`:

- Replaced `import anthropic` with `import claude_agent_sdk`. Module
  version bumped: `LLM_CLIENT_VERSION = "agent_llm_client_v1.1"`.
- `AnthropicClient` class name retained for backwards compat with all
  10 Category-A import sites + existing tests. New alias
  `AgentSdkClient = AnthropicClient` for callers who want to
  reference the migrated semantics explicitly.
- New `CallResult.cost_basis: str` field (`"api_estimate"` |
  `"subscription_credit"`) per kickoff Phase 1 Option A. Tracks
  whether the reported `cost_usd` came from local PRICING table
  (api_estimate) or the SDK's `ResultMessage.total_cost_usd`
  (subscription_credit).
- New `_resolve_auth_mode() -> 'subscription' | 'api_key' | 'none'`
  helper. `is_available()` now checks: SDK imported AND (CLI on PATH
  OR ANTHROPIC_API_KEY env). CLI takes priority over API key in
  auth-mode reporting.
- `unavailable_reason()` updated to mention both auth paths in the
  user-facing error message.
- `call_with_budget()`:
  - Replaced sync `self._client.messages.create(...)` with
    `asyncio.run(_invoke_agent_sdk(...))`. Sync surface preserved
    for callers (no async/await infection beyond the wrapper).
  - Maps legacy `max_output_tokens` cap to Agent SDK's
    `max_budget_usd` parameter via `estimate_cost_usd(0,
    max_output_tokens * 2)` (2x safety multiplier so the SDK
    doesn't terminate just below requested output).
  - Pre-call input-token budget guard preserved verbatim.
  - Retry loop honors BOTH legacy exception classification
    (`_is_retriable` on `RateLimitError`, `APIConnectionError`,
    plus new entries for `CLIConnectionError`/`ProcessError`) AND
    new Agent SDK error-enum classification
    (`_is_retriable_agent_sdk_error` on `rate_limit` +
    `server_error` categories).

- New private async helper `_invoke_agent_sdk(system, user, model,
  max_budget_usd) -> dict`:
  - `ClaudeAgentOptions(system_prompt=system, model=model,
    max_turns=1, allowed_tools=[], permission_mode=
    "bypassPermissions", max_budget_usd=max_budget_usd)`.
  - Walks the async iterator returned by `query(prompt=user,
    options=options)`.
  - Accumulates `AssistantMessage.content[].text` from TextBlocks.
  - Captures `AssistantMessage.error` (enum) for error
    classification.
  - Reads `ResultMessage.usage` (input/output tokens),
    `total_cost_usd`, `is_error`, `api_error_status`, `errors`.
  - Some Agent SDK paths put final text in `ResultMessage.result`
    instead of `AssistantMessage.content`; fallback covered.

- New `_classify_agent_sdk_error(category)` + `_is_retriable_agent_sdk_error(category)`
  free functions for the new error-enum classification surface. Map
  the 6 documented categories ('authentication_failed',
  'billing_error', 'rate_limit', 'invalid_request', 'server_error',
  'unknown') to stable `LLM_*` codes. Retriable: rate_limit +
  server_error only.

**Test updates** in `tests/test_agent_llm_client_v1.py`:

- Mock target updated from `anthropic.Anthropic` to
  `api.engine.layers.agent_llm_client_v1._invoke_agent_sdk` (the
  new async helper). Mocks use an async function (`async def
  _fake(*, system, user, model, max_budget_usd)`) returning the
  same dict shape `_invoke_agent_sdk` produces in production.
- `IsAvailableTests` rewritten: tests now patch `shutil.which` to
  control CLI-presence detection. New tests cover:
  - subscription-via-CLI-without-API-key path
  - auth-mode priority (CLI > API key > none)
- `BudgetGuardTests` rewritten: short-circuit assertion patches
  `_invoke_agent_sdk` (asserts not_called); fallback path patches
  `shutil.which`.
- `SuccessfulCallTests` rewritten via the new `_make_fake_invoke()`
  async-function builder. New tests cover:
  - `total_cost_usd` from SDK flipping `cost_basis` to
    `subscription_credit`
  - Agent SDK error enum classification end-to-end
- New `AgentSdkErrorEnumTests` (4 tests) covers the new helpers.
- New `AgentSdkClientAliasTests` (1 test) covers the alias.

**Result**: wrapper tests **35/35 pass** (was 26 -> +9 v13-specific).
Full regression: **2321 pass + 25 skip + 88 subtests** (iter-12
baseline 2312 + 9 new tests, no regressions).

~280 LOC production rewrite (wrapper) + ~180 LOC test additions /
updates.

**Commit message:** "Phase 1 (mega-task v13): migrate central LLM wrapper to claude-agent-sdk (preserves CallResult shape; adds cost_basis field)".

Committed as `367e26859`.

---

## Phase 2 — Pillar D + Pillar C + per-set automation verification (2026-05-24)

**No production code changes required.** The Phase 0 audit established
that all four Phase 2 targets are Category A (route through the
central wrapper), and Phase 1 migrated the wrapper. Phase 2 is
verification-only: confirm the test suites stay green now that the
wrapper invokes `claude-agent-sdk` underneath.

**Modules verified:**

- `api/engine/layers/agent_build_deck_v1.py` (Pillar D, 6 LLM phases)
- `api/engine/layers/new_set_report_writer_v1.py` (per-set automation)
- `api/engine/extractors/primitive_extractor_llm_v1.py` (Pillar C)
- `tools/backfill_primitives_v2.py` (ontology v2 backfill)

**Test results:**

- Pillar D: 70/70 across phase_a2 + phase_b2 + phase_c2_1 + phase_c2_2
  + phase_d2 + iter3_phase_3 + stream + stream_e2e.
- Pillar C extractor: 64/64 across primitive_extractor_golden +
  primitive_extractor_v2_counters_and_proliferate +
  primitive_extractor_v2_rules_modifiers + new_set_report_writer_v1.

**No commit for Phase 2** -- nothing to ship; the verification is
captured in the combined Phase 2+3 verification commit below.

---

## Phase 3 — Sub-B + sub-C verification (2026-05-24)

**No production code changes required.** Same logic as Phase 2:
sub-B's policy module + sub-C's playtest module all route through
the central wrapper via `get_default_client().call_with_budget()`.
Phase 1's wrapper migration carries them forward transparently.

**Modules verified** (all already audited as Category A):

- `api/engine/pillar_f/v0_2/policy/llm_responder.py`
- `api/engine/pillar_f/v0_2/policy/mulligan_decider.py`
- `api/engine/pillar_f/v0_2/playtest/orchestrator/game_runner.py`
- `api/engine/pillar_f/v0_2/playtest/cycle/cycle_runner.py`
- `api/engine/pillar_f/v0_2/playtest/combat_glue/combat_decider.py`
- `api/engine/layers/agent_graduated_playtest_stage_2_v1.py`

**Test results:**

- Sub-B + sub-C: 245/245 across `tests/pillar_f_v0_2_policy/` and
  `tests/pillar_f_v0_2_playtest/`.

**Cost-tracker reconciliation note.** Sub-B's `CostTracker` and
sub-C's per-cycle cost guardrails sum `CallResult.cost_usd`.
Post-migration, those USD figures will be labeled
`api_estimate` until the SDK's `ResultMessage.total_cost_usd`
returns a non-zero value (then they flip to `subscription_credit`
per call). Sub-B/sub-C UIs render the figure as `$X.XX` either
way; the new `cost_basis` field on CallResult lets future UI
improvements add a caption ("subscription credit"/"API rate
estimate"). No UI changes shipped in v13.

**No commit for Phase 3 alone.** Combined with Phase 2 in the
verification commit below.

---

## Phase 2 + 3 — Combined verification commit (2026-05-24)

**Verification-only commit.** Updates the progress log with the
Phase 2 + 3 outcomes. No production code or test files touched.

**Commit message:** "Phase 2 + 3 (mega-task v13): Pillar D + C + per-set + sub-B + sub-C verification -- all Category-A callers pass tests with migrated wrapper".

Committed as `4e430e7ff`.

---

## Phase 4 — Tests + conftest + mocking strategy (2026-05-24)

**Scope.** Phase 1 already updated `test_agent_llm_client_v1.py`
(primary target) and `tests/conftest.py` was already SDK-agnostic
(only references `MTG_ENGINE_DISABLE_LLM` env + the
`reset_default_client_for_tests` helper, both preserved verbatim in
the wrapper migration). Phase 4 sweeps the remaining test files
identified in the Category-D audit.

**Remaining test updates:**

- `tests/test_agent_build_deck_v1_phase_a2.py`: the
  `NoNetworkContactTests::test_no_anthropic_client_constructed_when_key_missing`
  test patched `anthropic.Anthropic` to assert "no network call
  occurs when API key missing." Post-v13 the wrapper never invokes
  `anthropic.Anthropic` (irrelevant target), so the assertion was
  vacuously satisfied -- it stopped guarding what it intended.
  Updated to patch `claude_agent_sdk.query` (the Agent SDK entry
  point the migrated wrapper invokes) AND `shutil.which` (force CLI
  absence). Renamed test to `test_no_sdk_call_when_no_auth_available`
  for clarity.

**Kill switch verification.** Manual check confirms
`MTG_ENGINE_DISABLE_LLM=1` -> `is_available()=False` ->
`unavailable_reason()` mentions the env var. The conftest autouse
fixture continues to work; pytest runs do not make live API calls.

**Conftest unchanged.** The autouse
`_disable_llm_layer_by_default` fixture continues to set
`MTG_ENGINE_DISABLE_LLM=1` and call
`reset_default_client_for_tests()`. The `enable_llm_layer` fixture
also unchanged (still sets `ANTHROPIC_API_KEY=sk-test-fixture` as
the legacy API-key fallback path; under v13 that's still a valid
auth route).

**Sub-B / sub-C smoke runners** in `tools/` (e.g.
`test_pillar_f_v0_2_policy_phase9_smoke.py`,
`test_pillar_f_v0_2_playtest_phase7_smoke.py`) use
`get_default_client()` to fetch the real wrapper -- no test mocks
needed; these are LIVE-API smoke runners that opt into network
calls explicitly. Post-migration they will hit the Agent SDK path
automatically. Verified by Phase 5's live smoke (next).

**Test results:**
- `test_agent_build_deck_v1_phase_a2.py`: 3/3 pass.
- Full pytest regression: **2321 pass + 25 skip + 88 subtests** -- no
  regressions vs Phase 1 baseline.

**Commit message:** "Phase 4 (mega-task v13): update remaining test mocks (test_agent_build_deck_v1_phase_a2 patches claude_agent_sdk.query + shutil.which instead of anthropic.Anthropic)".
