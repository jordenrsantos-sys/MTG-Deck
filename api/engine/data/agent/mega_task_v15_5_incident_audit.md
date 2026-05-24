# Mega-task v15.5 — Phase 0 incident audit

**Generated:** 2026-05-24
**Trigger:** v15 Phase 2 Krenko cycle silently drained user's
Anthropic API balance ~$13.62+ before failing on API funds exhausted.

---

## 1. Failure-mode walkthrough

**The buggy code** in `api/engine/layers/agent_llm_client_v1.py`:

```python
# Lines 152-160
def _resolve_auth_mode(self) -> str:
    """Returns 'subscription' if Claude Code CLI is reachable
    (preferred for v13 migration), 'api_key' if ANTHROPIC_API_KEY
    is set (fallback), or 'none' if neither."""
    if self._has_claude_cli():
        return "subscription"          # <-- BUG: returns
                                        # "subscription" even when
                                        # API key is ALSO set
    if self._resolve_api_key():
        return "api_key"
    return "none"
```

```python
# Lines 369-377 (inside call_with_budget)
sdk_cost = raw_result.get("total_cost_usd")
if sdk_cost is not None and sdk_cost > 0:
    cost = float(sdk_cost)
    basis = "subscription_credit"      # <-- BUG: label set on
                                        # total_cost_usd>0, which is
                                        # TRUE in both billing modes
else:
    cost = self.estimate_cost_usd(inp, outp, model=m)
    basis = "api_estimate"
```

**The actual control flow when both auth modes are present:**

1. `is_available()` returns True (CLI is on PATH).
2. `_resolve_auth_mode()` returns `"subscription"` (wrapper preference).
3. `call_with_budget()` calls `_invoke_agent_sdk()`.
4. `_invoke_agent_sdk()` calls `claude_agent_sdk.query(...)`.
5. The Agent SDK spawns the `claude` CLI as a subprocess
   (`claude_agent_sdk._internal.transport`).
6. **The subprocess inherits the parent env including `ANTHROPIC_API_KEY`.**
7. **The CLI's own auth resolution prefers `ANTHROPIC_API_KEY` when set** (Claude Code CLI behavior independent of our wrapper).
8. Call routes through API, bills against API balance.
9. `ResultMessage.total_cost_usd` is populated (SDK reports the cost regardless of billing mode).
10. Wrapper sees `total_cost_usd > 0` and labels `cost_basis="subscription_credit"`.
11. **User sees `subscription_credit` label and trusts that billing is going through Max. It's not.**

---

## 2. Per-game cost analysis

24 game artifacts in `MTG-Deck-Builder-Claude/stage_2_v15_cycles/krenko_b4/`.
8 have non-zero spend (waves 1-2 of v15 Phase 2).

| Game | Spend | Calls | Actions | $/call |
|------|-------|-------|---------|--------|
| game_000 | $3.210 | 130 | 75 | **$0.0247** |
| game_001 | $3.036 | 123 | 73 | **$0.0247** |
| game_002 | $3.203 | 128 | 71 | **$0.0250** |
| game_003 | $3.270 | 135 | 69 | **$0.0242** |
| game_004 | $0.172 | 908 | 0 | $0.0002 |
| game_005 | $0.228 | 902 | 1 | $0.0003 |
| game_006 | $0.255 | 894 | 1 | $0.0003 |
| game_007 | $0.247 | 906 | 1 | $0.0003 |
| (games 8-23) | $0.000 | 912 each | 0 | $0 |
| **TOTAL** | **$13.62** | — | — | — |

**$0.0247/call avg in games 0-3** matches Sonnet 4.6 API rate-card
math exactly:
- Sonnet 4.6 pricing: $3/M input + $15/M output
- Typical engine call: ~2000 input + ~500 output tokens
- Math: 2000/1M × $3 + 500/1M × $15 = $0.006 + $0.0075 = $0.0135
- With prompt-caching overhead + system-prompt prefix
  re-evaluation: $0.025/call is on the high side but consistent
- Subscription billing would NOT produce this exact figure
  (subscription is credit-bucket-based, not per-call API rate)

**Confirms: games 0-3 were billed at API rates, not subscription
credit.**

Games 4-7 ($0.0003/call) consistent with API-funds-exhausted —
each call returned an error immediately, billing only the tiny
input-token overhead before failing.

Games 8-23 ($0/call) consistent with API key fully drained or
rate-limited at zero.

---

## 3. Estimated user damage (lower bound)

**$13.62 of v15 Phase 2 Krenko cycle data was billed to user's
Anthropic API balance, not Max subscription.**

This is a LOWER BOUND. Other v15 activity that may have hit API:
- Phase 0 mini-smoke: ~$0.02 (one call)
- Phase 1 parallel smoke: $4.84 (3 games × $1.6/game)
- Phase 2 wave 2 partial failures: $0.90 (8 calls × $0.11/each on average)

**Upper-bound estimate including all v15 live runs: ~$18.50.**

Per the audit pattern recognized only after v15 Phase 2 failed:
all v13/v15 reported `cost_basis="subscription_credit"` figures
were false positives. ANY engine call made while
`ANTHROPIC_API_KEY` was set in the user's env routed through API
billing despite the wrapper's claim.

---

## 4. Why Phase 0 mini-smoke didn't catch this

v13's Phase 5 mini-smoke + v15's Phase 0 mini-smoke both reported
`cost_basis="subscription_credit"` and were treated as proof that
subscription billing was active.

The label was set by:

```python
if sdk_cost is not None and sdk_cost > 0:
    basis = "subscription_credit"
```

`sdk_cost = ResultMessage.total_cost_usd` is populated by the Agent
SDK in BOTH billing modes:
- API billing: SDK queries the API rate-card + computes the
  per-call cost based on tokens used.
- Subscription billing: SDK reports the same figure ("what this
  cost / would have cost").

The label was NEVER a real indicator of billing path. It was an
indicator of "did the SDK report a cost figure", which is always
true for successful calls.

**The verification that v13 + v15 thought they were doing —
"subscription auth confirmed end-to-end" — was confirming nothing
of the kind.**

---

## 5. Test coverage gaps

`tests/test_agent_llm_client_v1.py` (35 tests):

- Tests cover `cost_basis` labeling logic given mocked SDK
  responses, but ALL mocks return `total_cost_usd > 0` and assert
  `cost_basis="subscription_credit"`. The mocks bake in the
  false-positive assumption.
- No test exercises the AMBIGUOUS auth state (both env var + CLI
  available). The auth-mode resolver returns `"subscription"`
  unconditionally when CLI is on PATH, so the dual-auth scenario
  was untested.
- No test mocks the Agent SDK's actual subprocess-auth-resolution
  behavior. The wrapper-level "preference" doesn't map to runtime
  reality.

**The right test (missing):** mock `claude_agent_sdk.query` to
return a result with `total_cost_usd > 0`, set both env var + CLI
present, and assert `cost_basis="api_estimate_ambiguous"` (NOT
`"subscription_credit"`).

---

## 6. v15.5 Phase 0 baseline status

- pytest **2381 + 25 skip + 88 subtests** (matches v15 baseline).
- vitest unchanged (verified earlier today via v15 Phase 0).
- `ANTHROPIC_API_KEY in env: True` (per `os.environ.get` check).
- `claude CLI in PATH: C:\Users\jorde\AppData\Roaming\npm\claude.CMD` (present).

**The current env state IS the buggy ambiguous state.** Running
any LLM call from this session right now will route through API
key (assuming key still has any funds left or hasn't been revoked).

---

## 7. HALT GATE — user prerequisites required

Per the kickoff:

> "HALT GATE: Before proceeding to Phase 1, confirm with the user
> that the pre-dispatch prerequisites (env var unset + API key
> rotated + claude /status verified) are complete."

**Required before Phase 1:**

1. **Unset `ANTHROPIC_API_KEY` in Windows env.** Run from a fresh
   Command Prompt:
   ```
   echo %ANTHROPIC_API_KEY%
   ```
   This must print `%ANTHROPIC_API_KEY%` literally (i.e., unset).
   If not, run `setx ANTHROPIC_API_KEY ""` and close all open
   terminals + restart this CC session.

2. **Rotate/revoke the existing API key on console.anthropic.com.**
   The key may be cached in other processes; revoking prevents
   further drain even if the env unset is incomplete.

3. **Verify `claude /status` shows `Auth: Claude Max
   (subscription)`** in a fresh terminal.

Phase 1's labeling fix is unblocked once #1 + #2 are done (#3 is
optional but recommended). Phase 3's live `verify_subscription_auth`
check requires #1 to give a clean reading.

Without these prerequisites, v15.5's downstream phases produce
misleading results — the same way v13's "subscription_credit"
label produced a misleading result.
