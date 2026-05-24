"""
agent_llm_client_v1 — LLM client wrapper for Pillar D iteration 2+
and the iter-10/11/12 substrate + policy + playtest layers.

Mega-task v13 (2026-05-24) migrated the underlying SDK from the
`anthropic` package (API key auth) to `claude-agent-sdk` (Claude
Code subscription auth via local `~/.claude/` credentials, with
ANTHROPIC_API_KEY fallback).

The migration is plumbing-only. The CallResult contract is preserved
modulo a new `cost_basis` field. All 10 production call sites that
route through `get_default_client().call_with_budget(...)` pick up
the migration transparently.

Responsibilities (this module is intentionally narrow):
  - `is_available()` — answers "can we make an LLM call right now?"
    by checking that the Agent SDK imported AND either the Claude
    Code CLI is reachable OR ANTHROPIC_API_KEY is set as fallback.
  - `call_with_budget()` — one single-shot completion via the Agent
    SDK's `query()` async iterator. Returns a structured CallResult.
    Sync surface preserved for the engine's sync call sites; uses
    `asyncio.run()` once per call internally.
  - Retry with exponential backoff for transient errors (rate limit,
    server errors).
  - Pricing math (api_estimate) — useful as a benchmark even when
    subscription billing pays. The new `cost_basis` field tells
    consumers whether the `cost_usd` figure came from the SDK's
    `total_cost_usd` (subscription_credit) or our local table
    (api_estimate).

Fallback contract (UNCHANGED): if `is_available()` is False,
build_deck_v1 must NEVER call any function in this module that
performs a network request. It should log a warning and proceed
with iteration 1 behavior.

Kill switch (UNCHANGED): `MTG_ENGINE_DISABLE_LLM=1` forces
`is_available()` to False. Used by tests/conftest.py to prevent
stray real API calls.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


logger = logging.getLogger(__name__)


LLM_CLIENT_VERSION = "agent_llm_client_v1.1"  # v13 migration bump

DEFAULT_MODEL = "claude-sonnet-4-6"

# Per-model pricing in USD per million tokens. Used for the per-build
# cost estimate that surfaces in the response + UI. Numbers are public
# Anthropic pricing as of 2026-Q2. Post-v13 migration: this is now an
# "api_estimate" figure -- the actual billing under subscription auth
# is reported by the SDK via ResultMessage.total_cost_usd, surfaced in
# CallResult.cost_usd with cost_basis="subscription_credit" when
# available. The estimate is kept as a benchmark + fallback when the
# SDK does not surface a cost figure.
PRICING_USD_PER_MTOK: Dict[str, Dict[str, float]] = {
    "claude-sonnet-4-6": {"input": 3.00, "output": 15.00},
    "claude-opus-4-6":   {"input": 5.00, "output": 25.00},
    "claude-opus-4-7":   {"input": 5.00, "output": 25.00},
    "claude-haiku-4-5":  {"input": 1.00,  "output": 5.00},
}


# ============================================================
# Public dataclasses.
# ============================================================


@dataclass
class CallResult:
    """Result of a single LLM call. Always has all fields populated so
    callers can blindly forward the cost / token / latency fields to the
    response payload regardless of success or failure.

    v13 migration added `cost_basis` field to distinguish API-rate
    estimates (legacy) from real subscription-credit charges (Agent
    SDK ResultMessage.total_cost_usd)."""
    ok: bool
    text: str = ""
    parsed_json: Optional[Any] = None
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    cost_basis: str = "api_estimate"            # NEW v13: "api_estimate" | "subscription_credit"
    latency_ms: int = 0
    model: str = ""
    budget_exceeded: bool = False
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    retries: int = 0
    fallback_used: bool = False


# ============================================================
# Module-level client singleton + availability check.
# ============================================================


class AnthropicClient:
    """Wrapper around the Claude Agent SDK (subscription auth) with
    ANTHROPIC_API_KEY fallback. Class name retained from iter-2 for
    backwards compatibility with existing test imports + module
    references; functionally this is the Agent SDK client now.

    v13 migration: underlying SDK is `claude_agent_sdk.query()` instead
    of `anthropic.Anthropic().messages.create()`. CallResult contract
    preserved modulo the new cost_basis field.
    """

    def __init__(self, model: str = DEFAULT_MODEL,
                 api_key: Optional[str] = None) -> None:
        self.model = model
        # Resolve api_key lazily so a missing/late-set env var doesn't
        # crash module import.
        self._api_key = api_key
        self._sdk_import_error: Optional[str] = None
        # Probe SDK availability without raising. Tests may pre-import
        # the SDK and patch it; in production, a missing SDK should
        # surface as a clean fallback rather than a build-time crash.
        try:
            import claude_agent_sdk  # noqa: F401
            self._sdk_available = True
        except Exception as exc:  # pragma: no cover -- defensive
            self._sdk_available = False
            self._sdk_import_error = f"{exc.__class__.__name__}: {exc}"

    # ----- availability -----

    def _resolve_api_key(self) -> Optional[str]:
        if self._api_key:
            return self._api_key
        return os.environ.get("ANTHROPIC_API_KEY")

    def _has_claude_cli(self) -> bool:
        """True if the `claude` CLI is reachable on PATH. The Agent SDK
        invokes it as a subprocess transport; without it the SDK can't
        complete a call even if subscription credentials exist."""
        return shutil.which("claude") is not None

    def _resolve_auth_mode(self) -> str:
        """Returns 'subscription' if Claude Code CLI is reachable
        (preferred for v13 migration), 'api_key' if ANTHROPIC_API_KEY
        is set (fallback), or 'none' if neither."""
        if self._has_claude_cli():
            return "subscription"
        if self._resolve_api_key():
            return "api_key"
        return "none"

    def is_available(self) -> bool:
        """True iff the SDK imported AND at least one auth path works.

        Auth paths checked, in priority order:
          1. Claude Code CLI (`claude` in PATH) -- subscription auth
          2. ANTHROPIC_API_KEY env var -- API-key fallback

        Cheap to call repeatedly. Callers MUST guard every
        network-invoking method on this -- the fallback path (skip LLM
        augmentation, emit warning) is the only legal alternative.

        Kill switch (UNCHANGED): setting MTG_ENGINE_DISABLE_LLM=1 in
        the environment forces is_available() to False even if
        everything else is set. Used by tests/conftest.py.
        """
        if os.environ.get("MTG_ENGINE_DISABLE_LLM") == "1":
            return False
        if not self._sdk_available:
            return False
        return self._resolve_auth_mode() != "none"

    def unavailable_reason(self) -> str:
        """Human-readable reason why is_available() returned False.
        Surface this in the response warnings array so users know why
        they're seeing iteration-1 behavior."""
        if os.environ.get("MTG_ENGINE_DISABLE_LLM") == "1":
            return (
                "MTG_ENGINE_DISABLE_LLM=1 is set; the LLM reasoning "
                "layer is disabled by this kill switch. Unset the env "
                "var to re-enable."
            )
        if not self._sdk_available:
            return (
                f"`claude_agent_sdk` could not be imported: "
                f"{self._sdk_import_error}. Run `pip install -r "
                f"requirements.txt`."
            )
        auth = self._resolve_auth_mode()
        if auth == "none":
            return (
                "Neither Claude Code CLI nor ANTHROPIC_API_KEY is "
                "available. Sub-mega-task v13 migrated the LLM client "
                "to the Claude Agent SDK -- preferred auth is via the "
                "Claude Code CLI (run `claude login`). Without that, "
                "set ANTHROPIC_API_KEY=... in your shell as fallback. "
                "Without either, the agent falls back to iteration 1's "
                "deterministic-only behavior."
            )
        return ""

    # ----- pricing -----

    def estimate_cost_usd(self, input_tokens: int, output_tokens: int,
                          model: Optional[str] = None) -> float:
        m = model or self.model
        prices = PRICING_USD_PER_MTOK.get(m)
        if not prices:
            return 0.0
        return (input_tokens / 1_000_000.0) * prices["input"] + (output_tokens / 1_000_000.0) * prices["output"]

    # ----- token estimation -----

    @staticmethod
    def estimate_input_tokens(text: str) -> int:
        """Rough pre-call estimate (~3.5 chars/token conservative).
        Real token count comes back in `usage.input_tokens`.

        Not a substitute for the actual tokenizer.
        """
        if not text:
            return 0
        return max(1, int(len(text) / 3.5))

    # ----- the workhorse call -----

    def call_with_budget(
        self,
        *,
        system: str,
        user: str,
        max_input_tokens: int,
        max_output_tokens: int,
        model: Optional[str] = None,
        max_retries: int = 3,
        base_backoff_sec: float = 0.5,
    ) -> CallResult:
        """One Agent SDK `query()` call with cost/latency tracking,
        exponential-backoff retry on rate-limit / server-error, and a
        pre-call budget guard.

        Returns a CallResult -- never raises. Callers can branch on
        `result.ok` to decide whether to use the parsed output or fall
        back to deterministic behavior for that phase.
        """
        m = model or self.model
        started = time.perf_counter()

        if not self.is_available():
            return CallResult(
                ok=False, model=m, fallback_used=True,
                error_code="LLM_UNAVAILABLE",
                error_message=self.unavailable_reason(),
            )

        # ---- pre-call budget guard ----
        est_input = self.estimate_input_tokens(system) + self.estimate_input_tokens(user)
        if est_input > max_input_tokens:
            return CallResult(
                ok=False, model=m,
                budget_exceeded=True,
                input_tokens=est_input,
                error_code="INPUT_TOKEN_BUDGET_EXCEEDED",
                error_message=(
                    f"Estimated input tokens {est_input} exceeds budget "
                    f"{max_input_tokens}. Trim the prompt context "
                    f"(candidate pool, deck list) and retry."
                ),
                latency_ms=int((time.perf_counter() - started) * 1000),
            )

        # ---- compute max_budget_usd from max_output_tokens cap ----
        # The Agent SDK uses USD budget instead of token cap. We back
        # into one from the legacy max_output_tokens contract using
        # output-side pricing, with a 2x safety multiplier so the SDK
        # doesn't terminate just below the requested output size.
        max_budget = self.estimate_cost_usd(0, max_output_tokens * 2, model=m)
        if max_budget <= 0:
            # Unknown model -> permissive fallback cap so the call can
            # proceed (errors will surface naturally if cost explodes).
            max_budget = 1.0

        # ---- retry loop ----
        retries = 0
        last_error_code: Optional[str] = None
        last_error_message: Optional[str] = None
        while retries <= max_retries:
            try:
                raw_result = asyncio.run(_invoke_agent_sdk(
                    system=system, user=user, model=m,
                    max_budget_usd=max_budget,
                ))
            except Exception as exc:
                last_error_code = _classify_error_code(exc)
                last_error_message = f"{exc.__class__.__name__}: {exc}"
                if _is_retriable(exc) and retries < max_retries:
                    sleep_for = base_backoff_sec * (2 ** retries)
                    logger.warning(
                        "agent_llm_client retry %d/%d after %s: %s",
                        retries + 1, max_retries,
                        exc.__class__.__name__, exc,
                    )
                    time.sleep(sleep_for)
                    retries += 1
                    continue
                return CallResult(
                    ok=False, model=m,
                    error_code=last_error_code,
                    error_message=last_error_message,
                    latency_ms=int((time.perf_counter() - started) * 1000),
                    retries=retries,
                )

            # The async invocation returned a structured dict; convert
            # to CallResult.
            if raw_result.get("error_category"):
                # API-level error came back via AssistantMessage.error.
                err_code = _classify_agent_sdk_error(raw_result["error_category"])
                err_msg = raw_result.get("error_message") or err_code
                if _is_retriable_agent_sdk_error(raw_result["error_category"]) \
                        and retries < max_retries:
                    sleep_for = base_backoff_sec * (2 ** retries)
                    logger.warning(
                        "agent_llm_client retry %d/%d after %s: %s",
                        retries + 1, max_retries, err_code, err_msg,
                    )
                    time.sleep(sleep_for)
                    retries += 1
                    last_error_code = err_code
                    last_error_message = err_msg
                    continue
                return CallResult(
                    ok=False, model=m,
                    error_code=err_code,
                    error_message=err_msg,
                    latency_ms=int((time.perf_counter() - started) * 1000),
                    retries=retries,
                    input_tokens=raw_result.get("input_tokens", 0),
                    output_tokens=raw_result.get("output_tokens", 0),
                )

            text = raw_result.get("text", "")
            inp = int(raw_result.get("input_tokens", 0) or 0)
            outp = int(raw_result.get("output_tokens", 0) or 0)
            # Cost basis: prefer the SDK's reported total_cost_usd if
            # present (subscription_credit); fall back to api_estimate.
            sdk_cost = raw_result.get("total_cost_usd")
            if sdk_cost is not None and sdk_cost > 0:
                cost = float(sdk_cost)
                basis = "subscription_credit"
            else:
                cost = self.estimate_cost_usd(inp, outp, model=m)
                basis = "api_estimate"

            return CallResult(
                ok=True,
                text=text,
                parsed_json=_try_parse_json(text),
                input_tokens=inp,
                output_tokens=outp,
                cost_usd=cost,
                cost_basis=basis,
                latency_ms=int((time.perf_counter() - started) * 1000),
                model=m,
                retries=retries,
            )

        # All retries exhausted on retriable errors.
        return CallResult(
            ok=False, model=m,
            error_code=last_error_code or "RETRIES_EXHAUSTED",
            error_message=(
                last_error_message or
                f"Exhausted {max_retries} retries with no successful response."
            ),
            latency_ms=int((time.perf_counter() - started) * 1000),
            retries=retries,
        )


# ============================================================
# Agent SDK async invocation helper.
# ============================================================


async def _invoke_agent_sdk(
    *, system: str, user: str, model: str, max_budget_usd: float,
) -> Dict[str, Any]:
    """One async invocation of `claude_agent_sdk.query()`. Walks the
    async iterator, accumulates AssistantMessage TextBlocks, captures
    error categorization, and reads ResultMessage usage + cost.

    Returns a dict with keys: text, input_tokens, output_tokens,
    total_cost_usd, error_category, error_message, api_error_status,
    stop_reason.

    Raises (passing through to caller's retry loop):
      - CLINotFoundError if `claude` CLI missing
      - CLIConnectionError if subprocess fails
      - Other ClaudeSDKError variants
    """
    from claude_agent_sdk import (
        query, ClaudeAgentOptions,
        AssistantMessage, ResultMessage,
    )
    from claude_agent_sdk.types import TextBlock

    options = ClaudeAgentOptions(
        system_prompt=system,
        model=model,
        max_turns=1,                # single-shot completion semantics
        allowed_tools=[],           # no tool surface -- pure completion
        permission_mode="bypassPermissions",
        max_budget_usd=max_budget_usd,
    )

    text_parts: List[str] = []
    input_tokens = 0
    output_tokens = 0
    total_cost_usd: Optional[float] = None
    error_category: Optional[str] = None
    error_message: Optional[str] = None
    api_error_status: Optional[int] = None
    stop_reason: Optional[str] = None

    async for msg in query(prompt=user, options=options):
        if isinstance(msg, AssistantMessage):
            for block in msg.content:
                if isinstance(block, TextBlock):
                    text_parts.append(block.text or "")
            if msg.error:
                error_category = msg.error
            if msg.usage:
                # Usage may appear here per-message; we'll prefer
                # ResultMessage.usage if it arrives later (more
                # authoritative).
                input_tokens = max(
                    input_tokens,
                    int(msg.usage.get("input_tokens", 0) or 0),
                )
                output_tokens = max(
                    output_tokens,
                    int(msg.usage.get("output_tokens", 0) or 0),
                )
            if msg.stop_reason:
                stop_reason = msg.stop_reason
        elif isinstance(msg, ResultMessage):
            if msg.total_cost_usd is not None:
                total_cost_usd = msg.total_cost_usd
            if msg.is_error and not error_category:
                error_category = "unknown"
            if msg.api_error_status is not None:
                api_error_status = msg.api_error_status
            if msg.usage:
                # ResultMessage.usage is authoritative; override.
                rm_in = int(msg.usage.get("input_tokens", 0) or 0)
                rm_out = int(msg.usage.get("output_tokens", 0) or 0)
                if rm_in > 0:
                    input_tokens = rm_in
                if rm_out > 0:
                    output_tokens = rm_out
            if msg.errors:
                error_message = "; ".join(msg.errors)
            if msg.result and not text_parts:
                # Some SDK paths put the final text in ResultMessage.result
                # rather than AssistantMessage.content.
                text_parts.append(msg.result)

    return {
        "text": "".join(text_parts),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_cost_usd": total_cost_usd,
        "error_category": error_category,
        "error_message": error_message,
        "api_error_status": api_error_status,
        "stop_reason": stop_reason,
    }


# ============================================================
# Helpers (free functions, exposed for unit testing).
# ============================================================


_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)


def _try_parse_json(text: str) -> Optional[Any]:
    """Best-effort JSON extraction from an LLM response.

    Models sometimes wrap JSON in ```json fences``` or prefix it with
    "Here's the JSON:". We try three increasingly forgiving strategies:
      1. Parse the raw text.
      2. Parse the content inside the FIRST ```...``` fence.
      3. Find the first '{' or '[' and parse until the matching balanced bracket.
    Returns parsed object or None.
    """
    if not text:
        return None
    s = text.strip()
    try:
        return json.loads(s)
    except Exception:
        pass
    m = _JSON_FENCE_RE.search(s)
    if m:
        try:
            return json.loads(m.group(1).strip())
        except Exception:
            pass
    for opener, closer in (("{", "}"), ("[", "]")):
        start = s.find(opener)
        if start == -1:
            continue
        depth = 0
        in_str = False
        esc = False
        for i in range(start, len(s)):
            ch = s[i]
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    in_str = False
                continue
            if ch == '"':
                in_str = True
            elif ch == opener:
                depth += 1
            elif ch == closer:
                depth -= 1
                if depth == 0:
                    candidate = s[start:i + 1]
                    try:
                        return json.loads(candidate)
                    except Exception:
                        break
    return None


# ----- Legacy exception-name-based classification -----
# Kept for backwards compat with tests that pass synthetic Exception
# subclasses by name. Real Agent SDK errors flow through
# _classify_agent_sdk_error + _is_retriable_agent_sdk_error below.

_RETRIABLE_ERROR_NAMES = {
    "RateLimitError",
    "APIConnectionError",
    "APITimeoutError",
    "InternalServerError",
    "OverloadedError",
    "APIStatusError",
    # v13: Agent SDK transport-level retriable errors
    "CLIConnectionError",
    "ProcessError",
}


def _is_retriable(exc: BaseException) -> bool:
    name = exc.__class__.__name__
    if name in _RETRIABLE_ERROR_NAMES:
        status = getattr(exc, "status_code", None)
        if name == "APIStatusError" and isinstance(status, int):
            return status == 429 or status >= 500
        return True
    return False


def _classify_error_code(exc: BaseException) -> str:
    """Map an exception class name to a stable error_code string.

    Legacy contract: stable mapping for the old `anthropic` SDK
    exception types. The Agent SDK migration in v13 surfaces errors
    differently (via AssistantMessage.error enum) -- see
    `_classify_agent_sdk_error` below.
    """
    name = exc.__class__.__name__
    mapping = {
        "AuthenticationError": "LLM_AUTH_FAILED",
        "PermissionDeniedError": "LLM_PERMISSION_DENIED",
        "NotFoundError": "LLM_MODEL_NOT_FOUND",
        "BadRequestError": "LLM_BAD_REQUEST",
        "RateLimitError": "LLM_RATE_LIMITED",
        "APITimeoutError": "LLM_TIMEOUT",
        "APIConnectionError": "LLM_CONNECTION_FAILED",
        "InternalServerError": "LLM_SERVER_ERROR",
        "OverloadedError": "LLM_OVERLOADED",
        # v13: Agent SDK exceptions
        "CLINotFoundError": "LLM_CLI_MISSING",
        "CLIConnectionError": "LLM_CONNECTION_FAILED",
        "CLIJSONDecodeError": "LLM_BAD_RESPONSE",
        "ProcessError": "LLM_SERVER_ERROR",
        "ClaudeSDKError": "LLM_SDK_ERROR",
    }
    return mapping.get(name, "LLM_UNEXPECTED_ERROR")


# ----- Agent SDK error-enum classification (v13) -----

# Agent SDK's AssistantMessage.error is one of:
# 'authentication_failed' | 'billing_error' | 'rate_limit' |
# 'invalid_request' | 'server_error' | 'unknown' | None
_AGENT_SDK_ERROR_MAP: Dict[str, str] = {
    "authentication_failed": "LLM_AUTH_FAILED",
    "billing_error":         "LLM_BILLING_ERROR",
    "rate_limit":            "LLM_RATE_LIMITED",
    "invalid_request":       "LLM_BAD_REQUEST",
    "server_error":          "LLM_SERVER_ERROR",
    "unknown":               "LLM_UNEXPECTED_ERROR",
}

_AGENT_SDK_RETRIABLE_CATEGORIES = {
    "rate_limit",
    "server_error",
}


def _classify_agent_sdk_error(category: str) -> str:
    """Map an Agent SDK error category enum to a stable error_code."""
    return _AGENT_SDK_ERROR_MAP.get(category, "LLM_UNEXPECTED_ERROR")


def _is_retriable_agent_sdk_error(category: str) -> bool:
    return category in _AGENT_SDK_RETRIABLE_CATEGORIES


# ============================================================
# Module-level singleton convenience.
# ============================================================


# Alias for v13 callers who want to reference the migrated class name
# explicitly. AnthropicClient remains the canonical name for backwards
# compat with all existing import sites + tests.
AgentSdkClient = AnthropicClient


_default_client: Optional[AnthropicClient] = None


def get_default_client() -> AnthropicClient:
    """Return the process-wide LLM-client singleton. Initializes
    lazily so import is side-effect-free.
    """
    global _default_client
    if _default_client is None:
        _default_client = AnthropicClient()
    return _default_client


def reset_default_client_for_tests() -> None:
    """Forget the singleton. Used by tests that mock the SDK after
    import. Not part of the production interface."""
    global _default_client
    _default_client = None
