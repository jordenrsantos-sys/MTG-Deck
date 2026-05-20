"""
agent_llm_client_v1 — Pillar D iteration 2: Anthropic SDK wrapper used by
the LLM reasoning layer that sits on top of iteration 1's deterministic
deck-building skeleton.

Responsibilities (this module is intentionally narrow):
  - Single source of truth for `is_available()` — answers "can we make an
    LLM call right now?" by checking both that the `anthropic` import
    succeeded AND that the ANTHROPIC_API_KEY env var is set.
  - `call_with_budget()` — one Anthropic `messages.create()` call with a
    declared input/output token budget. Returns a structured CallResult
    that records the parsed response text, token counts, cost estimate,
    wall-clock time, error (if any), and whether the budget was exceeded.
  - Retry with exponential backoff for transient errors (429 + 5xx).
  - Pricing math for Sonnet 4.6 / Opus 4.6 / Haiku 4.5 so callers can sum
    per-build cost without re-deriving it from the docs.

Why a thin wrapper (and not just calling `anthropic.Anthropic()` directly
from each LLM call site): iteration 2 has four call sites (intent
interpreter, candidate critic, wild combo discovery, final rationale
rewrite). Centralizing budget-tracking, retry, fallback, and pricing
prevents drift across them and keeps the iteration 2 → 3 prompt-tuning
work confined to the prompt strings — not the plumbing.

Fallback contract: if `is_available()` is False, build_deck_v1 must NEVER
call any function in this module that performs a network request. It
should log a warning and proceed with iteration 1 behavior. The four
LLM-augmentation phases (B2 / C2.1 / C2.2 / D2) all guard on
`client.is_available()` before invoking `call_with_budget()`.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


logger = logging.getLogger(__name__)


LLM_CLIENT_VERSION = "agent_llm_client_v1.0"

# Default model for iteration 2. Iteration 3 will revisit (per project
# memory project_pillar_d_creative_agent_arc.md, iteration 3 is the
# Sonnet-4.6 → Opus-4.6 / Opus-4.7 upgrade arc).
DEFAULT_MODEL = "claude-sonnet-4-6"

# Per-model pricing in USD per million tokens. Used for the per-build
# cost estimate that surfaces in the response + UI. Numbers are public
# Anthropic pricing as of 2026-Q2; keep this table here so the cost
# accounting lives next to the LLM client (not scattered across the four
# call sites).
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
    response payload regardless of success or failure."""
    ok: bool
    text: str = ""                              # Raw assistant text, or empty on error.
    parsed_json: Optional[Any] = None           # JSON parsed from `text` if valid; else None.
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    latency_ms: int = 0
    model: str = ""
    budget_exceeded: bool = False               # True if input estimate exceeded `max_input_tokens`.
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    retries: int = 0
    fallback_used: bool = False                 # True if call_with_budget skipped (no API key, etc.)


# ============================================================
# Module-level client singleton + availability check.
# ============================================================


class AnthropicClient:
    """Thin wrapper around the Anthropic SDK. Instantiated once and shared
    across the four LLM call sites within a build. `is_available()` is
    cheap and idempotent — call sites can check it without paying any
    SDK-import cost.

    The client itself does NOT cache results; iteration 2 makes 4 calls
    per build, each with build-specific inputs, so caching would be
    counterproductive. (Iteration 3 may revisit if a deterministic
    'commander + bracket → intent interpretation' prefix emerges that
    benefits from prompt caching across builds.)
    """

    def __init__(self, model: str = DEFAULT_MODEL, api_key: Optional[str] = None) -> None:
        self.model = model
        # Resolve api_key + sdk lazily so a missing/late-set env var
        # doesn't crash module import.
        self._api_key = api_key
        self._client = None              # type: ignore[assignment]
        self._sdk_import_error: Optional[str] = None

        # Probe SDK availability without raising. Tests may pre-import
        # anthropic and patch it; in production, a missing SDK should
        # surface as a clean fallback rather than a build-time crash.
        try:
            import anthropic  # noqa: F401
            self._sdk_available = True
        except Exception as exc:  # pragma: no cover — defensive
            self._sdk_available = False
            self._sdk_import_error = f"{exc.__class__.__name__}: {exc}"

    # ----- availability -----

    def _resolve_api_key(self) -> Optional[str]:
        if self._api_key:
            return self._api_key
        return os.environ.get("ANTHROPIC_API_KEY")

    def is_available(self) -> bool:
        """True iff the SDK imported AND we have an API key. Cheap to
        call repeatedly. Callers MUST guard every network-invoking method
        on this — the fallback path (skip LLM augmentation, emit warning)
        is the only legal alternative."""
        return bool(self._sdk_available and self._resolve_api_key())

    def unavailable_reason(self) -> str:
        """Human-readable reason why is_available() returned False.
        Surface this in the response warnings array so users know why
        they're seeing iteration-1 behavior."""
        if not self._sdk_available:
            return (
                f"`anthropic` SDK could not be imported: {self._sdk_import_error}. "
                f"Run `pip install -r requirements.txt` (Pillar D iteration 2 added `anthropic>=0.50.0`)."
            )
        if not self._resolve_api_key():
            return (
                "ANTHROPIC_API_KEY environment variable is not set. "
                "Pillar D iteration 2 requires an Anthropic API key to run the LLM reasoning layer. "
                "Without it the agent falls back to iteration 1's deterministic-only behavior. "
                "Set ANTHROPIC_API_KEY=... in your shell and restart the server (or `launch_dev.cmd`)."
            )
        return ""

    # ----- pricing -----

    def estimate_cost_usd(self, input_tokens: int, output_tokens: int,
                          model: Optional[str] = None) -> float:
        m = model or self.model
        prices = PRICING_USD_PER_MTOK.get(m)
        if not prices:
            # Unknown model — return 0 rather than raise so the warning
            # surfaces but the build doesn't crash. Callers can detect
            # cost==0 + a token count > 0 and emit a warning if they want.
            return 0.0
        return (input_tokens / 1_000_000.0) * prices["input"] + (output_tokens / 1_000_000.0) * prices["output"]

    # ----- token estimation -----

    @staticmethod
    def estimate_input_tokens(text: str) -> int:
        """Rough pre-call estimate used by the budget check. Real token
        count comes back in `usage.input_tokens`; this is intentionally a
        conservative approximation (~3.5 chars/token for English + JSON
        prompts) so we never under-estimate and skip a call we should
        have made.

        Not a substitute for the actual tokenizer — Anthropic's tokenizer
        is BPE and ratios vary. For iteration 2 we only need this to
        bail BEFORE a call when the assembled prompt is wildly over
        budget (e.g. a 200k-token candidate dump).
        """
        if not text:
            return 0
        # ~3.5 chars/token is conservative; long technical text closer
        # to 4.0, short prose closer to 3.0. Stay conservative (smaller
        # divisor → larger estimate → more likely to fail-fast).
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
        """Make one Anthropic messages.create() call with cost/latency
        tracking, exponential-backoff retry on 429/5xx, and a pre-call
        budget guard.

        Returns a CallResult — never raises. Callers can branch on
        `result.ok` to decide whether to use the parsed output or fall
        back to iteration-1 behavior for that phase.
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
                    f"Estimated input tokens {est_input} exceeds budget {max_input_tokens}. "
                    f"Trim the prompt context (candidate pool, deck list) and retry."
                ),
                latency_ms=int((time.perf_counter() - started) * 1000),
            )

        # ---- import sdk lazily (we already verified import succeeds via is_available) ----
        import anthropic  # type: ignore

        # ---- build / cache client ----
        if self._client is None:
            try:
                self._client = anthropic.Anthropic(api_key=self._resolve_api_key())
            except Exception as exc:
                return CallResult(
                    ok=False, model=m,
                    error_code="LLM_CLIENT_INIT_FAILED",
                    error_message=f"{exc.__class__.__name__}: {exc}",
                    latency_ms=int((time.perf_counter() - started) * 1000),
                )

        # ---- retry loop ----
        retries = 0
        last_exc: Optional[BaseException] = None
        while retries <= max_retries:
            try:
                response = self._client.messages.create(
                    model=m,
                    max_tokens=max_output_tokens,
                    system=system,
                    messages=[{"role": "user", "content": user}],
                )
                # Extract text content. The SDK returns a Message with a
                # `content` list of typed blocks; we want the first text
                # block. Tool use is not supported in iteration 2 (kept
                # simple intentionally).
                text_parts: List[str] = []
                for block in getattr(response, "content", []) or []:
                    btype = getattr(block, "type", None)
                    if btype == "text":
                        text_parts.append(getattr(block, "text", "") or "")
                text = "".join(text_parts)

                usage = getattr(response, "usage", None)
                inp = int(getattr(usage, "input_tokens", 0) or 0) if usage else 0
                outp = int(getattr(usage, "output_tokens", 0) or 0) if usage else 0
                cost = self.estimate_cost_usd(inp, outp, model=m)

                parsed_json = _try_parse_json(text)

                return CallResult(
                    ok=True,
                    text=text,
                    parsed_json=parsed_json,
                    input_tokens=inp,
                    output_tokens=outp,
                    cost_usd=cost,
                    latency_ms=int((time.perf_counter() - started) * 1000),
                    model=m,
                    retries=retries,
                )

            except Exception as exc:
                last_exc = exc
                # Retry on transient errors only (rate limit / overloaded / 5xx /
                # connection drops). Other errors fall through and surface.
                if _is_retriable(exc) and retries < max_retries:
                    # Deterministic exponential backoff (no jitter). The
                    # `random` module is banned in engine runtime modules
                    # by test_no_random_imports; we don't need jitter for
                    # the small retry counts (max_retries=3 by default).
                    sleep_for = base_backoff_sec * (2 ** retries)
                    logger.warning(
                        "agent_llm_client retry %d/%d after %s: %s",
                        retries + 1, max_retries, exc.__class__.__name__, exc,
                    )
                    time.sleep(sleep_for)
                    retries += 1
                    continue
                # Permanent failure.
                code = _classify_error_code(exc)
                return CallResult(
                    ok=False, model=m,
                    error_code=code,
                    error_message=f"{exc.__class__.__name__}: {exc}",
                    latency_ms=int((time.perf_counter() - started) * 1000),
                    retries=retries,
                )

        # All retries exhausted on retriable errors.
        return CallResult(
            ok=False, model=m,
            error_code="RETRIES_EXHAUSTED",
            error_message=(
                f"Exhausted {max_retries} retries; last error: "
                f"{last_exc.__class__.__name__ if last_exc else 'unknown'}: {last_exc}"
            ),
            latency_ms=int((time.perf_counter() - started) * 1000),
            retries=retries,
        )


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
    # 1. Direct.
    try:
        return json.loads(s)
    except Exception:
        pass
    # 2. Code-fence content.
    m = _JSON_FENCE_RE.search(s)
    if m:
        try:
            return json.loads(m.group(1).strip())
        except Exception:
            pass
    # 3. Balanced-bracket extraction.
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


_RETRIABLE_ERROR_NAMES = {
    "RateLimitError",
    "APIConnectionError",
    "APITimeoutError",
    "InternalServerError",
    "OverloadedError",
    "APIStatusError",  # 5xx mapped here by older SDK versions
}


def _is_retriable(exc: BaseException) -> bool:
    name = exc.__class__.__name__
    if name in _RETRIABLE_ERROR_NAMES:
        # Some SDKs map all status errors to APIStatusError — distinguish
        # by status code where possible.
        status = getattr(exc, "status_code", None)
        if name == "APIStatusError" and isinstance(status, int):
            return status == 429 or status >= 500
        return True
    return False


def _classify_error_code(exc: BaseException) -> str:
    """Map an exception to a stable error_code string for the response.

    These codes show up in build_deck_v1 warnings so the UI can render a
    targeted message ("rate-limited; backoff", "auth failed; check key").
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
    }
    return mapping.get(name, "LLM_UNEXPECTED_ERROR")


# ============================================================
# Module-level singleton convenience.
# ============================================================


_default_client: Optional[AnthropicClient] = None


def get_default_client() -> AnthropicClient:
    """Return the process-wide AnthropicClient singleton. Initializing
    lazily so import is side-effect-free. build_deck_v1 calls this once
    per build and re-uses the same client across the four LLM phases.
    """
    global _default_client
    if _default_client is None:
        _default_client = AnthropicClient()
    return _default_client


def reset_default_client_for_tests() -> None:
    """Forget the singleton — used by unit tests that mock anthropic
    after import. Not part of the production interface."""
    global _default_client
    _default_client = None
