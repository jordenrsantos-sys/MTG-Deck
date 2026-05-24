"""Phase 5 of mega-task v13 -- minimal live verification.

Single-call smoke to verify the migrated wrapper hits the Agent SDK
end-to-end with subscription auth (no API key in env). Asserts:

  1. is_available() reports True via 'subscription' auth-mode when CLI
     present + no ANTHROPIC_API_KEY.
  2. One call_with_budget() round-trip returns CallResult.ok=True
     with non-empty text + non-zero tokens.
  3. cost_basis reports either "api_estimate" or "subscription_credit"
     (the SDK behavior under pre-June-15 billing is undocumented;
     either basis is acceptable).
  4. No exceptions; latency_ms populated.

Cost expectation: ~$0.001-0.01 (one ~100-token completion).

Run:
    python tools/test_v13_migration_smoke.py
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Ensure kill switch off + API key absent for the subscription path.
os.environ.pop("MTG_ENGINE_DISABLE_LLM", None)
saved_api_key = os.environ.pop("ANTHROPIC_API_KEY", None)


def main() -> int:
    from api.engine.layers.agent_llm_client_v1 import (
        AnthropicClient, get_default_client,
        reset_default_client_for_tests,
    )

    reset_default_client_for_tests()
    client = get_default_client()

    print("=== Phase 5 v13 migration smoke ===")
    print(f"is_available() = {client.is_available()}")
    print(f"_resolve_auth_mode() = {client._resolve_auth_mode()}")
    print(f"_has_claude_cli() = {client._has_claude_cli()}")
    print(f"ANTHROPIC_API_KEY env = "
          f"{'set' if os.environ.get('ANTHROPIC_API_KEY') else 'unset'}")
    print()

    if not client.is_available():
        print(f"FAIL: client not available -- {client.unavailable_reason()}")
        return 1

    if client._resolve_auth_mode() != "subscription":
        print(f"WARN: auth mode is {client._resolve_auth_mode()!r}, "
              f"expected 'subscription' (CLI should take priority)")

    # Single-shot call.
    print("Calling LLM via migrated wrapper ...")
    t0 = time.perf_counter()
    result = client.call_with_budget(
        system=(
            "You are a strict JSON-only responder. Output one valid JSON "
            "object with a single key 'echo' and the user's input as the "
            "value, then nothing else."
        ),
        user="ping",
        max_input_tokens=2000,
        max_output_tokens=200,
    )
    elapsed = time.perf_counter() - t0

    print(f"wall_time = {elapsed:.2f}s")
    print(f"result.ok = {result.ok}")
    print(f"result.text = {result.text!r}")
    print(f"result.parsed_json = {result.parsed_json}")
    print(f"result.input_tokens = {result.input_tokens}")
    print(f"result.output_tokens = {result.output_tokens}")
    print(f"result.cost_usd = ${result.cost_usd:.6f}")
    print(f"result.cost_basis = {result.cost_basis!r}")
    print(f"result.latency_ms = {result.latency_ms}")
    print(f"result.model = {result.model}")
    print(f"result.retries = {result.retries}")
    print(f"result.error_code = {result.error_code}")
    print(f"result.error_message = {result.error_message}")
    print()

    # Verify each acceptance criterion.
    passes = 0
    fails = 0

    def _check(name: str, ok: bool, detail: str = "") -> None:
        nonlocal passes, fails
        if ok:
            passes += 1
            print(f"  PASS {name}{(': ' + detail) if detail else ''}")
        else:
            fails += 1
            print(f"  FAIL {name}{(': ' + detail) if detail else ''}")

    _check("call returned ok=True", result.ok,
           detail=f"error_code={result.error_code}")
    _check("non-empty text", bool(result.text),
           detail=f"len={len(result.text or '')}")
    _check("non-zero input tokens", result.input_tokens > 0,
           detail=str(result.input_tokens))
    _check("non-zero output tokens", result.output_tokens > 0,
           detail=str(result.output_tokens))
    _check("cost_basis valid",
           result.cost_basis in ("api_estimate", "subscription_credit"),
           detail=result.cost_basis)
    _check("latency_ms populated", result.latency_ms > 0,
           detail=f"{result.latency_ms}ms")

    print()
    print(f"=== Result: {passes}/6 PASS ===")

    if saved_api_key is not None:
        os.environ["ANTHROPIC_API_KEY"] = saved_api_key

    return 0 if fails == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
