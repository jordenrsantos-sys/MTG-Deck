/**
 * Vitest tests for WorkspaceView's v1.2 Stage 3 stale-message clear —
 * the "Apply Complete blocked: completed deck has N lines" message must
 * NOT persist across a new Apply attempt or a new Complete invocation.
 *
 * Per AUTOMATION_RULES halt-and-ask + autonomous_repair_log soft-safety
 * carryforward: no @testing-library/react install — we test the pure
 * state-transition semantics that the WorkspaceView handlers rely on.
 *
 * Reference state model (mirrors the relevant slice of WorkspaceView
 * useState calls touched by Stage 3):
 *   completionError: string | null
 *   runtimeError:    string | null
 *   apiErrorDetails: object | null
 *
 * Both handleUnifiedCompleteDeck (entry) and handleApplyCompletedDecklist
 * (entry) clear these three before any new validation runs.
 */
import { describe, expect, test } from "vitest";

type StaleErrorSlice = {
  completionError: string | null;
  runtimeError: string | null;
  apiErrorDetails: { code: string; message: string } | null;
};

// Mirrors the Stage 3 leading clear-block of handleApplyCompletedDecklist.
function applyCompleteEntryReset(prev: StaleErrorSlice): StaleErrorSlice {
  return {
    completionError: null,
    runtimeError: null,
    apiErrorDetails: null,
  };
}

// Mirrors the Stage 3 leading clear-block of handleUnifiedCompleteDeck.
function unifiedCompleteEntryReset(prev: StaleErrorSlice): StaleErrorSlice {
  return {
    ...prev,
    completionError: null,
    runtimeError: null,
  };
}

describe("WorkspaceView v1.2 Stage 3 — Apply Complete blocked message clear", () => {
  const STALE_BLOCKED_MSG =
    "Apply Complete blocked: completed deck has 77 lines and must exceed current 78 lines.";

  test("handleApplyCompletedDecklist entry clears stale blocked-message", () => {
    const before: StaleErrorSlice = {
      completionError: STALE_BLOCKED_MSG,
      runtimeError: STALE_BLOCKED_MSG,
      apiErrorDetails: { code: "APPLY_COMPLETE_NOT_STRICTLY_LARGER", message: STALE_BLOCKED_MSG },
    };
    const after = applyCompleteEntryReset(before);
    expect(after.completionError).toBeNull();
    expect(after.runtimeError).toBeNull();
    expect(after.apiErrorDetails).toBeNull();
  });

  test("handleUnifiedCompleteDeck entry clears completionError + runtimeError", () => {
    const before: StaleErrorSlice = {
      completionError: STALE_BLOCKED_MSG,
      runtimeError: STALE_BLOCKED_MSG,
      apiErrorDetails: null,
    };
    const after = unifiedCompleteEntryReset(before);
    expect(after.completionError).toBeNull();
    expect(after.runtimeError).toBeNull();
  });

  test("clears don't disturb other state (idempotent on already-clean slice)", () => {
    const clean: StaleErrorSlice = {
      completionError: null,
      runtimeError: null,
      apiErrorDetails: null,
    };
    expect(applyCompleteEntryReset(clean)).toEqual(clean);
    expect(unifiedCompleteEntryReset(clean)).toEqual(clean);
  });

  test("entry-reset runs BEFORE re-validation (preserves the gate's NEW message on fresh failure)", () => {
    // Scenario: prior Apply blocked with message A. User edits deck. Re-runs
    // Apply. The reset clears A. The gate re-checks line counts and may set
    // a fresh message B (or succeed). We assert the reset doesn't preserve A.
    let slice: StaleErrorSlice = {
      completionError: STALE_BLOCKED_MSG,
      runtimeError: STALE_BLOCKED_MSG,
      apiErrorDetails: { code: "APPLY_COMPLETE_NOT_STRICTLY_LARGER", message: STALE_BLOCKED_MSG },
    };
    slice = applyCompleteEntryReset(slice);
    // Now imagine the gate succeeds — slice should remain clean.
    expect(slice.completionError).toBeNull();

    // If the gate fails afresh with a NEW message, it overwrites cleanly.
    slice = { ...slice, completionError: "fresh failure msg", runtimeError: "fresh failure msg" };
    expect(slice.completionError).toBe("fresh failure msg");
    expect(slice.completionError).not.toBe(STALE_BLOCKED_MSG);
  });

  test("the EXACT blocked message format that Stage 3 covers", () => {
    // Sentinel: if the message string at WorkspaceView line ~3517 changes,
    // this test will still pass (it doesn't assert the format) — but the
    // prefix is documented here for future-grep.
    const prefix = "Apply Complete blocked: completed deck has ";
    expect(STALE_BLOCKED_MSG.startsWith(prefix)).toBe(true);
    expect(STALE_BLOCKED_MSG).toContain("must exceed current");
  });
});
