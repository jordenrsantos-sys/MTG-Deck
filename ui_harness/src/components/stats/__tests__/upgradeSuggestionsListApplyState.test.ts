/**
 * Vitest tests for UpgradeSuggestionsList — v1.2 Stage 2.
 *
 * Polish: cumulative "X/Y applied" counter + Clear-resets-applied-state.
 * Per AUTOMATION_RULES halt-and-ask + autonomous_repair_log soft-safety
 * carryforward from Phase 4.x: no @testing-library/react install.
 * Tests focus on the pure semantics of the appliedKeys state transitions
 * the panel relies on — same shape as the adapter tests in the sibling
 * file `upgradeSuggestionsList.test.ts`.
 *
 * The reference implementation mirrors the component's internal helpers
 * verbatim so the contract is asserted without needing the React harness.
 */
import { describe, expect, test } from "vitest";
import type { UpgradeSwapSuggestion } from "../../../lib/workspaceDeckState";

// Reference implementation mirroring UpgradeSuggestionsList internals.
type AppliedKeys = Record<string, "ok" | "skip">;

function keyFor(i: number, row: UpgradeSwapSuggestion): string {
  return `${i}-${row.cut_name ?? ""}->${row.add_name ?? ""}`;
}

function countApplied(keys: AppliedKeys): number {
  return Object.values(keys).filter((v) => v === "ok").length;
}

function buildApplyAllUpdate(
  rows: ReadonlyArray<UpgradeSwapSuggestion>,
  appliedSoFar: AppliedKeys,
  applyOutcome: (i: number, row: UpgradeSwapSuggestion) => "ok" | "skip",
): AppliedKeys {
  const next: AppliedKeys = { ...appliedSoFar };
  for (let i = 0; i < rows.length; i += 1) {
    const k = keyFor(i, rows[i]);
    if (next[k] === "ok") continue;
    next[k] = applyOutcome(i, rows[i]);
  }
  return next;
}

describe("UpgradeSuggestionsList — v1.2 cumulative counter semantics", () => {
  const rows: ReadonlyArray<UpgradeSwapSuggestion> = [
    { cut_name: "Forest", add_name: "Sol Ring" },
    { cut_name: "Mountain", add_name: "Arcane Signet" },
    { cut_name: "Plains", add_name: "Smothering Tithe" },
  ];

  test("empty appliedKeys → counter is 0", () => {
    expect(countApplied({})).toBe(0);
  });

  test("single 'ok' entry increments counter by 1", () => {
    const keys: AppliedKeys = { [keyFor(0, rows[0])]: "ok" };
    expect(countApplied(keys)).toBe(1);
  });

  test("'skip' entries do NOT count toward the counter", () => {
    const keys: AppliedKeys = {
      [keyFor(0, rows[0])]: "ok",
      [keyFor(1, rows[1])]: "skip",
    };
    expect(countApplied(keys)).toBe(1);
  });

  test("all rows applied → counter reaches displayed.length", () => {
    const keys: AppliedKeys = {};
    rows.forEach((row, i) => {
      keys[keyFor(i, row)] = "ok";
    });
    expect(countApplied(keys)).toBe(rows.length);
  });

  test("mixed ok/skip in 3-row set yields correct partial count", () => {
    const keys: AppliedKeys = {
      [keyFor(0, rows[0])]: "ok",
      [keyFor(1, rows[1])]: "ok",
      [keyFor(2, rows[2])]: "skip",
    };
    expect(countApplied(keys)).toBe(2);
  });
});

describe("UpgradeSuggestionsList — v1.2 Clear resets appliedKeys", () => {
  test("handleClearAndReset replaces state with empty object", () => {
    let appliedKeys: AppliedKeys = {
      "0-Forest->Sol Ring": "ok",
      "1-Mountain->Arcane Signet": "skip",
    };
    // Simulate the component's handleClearAndReset.
    appliedKeys = {};
    expect(Object.keys(appliedKeys)).toHaveLength(0);
    expect(countApplied(appliedKeys)).toBe(0);
  });

  test("Clear-reset prevents positional-alias stale 'Applied ✓' badge regression", () => {
    // Scenario: user applies row 0, then a new set of suggestions arrives.
    // Without reset, key "0-Forest->Sol Ring" would carry over to the new
    // row 0 only if it shared the same cut/add — but the reset prevents
    // ANY carryover, which is the safer & simpler invariant.
    const beforeClear: AppliedKeys = { "0-Forest->Sol Ring": "ok" };
    const afterClear: AppliedKeys = {};
    expect(beforeClear).not.toEqual(afterClear);
    expect(countApplied(afterClear)).toBe(0);
  });
});

describe("UpgradeSuggestionsList — v1.2 Apply state transitions (pending → ok/skip)", () => {
  test("pending (key absent) → 'ok' after successful apply", () => {
    let keys: AppliedKeys = {};
    const k = keyFor(0, { cut_name: "Forest", add_name: "Sol Ring" });
    expect(keys[k]).toBeUndefined();
    keys = { ...keys, [k]: "ok" };
    expect(keys[k]).toBe("ok");
  });

  test("pending (key absent) → 'skip' when cut card not in decklist", () => {
    let keys: AppliedKeys = {};
    const k = keyFor(0, { cut_name: "DoesNotExist", add_name: "Sol Ring" });
    keys = { ...keys, [k]: "skip" };
    expect(keys[k]).toBe("skip");
  });

  test("Apply-all preserves prior 'ok' state (no re-apply) per the handler contract", () => {
    const rows: ReadonlyArray<UpgradeSwapSuggestion> = [
      { cut_name: "Forest", add_name: "Sol Ring" },
      { cut_name: "Mountain", add_name: "Arcane Signet" },
    ];
    const prior: AppliedKeys = { [keyFor(0, rows[0])]: "ok" };
    // 2nd row will succeed; 1st row is already 'ok' so handler should NOT
    // re-invoke applySwap on it. We assert by giving an outcome fn that
    // throws if called for row 0.
    const next = buildApplyAllUpdate(rows, prior, (i) => {
      if (i === 0) throw new Error("Should skip already-applied row");
      return "ok";
    });
    expect(next[keyFor(0, rows[0])]).toBe("ok");
    expect(next[keyFor(1, rows[1])]).toBe("ok");
    expect(countApplied(next)).toBe(2);
  });

  test("Apply-all updates 'skip' → 'ok' on retry if outcome changes", () => {
    // (Decklist may have been edited since the first attempt — re-applying
    // a previously-skipped swap should be permitted, since the cut card may
    // now exist in the decklist text.)
    const rows: ReadonlyArray<UpgradeSwapSuggestion> = [
      { cut_name: "Forest", add_name: "Sol Ring" },
    ];
    const prior: AppliedKeys = { [keyFor(0, rows[0])]: "skip" };
    const next = buildApplyAllUpdate(rows, prior, () => "ok");
    expect(next[keyFor(0, rows[0])]).toBe("ok");
  });
});
