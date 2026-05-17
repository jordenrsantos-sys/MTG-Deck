/**
 * Vitest tests for v1.4 Stage 2 — legacy "Added cards: 0 / Added lands: 0"
 * chips do NOT render under v1.4's prop-omission strategy.
 *
 * Background: DeckEditorPanel.tsx lines 1113-1114 conditionally render two
 * chips:
 *   {normalizedCompletionAddedCards !== null ? <span>Added cards: ...</span> : null}
 *   {normalizedCompletionLandsAdded !== null ? <span>Added lands: ...</span> : null}
 *
 * The normalization at lines 517-529 returns `null` when the prop is not
 * a finite number. v1.4 Stage 2 stopped passing these props from
 * WorkspaceView (HARD #8 preserved: DeckEditorPanel internals BYTE-IDENTICAL
 * — only the WorkspaceView render call changed). This test verifies the
 * normalization + chip-render gating logic so the chip doesn't appear
 * under the v1.4 setup.
 *
 * Also asserts (via fs read) that WorkspaceView's DeckEditorPanel render
 * call no longer passes the props — the literal source-level evidence.
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

/** Replicate DeckEditorPanel's normalization (lines 517-529). */
function normalizeCompletionCount(value: unknown): number | null {
  if (typeof value !== "number" || !Number.isFinite(value)) return null;
  return Math.max(0, Math.trunc(value));
}

/** Replicate the JSX chip-render gate (lines 1113-1114). */
function shouldRenderChip(normalized: number | null): boolean {
  return normalized !== null;
}

describe("v1.4 Stage 2 — legacy chip render gate", () => {
  test("undefined prop → null normalization → chip does NOT render", () => {
    expect(normalizeCompletionCount(undefined)).toBeNull();
    expect(shouldRenderChip(null)).toBe(false);
  });

  test("non-number prop → null normalization → chip does NOT render", () => {
    expect(normalizeCompletionCount("22")).toBeNull();
    expect(normalizeCompletionCount(null)).toBeNull();
    expect(normalizeCompletionCount(NaN)).toBeNull();
    expect(normalizeCompletionCount(Infinity)).toBeNull();
  });

  test("finite-number prop → numeric normalization → chip RENDERS (legacy path)", () => {
    expect(normalizeCompletionCount(22)).toBe(22);
    expect(shouldRenderChip(22)).toBe(true);
    expect(normalizeCompletionCount(0)).toBe(0);
    expect(shouldRenderChip(0)).toBe(true); // even 0 renders — that was the misleading "Added cards: 0" line
  });

  test("negative or fractional inputs clamp + truncate (legacy behavior preserved)", () => {
    expect(normalizeCompletionCount(-5)).toBe(0); // clamp
    expect(normalizeCompletionCount(3.7)).toBe(3); // truncate
  });
});

describe("v1.4 Stage 2 — WorkspaceView no longer passes the chip props", () => {
  // Source-level evidence: the legacy props are no longer in the
  // DeckEditorPanel render call. If a future refactor adds them back,
  // this test fires to remind that the v1.4 cleanup intent was to
  // omit them in favor of AddedCardsPanel.
  test("WorkspaceView source omits completionAddedCards prop from DeckEditorPanel render call", () => {
    const src = readFileSync(
      resolve(__dirname, "../../../views/WorkspaceView.tsx"),
      "utf-8",
    );
    // The legacy prop should not appear as an attribute pass on the
    // DeckEditorPanel render — locate the v1.4 cleanup marker and
    // assert no completionAddedCards/completionLandsAdded JSX attributes
    // are passed inside the same render block.
    expect(src).toContain("v1.4 Stage 2: completionAddedCards");
    // The literal attribute assignments should be GONE (only the comment
    // explaining the omission remains in the file).
    expect(src).not.toMatch(/completionAddedCards=\{completionCardsAddedCount\}/);
    expect(src).not.toMatch(/completionLandsAdded=\{completionLandsAddedCount\}/);
  });

  test("DeckEditorPanel source still HAS the chip render at lines 1113-1114 (BYTE-IDENTICAL per HARD #8)", () => {
    const src = readFileSync(
      resolve(__dirname, "../DeckEditorPanel.tsx"),
      "utf-8",
    );
    // The chip render is preserved — Stage 2 fix is at the call site, not the panel.
    expect(src).toContain("Added cards: ");
    expect(src).toContain("Added lands: ");
    expect(src).toContain("normalizedCompletionAddedCards !== null");
    expect(src).toContain("normalizedCompletionLandsAdded !== null");
  });
});
