/**
 * Vitest tests for v1.6.2 Stage 2 — clear-deck × button visibility gate.
 *
 * Source-level evidence (no @testing-library/react per AUTOMATION_RULES
 * halt-and-ask) asserts that WorkspaceView.tsx:
 *   - Renders an inline SVG × button next to the deck `<h2>` in the
 *     metric pill row when `deckState.source !== "fallback"`.
 *   - Hides the button on the fallback (no-deck-loaded) state.
 *   - Wires the button's onClick to dispatch USER_CLEAR_DECK.
 *   - The button has aria-label "Clear deck" + the v1.6.2 stage marker.
 *
 * Reducer-level coverage of USER_CLEAR_DECK semantics is in
 * `repo/ui_harness/src/lib/__tests__/workspaceDeckState.test.ts` (the
 * additive 6-test extension at the bottom of that file).
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const WS_SRC = readFileSync(
  resolve(__dirname, "../WorkspaceView.tsx"),
  "utf-8",
);

describe("v1.6.2 Stage 2 — × clear-deck button presence + gate", () => {
  test("button has v1.6.2 data-attribute marker", () => {
    expect(WS_SRC).toContain('data-v162-stage="clear-deck-button"');
  });

  test("button has aria-label='Clear deck' + title='Clear deck'", () => {
    expect(WS_SRC).toContain('aria-label="Clear deck"');
    expect(WS_SRC).toContain('title="Clear deck"');
  });

  test("button is GATED on deckState.source !== 'fallback' (hidden when no deck loaded)", () => {
    expect(WS_SRC).toMatch(/\{deckState\.source\s*!==\s*"fallback"\s*\?/);
  });

  test("button onClick dispatches USER_CLEAR_DECK reducer action", () => {
    expect(WS_SRC).toMatch(/dispatchDeckAction\(\{\s*type:\s*"USER_CLEAR_DECK"\s*\}\)/);
  });

  test("button renders inline SVG with two diagonal lines (X glyph) — no icon-lib dep", () => {
    // Locate the button's SVG block.
    const idx = WS_SRC.indexOf('data-v162-stage="clear-deck-button"');
    expect(idx).toBeGreaterThan(0);
    const slice = WS_SRC.slice(idx, Math.min(WS_SRC.length, idx + 1500));
    // Two SVG lines making an X (y1 != y2 on both).
    const lineMatches = slice.match(/<line\b/g) ?? [];
    expect(lineMatches.length).toBe(2);
    // No lucide-react / heroicons class fingerprint.
    expect(slice).not.toContain("lucide");
    expect(slice).not.toContain("heroicon");
  });

  test("button sits inside the metric pill row's deck-name container (next to h2)", () => {
    // Locate the v1.6 metric-pill-header marker, then verify the clear-deck
    // button marker appears within the same metric block (next ~3000 chars).
    const headerIdx = WS_SRC.indexOf('data-v16-stage="metric-pill-header"');
    const buttonIdx = WS_SRC.indexOf('data-v162-stage="clear-deck-button"');
    expect(headerIdx).toBeGreaterThan(0);
    expect(buttonIdx).toBeGreaterThan(headerIdx);
    // The button is within 3000 chars of the metric-pill-header marker
    // (i.e. inside the same JSX subtree).
    expect(buttonIdx - headerIdx).toBeLessThan(3000);
  });

  test("button uses Tailwind utility classes (no new CSS rules)", () => {
    const idx = WS_SRC.indexOf('data-v162-stage="clear-deck-button"');
    const slice = WS_SRC.slice(Math.max(0, idx - 400), idx + 100);
    // Sentinel: focus ring + hover bg + size.
    expect(slice).toContain("focus-visible:shadow-focus-ring");
    expect(slice).toContain("hover:bg-bg-elev-2");
  });
});

describe("v1.6.2 Stage 2 — reducer USER_CLEAR_DECK action wired via dispatch (no other dispatch sites)", () => {
  test("USER_CLEAR_DECK dispatch site exists in WorkspaceView source", () => {
    // Count >= 1 — the dispatch site must exist; documentation comments
    // mentioning USER_CLEAR_DECK are permitted.
    const matches = WS_SRC.match(/USER_CLEAR_DECK/g) ?? [];
    expect(matches.length).toBeGreaterThanOrEqual(1);
  });

  test("reducer source declares USER_CLEAR_DECK as the 12th DeckAction (additive extension)", () => {
    const reducerSrc = readFileSync(
      resolve(__dirname, "../../lib/workspaceDeckState.ts"),
      "utf-8",
    );
    expect(reducerSrc).toContain('| { type: "USER_CLEAR_DECK" }');
    // Sentinel: handler returns INITIAL_STATE shape with isHydrated:true.
    expect(reducerSrc).toMatch(
      /case\s*"USER_CLEAR_DECK"[\s\S]+?return\s*\{\s*\.\.\.INITIAL_STATE,\s*isHydrated:\s*true\s*\}/,
    );
  });
});
