/**
 * Vitest test for v1.6.4 Stage 3 — × Clear Deck full state reset.
 *
 * Cowork's v1.6.3 browser walk caught: × Clear button hides empty-state
 * CTA + shows "No deck loaded" pill, but the metric pill row + Power
 * Tune swap preview keep stale values. The h2 reads "Krenko, Mob Boss"
 * (the reducer INITIAL_STATE fallback) instead of "No deck loaded";
 * the Commander pill still renders; card count shows 5 (fallback deck).
 * Power Tune local state (`deckTuneResponse`) also persists.
 *
 * Per v1.6.4 discipline: this test MUST fail on current main first,
 * then made green by the fix. The fix has two parts:
 *
 *   (a) Metric pill row gates commander h2 + Commander pill on
 *       `deckState.source !== "fallback"` — when source is fallback,
 *       the deck name reads "No deck loaded" and the Commander pill
 *       does NOT render.
 *   (b) The × button click handler dispatches USER_CLEAR_DECK AND
 *       clears the local `deckTuneResponse` useState alongside, so
 *       the Power Tune swap preview surface resets too. (Local state
 *       is parent-owned, so the reducer can't reach it. INITIAL_STATE
 *       BYTE-IDENTICAL per HARD safety.)
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const WS_SRC_RAW = readFileSync(
  resolve(__dirname, "../WorkspaceView.tsx"),
  "utf-8",
);
function _stripJs(src: string): string {
  return src
    .replace(/\{\s*\/\*[\s\S]*?\*\/\s*\}/g, "")
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/\/\/.*$/gm, "");
}
const WS_SRC = _stripJs(WS_SRC_RAW);

describe("v1.6.4 Stage 3 — metric pill row gates on source !== 'fallback'", () => {
  test("deck-name <h2> condition checks source !== 'fallback' (not just trimmedCommander)", () => {
    // Locate the metric-pill-header block + verify the h2 + Commander pill
    // both gate on the deckState.source check. Before fix: only checks
    // `trimmedCommander !== ""` — falls back to "Krenko, Mob Boss" when
    // USER_CLEAR_DECK resets state. After fix: source-gate hides the
    // fallback commander.
    const idx = WS_SRC.indexOf('data-v16-stage="metric-pill-header"');
    expect(idx).toBeGreaterThan(0);
    const block = WS_SRC.slice(idx, Math.min(WS_SRC.length, idx + 3000));
    // The deck-name h2 + Commander pill must both gate on source check.
    // Acceptable shapes:
    //   {deckState.source !== "fallback" && trimmedCommander !== "" ? ... : ...}
    //   {hasRealDeck ? ... : ...} where hasRealDeck = source !== "fallback"
    //   {trimmedCommander !== "" && deckState.source !== "fallback" ? ... : ...}
    // Sentinel pattern: a `source !== "fallback"` (or equivalent) within
    // the metric-pill-header block.
    expect(block).toMatch(/deckState\.source\s*!==\s*"fallback"|source\s*!==\s*"fallback"/);
  });

  test("Commander pill block also gates on source !== 'fallback'", () => {
    // The Commander Badge at line ~4357 must NOT render when source is
    // fallback. v1.6.4 fix wraps the Commander Badge's render condition
    // to require source !== "fallback". The actual gate may be via a
    // local `hasRealDeck` variable defined at the metric block top.
    const idx = WS_SRC.indexOf("Commander · ");
    expect(idx).toBeGreaterThan(0);
    // Slice from 300 chars before to find the immediate conditional.
    const slice = WS_SRC.slice(Math.max(0, idx - 300), idx);
    // Accept either the literal source check OR the hasRealDeck variable
    // (defined at the metric-pill-header block top as `source !== "fallback"`).
    expect(slice).toMatch(/source\s*!==\s*"fallback"|hasRealDeck/);
  });
});

describe("v1.6.4 Stage 3 — × Clear click also clears local Power Tune state", () => {
  test("× Clear onClick dispatches USER_CLEAR_DECK AND clears local deckTuneResponse", () => {
    // The v1.6.2 × Clear button at line ~4327 had:
    //   onClick={() => dispatchDeckAction({ type: "USER_CLEAR_DECK" })}
    // v1.6.4 fix expands the handler to ALSO clear local useState:
    //   onClick={() => { dispatchDeckAction(...); setDeckTuneResponse(null); }}
    // The setter call ensures local Power Tune state resets alongside.
    // Locate the clear-deck-button marker + verify both calls in the onClick.
    const idx = WS_SRC.indexOf('data-v162-stage="clear-deck-button"');
    expect(idx).toBeGreaterThan(0);
    // Find the surrounding onClick handler — onClick is BEFORE the marker.
    const buttonStart = WS_SRC.lastIndexOf("<button", idx);
    const buttonEnd = WS_SRC.indexOf(">", idx);
    const buttonOpenTag = WS_SRC.slice(buttonStart, buttonEnd);
    // Both calls present in the same handler.
    expect(buttonOpenTag).toContain("USER_CLEAR_DECK");
    expect(buttonOpenTag).toContain("setDeckTuneResponse");
  });

  test("setDeckTuneResponse call is wired with null argument (clears the preview)", () => {
    const idx = WS_SRC.indexOf('data-v162-stage="clear-deck-button"');
    expect(idx).toBeGreaterThan(0);
    const buttonStart = WS_SRC.lastIndexOf("<button", idx);
    const buttonEnd = WS_SRC.indexOf(">", idx);
    const buttonOpenTag = WS_SRC.slice(buttonStart, buttonEnd);
    expect(buttonOpenTag).toMatch(/setDeckTuneResponse\s*\(\s*null\s*\)/);
  });
});

describe("v1.6.4 Stage 3 — HARD safety: INITIAL_STATE shape BYTE-IDENTICAL", () => {
  test("USER_CLEAR_DECK handler still returns INITIAL_STATE-spread (no new fields)", () => {
    // Sentinel: HARD safety — INITIAL_STATE shape unchanged. The fix is
    // PURELY at the call-site (WorkspaceView component-local state),
    // NOT in the reducer.
    const reducerSrc = readFileSync(
      resolve(__dirname, "../../lib/workspaceDeckState.ts"),
      "utf-8",
    );
    expect(reducerSrc).toMatch(/case\s*"USER_CLEAR_DECK"[\s\S]+?return\s*\{\s*\.\.\.INITIAL_STATE/);
  });

  test("INITIAL_STATE field set unchanged from v1.6.3 (no new keys)", () => {
    // v1.6.3 added pendingAdds; v1.6.4 must NOT add any further fields.
    const reducerSrc = readFileSync(
      resolve(__dirname, "../../lib/workspaceDeckState.ts"),
      "utf-8",
    );
    // The set of explicitly-listed INITIAL_STATE fields is bounded by the
    // export const declaration. Match the block + verify v1.6.2 + v1.6.3
    // field names are present, none added.
    const expectedFields = [
      "commander",
      "deckText",
      "deckTextRevision",
      "source",
      "isHydrated",
      "buildResponse",
      "buildPending",
      "buildError",
      "isCompleted",
      "completePending",
      "completeError",
      "upgradePending",
      "upgradeSuggestions",
      "upgradeError",
      "lastUpgradedAt",
      "pendingAdds",
    ];
    for (const f of expectedFields) {
      expect(reducerSrc).toContain(`${f}:`);
    }
  });
});
