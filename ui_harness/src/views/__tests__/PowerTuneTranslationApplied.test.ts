/**
 * Vitest test for v1.6.4 Stage 2 — Power Tune Swap Preview translation.
 *
 * Cowork's v1.6.3 browser walk caught: Power Tune output renders raw
 * engine reason codes (ADD_PRIMITIVE_COVERAGE / CUT_DEAD_SLOT /
 * GC_COMPLIANCE_PRESERVED) instead of human prose, because the
 * Tools-panel Power Tune Swap Preview render seam at
 * WorkspaceView.tsx:4990 joins `reasons` array with ", " directly —
 * never invokes `translateJustification`. v1.6.2's translation wiring
 * only covered the UpgradeSuggestionsList chip render; the Power Tune
 * sub-panel preview is a separate render seam.
 *
 * Per v1.6.4 discipline: this test MUST fail on current main first
 * (proof the test catches the regression), then made green by the fix.
 *
 * Source-level evidence: this test reads WorkspaceView.tsx + asserts
 * the swap-preview render seam either (a) maps each reason through
 * `translateJustification` before display OR (b) declares the
 * translation explicitly via a `JUSTIFICATION_LABELS[code] ?? code`
 * pattern. Either pattern catches the fix; raw-string display fails.
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const WS_SRC_RAW = readFileSync(
  resolve(__dirname, "../WorkspaceView.tsx"),
  "utf-8",
);
// Strip JS comments so doc-sentinel strings don't false-positive the
// pattern assertions (same idiom as v1.6.1+ hotfix test pattern).
function _stripJs(src: string): string {
  return src
    .replace(/\{\s*\/\*[\s\S]*?\*\/\s*\}/g, "")
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/\/\/.*$/gm, "");
}
const WS_SRC = _stripJs(WS_SRC_RAW);

/** Locate the Power Tune Swap Preview render block (line ~4962). The
 *  block is identified by the "Swap Preview" sentinel + tuneSwapRows.map. */
function _findSwapPreviewBlock(): string {
  const idx = WS_SRC.indexOf("Swap Preview (");
  if (idx < 0) {
    throw new Error("Power Tune Swap Preview render block not found in WorkspaceView source");
  }
  // Grab 4000 chars to cover the full map callback (the reasons render
  // sits ~2400 chars after the "Swap Preview" sentinel; 4000 gives slack).
  return WS_SRC.slice(idx, Math.min(WS_SRC.length, idx + 4000));
}

describe("v1.6.4 Stage 2 — Power Tune Swap Preview applies translateJustification", () => {
  test("translateJustification is IMPORTED in WorkspaceView (sentinel)", () => {
    // The translation map's helper function must be imported at the top
    // of WorkspaceView so the Power Tune render seam can use it.
    expect(WS_SRC_RAW).toMatch(
      /import\s*\{[^}]*translateJustification[^}]*\}\s*from\s*["']\.\.\/lib\/justificationLabels["']/,
    );
  });

  test("Swap Preview render block INVOKES translateJustification on each reason", () => {
    // The fix must apply translateJustification (or equivalent
    // JUSTIFICATION_LABELS[code] ?? code lookup) to each reason
    // string before display. The bug pattern is `reasons.join(", ")`
    // with no mapping.
    const block = _findSwapPreviewBlock();
    // Acceptable shapes (any of these proves the fix is wired):
    //   reasons.map(translateJustification)
    //   reasons.map(r => translateJustification(r))
    //   reasons.map((r) => translateJustification(r))
    //   translateJustification(r)
    const hasTranslateCall = /translateJustification\s*\(/.test(block);
    expect(hasTranslateCall).toBe(true);
  });

  test("Swap Preview render block does NOT emit raw `reasons.join(\", \")` as visible text", () => {
    // The bug pattern: `{reasons.join(", ")}` inside a JSX expression
    // displays raw engine codes. The fix replaces this with a mapped
    // version. Sentinel: raw-pattern absent.
    const block = _findSwapPreviewBlock();
    // The fix may keep the join but apply mapping first, e.g.
    // `reasons.map(translateJustification).join(", ")`. We accept that.
    // The REGRESSION pattern is `reasons.join(", ")` with NO map preceding.
    const rawJoinNoMap = /\breasons\.join\(", "\)/.test(block);
    if (rawJoinNoMap) {
      // If raw join appears, it MUST be preceded by a .map(translateJustification)
      // call in the same expression. Look for the combined pattern.
      const mappedJoinPattern = /reasons\.map\([^)]*translateJustification[^)]*\)\.join\(", "\)/.test(block) ||
        /reasons\.map\(\s*\(r\)\s*=>\s*translateJustification\(r\)\s*\)\.join\(", "\)/.test(block);
      // Either the raw join is gone OR a mapped variant is present.
      expect(mappedJoinPattern).toBe(true);
    } else {
      // Raw join pattern absent — fix may use a different rendering shape
      // (e.g., map to JSX Badge per reason). That's acceptable too.
      expect(rawJoinNoMap).toBe(false);
    }
  });
});

describe("v1.6.4 Stage 2 — translation map BYTE-IDENTICAL from v1.6.2", () => {
  test("JUSTIFICATION_LABELS map still exports the 3 Power Tune codes (sentinel)", () => {
    // HARD safety: justificationLabels.ts BYTE-IDENTICAL — Stage 2 only
    // adds the invocation at a new render seam, doesn't modify the map.
    const mapSrc = readFileSync(
      resolve(__dirname, "../../lib/justificationLabels.ts"),
      "utf-8",
    );
    expect(mapSrc).toContain("ADD_PRIMITIVE_COVERAGE:");
    expect(mapSrc).toContain("CUT_DEAD_SLOT:");
    expect(mapSrc).toContain("GC_COMPLIANCE_PRESERVED:");
    // The translations are the human-readable labels Cowork's browser
    // walk expects to see in place of the raw codes.
    expect(mapSrc).toContain("Fills missing primitive");
    expect(mapSrc).toContain("Replaces underperforming card");
    expect(mapSrc).toContain("Stays within bracket limits");
  });

  test("translateJustification function still exported (sentinel)", () => {
    const mapSrc = readFileSync(
      resolve(__dirname, "../../lib/justificationLabels.ts"),
      "utf-8",
    );
    expect(mapSrc).toMatch(/export\s+function\s+translateJustification\s*\(/);
  });
});

describe("v1.6.4 Stage 2 — UpgradeSuggestionsList translation BYTE-IDENTICAL from v1.6.2", () => {
  test("UpgradeSuggestionsList still imports + invokes translateJustification", () => {
    // The v1.6.2 wiring at the UpgradeSuggestionsList chip render seam
    // is preserved — Stage 2 ADDS a second invocation site at the
    // Power Tune Swap Preview seam, doesn't move/remove the existing one.
    const src = readFileSync(
      resolve(__dirname, "../../components/stats/UpgradeSuggestionsList.tsx"),
      "utf-8",
    );
    expect(src).toContain("translateJustification");
    expect(src).toMatch(/translateJustification\(/);
  });
});
