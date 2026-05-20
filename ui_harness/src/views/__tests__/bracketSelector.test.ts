/**
 * Source-level tests for the interactive Bracket selector in WorkspaceView.
 *
 * Background: the Bracket badge in the metric pill row was previously a
 * non-interactive `<Badge variant="info">Bracket · {bracketId}</Badge>`.
 * When the engine emits BRACKET_VIOLATION (B1/B2 with 2-card combos),
 * users had no way to act on the suggested remediation ("raise the bracket
 * to B3+"). This test pins the contract for the interactive selector:
 *
 *   1. A `<button>` (clickable) carries `data-v176-stage="bracket-selector-trigger"`
 *      and displays "Bracket · {bracketId}".
 *   2. A popover/menu lists all five brackets B1-B5 with labels
 *      Exhibition / Core / Upgraded / Optimized / cEDH.
 *   3. Selecting an option dispatches `setBracketId(...)`.
 *   4. `BracketViolationsBanner` receives a `currentBracketId` prop so it
 *      can self-hide when the user raises the bracket past B2.
 *
 * Pattern matches existing source-level tests (metricPillHeader.test.ts) —
 * no React rendering, just regex/substring assertions over the .tsx source.
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const WS_SRC = readFileSync(
  resolve(__dirname, "../../views/WorkspaceView.tsx"),
  "utf-8",
);
const BANNER_SRC = readFileSync(
  resolve(__dirname, "../../components/stats/BracketViolationsBanner.tsx"),
  "utf-8",
);

describe("v1.7.6 — Bracket selector trigger", () => {
  test("Bracket badge is rendered inside a clickable <button>, not a static <Badge>", () => {
    expect(WS_SRC).toContain('data-v176-stage="bracket-selector-trigger"');
    expect(WS_SRC).toMatch(/<button[^>]*data-v176-stage="bracket-selector-trigger"/);
  });

  test("Bracket trigger displays 'Bracket · {bracketId}'", () => {
    const idx = WS_SRC.indexOf('data-v176-stage="bracket-selector-trigger"');
    expect(idx).toBeGreaterThan(0);
    // Generous slice — the className tailwind tokens make the button
    // markup long; the visible label text comes after them.
    const slice = WS_SRC.slice(idx, idx + 2500);
    expect(slice).toContain("Bracket · {bracketId}");
  });

  test("Bracket trigger has aria-haspopup + aria-expanded for screen-reader popover semantics", () => {
    const idx = WS_SRC.indexOf('data-v176-stage="bracket-selector-trigger"');
    expect(idx).toBeGreaterThan(0);
    const slice = WS_SRC.slice(idx, idx + 800);
    expect(slice).toMatch(/aria-haspopup=/);
    expect(slice).toMatch(/aria-expanded=/);
  });
});

describe("v1.7.6 — Bracket selector menu", () => {
  test("Menu carries v176 data-attribute marker", () => {
    expect(WS_SRC).toContain('data-v176-stage="bracket-selector-menu"');
  });

  test("Menu lists all five brackets B1-B5", () => {
    // The selector source enumerates the five canonical brackets as a
    // tuple/array literal so each option renders with id + descriptive name.
    expect(WS_SRC).toMatch(/"B1"[^"]{0,40}"Exhibition"/);
    expect(WS_SRC).toMatch(/"B2"[^"]{0,40}"Core"/);
    expect(WS_SRC).toMatch(/"B3"[^"]{0,40}"Upgraded"/);
    expect(WS_SRC).toMatch(/"B4"[^"]{0,40}"Optimized"/);
    expect(WS_SRC).toMatch(/"B5"[^"]{0,40}"cEDH"/);
  });

  test("Menu options invoke setBracketId on click", () => {
    // The popover renders one button per bracket; each button's click
    // handler calls setBracketId with the option's id.
    const idx = WS_SRC.indexOf('data-v176-stage="bracket-selector-menu"');
    expect(idx).toBeGreaterThan(0);
    const slice = WS_SRC.slice(idx, idx + 2500);
    expect(slice).toMatch(/setBracketId\(/);
  });
});

describe("v1.7.6 — BracketViolationsBanner respects currentBracketId", () => {
  test("Banner accepts a currentBracketId prop", () => {
    expect(BANNER_SRC).toMatch(/currentBracketId\??:\s*string/);
  });

  test("Banner filters violations by currentBracketId suffix", () => {
    // When the user changes brackets, the previously-emitted violations
    // (whose codes end with the old bracket suffix) must self-hide.
    // The filter is `code.endsWith(\`_\${currentBracketId}\`)` — anchored
    // on the underscore so we don't match similarly-suffixed codes.
    expect(BANNER_SRC).toMatch(/code\.endsWith\(`_\$\{currentBracketId\}`\)/);
  });

  test("WorkspaceView passes bracketId as currentBracketId to the banner", () => {
    const idx = WS_SRC.indexOf("<BracketViolationsBanner");
    expect(idx).toBeGreaterThan(0);
    const slice = WS_SRC.slice(idx, idx + 1500);
    expect(slice).toMatch(/currentBracketId=\{bracketId\}/);
  });
});
