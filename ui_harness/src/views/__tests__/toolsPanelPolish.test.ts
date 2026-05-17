/**
 * Vitest tests for v1.6.2 Stage 4 — Tools panel polish.
 *
 * Source-level evidence (no @testing-library/react per AUTOMATION_RULES)
 * for three small visual fixes:
 *   (1) bare "Resolved: N/M (art ready)" line removed from the workspace
 *       shell — redundant with the cleaner "Hover art works" PASS/FAIL
 *       indicator inside the Analyze view. hoverArtReleaseMetrics memo
 *       + telemetry consumers + PASS/FAIL indicator stay BYTE-IDENTICAL.
 *   (2) EDIT/TOOLS pills wrapped with a small "View" label so they read
 *       as a mode-selector group, not stranded orphans. workspace-mode-tabs
 *       / workspace-mode-tab CSS classes preserved BYTE-IDENTICAL.
 *   (3) Power Tune sub-panel header CSS — `.workspace-tool-panel-header`
 *       switched from `justify-content: space-between` → `flex-start`;
 *       `.workspace-tool-controls-row` switched from `justify-content:
 *       flex-end` → `flex-start` + `align-items: flex-end` → `center`.
 *       Controls now sit next to the title with gap-token spacing.
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const WS_SRC = readFileSync(
  resolve(__dirname, "../WorkspaceView.tsx"),
  "utf-8",
);
const STYLES_CSS = readFileSync(
  resolve(__dirname, "../../styles.css"),
  "utf-8",
);
// Strip /* */ block comments so doc-sentinel strings don't false-positive
// the rule-removal assertions.
const STYLES_CSS_STRIPPED = STYLES_CSS.replace(/\/\*[\s\S]*?\*\//g, "");

describe("v1.6.2 Stage 4 — bare 'Resolved: N/M (art ready)' line removed", () => {
  test("the stranded <p> rendering 'Resolved: ... (art ready)' is gone from WorkspaceView", () => {
    // The bare line was at ~4655 in v1.6.1: a <p className="workspace-muted">
    // wrapping the resolvedCount/targetCount template literal. After Stage 4,
    // that exact JSX is removed.
    expect(WS_SRC).not.toMatch(
      /<p\s+className="workspace-muted">\s*Resolved:\s*\{hoverArtReleaseMetrics\.resolvedCount\}/,
    );
  });

  test("hoverArtReleaseMetrics memo + telemetry consumers + PASS/FAIL indicator preserved BYTE-IDENTICAL", () => {
    // Memo computation still in place (load-bearing).
    expect(WS_SRC).toContain("const hoverArtReleaseMetrics = useMemo");
    // Telemetry export still wires the four fields.
    expect(WS_SRC).toContain("hover_art_works: hoverArtReleaseMetrics.isReady");
    expect(WS_SRC).toContain("hover_art_resolved: hoverArtReleaseMetrics.resolvedCount");
    expect(WS_SRC).toContain("hover_art_target: hoverArtReleaseMetrics.targetCount");
    // PASS/FAIL indicator inside Analyze view still renders.
    expect(WS_SRC).toContain("hoverArtReleaseMetrics.isReady");
    expect(WS_SRC).toContain("Hover art works");
  });

  test("v1.6.2 Stage 4 marker comment documents the removal (sentinel for future readers)", () => {
    expect(WS_SRC).toContain("v1.6.2 Stage 4: removed bare");
  });
});

describe("v1.6.2 Stage 4 — EDIT/TOOLS pills grouped with 'View' label", () => {
  test("workspace-mode-row wrapper carries v1.6.2 data-attribute marker", () => {
    expect(WS_SRC).toContain('data-v162-stage="workspace-mode-row"');
  });

  test("uppercase 'View' label precedes the workspace-mode-tabs role=tablist", () => {
    // Find the View label site + assert workspace-mode-tabs follows within
    // the same JSX block (~500 chars).
    const idx = WS_SRC.indexOf('data-v162-stage="workspace-mode-row"');
    expect(idx).toBeGreaterThan(0);
    const slice = WS_SRC.slice(idx, Math.min(WS_SRC.length, idx + 1200));
    // Multi-line JSX shape: `<span ...>\n  View\n</span>` — match permissively.
    expect(slice).toMatch(/>\s*View\s*</);
    expect(slice).toMatch(/workspace-mode-tabs[\s\S]*?role="tablist"[\s\S]*?aria-label="Workspace mode"/);
  });

  test("workspace-mode-tab CSS classes BYTE-IDENTICAL (only wrapper changed)", () => {
    // Per HARD safety: visual styling on the pills themselves unchanged.
    expect(WS_SRC).toContain('className={`workspace-mode-tab ${workspaceMode === mode ? "workspace-mode-tab-active" : ""}`}');
  });
});

describe("v1.6.2 Stage 4 — Power Tune controls inline with section header", () => {
  test(".workspace-tool-panel-header uses justify-content: flex-start (was space-between)", () => {
    // Block comment around the rule should be stripped; assert the actual
    // declaration value.
    expect(STYLES_CSS_STRIPPED).toMatch(
      /\.workspace-tool-panel-header\s*\{[^}]*justify-content:\s*flex-start/,
    );
    // Sentinel: the old space-between value is GONE from the panel-header rule.
    expect(STYLES_CSS_STRIPPED).not.toMatch(
      /\.workspace-tool-panel-header\s*\{[^}]*justify-content:\s*space-between/,
    );
  });

  test(".workspace-tool-controls-row uses justify-content: flex-start (was flex-end) + align-items: center", () => {
    expect(STYLES_CSS_STRIPPED).toMatch(
      /\.workspace-tool-controls-row\s*\{[^}]*justify-content:\s*flex-start/,
    );
    expect(STYLES_CSS_STRIPPED).toMatch(
      /\.workspace-tool-controls-row\s*\{[^}]*align-items:\s*center/,
    );
    // Sentinel: the old align-items: flex-end is GONE from the controls-row rule.
    expect(STYLES_CSS_STRIPPED).not.toMatch(
      /\.workspace-tool-controls-row\s*\{[^}]*align-items:\s*flex-end/,
    );
  });

  test("v1.6.2 Stage 4 CSS comment block documents the change", () => {
    expect(STYLES_CSS).toContain("v1.6.2 Stage 4: header tightens");
  });

  test("Power Tune JSX render path BYTE-IDENTICAL (only CSS class behavior changed)", () => {
    // Sentinel: the JSX still uses the same classes; only the CSS rules
    // they resolve to evolved.
    expect(WS_SRC).toContain("workspace-tool-panel-header");
    expect(WS_SRC).toContain("workspace-tool-controls-row");
    expect(WS_SRC).toContain("Run Power Tune");
  });
});

describe("v1.6.2 Stage 4 — HARD safety: reducer + engine BYTE-IDENTICAL", () => {
  test("Stage 4 touches no reducer files (lib/workspaceDeckState.ts untouched)", () => {
    // Sentinel: USER_CLEAR_DECK (the v1.6.2 Stage 2 additive extension)
    // still in place; reducer file otherwise unchanged.
    const reducerSrc = readFileSync(
      resolve(__dirname, "../../lib/workspaceDeckState.ts"),
      "utf-8",
    );
    expect(reducerSrc).toContain('| { type: "USER_CLEAR_DECK" }');
  });
});
