/**
 * Vitest tests for v1.6 Stage 2 — toolbar semantic separation.
 *
 * Source-level evidence asserts (without rendering full WorkspaceView,
 * which is heavy): the toolbar render block at WorkspaceView.tsx
 * contains the v1.6 Stage 2 grouping data-attributes + uses the Button
 * primitive for action buttons + has a visible vertical divider element
 * between Mode tabs and Action buttons.
 *
 * Per AUTOMATION_RULES halt-and-ask + autonomous_repair_log soft-safety:
 * no @testing-library/react install. Source-level grep + structural
 * checks suffice because the change is local + the data-attributes
 * make the grouping explicit + the existing workspaceIntegration
 * tests cover the surrounding wiring.
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const WS_SRC = readFileSync(
  resolve(__dirname, "../../views/WorkspaceView.tsx"),
  "utf-8",
);

describe("v1.6 Stage 2 — toolbar groups + divider", () => {
  test("toolbar root carries v16 data-attribute marking the cleanup", () => {
    expect(WS_SRC).toContain('data-v16-stage="toolbar-semantic-separation"');
  });

  test("Mode tabs group is explicitly marked + role=tablist preserved", () => {
    expect(WS_SRC).toContain('data-v16-group="mode-tabs"');
    expect(WS_SRC).toMatch(/role="tablist"[^}]*aria-label="Page mode"/);
  });

  test("Action buttons group is explicitly marked + role=group + aria-label='Deck actions'", () => {
    expect(WS_SRC).toContain('data-v16-group="action-buttons"');
    expect(WS_SRC).toMatch(/role="group"[^}]*aria-label="Deck actions"/);
  });

  test("vertical divider between groups is present + aria-hidden", () => {
    expect(WS_SRC).toContain('data-v16-group="toolbar-divider"');
    // Divider is a span with `h-8 w-px bg-glass-border` classes + aria-hidden.
    expect(WS_SRC).toMatch(/h-8 w-px bg-glass-border[^"]*"\s*\n\s*aria-hidden="true"/);
  });
});

describe("v1.6 Stage 2 — Action buttons use Button primitive (variant=primary)", () => {
  test("Button import added from primitives", () => {
    expect(WS_SRC).toMatch(/import Button from ["']\.\.\/ui\/primitives\/Button["']/);
  });

  test("'1. Complete deck' is rendered via Button primitive variant=primary", () => {
    // Use lastIndexOf to find the JSX render site (not earlier doc comments
    // that reference the button label). The JSX ternary uses `: "1. Complete
    // deck"}` which appears only at the actual render.
    const idx = WS_SRC.lastIndexOf(': "1. Complete deck"}');
    expect(idx).toBeGreaterThan(0);
    const slice = WS_SRC.slice(Math.max(0, idx - 1500), idx);
    expect(slice).toMatch(/<Button[\s\S]+?variant="primary"[\s\S]+$/);
  });

  test("'2. Build' is rendered via Button primitive variant=primary", () => {
    const idx = WS_SRC.lastIndexOf(': "2. Build"}');
    expect(idx).toBeGreaterThan(0);
    const slice = WS_SRC.slice(Math.max(0, idx - 1500), idx);
    expect(slice).toMatch(/<Button[\s\S]+?variant="primary"[\s\S]+$/);
  });

  test("'3. Upgrade Deck' is rendered via Button primitive variant=primary", () => {
    const idx = WS_SRC.lastIndexOf(': "3. Upgrade Deck"}');
    expect(idx).toBeGreaterThan(0);
    const slice = WS_SRC.slice(Math.max(0, idx - 1500), idx);
    expect(slice).toMatch(/<Button[\s\S]+?variant="primary"[\s\S]+$/);
  });

  test("Mode tabs still use workspace-mode-tab CSS class (NOT restyled to Button)", () => {
    // The mode tabs' active/inactive toggle styling lives in workspace-mode-tab*
    // CSS classes — preserved BYTE-IDENTICAL. Only Action buttons changed.
    expect(WS_SRC).toContain('workspace-mode-tab ${pageMode === "WORKSPACE"');
    expect(WS_SRC).toContain('workspace-mode-tab ${pageMode === "SEED_BUILDER"');
  });

  test("disabled-while-pending labels preserved (Completing… / Building… / Upgrading…)", () => {
    expect(WS_SRC).toContain('"Completing…" : "1. Complete deck"');
    expect(WS_SRC).toContain('"Building…" : "2. Build"');
    expect(WS_SRC).toContain('"Upgrading…" : "3. Upgrade Deck"');
  });

  test("tooltip copy preserved (Decision-10 spec)", () => {
    expect(WS_SRC).toContain("fill the deck to 99 cards by adding suggested staples");
    expect(WS_SRC).toContain("Runs the full sufficiency + recommendation pipeline");
    expect(WS_SRC).toContain("Asks the engine to suggest swaps that would improve this deck");
  });
});
