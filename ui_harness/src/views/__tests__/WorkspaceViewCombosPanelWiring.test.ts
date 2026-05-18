/**
 * Wiring test for the Combos surface in WorkspaceView.
 *
 * Originally (v1.7.2 Stage 3) DeckCombosPanel rendered inline within the
 * workspace render branch. The combos surface has since moved into a
 * right-anchored drawer (CombosDrawer) toggled by a toolbar button so
 * the inline assertion flipped to "renders inside the drawer when the
 * toggle is clicked" — exercised here via source-string sentinels +
 * shape contracts (no @testing-library/react per project convention).
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";


const WORKSPACE_VIEW_SRC = readFileSync(
  resolve(__dirname, "../WorkspaceView.tsx"),
  "utf-8",
);


describe("WorkspaceView source sentinels — CombosDrawer wire-up", () => {
  test("CombosDrawer imported from the components/stats path", () => {
    expect(WORKSPACE_VIEW_SRC).toMatch(
      /import\s+CombosDrawer\s+from\s+["']\.\.\/components\/stats\/CombosDrawer["']/,
    );
  });

  test("<CombosDrawer/> renders with detected_combos_v1 + missing_partners_v1 props", () => {
    expect(WORKSPACE_VIEW_SRC).toMatch(/<CombosDrawer[\s\S]*?detected_combos_v1=/);
    expect(WORKSPACE_VIEW_SRC).toMatch(/<CombosDrawer[\s\S]*?missing_partners_v1=/);
  });

  test("Props sourced from completionResult (the /deck/complete_v1 response)", () => {
    const drawerBlock = WORKSPACE_VIEW_SRC.match(/<CombosDrawer[\s\S]*?\/>/);
    expect(drawerBlock).not.toBeNull();
    const block = drawerBlock?.[0] ?? "";
    expect(block).toMatch(/completionResult\?\.detected_combos_v1/);
    expect(block).toMatch(/completionResult\?\.missing_partners_v1/);
  });

  test("Legacy response shape — coerce undefined to [] (back-compat path)", () => {
    const drawerBlock = WORKSPACE_VIEW_SRC.match(/<CombosDrawer[\s\S]*?\/>/);
    const block = drawerBlock?.[0] ?? "";
    const arrayIsArrayCount = (block.match(/Array\.isArray/g) || []).length;
    expect(arrayIsArrayCount).toBe(2);
  });

  test("CombosDrawer receives open + onOpenChange wired to local useState", () => {
    const drawerBlock = WORKSPACE_VIEW_SRC.match(/<CombosDrawer[\s\S]*?\/>/);
    const block = drawerBlock?.[0] ?? "";
    expect(block).toMatch(/open=\{combosDrawerOpen\}/);
    expect(block).toMatch(/onOpenChange=\{setCombosDrawerOpen\}/);
    expect(WORKSPACE_VIEW_SRC).toMatch(
      /const\s+\[combosDrawerOpen,\s*setCombosDrawerOpen\]\s*=\s*useState/,
    );
  });

  test("Inline DeckCombosPanel render is removed (drawer-only surface)", () => {
    // The drawer is now the canonical surface; the standalone inline
    // <DeckCombosPanel ... /> render must not return.
    expect(WORKSPACE_VIEW_SRC).not.toMatch(/<DeckCombosPanel\b/);
  });

  test("Combos toolbar toggle button renders next to the action buttons", () => {
    // The toolbar carries a Combos toggle that flips combosDrawerOpen.
    expect(WORKSPACE_VIEW_SRC).toMatch(/setCombosDrawerOpen\(\(prev\)\s*=>\s*!prev\)/);
    expect(WORKSPACE_VIEW_SRC).toMatch(/data-testid="combos-drawer-toggle"/);
  });

  test("Toolbar toggle button surfaces combosTotalCount badge", () => {
    expect(WORKSPACE_VIEW_SRC).toMatch(/combosTotalCount\s*>\s*0/);
    expect(WORKSPACE_VIEW_SRC).toMatch(/const\s+combosTotalCount\s*=\s*useMemo/);
  });
});


describe("Workspace combos count derivation (logic-level)", () => {
  function deriveCombosTotalCount(completionResult: {
    detected_combos_v1?: unknown;
    missing_partners_v1?: unknown;
  } | null | undefined): number {
    const det = completionResult?.detected_combos_v1;
    const miss = completionResult?.missing_partners_v1;
    const detLen = Array.isArray(det) ? det.length : 0;
    const missLen = Array.isArray(miss) ? miss.length : 0;
    return detLen + missLen;
  }

  test("zero count for legacy / empty response", () => {
    expect(deriveCombosTotalCount(null)).toBe(0);
    expect(deriveCombosTotalCount({})).toBe(0);
    expect(deriveCombosTotalCount({ detected_combos_v1: [], missing_partners_v1: [] })).toBe(0);
  });

  test("non-zero count sums detected + missing", () => {
    const cr = {
      detected_combos_v1: [{ variant_id: "a" }, { variant_id: "b" }],
      missing_partners_v1: [{ variant_id: "c" }],
    };
    expect(deriveCombosTotalCount(cr)).toBe(3);
  });

  test("ignores non-array values gracefully", () => {
    const cr = { detected_combos_v1: "oops" as unknown, missing_partners_v1: 99 as unknown };
    expect(deriveCombosTotalCount(cr)).toBe(0);
  });
});
