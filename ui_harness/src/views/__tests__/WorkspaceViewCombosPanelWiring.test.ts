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

  test("Props sourced from the shared completionResult memos (Pillar A.6)", () => {
    // Pillar A.6: the drawer used to inline two Array.isArray expressions
    // reading completionResult, and the toolbar badge had its own memo —
    // two distinct dependency paths off the same response object. The
    // refactor hoists the typed arrays into shared useMemo hooks so the
    // badge and the drawer literally cannot diverge. Both memos still
    // derive from completionResult.
    expect(WORKSPACE_VIEW_SRC).toMatch(
      /const\s+detectedCombosForDrawer\s*=\s*useMemo<ReadonlyArray<DetectedComboEntry>>/,
    );
    expect(WORKSPACE_VIEW_SRC).toMatch(
      /const\s+missingPartnersForDrawer\s*=\s*useMemo<ReadonlyArray<MissingPartnerEntry>>/,
    );
    expect(WORKSPACE_VIEW_SRC).toMatch(/completionResult\?\.detected_combos_v1/);
    expect(WORKSPACE_VIEW_SRC).toMatch(/completionResult\?\.missing_partners_v1/);
  });

  test("Drawer props reference the shared memos (single source of truth)", () => {
    const drawerBlock = WORKSPACE_VIEW_SRC.match(/<CombosDrawer[\s\S]*?\/>/);
    const block = drawerBlock?.[0] ?? "";
    expect(block).toMatch(/detected_combos_v1=\{detectedCombosForDrawer\}/);
    expect(block).toMatch(/missing_partners_v1=\{missingPartnersForDrawer\}/);
  });

  test("Legacy response shape — coerce undefined to [] inside the memos", () => {
    // Array.isArray now lives in the two useMemo blocks, not in the
    // drawer JSX. There should be exactly two such checks for the combo
    // surfaces (one per memo).
    const detectedMemo = WORKSPACE_VIEW_SRC.match(
      /detectedCombosForDrawer\s*=\s*useMemo[\s\S]*?\},\s*\[completionResult\]\s*\)/,
    );
    const missingMemo = WORKSPACE_VIEW_SRC.match(
      /missingPartnersForDrawer\s*=\s*useMemo[\s\S]*?\},\s*\[completionResult\]\s*\)/,
    );
    expect(detectedMemo).not.toBeNull();
    expect(missingMemo).not.toBeNull();
    expect((detectedMemo?.[0] ?? "")).toMatch(/Array\.isArray\(raw\)/);
    expect((missingMemo?.[0] ?? "")).toMatch(/Array\.isArray\(raw\)/);
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
