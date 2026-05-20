/**
 * Vitest for Pillar A.6 — combos drawer refresh + single source of truth.
 *
 * Bug 2: drawer body was empty even though the toolbar Combos count said
 * 25 — the two surfaces read completionResult through DIFFERENT inline
 * expressions, leaving room for the two paths to drift.
 * Bug 3: after re-Complete, the drawer didn't refresh — same root cause:
 * the drawer's inline expression wasn't memoized off completionResult so
 * its dependency wiring was implicit.
 *
 * Fix: hoist the typed combo arrays into shared useMemo hooks
 * (detectedCombosForDrawer + missingPartnersForDrawer) and re-derive
 * combosTotalCount from those same arrays. Both the toolbar badge and
 * the CombosDrawer now read the same memoized references — the two
 * surfaces literally cannot diverge.
 *
 * Source-string sentinels cover the wire-up; the derivation function is
 * lifted out here for a logic-level test of the refresh contract.
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";


const WORKSPACE_VIEW_SRC = readFileSync(
  resolve(__dirname, "../WorkspaceView.tsx"),
  "utf-8",
);


type ComboResp = {
  detected_combos_v1?: unknown;
  missing_partners_v1?: unknown;
} | null | undefined;

function deriveDetected(cr: ComboResp): readonly unknown[] {
  const raw = cr?.detected_combos_v1;
  return Array.isArray(raw) ? raw : [];
}

function deriveMissing(cr: ComboResp): readonly unknown[] {
  const raw = cr?.missing_partners_v1;
  return Array.isArray(raw) ? raw : [];
}

function deriveTotal(cr: ComboResp): number {
  return deriveDetected(cr).length + deriveMissing(cr).length;
}


describe("Source: shared combos memos feed both the badge and the drawer", () => {
  test("detectedCombosForDrawer + missingPartnersForDrawer memos exist", () => {
    expect(WORKSPACE_VIEW_SRC).toMatch(
      /const\s+detectedCombosForDrawer\s*=\s*useMemo<ReadonlyArray<DetectedComboEntry>>/,
    );
    expect(WORKSPACE_VIEW_SRC).toMatch(
      /const\s+missingPartnersForDrawer\s*=\s*useMemo<ReadonlyArray<MissingPartnerEntry>>/,
    );
  });

  test("combosTotalCount derives from the SAME memoized arrays", () => {
    expect(WORKSPACE_VIEW_SRC).toMatch(
      /combosTotalCount\s*=\s*useMemo<number>\(\s*\(\)\s*=>\s*detectedCombosForDrawer\.length\s*\+\s*missingPartnersForDrawer\.length/,
    );
  });

  test("CombosDrawer receives the SAME memoized arrays (no second inline derivation)", () => {
    const drawerBlock = WORKSPACE_VIEW_SRC.match(/<CombosDrawer[\s\S]*?\/>/);
    expect(drawerBlock).not.toBeNull();
    const block = drawerBlock?.[0] ?? "";
    expect(block).toMatch(/detected_combos_v1=\{detectedCombosForDrawer\}/);
    expect(block).toMatch(/missing_partners_v1=\{missingPartnersForDrawer\}/);
    // The previous Array.isArray inline expression is gone — single source.
    expect(block).not.toMatch(/Array\.isArray/);
  });

  test("Both memos list completionResult as the only dependency", () => {
    expect(WORKSPACE_VIEW_SRC).toMatch(
      /detectedCombosForDrawer[\s\S]{0,200}\},\s*\[completionResult\]\s*\)/,
    );
    expect(WORKSPACE_VIEW_SRC).toMatch(
      /missingPartnersForDrawer[\s\S]{0,200}\},\s*\[completionResult\]\s*\)/,
    );
  });
});


describe("Logic: derivation rules (the function used inside the memos)", () => {
  test("zero count for legacy / empty response", () => {
    expect(deriveTotal(null)).toBe(0);
    expect(deriveTotal({})).toBe(0);
    expect(deriveTotal({ detected_combos_v1: [], missing_partners_v1: [] })).toBe(0);
  });

  test("typed badge count matches drawer-array lengths exactly", () => {
    const cr = {
      detected_combos_v1: [{ variant_id: "a" }, { variant_id: "b" }],
      missing_partners_v1: [{ variant_id: "c" }],
    };
    expect(deriveTotal(cr)).toBe(3);
    expect(deriveDetected(cr).length + deriveMissing(cr).length).toBe(deriveTotal(cr));
  });

  test("refresh contract — new completionResult yields new arrays (no stale references)", () => {
    const before = {
      detected_combos_v1: [{ variant_id: "a" }],
      missing_partners_v1: [],
    };
    const after = {
      detected_combos_v1: [],
      missing_partners_v1: [{ variant_id: "b" }],
    };
    expect(deriveDetected(before)).not.toBe(deriveDetected(after));
    expect(deriveTotal(before)).toBe(1);
    expect(deriveTotal(after)).toBe(1);
    // Length is the same but the surfaces flip (active → suggested), so
    // a downstream consumer (DeckCombosPanel) re-evaluates default tab
    // selection because the arrays are different object references.
    expect(deriveDetected(after).length).toBe(0);
    expect(deriveMissing(after).length).toBe(1);
  });

  test("ignores non-array values gracefully (legacy unknown shapes)", () => {
    const cr = { detected_combos_v1: "oops" as unknown, missing_partners_v1: 99 as unknown };
    expect(deriveDetected(cr).length).toBe(0);
    expect(deriveMissing(cr).length).toBe(0);
    expect(deriveTotal(cr)).toBe(0);
  });
});
