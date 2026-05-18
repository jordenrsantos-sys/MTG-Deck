/**
 * Vitest tests for CombosDrawer — wraps DeckCombosPanel in a right-anchored
 * Drawer. Open state is parent-owned (WorkspaceView toggles via toolbar
 * button); closed state must render NO drawer content.
 *
 * Per project convention, no @testing-library/react — uses
 * react-dom/server.renderToString.
 */
import * as React from "react";
import { renderToString } from "react-dom/server";
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import CombosDrawer from "../CombosDrawer";
import type {
  DetectedComboEntry,
  MissingPartnerEntry,
} from "../DeckCombosPanel";


const COMBOS_DRAWER_SRC = readFileSync(
  resolve(__dirname, "../CombosDrawer.tsx"),
  "utf-8",
);


function makeDetected(variantId: string, a: string, b: string, label: string): DetectedComboEntry {
  return {
    variant_id: variantId,
    card_a_name: a,
    card_a_oracle_id: `oid-${a}`,
    card_b_name: b,
    card_b_oracle_id: `oid-${b}`,
    combo_outcome_label: label,
  };
}


function makeMissing(variantId: string, present: string, partner: string, label: string): MissingPartnerEntry {
  return {
    variant_id: variantId,
    present_card_name: present,
    present_card_oracle_id: `oid-${present}`,
    partner_card_name: partner,
    partner_card_oracle_id: `oid-${partner}`,
    combo_outcome_label: label,
  };
}


function renderDrawer(open: boolean, hasContent: boolean): string {
  const detected: DetectedComboEntry[] = hasContent
    ? [makeDetected("v1", "Sol Ring", "Mana Crypt", "Infinite mana")]
    : [];
  const missing: MissingPartnerEntry[] = hasContent
    ? [makeMissing("v2", "Thassa's Oracle", "Demonic Consultation", "Wins the game")]
    : [];
  const raw = renderToString(
    React.createElement(CombosDrawer, {
      open,
      onOpenChange: () => undefined,
      detected_combos_v1: detected,
      missing_partners_v1: missing,
    }),
  );
  return raw.replace(/<!-- -->/g, "");
}


describe("CombosDrawer — module exports + source sentinels", () => {
  test("default export is a function (React component)", () => {
    expect(typeof CombosDrawer).toBe("function");
  });

  test("imports Drawer + DeckCombosPanel from the expected paths", () => {
    expect(COMBOS_DRAWER_SRC).toMatch(/from\s+["']\.\.\/\.\.\/ui\/primitives\/Drawer["']/);
    expect(COMBOS_DRAWER_SRC).toMatch(/import\s+DeckCombosPanel/);
  });

  test('Drawer rendered with side="right"', () => {
    expect(COMBOS_DRAWER_SRC).toMatch(/side="right"/);
  });

  test("Title text is 'Deck combos'", () => {
    expect(COMBOS_DRAWER_SRC).toMatch(/Deck combos/);
  });
});


describe("CombosDrawer — closed state", () => {
  test("closed → empty render (no backdrop, no panel)", () => {
    expect(renderDrawer(false, true)).toBe("");
  });

  test("closed even with content → empty render", () => {
    expect(renderDrawer(false, true)).toBe("");
    expect(renderDrawer(false, false)).toBe("");
  });
});


describe("CombosDrawer — open state", () => {
  test("open with detected combo → renders title + Spellbook anchor", () => {
    const html = renderDrawer(true, true);
    expect(html).toContain('role="dialog"');
    expect(html).toContain('aria-modal="true"');
    expect(html).toContain("Deck combos");
    // DeckCombosPanel's data-v172-stage attribute confirms the panel mounts.
    expect(html).toContain('data-v172-stage="deck-combos-panel"');
    // Detected combo card names render.
    expect(html).toContain("Sol Ring");
    expect(html).toContain("Mana Crypt");
    // Spellbook anchor wires the panel's Spellbook link.
    expect(html).toContain("commanderspellbook.com/combo/");
  });

  test("open with empty combos → drawer chrome renders, panel null-renders", () => {
    const html = renderDrawer(true, false);
    expect(html).toContain('role="dialog"');
    expect(html).toContain("Deck combos");
    // DeckCombosPanel returns null when both arrays empty, so the
    // panel discriminator is absent.
    expect(html).not.toContain('data-v172-stage="deck-combos-panel"');
  });

  test("open → drawer is right-anchored (top-0 right-0 + border-l)", () => {
    const html = renderDrawer(true, true);
    expect(html).toContain("fixed top-0 right-0");
    expect(html).toContain("border-l");
  });
});
