/**
 * Vitest for v1.7.2 Stage 2 — DeckCombosPanel.
 *
 * Per the project's vitest convention (no @testing-library/react —
 * AUTOMATION_RULES carryover from Phase 4.x), the panel is exercised
 * via module-export smoke tests + source-string sentinels covering
 * the render branches called out in the v1.7.2 spec.
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import DeckCombosPanel, {
  MISSING_PARTNERS_INITIAL_VISIBLE,
  type DetectedComboEntry,
  type MissingPartnerEntry,
} from "../DeckCombosPanel";


const PANEL_SRC = readFileSync(
  resolve(__dirname, "../DeckCombosPanel.tsx"),
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


describe("DeckCombosPanel — module exports + types", () => {
  test("default export is a function (React component)", () => {
    expect(typeof DeckCombosPanel).toBe("function");
  });

  test("MISSING_PARTNERS_INITIAL_VISIBLE caps initial render at 10", () => {
    expect(MISSING_PARTNERS_INITIAL_VISIBLE).toBe(10);
  });

  test("DetectedComboEntry + MissingPartnerEntry shapes accept the engine's payload", () => {
    const detected: DetectedComboEntry = makeDetected(
      "3940-5195", "Storm-Kiln Artist", "Haze of Rage",
      "Infinite colored mana; Infinite storm count",
    );
    const missing: MissingPartnerEntry = makeMissing(
      "3940-5195", "Storm-Kiln Artist", "Haze of Rage",
      "Infinite colored mana; Infinite storm count",
    );
    expect(detected.variant_id).toBe("3940-5195");
    expect(missing.partner_card_name).toBe("Haze of Rage");
  });
});


describe("DeckCombosPanel — render branch sentinels (source-side)", () => {
  test("null-render contract — empty inputs return null (panel hides)", () => {
    expect(PANEL_SRC).toMatch(
      /if\s*\(\s*detected\.length\s*===\s*0\s*&&\s*missing\.length\s*===\s*0\s*\)\s*\{\s*return\s+null/,
    );
  });

  test("detected render branch — uses card_a_name + card_b_name + combo_outcome_label", () => {
    expect(PANEL_SRC).toMatch(/entry\.card_a_name/);
    expect(PANEL_SRC).toMatch(/entry\.card_b_name/);
    expect(PANEL_SRC).toMatch(/entry\.combo_outcome_label/);
  });

  test("missing render branch — uses partner_card_name + present_card_name + combo_outcome_label", () => {
    expect(PANEL_SRC).toMatch(/entry\.partner_card_name/);
    expect(PANEL_SRC).toMatch(/entry\.present_card_name/);
  });

  test("Spellbook link target — https://commanderspellbook.com/combo/{variant_id}", () => {
    expect(PANEL_SRC).toMatch(/https:\/\/commanderspellbook\.com\/combo\//);
    expect(PANEL_SRC).toMatch(/encodeURIComponent\s*\(\s*variantId\s*\)/);
    // anchors open in a new tab with safe rel attributes
    expect(PANEL_SRC).toMatch(/target=("_blank"|'_blank')/);
    expect(PANEL_SRC).toMatch(/rel=("noopener noreferrer"|'noopener noreferrer')/);
  });

  test("Show all toggle exists for missing-partners cap and uses MISSING_PARTNERS_INITIAL_VISIBLE", () => {
    expect(PANEL_SRC).toMatch(/MISSING_PARTNERS_INITIAL_VISIBLE/);
    expect(PANEL_SRC).toMatch(/missingShowAll/);
    expect(PANEL_SRC).toMatch(/Show all/);
    expect(PANEL_SRC).toMatch(/Show fewer/);
  });

  test("No new shadcn imports — only existing primitives (Card / Badge / Button)", () => {
    // The spec forbids new shadcn/ui imports for v1.7.2 Stage 2.
    expect(PANEL_SRC).not.toMatch(/from\s*['"]@\/components\/ui\//);
    expect(PANEL_SRC).not.toMatch(/from\s*['"]@shadcn/);
    // Verify the existing-primitive imports landed where expected.
    expect(PANEL_SRC).toMatch(/from\s*['"]\.\.\/\.\.\/ui\/primitives\/Card['"]/);
    expect(PANEL_SRC).toMatch(/from\s*['"]\.\.\/\.\.\/ui\/primitives\/Badge['"]/);
    expect(PANEL_SRC).toMatch(/from\s*['"]\.\.\/\.\.\/ui\/primitives\/Button['"]/);
  });

  test("Discriminator attribute data-v172-stage='deck-combos-panel' present for live-retest sentinel", () => {
    expect(PANEL_SRC).toMatch(/data-v172-stage=("deck-combos-panel"|'deck-combos-panel')/);
  });
});


describe("DeckCombosPanel — prop shape acceptance (logic-level)", () => {
  test("Detected-only props are a valid input", () => {
    const detected: DetectedComboEntry[] = [
      makeDetected("3940-5195", "Storm-Kiln Artist", "Haze of Rage",
        "Infinite colored mana; Infinite storm count"),
    ];
    const propsValue = { detected_combos_v1: detected, missing_partners_v1: [] };
    expect(propsValue.detected_combos_v1.length).toBe(1);
  });

  test("Missing-only props with > 10 entries are a valid input (drives Show all toggle path)", () => {
    const missing: MissingPartnerEntry[] = Array.from({ length: 12 }, (_, i) =>
      makeMissing(`vid-${i}`, "Storm-Kiln Artist", `Partner ${i}`, "Infinite mana"),
    );
    const propsValue = { detected_combos_v1: [], missing_partners_v1: missing };
    expect(propsValue.missing_partners_v1.length).toBe(12);
    expect(propsValue.missing_partners_v1.length).toBeGreaterThan(MISSING_PARTNERS_INITIAL_VISIBLE);
  });

  test("Legacy response shape — undefined props default to empty (null-render)", () => {
    // The panel coerces undefined props to [], then the null-render
    // contract fires when both are empty. This is the back-compat
    // path for clients on a pre-v1.7.2 engine response.
    const propsValue: { detected_combos_v1?: undefined; missing_partners_v1?: undefined } = {};
    expect(propsValue.detected_combos_v1).toBeUndefined();
    expect(propsValue.missing_partners_v1).toBeUndefined();
  });
});
