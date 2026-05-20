/**
 * Vitest for DeckCombosPanel — Pillar A.6 Stage 4 (tabbed layout).
 *
 * Verifies the tab triggers, default-tab selection, and tab-panel
 * exclusivity. Per the project's vitest convention (no
 * @testing-library/react) — uses react-dom/server.renderToString plus
 * source-string sentinels.
 */
import * as React from "react";
import { renderToString } from "react-dom/server";
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import DeckCombosPanel, {
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


function renderPanel(detected: DetectedComboEntry[], missing: MissingPartnerEntry[]): string {
  const raw = renderToString(
    React.createElement(DeckCombosPanel, {
      detected_combos_v1: detected,
      missing_partners_v1: missing,
    }),
  );
  return raw.replace(/<!-- -->/g, "");
}


describe("DeckCombosPanel — tabbed layout shape", () => {
  test("renders a tablist with two tab triggers", () => {
    const html = renderPanel(
      [makeDetected("v1", "Sol Ring", "Mana Crypt", "Infinite mana")],
      [makeMissing("v2", "Thassa's Oracle", "Demonic Consultation", "Wins the game")],
    );
    expect(html).toContain('role="tablist"');
    expect(html).toContain('data-combo-tab="active"');
    expect(html).toContain('data-combo-tab="suggested"');
  });

  test("tab labels include parenthesized counts", () => {
    const html = renderPanel(
      [makeDetected("v1", "Sol Ring", "Mana Crypt", "Infinite mana")],
      [
        makeMissing("v2", "Oracle", "Consult", "Wins"),
        makeMissing("v3", "Helm", "Rest", "Mills"),
      ],
    );
    expect(html).toContain("Active (1)");
    expect(html).toContain("Suggested (2)");
  });
});


describe("DeckCombosPanel — default-tab selection", () => {
  test("defaults to Active when detected has entries", () => {
    const html = renderPanel(
      [makeDetected("v1", "Sol Ring", "Mana Crypt", "Infinite mana")],
      [makeMissing("v2", "Thassa's Oracle", "Demonic Consultation", "Wins")],
    );
    // Detected list visible on the Active tab.
    expect(html).toContain('data-v172-section="detected"');
    // Suggested tab panel hidden by initial-tab selection.
    expect(html).not.toContain('data-v172-section="missing"');
    // Active trigger marked aria-selected via data-active sentinel.
    expect(html).toMatch(/data-combo-tab="active"[^>]*data-active="true"/);
    expect(html).toMatch(/data-combo-tab="suggested"[^>]*data-active="false"/);
  });

  test("falls back to Suggested when no detected combos", () => {
    const html = renderPanel(
      [],
      [makeMissing("v2", "Thassa's Oracle", "Demonic Consultation", "Wins")],
    );
    expect(html).not.toContain('data-v172-section="detected"');
    expect(html).toContain('data-v172-section="missing"');
    expect(html).toMatch(/data-combo-tab="suggested"[^>]*data-active="true"/);
  });

  test("null-render contract still fires when both sections are empty", () => {
    const html = renderPanel([], []);
    expect(html).toBe("");
  });
});


describe("DeckCombosPanel — tab body exclusivity", () => {
  test("Active tab panel contains tabpanel role + suggested missing", () => {
    const html = renderPanel(
      [makeDetected("v1", "Sol Ring", "Mana Crypt", "Infinite mana")],
      [],
    );
    expect(html).toContain('id="deck-combos-tab-active"');
    expect(html).toContain('role="tabpanel"');
    expect(html).toContain('Sol Ring');
    expect(html).toContain('Mana Crypt');
  });

  test("Suggested tab trigger stays clickable even when empty (no disabled attribute)", () => {
    // Both Button onClicks are bound unconditionally — neither tab is
    // disabled when its section is empty. The Button primitive carries
    // `disabled:*` Tailwind CSS hooks unconditionally; what matters here
    // is the actual `disabled=""` HTML attribute is NOT emitted.
    const html = renderPanel(
      [makeDetected("v1", "Sol Ring", "Mana Crypt", "Infinite mana")],
      [],
    );
    expect(html).toContain('data-combo-tab="suggested"');
    // The HTML attribute would render as `disabled=""` or `disabled>` on
    // the <button>; neither should be present.
    const suggestedButtonMatch = html.match(
      /<button[^>]*data-combo-tab="suggested"[^>]*>/,
    );
    expect(suggestedButtonMatch).not.toBeNull();
    const buttonOpenTag = suggestedButtonMatch?.[0] ?? "";
    expect(buttonOpenTag).not.toMatch(/\sdisabled(=|\s|>)/);
  });
});


describe("DeckCombosPanel — source sentinels for the tabbed structure", () => {
  test("uses Button primitive for tab triggers (no shadcn deps)", () => {
    expect(PANEL_SRC).toMatch(/from\s*['"]\.\.\/\.\.\/ui\/primitives\/Button['"]/);
    expect(PANEL_SRC).not.toMatch(/from\s*['"]@shadcn/);
  });

  test("uses local component state for active tab (no localStorage)", () => {
    expect(PANEL_SRC).toMatch(/useState<ComboTabKey>/);
    expect(PANEL_SRC).not.toMatch(/localStorage/);
  });

  test("active-tab default rule wired", () => {
    expect(PANEL_SRC).toMatch(
      /detected\.length\s*>\s*0\s*\?\s*"active"\s*:\s*"suggested"/,
    );
  });
});
