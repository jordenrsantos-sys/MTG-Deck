/**
 * Integration vitest for v1.7.2 Stage 3 — WorkspaceView wires
 * DeckCombosPanel into the Complete-deck render branch.
 *
 * Stage 1 ships the engine layer (detected_combos_v1 +
 * missing_partners_v1 on /deck/complete_v1).
 * Stage 2 ships the panel component (DeckCombosPanel).
 * Stage 3 wires the two together — WorkspaceView reads the engine
 * response fields and passes them as props.
 *
 * Per the project's vitest convention (no @testing-library/react),
 * this test pair exercises the wiring via source-string sentinels +
 * shape contracts. Two failing-first scenarios:
 *
 *   (a) Populated response shape — WorkspaceView renders
 *       <DeckCombosPanel detected_combos_v1=… missing_partners_v1=…/>
 *       with props sourced from completionResult.
 *   (b) Legacy response shape (fields absent) — WorkspaceView's
 *       passthrough coerces undefined to [] so the panel null-renders.
 *
 * The wiring branch must fall under the existing AddedCardsPanel
 * render section so the combos panel sits with the rest of the
 * Complete-deck output (not buried in a sufficiency tab).
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";


const WORKSPACE_VIEW_SRC = readFileSync(
  resolve(__dirname, "../WorkspaceView.tsx"),
  "utf-8",
);


describe("v1.7.2 Stage 3 — WorkspaceView source sentinels: DeckCombosPanel wire-up", () => {
  test("DeckCombosPanel imported from the components/stats path", () => {
    expect(WORKSPACE_VIEW_SRC).toMatch(
      /import\s+DeckCombosPanel\s+from\s+["']\.\.\/components\/stats\/DeckCombosPanel["']/,
    );
  });

  test("<DeckCombosPanel/> renders with detected_combos_v1 + missing_partners_v1 props", () => {
    // Both props must be passed (otherwise the panel renders blank
    // even when the engine returned data).
    expect(WORKSPACE_VIEW_SRC).toMatch(/<DeckCombosPanel[\s\S]*?detected_combos_v1=/);
    expect(WORKSPACE_VIEW_SRC).toMatch(/<DeckCombosPanel[\s\S]*?missing_partners_v1=/);
  });

  test("Props sourced from completionResult (the /deck/complete_v1 response)", () => {
    // Sentinel against accidentally sourcing from buildResponse (the
    // /build endpoint payload, which does NOT carry the new fields).
    const panelBlock = WORKSPACE_VIEW_SRC.match(/<DeckCombosPanel[\s\S]*?\/>/);
    expect(panelBlock).not.toBeNull();
    const block = panelBlock?.[0] ?? "";
    expect(block).toMatch(/completionResult\?\.detected_combos_v1/);
    expect(block).toMatch(/completionResult\?\.missing_partners_v1/);
  });

  test("Legacy response shape — coerce undefined to [] (back-compat path)", () => {
    // The wiring uses Array.isArray(…) ? … : [] to guard against
    // pre-v1.7.2 engine responses where the fields are absent.
    const panelBlock = WORKSPACE_VIEW_SRC.match(/<DeckCombosPanel[\s\S]*?\/>/);
    const block = panelBlock?.[0] ?? "";
    // Two `Array.isArray(...) ? ... : []` guards, one per prop.
    const arrayIsArrayCount = (block.match(/Array\.isArray/g) || []).length;
    expect(arrayIsArrayCount).toBe(2);
  });

  test("Panel is placed within the workspace render section (after AddedCardsPanel block, before UpgradeSuggestionsList)", () => {
    // Ordering sentinel — combos surface sits with Complete output,
    // not buried under sufficiency or Upgrade panels.
    const addedIdx = WORKSPACE_VIEW_SRC.indexOf("<AddedCardsPanel");
    const combosIdx = WORKSPACE_VIEW_SRC.indexOf("<DeckCombosPanel");
    const upgradeIdx = WORKSPACE_VIEW_SRC.indexOf("<UpgradeSuggestionsList");
    expect(addedIdx).toBeGreaterThan(0);
    expect(combosIdx).toBeGreaterThan(addedIdx);
    expect(upgradeIdx).toBeGreaterThan(combosIdx);
  });
});


describe("v1.7.2 Stage 3 — props-shape acceptance (logic-level)", () => {
  test("Populated completionResult shape is a valid DeckCombosPanel input", () => {
    // The wiring expects completionResult.detected_combos_v1 to be an
    // array of {variant_id, card_a_name, card_b_name,
    // combo_outcome_label, …} per Stage 1 engine contract.
    const completionResult = {
      detected_combos_v1: [
        {
          variant_id: "3940-5195",
          card_a_name: "Haze of Rage",
          card_a_oracle_id: "f17d0fb8-c157-43b8-be26-f5ba4c6aed14",
          card_b_name: "Storm-Kiln Artist",
          card_b_oracle_id: "a145ff8c-5812-4bcb-bd16-9839dc25121d",
          combo_outcome_label: "Infinite colored mana; Infinite storm count",
        },
      ],
      missing_partners_v1: [],
    };
    expect(Array.isArray(completionResult.detected_combos_v1)).toBe(true);
    expect(completionResult.detected_combos_v1[0].variant_id).toBe("3940-5195");
  });

  test("Legacy completionResult shape (no new fields) coerces to [] arrays", () => {
    const legacyCompletionResult: { detected_combos_v1?: unknown; missing_partners_v1?: unknown } = {
      // intentionally absent — matches a pre-v1.7.2 engine response
    };
    const detected = Array.isArray(legacyCompletionResult.detected_combos_v1)
      ? legacyCompletionResult.detected_combos_v1
      : [];
    const missing = Array.isArray(legacyCompletionResult.missing_partners_v1)
      ? legacyCompletionResult.missing_partners_v1
      : [];
    expect(detected).toEqual([]);
    expect(missing).toEqual([]);
  });
});
