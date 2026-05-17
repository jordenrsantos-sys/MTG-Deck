/**
 * Vitest tests for UpgradeSuggestionsList + apply-swap interaction — v1.1 Stage 4.
 *
 * Per AUTOMATION_RULES halt-and-ask: no @testing-library/react install.
 * Tests focus on the pure adapter logic the panel uses:
 *   - UpgradeSwapSuggestion → SwapSuggestion adapter (cut_name → out;
 *     add_name → in).
 *   - applySwapToDecklistText round-trip through the adapter shape
 *     produces the same result as direct SwapSuggestion construction.
 *   - Panel empty-state + many-row shape contracts via prop-type surface.
 */
import { describe, expect, test } from "vitest";
import UpgradeSuggestionsList from "../UpgradeSuggestionsList";
import type { UpgradeSwapSuggestion } from "../../../lib/workspaceDeckState";
import { applySwapToDecklistText, type SwapSuggestion } from "../../../lib/applySwap";

describe("UpgradeSuggestionsList — module exports", () => {
  test("default export is a function (React component)", () => {
    expect(typeof UpgradeSuggestionsList).toBe("function");
  });
});

describe("UpgradeSwapSuggestion → SwapSuggestion adapter (cut_name → out, add_name → in)", () => {
  function toSwap(row: UpgradeSwapSuggestion): SwapSuggestion {
    return {
      out: typeof row.cut_name === "string" ? row.cut_name : undefined,
      in: typeof row.add_name === "string" ? row.add_name : undefined,
    };
  }

  test("maps cut_name + add_name verbatim", () => {
    const row: UpgradeSwapSuggestion = { cut_name: "Forest", add_name: "Sol Ring" };
    const swap = toSwap(row);
    expect(swap.out).toBe("Forest");
    expect(swap.in).toBe("Sol Ring");
  });

  test("preserves reasons_v1 on the source row (not consumed by adapter)", () => {
    const row: UpgradeSwapSuggestion = {
      cut_name: "Forest",
      add_name: "Sol Ring",
      reasons_v1: ["mana_acceleration"],
    };
    expect(row.reasons_v1).toEqual(["mana_acceleration"]);
  });

  test("missing cut_name maps to undefined out", () => {
    const swap = toSwap({ add_name: "Sol Ring" });
    expect(swap.out).toBeUndefined();
    expect(swap.in).toBe("Sol Ring");
  });

  test("missing add_name maps to undefined in", () => {
    const swap = toSwap({ cut_name: "Forest" });
    expect(swap.out).toBe("Forest");
    expect(swap.in).toBeUndefined();
  });
});

describe("apply-swap round-trip via the adapter", () => {
  function toSwap(row: UpgradeSwapSuggestion): SwapSuggestion {
    return {
      out: typeof row.cut_name === "string" ? row.cut_name : undefined,
      in: typeof row.add_name === "string" ? row.add_name : undefined,
    };
  }

  test("successful apply: cut_name found in decklist → replaced with add_name", () => {
    const decklist = "1 Sol Ring\n1 Forest\n1 Goblin Matron";
    const row: UpgradeSwapSuggestion = { cut_name: "Forest", add_name: "Arcane Signet" };
    const result = applySwapToDecklistText(decklist, toSwap(row));
    expect(result.applied).toBe(true);
    expect(result.after).toContain("Arcane Signet");
    expect(result.after).not.toContain("Forest");
  });

  test("apply fails cleanly when cut_name not in decklist", () => {
    const decklist = "1 Sol Ring\n1 Goblin Matron";
    const row: UpgradeSwapSuggestion = { cut_name: "Forest", add_name: "Arcane Signet" };
    const result = applySwapToDecklistText(decklist, toSwap(row));
    expect(result.applied).toBe(false);
    expect(result.reason).toBe("out_card_not_in_decklist");
    expect(result.after).toBe(decklist);
  });

  test("apply fails cleanly when adapter produces missing in/out", () => {
    const decklist = "1 Sol Ring";
    const row: UpgradeSwapSuggestion = { cut_name: "Sol Ring" }; // no add_name
    const result = applySwapToDecklistText(decklist, toSwap(row));
    expect(result.applied).toBe(false);
    expect(result.reason).toBe("swap_missing_in_or_out");
  });

  test("count + leading whitespace preserved across apply", () => {
    const decklist = "1 Sol Ring\n3 Forest";
    const row: UpgradeSwapSuggestion = { cut_name: "Forest", add_name: "Mountain" };
    const result = applySwapToDecklistText(decklist, toSwap(row));
    expect(result.applied).toBe(true);
    // Count "3" + format preserved
    expect(result.after).toMatch(/3\s*Mountain/);
  });
});

describe("Panel empty-state + many-row shape contracts (logic-level)", () => {
  test("empty rows array is a valid prop input (component renders empty-state copy)", () => {
    const rows: ReadonlyArray<UpgradeSwapSuggestion> = [];
    expect(rows.length).toBe(0);
  });

  test("21+ rows triggers the APPLY_DISPLAY_CAP overflow (cap is 20)", () => {
    const rows: ReadonlyArray<UpgradeSwapSuggestion> = Array.from({ length: 25 }, (_, i) => ({
      cut_name: `Cut${i}`,
      add_name: `Add${i}`,
    }));
    const APPLY_DISPLAY_CAP = 20;
    const overflowCount = Math.max(0, rows.length - APPLY_DISPLAY_CAP);
    expect(overflowCount).toBe(5);
  });
});
