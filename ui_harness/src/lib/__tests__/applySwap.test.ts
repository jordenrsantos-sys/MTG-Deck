/** Vitest tests for lib/applySwap (Phase 4 BUNDLE Stage 2). */
import { describe, expect, test } from "vitest";
import { applySwapToDecklistText, applySwapsInOrder } from "../applySwap";

describe("applySwapToDecklistText", () => {
  test("replaces the out card preserving leading count", () => {
    const before = "1 Sol Ring\n1x Forest\nLightning Bolt";
    const result = applySwapToDecklistText(before, { out: "Sol Ring", in: "Mox Diamond" });
    expect(result.applied).toBe(true);
    expect(result.after).toBe("1 Mox Diamond\n1x Forest\nLightning Bolt");
  });

  test("matches case-insensitively", () => {
    const result = applySwapToDecklistText("1 sol ring", { out: "Sol Ring", in: "Mana Crypt" });
    expect(result.applied).toBe(true);
    expect(result.after).toBe("1 Mana Crypt");
  });

  test("returns reason when out card missing", () => {
    const result = applySwapToDecklistText("1 Forest", { out: "Sol Ring", in: "Mox Diamond" });
    expect(result.applied).toBe(false);
    expect(result.reason).toBe("out_card_not_in_decklist");
    expect(result.after).toBe(result.before);
  });

  test("rejects empty in/out", () => {
    expect(applySwapToDecklistText("1 Sol Ring", { out: "", in: "Mox" }).applied).toBe(false);
    expect(applySwapToDecklistText("1 Sol Ring", { out: "Sol Ring", in: "" }).applied).toBe(false);
  });

  test("non-string decklist returns applied=false", () => {
    const r = applySwapToDecklistText(null as unknown as string, { out: "X", in: "Y" });
    expect(r.applied).toBe(false);
    expect(r.reason).toBe("decklist_not_string");
  });

  test("only replaces first occurrence", () => {
    const before = "1 Forest\n1 Forest\n1 Forest";
    const r = applySwapToDecklistText(before, { out: "Forest", in: "Plains" });
    expect(r.applied).toBe(true);
    expect(r.after).toBe("1 Plains\n1 Forest\n1 Forest");
  });
});

describe("applySwapsInOrder", () => {
  test("applies multiple swaps sequentially", () => {
    const before = "1 Sol Ring\n1 Forest";
    const results = applySwapsInOrder(before, [
      { out: "Sol Ring", in: "Mox Diamond" },
      { out: "Forest", in: "Plains" },
    ]);
    expect(results.every((r) => r.applied)).toBe(true);
    expect(results[1].after).toBe("1 Mox Diamond\n1 Plains");
  });

  test("stops at first non-applied swap", () => {
    const before = "1 Sol Ring";
    const results = applySwapsInOrder(before, [
      { out: "Sol Ring", in: "Mox Diamond" },
      { out: "Forest", in: "Plains" }, // not in decklist
      { out: "Mox Diamond", in: "Mana Crypt" }, // would have applied if loop continued
    ]);
    expect(results).toHaveLength(2);
    expect(results[0].applied).toBe(true);
    expect(results[1].applied).toBe(false);
  });
});
