/**
 * Vitest for v1.7.3 Stage 2 — PROACTIVE_COMBO_TARGET label entry.
 *
 * The proactive layer (api/engine/layers/proactive_combo_completion_v1.py)
 * emits a plain-string reason `PROACTIVE_COMBO_TARGET` on added rows
 * representing combo partners it injected. justificationLabels.ts
 * adds an additive 16th entry mapping the raw code to the chip prose
 * "Added to enable a known combo".
 *
 * The existing AddedCardRow render path runs `translateJustification`
 * over each reason — pre-fix the raw code rendered verbatim; post-fix
 * the human prose surfaces. v1.7 Stage 2's pattern: simple key-value
 * map entry, no decoder needed (the reason carries no JSON payload —
 * the COMBO_ENABLER chip on the same row carries the structured
 * partner + outcome data).
 *
 * Stage 2 additive — existing 15 entries BYTE-IDENTICAL.
 */
import { describe, expect, test } from "vitest";
import {
  JUSTIFICATION_LABELS,
  translateJustification,
} from "../justificationLabels";


describe("v1.7.3 Stage 2 — JUSTIFICATION_LABELS additive entry (15 → 16)", () => {
  test("PROACTIVE_COMBO_TARGET mapped to human-readable chip prose", () => {
    expect(JUSTIFICATION_LABELS["PROACTIVE_COMBO_TARGET"]).toBe(
      "Added to enable a known combo",
    );
  });

  test("map remains frozen after additive change", () => {
    expect(Object.isFrozen(JUSTIFICATION_LABELS)).toBe(true);
  });

  test("translateJustification surfaces the human prose for the raw code", () => {
    expect(translateJustification("PROACTIVE_COMBO_TARGET")).toBe(
      "Added to enable a known combo",
    );
  });

  test("Prior v1.7 entries unchanged (additive doesn't touch them)", () => {
    expect(JUSTIFICATION_LABELS["COMBO_ENABLER"]).toBe("Enables a 2-card combo");
    expect(JUSTIFICATION_LABELS["BRACKET_AWARE_GC"]).toBe("Bracket allows game-changer");
    expect(JUSTIFICATION_LABELS["ADD_REQUIRED_COVERAGE"]).toBe("Required for sufficiency");
  });

  test("Entry count is at least 16 after v1.7.3 additive addition", () => {
    // The v1.7.2 Stage 4 close-out documented 15 entries; v1.7.3 takes
    // it to 16. Assertion uses >= so future additive entries don't
    // regress this sentinel.
    expect(Object.keys(JUSTIFICATION_LABELS).length).toBeGreaterThanOrEqual(16);
  });
});
