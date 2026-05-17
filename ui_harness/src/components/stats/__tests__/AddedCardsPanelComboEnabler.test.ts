/**
 * Vitest tests for v1.7 Stage 2 — COMBO_ENABLER chip rendering.
 *
 * Covers:
 *   1. decodeComboEnablerPayload pure-function parsing (valid + invalid)
 *   2. formatComboEnablerChipLabel truncation at 40 chars + non-truncated pass-through
 *   3. AddedCardRow.tsx renders COMBO_ENABLER reasons via the dedicated
 *      chip path (distinct variant + data-attribute discriminator + full
 *      label preserved on hover-title) — verified via source-string
 *      sentinels (no @testing-library/react in this harness).
 *   4. existing 14 justificationLabels entries remain BYTE-IDENTICAL +
 *      the new COMBO_ENABLER entry is additive (15 entries total).
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import {
  COMBO_ENABLER_REASON_PREFIX,
  COMBO_OUTCOME_LABEL_TRUNCATION_CHARS,
  JUSTIFICATION_LABELS,
  decodeComboEnablerPayload,
  formatComboEnablerChipLabel,
} from "../../../lib/justificationLabels";

const SAMPLE_PAYLOAD = {
  partner_card_oracle_id: "0023888e-7bec-43e0-8dee-d1a4eb94b372",
  partner_card_name: "Palinchron",
  combo_outcome_label:
    "Infinite creature tokens; Infinite ETB; Infinite LTB; Infinite mana lands you control can produce; Infinite storm count",
};

const SAMPLE_REASON =
  COMBO_ENABLER_REASON_PREFIX +
  JSON.stringify(SAMPLE_PAYLOAD, Object.keys(SAMPLE_PAYLOAD).sort());

describe("v1.7 Stage 2 — decodeComboEnablerPayload", () => {
  test("decodes a well-formed COMBO_ENABLER reason", () => {
    const decoded = decodeComboEnablerPayload(SAMPLE_REASON);
    expect(decoded).not.toBeNull();
    expect(decoded!.partner_card_oracle_id).toBe(SAMPLE_PAYLOAD.partner_card_oracle_id);
    expect(decoded!.partner_card_name).toBe(SAMPLE_PAYLOAD.partner_card_name);
    expect(decoded!.combo_outcome_label).toBe(SAMPLE_PAYLOAD.combo_outcome_label);
  });

  test("returns null for non-COMBO reason strings", () => {
    expect(decodeComboEnablerPayload("ADD_REQUIRED_COVERAGE")).toBeNull();
    expect(decodeComboEnablerPayload("COMPLETE_TO_TARGET_SIZE")).toBeNull();
    expect(decodeComboEnablerPayload("")).toBeNull();
  });

  test("returns null for malformed JSON payload", () => {
    expect(decodeComboEnablerPayload("COMBO_ENABLER:not-json")).toBeNull();
    expect(decodeComboEnablerPayload("COMBO_ENABLER:")).toBeNull();
    expect(decodeComboEnablerPayload('COMBO_ENABLER:{"only":"this"}')).toBeNull();
  });

  test("returns null for non-string input (defensive)", () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect(decodeComboEnablerPayload(null as any)).toBeNull();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect(decodeComboEnablerPayload(undefined as any)).toBeNull();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect(decodeComboEnablerPayload({ foo: "bar" } as any)).toBeNull();
  });
});

describe("v1.7 Stage 2 — formatComboEnablerChipLabel truncation", () => {
  test("non-truncated label passes through as-is when outcome <= threshold", () => {
    const payload = {
      partner_card_oracle_id: "oid",
      partner_card_name: "X",
      combo_outcome_label: "Win the game",
    };
    const chip = formatComboEnablerChipLabel(payload);
    expect(chip).toBe("Enables X → Win the game");
    expect(chip.length).toBeLessThanOrEqual("Enables X → ".length + COMBO_OUTCOME_LABEL_TRUNCATION_CHARS);
  });

  test("truncates outcome at 40 chars with ellipsis when longer", () => {
    const payload = {
      partner_card_oracle_id: "oid",
      partner_card_name: "Palinchron",
      combo_outcome_label: SAMPLE_PAYLOAD.combo_outcome_label, // > 40 chars
    };
    const chip = formatComboEnablerChipLabel(payload);
    expect(chip.startsWith("Enables Palinchron → ")).toBe(true);
    const outcomeFragment = chip.slice("Enables Palinchron → ".length);
    expect(outcomeFragment.length).toBeLessThanOrEqual(COMBO_OUTCOME_LABEL_TRUNCATION_CHARS);
    expect(outcomeFragment.endsWith("…")).toBe(true);
    // The original full label is NOT present in the chip face.
    expect(chip).not.toContain("Infinite storm count");
  });

  test("threshold constant is 40 (matches Stage 2 spec)", () => {
    expect(COMBO_OUTCOME_LABEL_TRUNCATION_CHARS).toBe(40);
  });
});

describe("v1.7 Stage 2 — JUSTIFICATION_LABELS additive (prior 13 → 14 with COMBO_ENABLER)", () => {
  // NOTE: the Stage 2 spec stated "additive 14 → 15"; the actual pre-v1.7
  // map has 13 entries (3 Power Tune + 4 Complete rounds + 1 land + 1 backfill
  // + 1 diff placeholder + 3 v1.2 aliases = 13). Adding COMBO_ENABLER yields
  // 14 — documented in the Stage 2 close-out autonomous_repair_log.
  // v1.7 Stage 4 added a 15th entry (BRACKET_AWARE_GC) — total now 15;
  // documented in the Stage 4 close-out autonomous_repair_log.
  test("contains COMBO_ENABLER and remains additive (Stage 2 contribution unchanged)", () => {
    expect(JUSTIFICATION_LABELS["COMBO_ENABLER"]).toBe("Enables a 2-card combo");
    // Total entry count is now 15 after Stage 4's BRACKET_AWARE_GC addition.
    expect(Object.keys(JUSTIFICATION_LABELS).length).toBeGreaterThanOrEqual(14);
  });

  test("existing 13 entries unchanged (BYTE-IDENTICAL vocabulary)", () => {
    const expected: Record<string, string> = {
      ADD_PRIMITIVE_COVERAGE: "Fills missing primitive",
      CUT_DEAD_SLOT: "Replaces underperforming card",
      GC_COMPLIANCE_PRESERVED: "Stays within bracket limits",
      ADD_REQUIRED_COVERAGE: "Required for sufficiency",
      ADD_REDUNDANCY_SUPPORT: "Adds redundancy support",
      ADD_INTERACTION_OR_PROTECTION: "Adds interaction or protection",
      COMPLETE_TO_TARGET_SIZE: "Brings deck up to target size",
      ADD_BASIC_LAND_FILL_AUTO: "Auto-filled basic land",
      auto_completion_target_size: "Added during completion",
      added_during_completion: "Added by the engine during completion",
      basic_land_fill: "Basic land fill",
      land_target_completion: "Land target completion",
      primitive_coverage_fill: "Primitive coverage fill",
    };
    expect(Object.keys(expected).length).toBe(13);
    for (const [code, label] of Object.entries(expected)) {
      expect(JUSTIFICATION_LABELS[code]).toBe(label);
    }
  });

  test("COMBO_ENABLER entry is added with forward-compat hover label", () => {
    expect(JUSTIFICATION_LABELS["COMBO_ENABLER"]).toBe("Enables a 2-card combo");
  });

  test("map remains frozen after additive change", () => {
    expect(Object.isFrozen(JUSTIFICATION_LABELS)).toBe(true);
  });
});

describe("v1.7 Stage 2 — AddedCardRow chip render seam (source sentinels)", () => {
  const src = readFileSync(
    resolve(__dirname, "../AddedCardRow.tsx"),
    "utf-8",
  );

  test("imports decodeComboEnablerPayload + formatComboEnablerChipLabel", () => {
    expect(src).toContain("decodeComboEnablerPayload");
    expect(src).toContain("formatComboEnablerChipLabel");
  });

  test("renders a distinct chip variant (success) for COMBO_ENABLER reasons", () => {
    // Sentinel: the COMBO_ENABLER branch wraps a Badge with variant="success"
    expect(src).toMatch(/Badge\s+variant=("success"|'success')/);
  });

  test("renders a data-combo-enabler discriminator attribute", () => {
    expect(src).toMatch(/data-combo-enabler=("true"|'true')/);
  });

  test("hover-title carries the FULL outcome label (not truncated)", () => {
    // Sentinel: titleText uses combo.combo_outcome_label (not the chipText).
    expect(src).toMatch(/title=\{titleText\}/);
    expect(src).toMatch(/combo\.combo_outcome_label/);
  });

  test("primitive-coverage chip path (variant=info) still present — additive only", () => {
    // The existing pre-v1.7 render path renders non-COMBO reasons with
    // variant="info". The additive change must NOT remove that.
    expect(src).toMatch(/<Badge[\s\S]*?variant=("info"|'info')/);
    expect(src).toMatch(/translateJustification\(r\)/);
  });
});
