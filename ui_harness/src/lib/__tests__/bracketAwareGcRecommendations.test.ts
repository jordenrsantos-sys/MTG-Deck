/**
 * Vitest tests for v1.7 Stage 4 — BRACKET_AWARE_GC label + helpers.
 *
 * Covers:
 *   1. JUSTIFICATION_LABELS additive entry (BRACKET_AWARE_GC mapped).
 *   2. decodeBracketAwareGcPayload pure-function parsing (valid + invalid).
 *   3. formatBracketAwareGcChipLabel formatting.
 *   4. translateJustification gracefully decodes tagged-string reasons —
 *      Upgrade Swap Preview's render seam thus surfaces a clean chip
 *      label without component-level changes.
 */
import { describe, expect, test } from "vitest";
import {
  BRACKET_AWARE_GC_REASON_PREFIX,
  JUSTIFICATION_LABELS,
  decodeBracketAwareGcPayload,
  formatBracketAwareGcChipLabel,
  translateJustification,
} from "../justificationLabels";

const SAMPLE_PAYLOAD = {
  recommended_gc_oracle_id: "ORA_GC_RHYSTIC",
  recommended_gc_name: "Rhystic Study",
  current_deck_gc_count: 0,
  bracket_max_gc: 3,
};

const SAMPLE_REASON =
  BRACKET_AWARE_GC_REASON_PREFIX +
  JSON.stringify(SAMPLE_PAYLOAD, Object.keys(SAMPLE_PAYLOAD).sort());

describe("v1.7 Stage 4 — JUSTIFICATION_LABELS additive entry", () => {
  test("BRACKET_AWARE_GC mapped to a forward-compat hover label", () => {
    expect(JUSTIFICATION_LABELS["BRACKET_AWARE_GC"]).toBe(
      "Bracket allows game-changer",
    );
  });

  test("map remains frozen after additive change", () => {
    expect(Object.isFrozen(JUSTIFICATION_LABELS)).toBe(true);
  });

  test("Stage 2 COMBO_ENABLER entry unchanged (additive doesn't touch prior entry)", () => {
    expect(JUSTIFICATION_LABELS["COMBO_ENABLER"]).toBe(
      "Enables a 2-card combo",
    );
  });
});

describe("v1.7 Stage 4 — decodeBracketAwareGcPayload", () => {
  test("decodes a well-formed BRACKET_AWARE_GC reason", () => {
    const decoded = decodeBracketAwareGcPayload(SAMPLE_REASON);
    expect(decoded).not.toBeNull();
    expect(decoded!.recommended_gc_oracle_id).toBe("ORA_GC_RHYSTIC");
    expect(decoded!.recommended_gc_name).toBe("Rhystic Study");
    expect(decoded!.current_deck_gc_count).toBe(0);
    expect(decoded!.bracket_max_gc).toBe(3);
  });

  test("decodes null bracket_max_gc (B5 unlimited)", () => {
    const reason =
      BRACKET_AWARE_GC_REASON_PREFIX +
      JSON.stringify({
        recommended_gc_oracle_id: "ORA_GC_X",
        recommended_gc_name: "Ancient Tomb",
        current_deck_gc_count: 0,
        bracket_max_gc: null,
      });
    const decoded = decodeBracketAwareGcPayload(reason);
    expect(decoded).not.toBeNull();
    expect(decoded!.bracket_max_gc).toBeNull();
  });

  test("returns null for non-BRACKET reason strings", () => {
    expect(decodeBracketAwareGcPayload("ADD_REQUIRED_COVERAGE")).toBeNull();
    expect(decodeBracketAwareGcPayload("COMBO_ENABLER:{}")).toBeNull();
    expect(decodeBracketAwareGcPayload("")).toBeNull();
  });

  test("returns null for malformed JSON or missing required fields", () => {
    expect(decodeBracketAwareGcPayload("BRACKET_AWARE_GC:not-json")).toBeNull();
    expect(decodeBracketAwareGcPayload("BRACKET_AWARE_GC:")).toBeNull();
    expect(
      decodeBracketAwareGcPayload(
        'BRACKET_AWARE_GC:{"recommended_gc_name":"X"}',
      ),
    ).toBeNull();
  });

  test("returns null for non-string input (defensive)", () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect(decodeBracketAwareGcPayload(null as any)).toBeNull();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect(decodeBracketAwareGcPayload(undefined as any)).toBeNull();
  });
});

describe("v1.7 Stage 4 — formatBracketAwareGcChipLabel", () => {
  test("formats chip label with recommended GC name", () => {
    expect(formatBracketAwareGcChipLabel(SAMPLE_PAYLOAD)).toBe(
      "Bracket allows game-changer — recommends Rhystic Study",
    );
  });
});

describe("v1.7 Stage 4 — translateJustification handles tagged-string reasons", () => {
  test("BRACKET_AWARE_GC tagged string is decoded into chip prose (Upgrade Swap Preview render seam)", () => {
    expect(translateJustification(SAMPLE_REASON)).toBe(
      "Bracket allows game-changer — recommends Rhystic Study",
    );
  });

  test("Malformed BRACKET_AWARE_GC tag falls through to raw code (forward-compat)", () => {
    const malformed = "BRACKET_AWARE_GC:not-json";
    expect(translateJustification(malformed)).toBe(malformed);
  });

  test("Existing non-tagged codes unchanged (Stage 4 additive)", () => {
    expect(translateJustification("ADD_PRIMITIVE_COVERAGE")).toBe(
      "Fills missing primitive",
    );
    expect(translateJustification("SOME_FUTURE_CODE")).toBe("SOME_FUTURE_CODE");
  });

  test("Raw BRACKET_AWARE_GC code (no tag suffix) returns the map's forward-compat label", () => {
    expect(translateJustification("BRACKET_AWARE_GC")).toBe(
      "Bracket allows game-changer",
    );
  });
});
