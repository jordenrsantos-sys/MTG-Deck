/** Vitest tests for sufficiencyTypes (Phase 4 BUNDLE Stage 1). */
import { describe, expect, test } from "vitest";
import {
  SUFFICIENCY_DOMAIN_ORDER,
  computeHeadlineCounts,
  statusToBadgeVariant,
  type SufficiencySummary,
} from "../sufficiencyTypes";

describe("computeHeadlineCounts", () => {
  test("null summary → all unknown", () => {
    const counts = computeHeadlineCounts(null);
    expect(counts.pass).toBe(0);
    expect(counts.warn).toBe(0);
    expect(counts.fail).toBe(0);
    expect(counts.unknown).toBe(SUFFICIENCY_DOMAIN_ORDER.length);
    expect(counts.total).toBe(SUFFICIENCY_DOMAIN_ORDER.length);
  });

  test("mixed verdicts tally correctly", () => {
    const summary: SufficiencySummary = {
      domain_verdicts: {
        probability: { status: "PASS", codes: [] },
        stress: { status: "WARN", codes: ["S_001"] },
        resilience: { status: "PASS", codes: [] },
        commander_reliability: { status: "FAIL", codes: ["CR_X"] },
        required_effects: { status: "PASS", codes: [] },
        bracket_compliance: { status: "PASS", codes: [] },
      },
    };
    const counts = computeHeadlineCounts(summary);
    expect(counts.pass).toBe(4);
    expect(counts.warn).toBe(1);
    expect(counts.fail).toBe(1);
    expect(counts.unknown).toBe(0);
  });

  test("missing domain counted as unknown", () => {
    const summary: SufficiencySummary = {
      domain_verdicts: {
        probability: { status: "PASS", codes: [] },
        // others omitted
      },
    };
    const counts = computeHeadlineCounts(summary);
    expect(counts.pass).toBe(1);
    expect(counts.unknown).toBe(SUFFICIENCY_DOMAIN_ORDER.length - 1);
  });
});

describe("statusToBadgeVariant", () => {
  test("maps to Phase 4.2 Badge variants", () => {
    expect(statusToBadgeVariant("PASS")).toBe("success");
    expect(statusToBadgeVariant("WARN")).toBe("warn");
    expect(statusToBadgeVariant("FAIL")).toBe("error");
    expect(statusToBadgeVariant("UNKNOWN")).toBe("neutral");
    expect(statusToBadgeVariant(null)).toBe("neutral");
    expect(statusToBadgeVariant(undefined)).toBe("neutral");
  });
});

describe("SUFFICIENCY_DOMAIN_ORDER", () => {
  test("has six canonical engine domains per OQ-3", () => {
    expect(SUFFICIENCY_DOMAIN_ORDER).toEqual([
      "probability",
      "stress",
      "resilience",
      "commander_reliability",
      "required_effects",
      "bracket_compliance",
    ]);
  });
});
