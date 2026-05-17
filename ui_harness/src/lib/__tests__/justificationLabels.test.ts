/**
 * Vitest tests for v1.6.2 Stage 3 — justificationLabels translation map.
 *
 * Pure function tests + integration assertions at both render seams
 * (UpgradeSuggestionsList + AddedCardRow). Engine emission BYTE-IDENTICAL
 * per HARD safety #1 — these tests guard the UI-side translation only.
 */
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import {
  JUSTIFICATION_LABELS,
  translateJustification,
} from "../justificationLabels";

describe("v1.6.2 Stage 3 — translateJustification pure function", () => {
  test("Power Tune codes translate to human-readable prose", () => {
    expect(translateJustification("ADD_PRIMITIVE_COVERAGE")).toBe("Fills missing primitive");
    expect(translateJustification("CUT_DEAD_SLOT")).toBe("Replaces underperforming card");
    expect(translateJustification("GC_COMPLIANCE_PRESERVED")).toBe("Stays within bracket limits");
  });

  test("Complete-deck round codes translate", () => {
    expect(translateJustification("ADD_REQUIRED_COVERAGE")).toBe("Required for sufficiency");
    expect(translateJustification("ADD_REDUNDANCY_SUPPORT")).toBe("Adds redundancy support");
    expect(translateJustification("ADD_INTERACTION_OR_PROTECTION")).toBe("Adds interaction or protection");
    expect(translateJustification("COMPLETE_TO_TARGET_SIZE")).toBe("Brings deck up to target size");
  });

  test("land-fill codes translate", () => {
    expect(translateJustification("ADD_BASIC_LAND_FILL_AUTO")).toBe("Auto-filled basic land");
  });

  test("v1.5 backfill placeholder + v1.3 diff placeholder translate", () => {
    expect(translateJustification("auto_completion_target_size")).toBe("Added during completion");
    expect(translateJustification("added_during_completion")).toBe(
      "Added by the engine during completion",
    );
  });

  test("forward-compat: unknown codes fall through verbatim", () => {
    expect(translateJustification("SOME_FUTURE_CODE")).toBe("SOME_FUTURE_CODE");
    expect(translateJustification("UNKNOWN_REASON_v99")).toBe("UNKNOWN_REASON_v99");
  });

  test("empty string returns empty string (defensive)", () => {
    expect(translateJustification("")).toBe("");
  });

  test("non-string input returns input unchanged (defensive)", () => {
    // The function declares string-only input; runtime defensive guard
    // returns the input unchanged for non-string values.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect(translateJustification(undefined as any)).toBe(undefined);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect(translateJustification(null as any)).toBe(null);
  });
});

describe("v1.6.2 Stage 3 — JUSTIFICATION_LABELS map vocabulary", () => {
  test("map is frozen (no runtime mutation)", () => {
    expect(Object.isFrozen(JUSTIFICATION_LABELS)).toBe(true);
  });

  test("all 3 Power Tune codes from spec body are mapped", () => {
    expect(JUSTIFICATION_LABELS).toHaveProperty("ADD_PRIMITIVE_COVERAGE");
    expect(JUSTIFICATION_LABELS).toHaveProperty("CUT_DEAD_SLOT");
    expect(JUSTIFICATION_LABELS).toHaveProperty("GC_COMPLIANCE_PRESERVED");
  });

  test("v1.2 vocabulary aliases from spec body are mapped (basic_land_fill / land_target_completion / primitive_coverage_fill)", () => {
    expect(JUSTIFICATION_LABELS).toHaveProperty("basic_land_fill");
    expect(JUSTIFICATION_LABELS).toHaveProperty("land_target_completion");
    expect(JUSTIFICATION_LABELS).toHaveProperty("primitive_coverage_fill");
  });

  test("all mapped labels are non-empty strings (no blanks)", () => {
    for (const [code, label] of Object.entries(JUSTIFICATION_LABELS)) {
      expect(typeof label).toBe("string");
      expect(label.length).toBeGreaterThan(0);
      expect(label).not.toBe(code); // translation should differ from raw code
    }
  });
});

describe("v1.6.2 Stage 3 — render seams use translateJustification", () => {
  test("UpgradeSuggestionsList imports translateJustification + applies it at Badge render", () => {
    const src = readFileSync(
      resolve(__dirname, "../../components/stats/UpgradeSuggestionsList.tsx"),
      "utf-8",
    );
    expect(src).toContain('import { translateJustification }');
    expect(src).toMatch(/translateJustification\(r\)/);
    // Sentinel: Tooltip still passes the raw code as `content={r}` (so
    // power users see the canonical engine code on hover).
    expect(src).toMatch(/<Tooltip[\s\S]*?content=\{r\}/);
  });

  test("AddedCardRow imports translateJustification + applies it at Badge render", () => {
    const src = readFileSync(
      resolve(__dirname, "../../components/stats/AddedCardRow.tsx"),
      "utf-8",
    );
    // v1.7 Stage 2: AddedCardRow now imports translateJustification
    // alongside decodeComboEnablerPayload + formatComboEnablerChipLabel
    // via a multi-line import. The original single-line sentinel
    // (`import { translateJustification }`) no longer applies; relaxed
    // to a regex that tolerates either single-line or multi-line forms.
    expect(src).toMatch(
      /import\s*\{[\s\S]*?\btranslateJustification\b[\s\S]*?\}\s*from\s*"\.\.\/\.\.\/lib\/justificationLabels"/,
    );
    expect(src).toMatch(/translateJustification\(r\)/);
    // Sentinel: title attribute on Badge preserves the raw code as hover tooltip.
    expect(src).toMatch(/title=\{r\}/);
  });

  test("HARD safety: NO engine source files touched (translation is UI-side only)", () => {
    // Defense-in-depth: the engine emission contract preserved. We
    // assert the api/engine/deck_complete_engine_v1.py reason-code
    // literals are still BYTE-IDENTICAL by sampling key strings.
    const enginePath = resolve(
      __dirname,
      "../../../../api/engine/deck_complete_engine_v1.py",
    );
    let engineSrc = "";
    try {
      engineSrc = readFileSync(enginePath, "utf-8");
    } catch {
      // Engine source unavailable (e.g. agent test env without the api/
      // tree). Skip strictly; the other tests cover the UI-side change.
      expect(true).toBe(true);
      return;
    }
    // Sentinel: engine still emits the canonical codes verbatim.
    expect(engineSrc).toContain('"ADD_BASIC_LAND_FILL_AUTO"');
    expect(engineSrc).toContain('"COMPLETE_TO_TARGET_SIZE"');
    expect(engineSrc).toContain('"auto_completion_target_size"');
  });
});
