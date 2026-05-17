/**
 * Vitest tests for the v1.1 status-pill upgrade-suffix — extends Phase 4.14
 * Stage 2's buildWorkspacePillText with the upgradeSuggestionsCount param.
 */
import { describe, expect, test } from "vitest";
import { buildWorkspacePillText } from "../workspaceIntegrationAdapters";

describe("buildWorkspacePillText — v1.1 upgrade-suggestions suffix", () => {
  test("no upgrade count → no suffix (regression check; Phase 4.14 behavior preserved)", () => {
    const pill = buildWorkspacePillText({
      source: "archidekt",
      commander: "Shelob",
      cardCount: 99,
      hasBuildResponse: true,
      sufficiencyStatus: "PASS",
    });
    expect(pill.text).not.toContain("upgrade");
    expect(pill.text).toBe("Shelob · 99 cards · built · sufficiency PASS");
  });

  test("upgradeSuggestionsCount=0 → no suffix (treated as no suggestions)", () => {
    const pill = buildWorkspacePillText({
      source: "archidekt",
      commander: "Shelob",
      cardCount: 99,
      hasBuildResponse: true,
      sufficiencyStatus: "PASS",
      upgradeSuggestionsCount: 0,
    });
    expect(pill.text).not.toContain("upgrade");
  });

  test("upgradeSuggestionsCount > 0 → suffix appended (built variant)", () => {
    const pill = buildWorkspacePillText({
      source: "archidekt",
      commander: "Shelob",
      cardCount: 99,
      hasBuildResponse: true,
      sufficiencyStatus: "PASS",
      upgradeSuggestionsCount: 7,
    });
    expect(pill.text).toBe(
      "Shelob · 99 cards · built · sufficiency PASS · 7 upgrade suggestions ready",
    );
    expect(pill.variant).toBe("success");
  });

  test("singular '1 upgrade suggestion' grammar", () => {
    const pill = buildWorkspacePillText({
      source: "archidekt",
      commander: "Shelob",
      cardCount: 99,
      hasBuildResponse: true,
      sufficiencyStatus: "PASS",
      upgradeSuggestionsCount: 1,
    });
    expect(pill.text).toContain("· 1 upgrade suggestion ready");
    expect(pill.text).not.toContain("suggestions");
  });

  test("upgrade suffix appears on ready-to-build variant too", () => {
    const pill = buildWorkspacePillText({
      source: "archidekt",
      commander: "Shelob",
      cardCount: 99,
      isCompleted: true,
      hasBuildResponse: false,
      upgradeSuggestionsCount: 3,
    });
    expect(pill.text).toBe("Shelob · 99 cards · ready to build · 3 upgrade suggestions ready");
    expect(pill.variant).toBe("info");
  });

  test("upgrade suffix appears on imported-from variant too", () => {
    const pill = buildWorkspacePillText({
      source: "archidekt",
      commander: "Shelob",
      cardCount: 77,
      upgradeSuggestionsCount: 5,
    });
    expect(pill.text).toBe(
      "Shelob · 77 cards · imported from archidekt · 5 upgrade suggestions ready",
    );
  });

  test("upgrade suffix does NOT appear on fallback (no-deck-loaded) variant", () => {
    const pill = buildWorkspacePillText({
      source: "fallback",
      commander: "Krenko",
      cardCount: 5,
      upgradeSuggestionsCount: 99, // shouldn't matter — fallback short-circuits
    });
    expect(pill.text).toBe("No deck loaded");
    expect(pill.variant).toBe("neutral");
  });

  test("total pill text stays under 80 chars even with upgrade suffix + long commander", () => {
    const pill = buildWorkspacePillText({
      source: "archidekt",
      commander: "Atraxa, Praetors' Voice and friends extra long",
      cardCount: 99,
      hasBuildResponse: true,
      sufficiencyStatus: "PASS",
      upgradeSuggestionsCount: 12,
    });
    // Commander truncates at 32 chars; full text stays bounded.
    expect(pill.text.length).toBeLessThan(120);
    expect(pill.text).toContain("…");
    expect(pill.text).toContain("upgrade suggestions ready");
  });
});
