/**
 * Vitest tests for Phase 4.14 Workspace UX polish hotfix adapters.
 * Logic-level coverage for the status-pill text computation.
 *
 * (Reducer COMPLETE_* coverage lives in lib/__tests__/workspaceDeckState.test.ts
 * — the reducer is the single source of truth for state transitions; this
 * file covers the workspace-side wiring helpers added in Stage 2.)
 */
import { describe, expect, test } from "vitest";
import { buildWorkspacePillText } from "../workspaceIntegrationAdapters";

describe("buildWorkspacePillText — Phase 4.14 Stage 2", () => {
  test("source=fallback → 'No deck loaded' / neutral", () => {
    const pill = buildWorkspacePillText({
      source: "fallback",
      commander: "Krenko, Mob Boss",
      cardCount: 5,
    });
    expect(pill.text).toBe("No deck loaded");
    expect(pill.variant).toBe("neutral");
  });

  test("imported deck (not completed, no build) → '{cmdr} · {N} cards · imported from {source}' / info", () => {
    const pill = buildWorkspacePillText({
      source: "archidekt",
      commander: "Shelob, Child of Ungoliant",
      cardCount: 77,
    });
    expect(pill.text).toBe("Shelob, Child of Ungoliant · 77 cards · imported from archidekt");
    expect(pill.variant).toBe("info");
  });

  test("completed (no build response) → '{cmdr} · {N} cards · ready to build' / info", () => {
    const pill = buildWorkspacePillText({
      source: "archidekt",
      commander: "Shelob",
      cardCount: 99,
      isCompleted: true,
      hasBuildResponse: false,
    });
    expect(pill.text).toBe("Shelob · 99 cards · ready to build");
    expect(pill.variant).toBe("info");
  });

  test("built (sufficiency present) → '{cmdr} · {N} cards · built · sufficiency {status}' / success", () => {
    const pill = buildWorkspacePillText({
      source: "archidekt",
      commander: "Shelob",
      cardCount: 99,
      isCompleted: true,
      hasBuildResponse: true,
      sufficiencyStatus: "PASS",
    });
    expect(pill.text).toBe("Shelob · 99 cards · built · sufficiency PASS");
    expect(pill.variant).toBe("success");
  });

  test("built without sufficiency → falls back to '—' suffix", () => {
    const pill = buildWorkspacePillText({
      source: "archidekt",
      commander: "Shelob",
      cardCount: 99,
      hasBuildResponse: true,
      sufficiencyStatus: null,
    });
    expect(pill.text).toContain("sufficiency —");
    expect(pill.variant).toBe("success");
  });

  test("commander name truncates at 32 chars with ellipsis (banner under 80 chars)", () => {
    const pill = buildWorkspacePillText({
      source: "archidekt",
      commander: "An Extremely Long Commander Name That Goes On And On Forever",
      cardCount: 99,
    });
    expect(pill.text.length).toBeLessThan(80);
    expect(pill.text).toContain("…");
  });

  test("singular vs plural card count copy", () => {
    expect(
      buildWorkspacePillText({ source: "manual", commander: "X", cardCount: 1 }).text,
    ).toContain("1 card · ");
    expect(
      buildWorkspacePillText({ source: "manual", commander: "X", cardCount: 7 }).text,
    ).toContain("7 cards · ");
  });

  test("missing/empty commander defaults to '(unnamed)'", () => {
    const pill = buildWorkspacePillText({ source: "manual", commander: "", cardCount: 5 });
    expect(pill.text).toContain("(unnamed)");
  });
});
